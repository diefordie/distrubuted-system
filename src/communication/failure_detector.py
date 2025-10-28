# src/communication/failure_detector.py
import asyncio
import time
import statistics
from loguru import logger
from src.communication.message_passing import send_json
from src.utils.config import PING_INTERVAL, RTT_WINDOW, ADAPTIVE_K


class AdaptiveFailureDetector:
    """
    Adaptive Failure Detector berbasis statistik RTT.
    Memantau setiap node dalam cluster dan menandai node yang gagal
    menggunakan timeout adaptif (μ + Kσ).
    
    Updated to work with new Raft and components.
    """

    def __init__(self, node):
        self.node = node
        self.check_interval = PING_INTERVAL
        self.window_size = RTT_WINDOW
        self.k = ADAPTIVE_K
        self.failed_nodes = set()
        self.rtt_history = {}  # addr -> list of RTT values
        self.running = False
        
        # Initialize RTT history for all cluster nodes
        for addr in self.node.raft.cluster_addrs:
            if self.node.node_id not in addr:
                self.rtt_history[addr] = []

    async def start(self):
        """Start the failure detector"""
        self.running = True
        asyncio.create_task(self.monitor_nodes())
        logger.success("🩺 Adaptive Failure Detector started")

    async def stop(self):
        """Stop the failure detector"""
        self.running = False
        logger.info("🛑 Failure Detector stopped")

    async def monitor_nodes(self):
        """
        Main monitoring loop - periodically ping all nodes in cluster
        """
        # Wait a bit for node to fully start
        await asyncio.sleep(2)
        
        while self.running:
            tasks = []
            for addr in self.node.raft.cluster_addrs:
                if self.node.node_id in addr:
                    continue
                tasks.append(self.ping_node(addr))
            
            # Ping all nodes concurrently
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            await asyncio.sleep(self.check_interval)

    async def ping_node(self, addr):
        """
        Send ping to a node and measure RTT.
        Mark node as failed if timeout exceeds adaptive threshold.
        """
        start_time = time.time()
        
        # Calculate adaptive timeout for this node
        timeout = self._calculate_adaptive_timeout(addr)
        
        try:
            # Send ping with adaptive timeout
            res = await send_json(
                addr, 
                "/ping", 
                {"from": self.node.node_id, "timestamp": start_time}, 
                timeout=timeout
            )
            
            if not res:
                raise ConnectionError("No response")

            # Calculate RTT
            rtt = time.time() - start_time
            self._update_rtt(addr, rtt)

            # Record to metrics
            self.node.metrics.record_ping(rtt)

            # If node was previously failed, mark as recovered
            if addr in self.failed_nodes:
                self.failed_nodes.remove(addr)
                self.node.metrics.record_recovery(addr)
                logger.success(f"✅ Node {addr} recovered (RTT={rtt:.3f}s)")
            else:
                logger.trace(f"Ping {addr}: RTT={rtt:.3f}s")

        except asyncio.TimeoutError:
            self._handle_node_failure(addr, "timeout")
        except Exception as e:
            self._handle_node_failure(addr, str(e))

    def _calculate_adaptive_timeout(self, addr: str) -> float:
        """
        Calculate adaptive timeout using μ + Kσ formula.
        Falls back to default if not enough history.
        """
        hist = self.rtt_history.get(addr, [])
        
        if len(hist) < 2:
            # Not enough data, use default
            return 2.0
        
        mean_rtt = statistics.mean(hist)
        std_rtt = statistics.stdev(hist)
        adaptive_timeout = mean_rtt + self.k * std_rtt
        
        # Enforce minimum and maximum bounds
        adaptive_timeout = max(1.0, min(adaptive_timeout, 10.0))
        
        return adaptive_timeout

    def _update_rtt(self, addr: str, rtt: float):
        """
        Update RTT history for a node.
        Maintains a sliding window of recent RTT values.
        """
        hist = self.rtt_history.setdefault(addr, [])
        hist.append(rtt)
        
        # Keep only recent history (sliding window)
        if len(hist) > self.window_size:
            hist.pop(0)
        
        # Calculate statistics for logging
        if len(hist) >= 2:
            mean_rtt = statistics.mean(hist)
            std_rtt = statistics.stdev(hist)
            adaptive_timeout = mean_rtt + self.k * std_rtt
            
            logger.trace(
                f"[RTT Update] {addr}: μ={mean_rtt:.3f}s, σ={std_rtt:.3f}s, "
                f"timeout={adaptive_timeout:.3f}s"
            )

    def _handle_node_failure(self, addr: str, reason: str):
        """
        Handle detection of node failure.
        Updates metrics and triggers recovery mechanisms.
        """
        if addr not in self.failed_nodes:
            self.failed_nodes.add(addr)
            self.node.metrics.record_failure(addr)
            logger.warning(f"⚠️  Node {addr} marked as FAILED (reason: {reason})")
            
            # Notify components that might need to react to failure
            asyncio.create_task(self._handle_failure_recovery(addr))
        else:
            logger.trace(f"Node {addr} still failed ({reason})")

    async def _handle_failure_recovery(self, failed_addr: str):
        """
        Handle failure recovery procedures:
        - Update consistent hash ring (for queue)
        - Trigger Raft election if leader failed
        - Invalidate locks held by failed node (optional)
        """
        try:
            # Check if failed node was the Raft leader
            if self.node.raft.leader_id and self.node.raft.leader_id in failed_addr:
                logger.warning(f"🚨 Leader {failed_addr} failed! Election will be triggered.")
                # Raft will automatically handle this via heartbeat timeout
            
            # Update queue hash ring if available
            if hasattr(self.node, 'queue_node'):
                # Remove failed node from hash ring
                self.node.queue_node.hash_ring.remove_node(failed_addr)
                logger.info(f"Updated queue hash ring: removed {failed_addr}")
            
            # Optional: Force release locks held by failed node's clients
            # This would require tracking which client_ids belong to which nodes
            
        except Exception as e:
            logger.error(f"Error in failure recovery: {e}")

    def get_healthy_nodes(self) -> list:
        """Get list of currently healthy nodes"""
        healthy = []
        for addr in self.node.raft.cluster_addrs:
            if self.node.node_id in addr:
                healthy.append(addr)  # Self is always healthy
            elif addr not in self.failed_nodes:
                healthy.append(addr)
        return healthy

    def get_failure_stats(self) -> dict:
        """Get failure detection statistics"""
        stats = {
            "total_nodes": len(self.node.raft.cluster_addrs) - 1,  # Exclude self
            "failed_nodes": len(self.failed_nodes),
            "healthy_nodes": len(self.get_healthy_nodes()) - 1,  # Exclude self
            "failed_list": list(self.failed_nodes),
            "rtt_stats": {}
        }
        
        # Add RTT statistics for each node
        for addr, hist in self.rtt_history.items():
            if hist:
                stats["rtt_stats"][addr] = {
                    "mean_ms": round(statistics.mean(hist) * 1000, 2),
                    "stddev_ms": round(statistics.stdev(hist) * 1000, 2) if len(hist) > 1 else 0,
                    "min_ms": round(min(hist) * 1000, 2),
                    "max_ms": round(max(hist) * 1000, 2),
                    "samples": len(hist)
                }
        
        return stats