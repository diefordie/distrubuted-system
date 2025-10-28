
import asyncio
import time
import statistics
from loguru import logger
from src.communication.message_passing import send_json
from src.utils.config import PING_INTERVAL, RTT_WINDOW, ADAPTIVE_K


class AdaptiveFailureDetector:

    def __init__(self, node):
        self.node = node
        self.check_interval = PING_INTERVAL
        self.window_size = RTT_WINDOW
        self.k = ADAPTIVE_K
        self.failed_nodes = set()
        self.rtt_history = {}  
        self.running = False
        
        
        for addr in self.node.raft.cluster_addrs:
            if self.node.node_id not in addr:
                self.rtt_history[addr] = []

    async def start(self):
        self.running = True
        asyncio.create_task(self.monitor_nodes())
        logger.success("🩺 Adaptive Failure Detector started")

    async def stop(self):
        self.running = False
        logger.info("🛑 Failure Detector stopped")

    async def monitor_nodes(self):
        
        await asyncio.sleep(2)
        
        while self.running:
            tasks = []
            for addr in self.node.raft.cluster_addrs:
                if self.node.node_id in addr:
                    continue
                tasks.append(self.ping_node(addr))
            
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            await asyncio.sleep(self.check_interval)

    async def ping_node(self, addr):
        start_time = time.time()
        
        
        timeout = self._calculate_adaptive_timeout(addr)
        
        try:
            
            res = await send_json(
                addr, 
                "/ping", 
                {"from": self.node.node_id, "timestamp": start_time}, 
                timeout=timeout
            )
            
            if not res:
                raise ConnectionError("No response")

            
            rtt = time.time() - start_time
            self._update_rtt(addr, rtt)

            
            self.node.metrics.record_ping(rtt)

            
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
        hist = self.rtt_history.get(addr, [])
        
        if len(hist) < 2:
            
            return 2.0
        
        mean_rtt = statistics.mean(hist)
        std_rtt = statistics.stdev(hist)
        adaptive_timeout = mean_rtt + self.k * std_rtt
        
        
        adaptive_timeout = max(1.0, min(adaptive_timeout, 10.0))
        
        return adaptive_timeout

    def _update_rtt(self, addr: str, rtt: float):
        hist = self.rtt_history.setdefault(addr, [])
        hist.append(rtt)
        
        
        if len(hist) > self.window_size:
            hist.pop(0)
        
        
        if len(hist) >= 2:
            mean_rtt = statistics.mean(hist)
            std_rtt = statistics.stdev(hist)
            adaptive_timeout = mean_rtt + self.k * std_rtt
            
            logger.trace(
                f"[RTT Update] {addr}: μ={mean_rtt:.3f}s, σ={std_rtt:.3f}s, "
                f"timeout={adaptive_timeout:.3f}s"
            )

    def _handle_node_failure(self, addr: str, reason: str):
        if addr not in self.failed_nodes:
            self.failed_nodes.add(addr)
            self.node.metrics.record_failure(addr)
            logger.warning(f"⚠️  Node {addr} marked as FAILED (reason: {reason})")
            
            
            asyncio.create_task(self._handle_failure_recovery(addr))
        else:
            logger.trace(f"Node {addr} still failed ({reason})")

    async def _handle_failure_recovery(self, failed_addr: str):
        try:
            
            if self.node.raft.leader_id and self.node.raft.leader_id in failed_addr:
                logger.warning(f"🚨 Leader {failed_addr} failed! Election will be triggered.")
                
            
            
            if hasattr(self.node, 'queue_node'):
                
                self.node.queue_node.hash_ring.remove_node(failed_addr)
                logger.info(f"Updated queue hash ring: removed {failed_addr}")
            
            
            
            
        except Exception as e:
            logger.error(f"Error in failure recovery: {e}")

    def get_healthy_nodes(self) -> list:
        healthy = []
        for addr in self.node.raft.cluster_addrs:
            if self.node.node_id in addr:
                healthy.append(addr)  
            elif addr not in self.failed_nodes:
                healthy.append(addr)
        return healthy

    def get_failure_stats(self) -> dict:
        stats = {
            "total_nodes": len(self.node.raft.cluster_addrs) - 1,  
            "failed_nodes": len(self.failed_nodes),
            "healthy_nodes": len(self.get_healthy_nodes()) - 1,  
            "failed_list": list(self.failed_nodes),
            "rtt_stats": {}
        }
        
        
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
