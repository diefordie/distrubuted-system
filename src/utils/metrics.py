# src/utils/metrics.py
import time
import statistics
from loguru import logger
from collections import defaultdict, deque

class MetricsCollector:
    """
    Enhanced metrics collector for distributed system performance monitoring.
    Tracks Raft, failure detection, cache, queue, and lock performance.
    """
    def __init__(self):
        self.start_time = time.time()
        
        # Raft metrics
        self.raft_election_count = 0
        self.raft_heartbeat_count = 0
        self.raft_log_entries = 0
        self.raft_term = 0
        self.raft_state_changes = []  # [(timestamp, from_state, to_state)]
        
        # Failure detector metrics
        self.ping_rtt = deque(maxlen=50)  # Keep last 50 RTT measurements
        self.failed_nodes_count = 0
        self.recovery_events = 0
        self.current_failed_nodes = set()
        
        # Cache metrics
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_invalidations = 0
        self.cache_evictions = 0
        self.cache_size = 0
        
        # Queue metrics
        self.queue_push_count = 0
        self.queue_pop_count = 0
        self.queue_sizes = defaultdict(int)  # queue_name -> size
        
        # Lock metrics
        self.lock_acquire_count = 0
        self.lock_release_count = 0
        self.lock_denied_count = 0
        self.lock_deadlock_count = 0
        self.active_locks = 0
        
        # Request metrics (API)
        self.total_requests = 0
        self.request_latencies = deque(maxlen=100)  # Keep last 100 request times
        self.endpoint_counts = defaultdict(int)  # endpoint -> count

    # =========================
    # RAFT METRICS
    # =========================
    def record_raft_election(self):
        """Record Raft election started"""
        self.raft_election_count += 1
    
    def record_raft_heartbeat(self):
        """Record Raft heartbeat sent"""
        self.raft_heartbeat_count += 1
    
    def record_raft_log_entry(self):
        """Record new log entry appended"""
        self.raft_log_entries += 1
    
    def record_raft_state_change(self, from_state: str, to_state: str):
        """Record Raft state transition"""
        self.raft_state_changes.append((time.time(), from_state, to_state))
        if len(self.raft_state_changes) > 100:
            self.raft_state_changes.pop(0)
    
    def update_raft_term(self, term: int):
        """Update current Raft term"""
        self.raft_term = term

    # =========================
    # FAILURE DETECTOR METRICS
    # =========================
    def record_ping(self, rtt: float):
        """Record RTT from ping"""
        self.ping_rtt.append(rtt)
    
    def record_failure(self, node_addr: str):
        """Record node failure detected"""
        if node_addr not in self.current_failed_nodes:
            self.failed_nodes_count += 1
            self.current_failed_nodes.add(node_addr)
    
    def record_recovery(self, node_addr: str):
        """Record node recovery"""
        if node_addr in self.current_failed_nodes:
            self.recovery_events += 1
            self.current_failed_nodes.remove(node_addr)
    
    def average_rtt(self) -> float:
        """Calculate average RTT"""
        return statistics.mean(self.ping_rtt) if self.ping_rtt else 0.0
    
    def rtt_stddev(self) -> float:
        """Calculate RTT standard deviation"""
        return statistics.stdev(self.ping_rtt) if len(self.ping_rtt) > 1 else 0.0

    # =========================
    # CACHE METRICS
    # =========================
    def record_cache_hit(self):
        """Record cache hit"""
        self.cache_hits += 1
    
    def record_cache_miss(self):
        """Record cache miss"""
        self.cache_misses += 1
    
    def record_cache_invalidation(self):
        """Record cache invalidation"""
        self.cache_invalidations += 1
    
    def record_cache_eviction(self):
        """Record cache eviction (LRU)"""
        self.cache_evictions += 1
    
    def update_cache_size(self, size: int):
        """Update current cache size"""
        self.cache_size = size
    
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.cache_hits + self.cache_misses
        return (self.cache_hits / total * 100) if total > 0 else 0.0

    # =========================
    # QUEUE METRICS
    # =========================
    def record_queue_push(self, queue_name: str = "default"):
        """Record message pushed to queue"""
        self.queue_push_count += 1
        self.queue_sizes[queue_name] += 1
    
    def record_queue_pop(self, queue_name: str = "default"):
        """Record message popped from queue"""
        self.queue_pop_count += 1
        if self.queue_sizes[queue_name] > 0:
            self.queue_sizes[queue_name] -= 1
    
    def update_queue_size(self, queue_name: str, size: int):
        """Update queue size directly"""
        self.queue_sizes[queue_name] = size
    
    def total_queue_size(self) -> int:
        """Get total size across all queues"""
        return sum(self.queue_sizes.values())

    # =========================
    # LOCK METRICS
    # =========================
    def record_lock_acquire(self, success: bool = True):
        """Record lock acquisition attempt"""
        if success:
            self.lock_acquire_count += 1
            self.active_locks += 1
        else:
            self.lock_denied_count += 1
    
    def record_lock_release(self):
        """Record lock release"""
        self.lock_release_count += 1
        if self.active_locks > 0:
            self.active_locks -= 1
    
    def record_deadlock(self):
        """Record deadlock detection"""
        self.lock_deadlock_count += 1
    
    def update_active_locks(self, count: int):
        """Update active locks count directly"""
        self.active_locks = count

    # =========================
    # REQUEST METRICS (API)
    # =========================
    def record_request(self, endpoint: str = None, latency: float = None):
        """Record API request"""
        self.total_requests += 1
        if endpoint:
            self.endpoint_counts[endpoint] += 1
        if latency:
            self.request_latencies.append(latency)
    
    def average_request_latency(self) -> float:
        """Calculate average request latency"""
        return statistics.mean(self.request_latencies) if self.request_latencies else 0.0

    # =========================
    # SYSTEM METRICS
    # =========================
    def uptime(self) -> float:
        """Calculate node uptime in seconds"""
        return round(time.time() - self.start_time, 2)
    
    def uptime_formatted(self) -> str:
        """Get formatted uptime string"""
        seconds = int(self.uptime())
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"

    # =========================
    # SUMMARY OUTPUT
    # =========================
    def summary(self) -> dict:
        """Return all metrics as dictionary (for JSON API response)"""
        return {
            "system": {
                "uptime_seconds": self.uptime(),
                "uptime_formatted": self.uptime_formatted(),
                "total_requests": self.total_requests,
                "avg_request_latency_ms": round(self.average_request_latency() * 1000, 2)
            },
            "raft": {
                "term": self.raft_term,
                "elections": self.raft_election_count,
                "heartbeats": self.raft_heartbeat_count,
                "log_entries": self.raft_log_entries,
                "state_changes": len(self.raft_state_changes)
            },
            "failure_detector": {
                "avg_ping_rtt_ms": round(self.average_rtt() * 1000, 2),
                "rtt_stddev_ms": round(self.rtt_stddev() * 1000, 2),
                "failed_nodes_detected": self.failed_nodes_count,
                "recovery_events": self.recovery_events,
                "current_failed_count": len(self.current_failed_nodes)
            },
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate_percent": round(self.cache_hit_rate(), 2),
                "invalidations": self.cache_invalidations,
                "evictions": self.cache_evictions,
                "current_size": self.cache_size
            },
            "queue": {
                "total_pushes": self.queue_push_count,
                "total_pops": self.queue_pop_count,
                "current_total_size": self.total_queue_size(),
                "queues": dict(self.queue_sizes)
            },
            "locks": {
                "total_acquires": self.lock_acquire_count,
                "total_releases": self.lock_release_count,
                "denied": self.lock_denied_count,
                "deadlocks": self.lock_deadlock_count,
                "active_locks": self.active_locks
            },
            "api": {
                "total_requests": self.total_requests,
                "avg_latency_ms": round(self.average_request_latency() * 1000, 2),
                "top_endpoints": dict(sorted(
                    self.endpoint_counts.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10])
            }
        }
    
    def log_summary(self):
        """Log metrics summary to console"""
        logger.info("=" * 60)
        logger.info("METRICS SUMMARY")
        logger.info("=" * 60)
        summary = self.summary()
        
        logger.info(f"Uptime: {summary['system']['uptime_formatted']}")
        logger.info(f"Raft Term: {summary['raft']['term']} | Elections: {summary['raft']['elections']}")
        logger.info(f"Cache Hit Rate: {summary['cache']['hit_rate_percent']}%")
        logger.info(f"Active Locks: {summary['locks']['active_locks']}")
        logger.info(f"Queue Size: {summary['queue']['current_total_size']}")
        logger.info(f"Failed Nodes: {summary['failure_detector']['current_failed_count']}")
        logger.info("=" * 60)