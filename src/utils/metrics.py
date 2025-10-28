
import time
import statistics
from loguru import logger
from collections import defaultdict, deque

class MetricsCollector:
    def __init__(self):
        self.start_time = time.time()
        
        
        self.raft_election_count = 0
        self.raft_heartbeat_count = 0
        self.raft_log_entries = 0
        self.raft_term = 0
        self.raft_state_changes = []  
        
        
        self.ping_rtt = deque(maxlen=50)  
        self.failed_nodes_count = 0
        self.recovery_events = 0
        self.current_failed_nodes = set()
        
        
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_invalidations = 0
        self.cache_evictions = 0
        self.cache_size = 0
        
        
        self.queue_push_count = 0
        self.queue_pop_count = 0
        self.queue_sizes = defaultdict(int)  
        
        
        self.lock_acquire_count = 0
        self.lock_release_count = 0
        self.lock_denied_count = 0
        self.lock_deadlock_count = 0
        self.active_locks = 0
        
        
        self.total_requests = 0
        self.request_latencies = deque(maxlen=100)  
        self.endpoint_counts = defaultdict(int)  

    
    
    
    def record_raft_election(self):
        self.raft_election_count += 1
    
    def record_raft_heartbeat(self):
        self.raft_heartbeat_count += 1
    
    def record_raft_log_entry(self):
        self.raft_log_entries += 1
    
    def record_raft_state_change(self, from_state: str, to_state: str):
        self.raft_state_changes.append((time.time(), from_state, to_state))
        if len(self.raft_state_changes) > 100:
            self.raft_state_changes.pop(0)
    
    def update_raft_term(self, term: int):
        self.raft_term = term

    
    
    
    def record_ping(self, rtt: float):
        self.ping_rtt.append(rtt)
    
    def record_failure(self, node_addr: str):
        if node_addr not in self.current_failed_nodes:
            self.failed_nodes_count += 1
            self.current_failed_nodes.add(node_addr)
    
    def record_recovery(self, node_addr: str):
        if node_addr in self.current_failed_nodes:
            self.recovery_events += 1
            self.current_failed_nodes.remove(node_addr)
    
    def average_rtt(self) -> float:
        return statistics.mean(self.ping_rtt) if self.ping_rtt else 0.0
    
    def rtt_stddev(self) -> float:
        return statistics.stdev(self.ping_rtt) if len(self.ping_rtt) > 1 else 0.0

    
    
    
    def record_cache_hit(self):
        self.cache_hits += 1
    
    def record_cache_miss(self):
        self.cache_misses += 1
    
    def record_cache_invalidation(self):
        self.cache_invalidations += 1
    
    def record_cache_eviction(self):
        self.cache_evictions += 1
    
    def update_cache_size(self, size: int):
        self.cache_size = size
    
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return (self.cache_hits / total * 100) if total > 0 else 0.0

    
    
    
    def record_queue_push(self, queue_name: str = "default"):
        self.queue_push_count += 1
        self.queue_sizes[queue_name] += 1
    
    def record_queue_pop(self, queue_name: str = "default"):
        self.queue_pop_count += 1
        if self.queue_sizes[queue_name] > 0:
            self.queue_sizes[queue_name] -= 1
    
    def update_queue_size(self, queue_name: str, size: int):
        self.queue_sizes[queue_name] = size
    
    def total_queue_size(self) -> int:
        return sum(self.queue_sizes.values())

    
    
    
    def record_lock_acquire(self, success: bool = True):
        if success:
            self.lock_acquire_count += 1
            self.active_locks += 1
        else:
            self.lock_denied_count += 1
    
    def record_lock_release(self):
        self.lock_release_count += 1
        if self.active_locks > 0:
            self.active_locks -= 1
    
    def record_deadlock(self):
        self.lock_deadlock_count += 1
    
    def update_active_locks(self, count: int):
        self.active_locks = count

    
    
    
    def record_request(self, endpoint: str = None, latency: float = None):
        self.total_requests += 1
        if endpoint:
            self.endpoint_counts[endpoint] += 1
        if latency:
            self.request_latencies.append(latency)
    
    def average_request_latency(self) -> float:
        return statistics.mean(self.request_latencies) if self.request_latencies else 0.0

    
    
    
    def uptime(self) -> float:
        return round(time.time() - self.start_time, 2)
    
    def uptime_formatted(self) -> str:
        seconds = int(self.uptime())
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours}h {minutes}m {seconds}s"

    
    
    
    def summary(self) -> dict:
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
