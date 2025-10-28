
import asyncio
import json
import time
from collections import OrderedDict
from typing import Dict, Optional
from loguru import logger
from aiohttp import web

from src.communication.message_passing import send_json
from src.utils.redis_client import get_redis
from src.utils.config import CACHE_MAX_SIZE, CACHE_TTL


class CacheEntry:
    
    def __init__(self, key: str, value: any, state: str = "Exclusive", 
                 timestamp: float = None, ttl: int = CACHE_TTL, source_node: str = None):
        self.key = key
        self.value = value
        self.state = state  
        self.timestamp = timestamp or time.time()
        self.ttl = ttl
        self.access_count = 0
        self.source_node = source_node 
    
    def is_valid(self) -> bool:
        if self.state == "Invalid":
            return False
        if time.time() - self.timestamp > self.ttl:
            return False
        return True
    
    def to_dict(self):
        return {
            "key": self.key,
            "value": self.value,
            "state": self.state,
            "timestamp": self.timestamp,
            "ttl": self.ttl,
            "access_count": self.access_count,
            "source_node": self.source_node
        }
    
    @staticmethod
    def from_dict(data: dict):
        entry = CacheEntry(
            key=data["key"],
            value=data["value"],
            state=data.get("state", "Exclusive"),
            timestamp=data.get("timestamp", time.time()),
            ttl=data.get("ttl", CACHE_TTL),
            source_node=data.get("source_node")
        )
        entry.access_count = data.get("access_count", 0)
        return entry


class CacheNode:
    
    def __init__(self, node):
        self.node = node
        self.cache = OrderedDict()  
        self.max_size = CACHE_MAX_SIZE
        
        
        asyncio.create_task(self._cleanup_expired_entries())
        
        logger.info(f"💾 Cache Node initialized (max_size={self.max_size}, protocol=MESI)")

    def routes(self):
        return {
            "/cache/get": self.get_handler,
            "/cache/put": self.put_handler,
            "/cache/invalidate": self.invalidate_handler,
            "/cache/snoop": self.snoop_handler,  
            "/cache/status": self.status_handler,
        }

    
    
    
    def _access_entry(self, key: str):
        if key in self.cache:
            entry = self.cache.pop(key)
            entry.access_count += 1
            self.cache[key] = entry  

    def _evict_lru(self):
        if not self.cache:
            return
        
        
        key, entry = self.cache.popitem(last=False)
        
        
        if entry.state == "Modified":
            logger.debug(f"Evicting Modified entry (write-back): {key}")
            asyncio.create_task(self._write_back_to_redis(entry))
        
        self.node.metrics.record_cache_eviction()
        logger.debug(f"🗑️  LRU evicted: {key} (state={entry.state})")

    
    
    
    async def _write_back_to_redis(self, entry: CacheEntry):
        try:
            redis = await get_redis()
            if redis:
                key = f"cache:data:{entry.key}"
                await redis.setex(key, entry.ttl, json.dumps(entry.to_dict()))
                logger.trace(f"Write-back to Redis: {entry.key}")
        except Exception as e:
            logger.warning(f"Failed to write-back to Redis: {e}")

    async def _load_from_redis(self, key: str) -> Optional[CacheEntry]:
        try:
            redis = await get_redis()
            if redis:
                redis_key = f"cache:data:{key}"
                data = await redis.get(redis_key)
                if data:
                    return CacheEntry.from_dict(json.loads(data))
        except Exception as e:
            logger.warning(f"Failed to load from Redis: {e}")
        return None

    
    
    
    async def _transition_to_shared(self, key: str):
        if key in self.cache:
            entry = self.cache[key]
            if entry.state in ["Exclusive", "Modified"]:
                
                if entry.state == "Modified":
                    await self._write_back_to_redis(entry)
                entry.state = "Shared"
                logger.debug(f"MESI: {key} -> Shared")

    async def _transition_to_invalid(self, key: str):
        if key in self.cache:
            entry = self.cache[key]
            entry.state = "Invalid"
            logger.debug(f"MESI: {key} -> Invalid")

    async def _broadcast_snoop(self, key: str, operation: str):
        tasks = []
        for addr in self.node.raft.cluster_addrs:
            if self.node.node_id in addr:
                continue
            if hasattr(self.node, 'failure_detector') and addr in self.node.failure_detector.failed_nodes:
                continue
            
            tasks.append(send_json(addr, "/cache/snoop", {
                "key": key,
                "operation": operation,
                "from_node": self.node.node_id
            }, timeout=1.0))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _snoop_read(self, key: str) -> Optional[any]:
        if key not in self.cache:
            return None
        
        entry = self.cache[key]
        
        if entry.state == "Modified":
            
            await self._write_back_to_redis(entry)
            entry.state = "Shared"
            logger.debug(f"MESI Snoop: {key} Modified -> Shared (write-back)")
            return {"value": entry.value, "from_node": self.node.node_id}
        
        elif entry.state == "Exclusive":
            
            entry.state = "Shared"
            logger.debug(f"MESI Snoop: {key} Exclusive -> Shared")
            return {"value": entry.value, "from_node": self.node.node_id}
        
        elif entry.state == "Shared":
            
            return {"value": entry.value, "from_node": self.node.node_id}
        
        return None

    async def _snoop_write(self, key: str):
        if key in self.cache:
            entry = self.cache[key]
            if entry.state == "Modified":
                
                await self._write_back_to_redis(entry)
            entry.state = "Invalid"
            logger.debug(f"MESI Snoop: {key} -> Invalid (another cache writing)")

    
    
    
    async def _get_from_cache(self, key: str) -> Optional[any]:
        
        if key in self.cache:
            entry = self.cache[key]
            if entry.is_valid():
                
                self._access_entry(key)
                self.node.metrics.record_cache_hit()
                
                
                
                
                logger.debug(f"Cache HIT: {key} (state={entry.state})")
                return entry.value
            else:
                
                del self.cache[key]
        
        
        self.node.metrics.record_cache_miss()
        logger.debug(f"Cache MISS: {key}")
        
        
        snoop_tasks = []
        for addr in self.node.raft.cluster_addrs:
            if self.node.node_id in addr:
                continue
            if hasattr(self.node, 'failure_detector') and addr in self.node.failure_detector.failed_nodes:
                continue
            
            snoop_tasks.append(send_json(addr, "/cache/snoop", {
                "key": key,
                "operation": "read",
                "from_node": self.node.node_id
            }, timeout=1.0))
        
        if snoop_tasks:
            results = await asyncio.gather(*snoop_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, dict) and result.get("has_data"):
                    
                    value = result.get("value")
                    from_node = result.get("from_node")
                    
                    entry = CacheEntry(key, value, state="Shared", source_node=from_node)
                    self.cache[key] = entry
                    self.node.metrics.update_cache_size(len(self.cache))
                    logger.debug(f"MESI: Loaded {key} from peer cache (Shared) from {from_node}")
                    return value

        
        
        entry = await self._load_from_redis(key)
        if entry:
            
            entry.state = "Exclusive"
            self.cache[key] = entry
            self.node.metrics.update_cache_size(len(self.cache))
            logger.debug(f"MESI: Loaded {key} from Redis (Exclusive)")
            return entry.value
        
        return None

    async def _put_to_cache(self, key: str, value: any, ttl: int = CACHE_TTL):
        
        if len(self.cache) >= self.max_size:
            self._evict_lru()
        
        
        await self._broadcast_snoop(key, "write")
        
        
        entry = CacheEntry(key, value, state="Modified", ttl=ttl, source_node=self.node.node_id)
        entry.source_node = self.node.node_id

        
        
        if key in self.cache:
            del self.cache[key]
        
        self.cache[key] = entry
        self.node.metrics.update_cache_size(len(self.cache))
        
        
        await self._write_back_to_redis(entry)
        
        logger.debug(f"Cache PUT: {key} (state=Modified)")

    
    
    
    async def _cleanup_expired_entries(self):
        while True:
            try:
                await asyncio.sleep(60)  
                
                expired = []
                for key, entry in self.cache.items():
                    if not entry.is_valid():
                        expired.append(key)
                
                for key in expired:
                    logger.debug(f"🧹 Cleaning expired entry: {key}")
                    del self.cache[key]
                    self.node.metrics.record_cache_eviction()
                
                self.node.metrics.update_cache_size(len(self.cache))
                
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")

    
    
    
    async def get_handler(self, request):
        key = request.query.get("key") or (await request.json()).get("key")
        
        if not key:
            return web.json_response({"error": "key is required"}, status=400)
        
        value = await self._get_from_cache(key)
        
        if value is not None:
            return web.json_response({
                "key": key,
                "value": value,
                "hit": True,
                "state": self.cache[key].state if key in self.cache else None
            })
        else:
            return web.json_response({
                "key": key,
                "value": None,
                "hit": False
            })

    async def put_handler(self, request):
        body = await request.json()
        key = body.get("key")
        value = body.get("value")
        ttl = body.get("ttl", CACHE_TTL)

        if not key or value is None:
            return web.json_response({"error": "key and value are required"}, status=400)

        await self._put_to_cache(key, value, ttl)
        
        return web.json_response({
            "status": "stored",
            "key": key,
            "state": "Modified",
            "cache_size": len(self.cache)
        })

    async def invalidate_handler(self, request):
        body = await request.json()
        key = body.get("key")

        if key in self.cache:
            await self._transition_to_invalid(key)
            self.node.metrics.record_cache_invalidation()
            logger.info(f"🚫 Cache invalidated: {key}")
            return web.json_response({"status": "invalidated", "key": key})
        
        return web.json_response({"status": "not_found", "key": key})

    async def snoop_handler(self, request):
        body = await request.json()
        key = body.get("key")
        operation = body.get("operation")
        from_node = body.get("from_node", "unknown")

        logger.trace(f"Snoop request from {from_node}: {operation} {key}")

        if operation == "read":
            result = await self._snoop_read(key)
            has_data = result is not None
            value = None
            from_node = None
            if isinstance(result, dict):
                value = result.get("value")
                from_node = result.get("from_node")
            else:
                value = result  
                from_node = self.node.node_id if key in self.cache else None

            return web.json_response({
                "has_data": has_data,
                "value": value,
                "from_node": from_node,
                "state": self.cache[key].state if key in self.cache else None
            })
        
        elif operation == "write":
            await self._snoop_write(key)
            return web.json_response({"status": "invalidated"})
        
        return web.json_response({"error": "invalid operation"}, status=400)

    async def status_handler(self, request):
        
        state_distribution = {"Modified": 0, "Exclusive": 0, "Shared": 0, "Invalid": 0}
        for entry in self.cache.values():
            state_distribution[entry.state] += 1
        
        return web.json_response({
            "node_id": self.node.node_id,
            "cache_size": len(self.cache),
            "max_size": self.max_size,
            "hit_rate": self.node.metrics.cache_hit_rate(),
            "state_distribution": state_distribution,
            "metrics": {
                "hits": self.node.metrics.cache_hits,
                "misses": self.node.metrics.cache_misses,
                "invalidations": self.node.metrics.cache_invalidations,
                "evictions": self.node.metrics.cache_evictions
            }
        })

    
    async def debug_cache_state(self, request):
        data = {}
        for key, entry in self.cache.items():
            data[key] = {
                "value": entry.value,
                "state": entry.state,
                "source": getattr(entry, "source_node", None),
                "timestamp": entry.timestamp,
                "access_count": entry.access_count,
            }
        return web.json_response({
            "node": self.node.node_id,
            "cache_size": len(self.cache),
            "entries": data
        })
