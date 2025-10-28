# src/nodes/cache_node.py
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
    """
    Represents a cache entry with MESI state
    
    MESI States:
    - Modified (M): This cache has the only valid copy, modified from memory
    - Exclusive (E): This cache has the only valid copy, same as memory
    - Shared (S): Multiple caches may have this data, same as memory
    - Invalid (I): Data is invalid, must be fetched
    """
    
    def __init__(self, key: str, value: any, state: str = "Exclusive", 
                 timestamp: float = None, ttl: int = CACHE_TTL):
        self.key = key
        self.value = value
        self.state = state  # "Modified", "Exclusive", "Shared", "Invalid"
        self.timestamp = timestamp or time.time()
        self.ttl = ttl
        self.access_count = 0
    
    def is_valid(self) -> bool:
        """Check if entry is valid (not Invalid state and not expired)"""
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
            "access_count": self.access_count
        }
    
    @staticmethod
    def from_dict(data: dict):
        entry = CacheEntry(
            key=data["key"],
            value=data["value"],
            state=data.get("state", "Exclusive"),
            timestamp=data.get("timestamp", time.time()),
            ttl=data.get("ttl", CACHE_TTL)
        )
        entry.access_count = data.get("access_count", 0)
        return entry


class CacheNode:
    """
    Distributed Cache with:
    - MESI cache coherence protocol
    - LRU eviction policy
    - Redis persistence
    - Performance metrics
    """
    
    def __init__(self, node):
        self.node = node
        self.cache = OrderedDict()  # key -> CacheEntry (LRU order)
        self.max_size = CACHE_MAX_SIZE
        
        # Start background tasks
        asyncio.create_task(self._cleanup_expired_entries())
        
        logger.info(f"💾 Cache Node initialized (max_size={self.max_size}, protocol=MESI)")

    def routes(self):
        """Register HTTP routes"""
        return {
            "/cache/get": self.get_handler,
            "/cache/put": self.put_handler,
            "/cache/invalidate": self.invalidate_handler,
            "/cache/snoop": self.snoop_handler,  # For MESI protocol
            "/cache/status": self.status_handler,
        }

    # =========================
    # LRU EVICTION
    # =========================
    def _access_entry(self, key: str):
        """Mark entry as recently used (move to end for LRU)"""
        if key in self.cache:
            entry = self.cache.pop(key)
            entry.access_count += 1
            self.cache[key] = entry  # Move to end

    def _evict_lru(self):
        """Evict least recently used entry"""
        if not self.cache:
            return
        
        # Pop from front (least recently used)
        key, entry = self.cache.popitem(last=False)
        
        # If entry is Modified, should write back (simulate)
        if entry.state == "Modified":
            logger.debug(f"Evicting Modified entry (write-back): {key}")
            asyncio.create_task(self._write_back_to_redis(entry))
        
        self.node.metrics.record_cache_eviction()
        logger.debug(f"🗑️  LRU evicted: {key} (state={entry.state})")

    # =========================
    # REDIS PERSISTENCE
    # =========================
    async def _write_back_to_redis(self, entry: CacheEntry):
        """Write modified entry back to Redis"""
        try:
            redis = await get_redis()
            if redis:
                key = f"cache:data:{entry.key}"
                await redis.setex(key, entry.ttl, json.dumps(entry.to_dict()))
                logger.trace(f"Write-back to Redis: {entry.key}")
        except Exception as e:
            logger.warning(f"Failed to write-back to Redis: {e}")

    async def _load_from_redis(self, key: str) -> Optional[CacheEntry]:
        """Load entry from Redis (simulates memory)"""
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

    # =========================
    # MESI PROTOCOL
    # =========================
    async def _transition_to_shared(self, key: str):
        """Transition entry from Exclusive/Modified to Shared"""
        if key in self.cache:
            entry = self.cache[key]
            if entry.state in ["Exclusive", "Modified"]:
                # If Modified, write back first
                if entry.state == "Modified":
                    await self._write_back_to_redis(entry)
                entry.state = "Shared"
                logger.debug(f"MESI: {key} -> Shared")

    async def _transition_to_invalid(self, key: str):
        """Transition entry to Invalid state"""
        if key in self.cache:
            entry = self.cache[key]
            entry.state = "Invalid"
            logger.debug(f"MESI: {key} -> Invalid")

    async def _broadcast_snoop(self, key: str, operation: str):
        """
        Broadcast snoop request to all other caches
        
        Operations:
        - "read": Another cache wants to read this key
        - "write": Another cache wants to write this key
        """
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
        """
        Handle snoop read request
        
        MESI Rules for snoop read:
        - If Modified: write-back, transition to Shared, provide data
        - If Exclusive: transition to Shared, provide data
        - If Shared: stay Shared, provide data
        - If Invalid: do nothing
        """
        if key not in self.cache:
            return None
        
        entry = self.cache[key]
        
        if entry.state == "Modified":
            # Write back and transition to Shared
            await self._write_back_to_redis(entry)
            entry.state = "Shared"
            logger.debug(f"MESI Snoop: {key} Modified -> Shared (write-back)")
            return entry.value
        
        elif entry.state == "Exclusive":
            # Transition to Shared
            entry.state = "Shared"
            logger.debug(f"MESI Snoop: {key} Exclusive -> Shared")
            return entry.value
        
        elif entry.state == "Shared":
            # Stay Shared
            return entry.value
        
        return None

    async def _snoop_write(self, key: str):
        """
        Handle snoop write request
        
        MESI Rules for snoop write:
        - Any state -> Invalid (another cache is writing)
        """
        if key in self.cache:
            entry = self.cache[key]
            if entry.state == "Modified":
                # Write back before invalidating
                await self._write_back_to_redis(entry)
            entry.state = "Invalid"
            logger.debug(f"MESI Snoop: {key} -> Invalid (another cache writing)")

    # =========================
    # CACHE OPERATIONS
    # =========================
    async def _get_from_cache(self, key: str) -> Optional[any]:
        """
        Get value from cache with MESI protocol
        
        MESI Rules for local read:
        1. Hit (Valid state): return value, mark as accessed
        2. Miss or Invalid: 
           - Snoop other caches (might be in Shared/Modified state)
           - If found in other cache: add as Shared
           - If not found: load from memory (Redis), add as Exclusive
        """
        # Check local cache
        if key in self.cache:
            entry = self.cache[key]
            if entry.is_valid():
                # Hit
                self._access_entry(key)
                self.node.metrics.record_cache_hit()
                
                # MESI: If Exclusive, could transition to Shared if we broadcast
                # (but for read, we don't need to notify others unless we want coherence tracking)
                
                logger.debug(f"Cache HIT: {key} (state={entry.state})")
                return entry.value
            else:
                # Entry expired or invalid
                del self.cache[key]
        
        # Miss
        self.node.metrics.record_cache_miss()
        logger.debug(f"Cache MISS: {key}")
        
        # Try to snoop from other caches
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
                    # Add to our cache as Shared
                    entry = CacheEntry(key, value, state="Shared")
                    self.cache[key] = entry
                    self.node.metrics.update_cache_size(len(self.cache))
                    logger.debug(f"MESI: Loaded {key} from peer cache (Shared)")
                    return value
        
        # Not found in other caches, load from memory (Redis)
        entry = await self._load_from_redis(key)
        if entry:
            # Add as Exclusive (we're the only cache with this data)
            entry.state = "Exclusive"
            self.cache[key] = entry
            self.node.metrics.update_cache_size(len(self.cache))
            logger.debug(f"MESI: Loaded {key} from Redis (Exclusive)")
            return entry.value
        
        return None

    async def _put_to_cache(self, key: str, value: any, ttl: int = CACHE_TTL):
        """
        Put value to cache with MESI protocol
        
        MESI Rules for local write:
        1. Invalidate all other caches (broadcast snoop write)
        2. Add to local cache as Modified
        3. Eventually write-back to memory (Redis)
        """
        # Evict if at capacity
        if len(self.cache) >= self.max_size:
            self._evict_lru()
        
        # Invalidate other caches
        await self._broadcast_snoop(key, "write")
        
        # Add to local cache as Modified
        entry = CacheEntry(key, value, state="Modified", ttl=ttl)
        
        # If key exists, remove first (for OrderedDict to maintain order)
        if key in self.cache:
            del self.cache[key]
        
        self.cache[key] = entry
        self.node.metrics.update_cache_size(len(self.cache))
        
        # Write-back to Redis (could be lazy, but we do it now for persistence)
        await self._write_back_to_redis(entry)
        
        logger.debug(f"Cache PUT: {key} (state=Modified)")

    # =========================
    # BACKGROUND TASKS
    # =========================
    async def _cleanup_expired_entries(self):
        """Periodic cleanup of expired entries"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
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

    # =========================
    # HTTP HANDLERS
    # =========================
    async def get_handler(self, request):
        """
        Get value from cache
        
        ---
        summary: Get cached value
        tags:
          - Cache
        parameters:
          - name: key
            in: query
            required: true
            schema:
              type: string
        responses:
          '200':
            description: Value retrieved or not found
        """
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
        """
        Put value to cache
        
        ---
        summary: Cache a value
        tags:
          - Cache
        requestBody:
          content:
            application/json:
              schema:
                type: object
                properties:
                  key:
                    type: string
                  value:
                    type: object
                  ttl:
                    type: integer
                    default: 300
                required:
                  - key
                  - value
        responses:
          '200':
            description: Value cached successfully
        """
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
        """
        Invalidate cache entry (internal, called by other caches)
        
        ---
        summary: Invalidate cached value
        tags:
          - Cache
        requestBody:
          content:
            application/json:
              schema:
                type: object
                properties:
                  key:
                    type: string
                required:
                  - key
        responses:
          '200':
            description: Entry invalidated
        """
        body = await request.json()
        key = body.get("key")

        if key in self.cache:
            await self._transition_to_invalid(key)
            self.node.metrics.record_cache_invalidation()
            logger.info(f"🚫 Cache invalidated: {key}")
            return web.json_response({"status": "invalidated", "key": key})
        
        return web.json_response({"status": "not_found", "key": key})

    async def snoop_handler(self, request):
        """
        Handle snoop requests from other caches (MESI protocol)
        
        ---
        summary: Cache snoop request
        tags:
          - Cache
        requestBody:
          content:
            application/json:
              schema:
                type: object
                properties:
                  key:
                    type: string
                  operation:
                    type: string
                    enum: [read, write]
                  from_node:
                    type: string
                required:
                  - key
                  - operation
        responses:
          '200':
            description: Snoop response
        """
        body = await request.json()
        key = body.get("key")
        operation = body.get("operation")
        from_node = body.get("from_node", "unknown")

        logger.trace(f"Snoop request from {from_node}: {operation} {key}")

        if operation == "read":
            value = await self._snoop_read(key)
            return web.json_response({
                "has_data": value is not None,
                "value": value,
                "state": self.cache[key].state if key in self.cache else None
            })
        
        elif operation == "write":
            await self._snoop_write(key)
            return web.json_response({"status": "invalidated"})
        
        return web.json_response({"error": "invalid operation"}, status=400)

    async def status_handler(self, request):
        """
        Get cache status
        
        ---
        summary: Get cache statistics
        tags:
          - Cache
        responses:
          '200':
            description: Cache statistics
        """
        # Calculate state distribution
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