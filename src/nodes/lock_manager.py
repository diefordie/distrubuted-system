
import asyncio
import time
import json
from typing import Dict, Set, Optional
from loguru import logger
from aiohttp import web

from src.communication.message_passing import send_json
from src.utils.redis_client import get_redis
from src.utils.config import LOCK_DEFAULT_TTL, LOCK_MAX_WAIT_TIME


class Lock:
    def __init__(self, resource: str, mode: str, owners: Set[str], acquired_at: float, ttl: int = LOCK_DEFAULT_TTL):
        self.resource = resource
        self.mode = mode  
        self.owners = owners  
        self.acquired_at = acquired_at
        self.ttl = ttl
    
    def is_expired(self) -> bool:
        return time.time() - self.acquired_at > self.ttl
    
    def to_dict(self) -> dict:
        return {
            "resource": self.resource,
            "mode": self.mode,
            "owners": list(self.owners),
            "acquired_at": self.acquired_at,
            "ttl": self.ttl
        }
    
    @staticmethod
    def from_dict(data: dict):
        return Lock(
            resource=data["resource"],
            mode=data["mode"],
            owners=set(data["owners"]),
            acquired_at=data["acquired_at"],
            ttl=data["ttl"]
        )


class LockManager:
    
    def __init__(self, node):
        self.node = node
        self.locks: Dict[str, Lock] = {}  
        self.wait_for_graph: Dict[str, Set[str]] = {}  
        self.lock_waiters: Dict[str, list] = {}  
        
        
        self._cleanup_task = None
        
        logger.info("🔒 Lock Manager initialized")

    async def start(self):
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_locks())
            logger.debug("Lock Manager cleanup task started")
    
    async def stop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            logger.debug("Lock Manager cleanup task stopped")

    def routes(self):
        return {
            "/lock/acquire": self.acquire_handler,
            "/lock/release": self.release_handler,
            "/lock/status": self.status_handler,
            "/lock/force_release": self.force_release_handler,
        }

    
    
    
    async def _save_lock_to_redis(self, lock: Lock):
        try:
            redis = await get_redis()
            if redis:
                key = f"lock:{lock.resource}"
                await redis.setex(key, lock.ttl, json.dumps(lock.to_dict()))
                logger.trace(f"Saved lock to Redis: {lock.resource}")
        except Exception as e:
            logger.warning(f"Failed to save lock to Redis: {e}")

    async def _load_lock_from_redis(self, resource: str) -> Optional[Lock]:
        try:
            redis = await get_redis()
            if redis:
                key = f"lock:{resource}"
                data = await redis.get(key)
                if data:
                    return Lock.from_dict(json.loads(data))
        except Exception as e:
            logger.warning(f"Failed to load lock from Redis: {e}")
        return None

    async def _delete_lock_from_redis(self, resource: str):
        try:
            redis = await get_redis()
            if redis:
                key = f"lock:{resource}"
                await redis.delete(key)
                logger.trace(f"Deleted lock from Redis: {resource}")
        except Exception as e:
            logger.warning(f"Failed to delete lock from Redis: {e}")

    
    
    
    def _add_to_wait_graph(self, client_id: str, resource: str):
        if client_id not in self.wait_for_graph:
            self.wait_for_graph[client_id] = set()
        self.wait_for_graph[client_id].add(resource)
        logger.trace(f"Wait graph: {client_id} -> {resource}")

    def _remove_from_wait_graph(self, client_id: str, resource: str = None):
        if client_id in self.wait_for_graph:
            if resource:
                self.wait_for_graph[client_id].discard(resource)
                if not self.wait_for_graph[client_id]:
                    del self.wait_for_graph[client_id]
            else:
                del self.wait_for_graph[client_id]

    def _detect_deadlock(self, client_id: str, resource: str) -> bool:
        
        
        
        
        
        graph = {}
        for client, resources in self.wait_for_graph.items():
            graph[client] = set()
            for res in resources:
                if res in self.locks:
                    
                    graph[client].update(self.locks[res].owners)
        
        
        if client_id not in graph:
            graph[client_id] = set()
        if resource in self.locks:
            graph[client_id].update(self.locks[resource].owners)
        
        logger.debug(f"Wait-for graph state: {json.dumps({k: list(v) for k,v in self.wait_for_graph.items()}, indent=2)}")

        
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        
        if client_id in graph:
            if has_cycle(client_id):
                logger.warning(f"🔴 Deadlock detected: {client_id} -> {resource}")
                self.node.metrics.record_deadlock()
                return True
        
        return False

    
    
    
    async def _can_grant_lock(self, resource: str, mode: str, client_id: str) -> tuple[bool, str]:
        
        if resource not in self.locks:
            return True, "no_existing_lock"
        
        lock = self.locks[resource]
        
        
        if lock.is_expired():
            logger.info(f"Lock expired: {resource}")
            await self._release_lock_internal(resource)
            return True, "expired_lock"
        
        
        if client_id in lock.owners:
            return True, "already_owned"
        
        
        if mode == "shared" and lock.mode == "shared":
            return True, "shared_compatible"
        
        
        return False, f"held_by_{lock.owners}"

    async def _grant_lock(self, resource: str, mode: str, client_id: str, ttl: int = LOCK_DEFAULT_TTL):
        if resource not in self.locks:
            
            lock = Lock(resource, mode, {client_id}, time.time(), ttl)
            self.locks[resource] = lock
        else:
            
            lock = self.locks[resource]
            lock.owners.add(client_id)
        
        
        await self._save_lock_to_redis(lock)
        
        
        self._remove_from_wait_graph(client_id, resource)
        
        
        self.node.metrics.record_lock_acquire(success=True)
        self.node.metrics.update_active_locks(len(self.locks))
        
        logger.info(f"✅ Lock granted: {resource} ({mode}) to {client_id}")

    async def _release_lock_internal(self, resource: str, client_id: str = None):
        if resource not in self.locks:
            return
        
        lock = self.locks[resource]
        
        if client_id:
            
            lock.owners.discard(client_id)
            if not lock.owners:
                
                del self.locks[resource]
                await self._delete_lock_from_redis(resource)
                self.node.metrics.record_lock_release()
            else:
                
                await self._save_lock_to_redis(lock)
        else:
            
            del self.locks[resource]
            await self._delete_lock_from_redis(resource)
            self.node.metrics.record_lock_release()
        
        self.node.metrics.update_active_locks(len(self.locks))
        
        
        await self._notify_waiters(resource)

    async def _notify_waiters(self, resource: str):
        if resource not in self.lock_waiters:
            return
        
        waiters = self.lock_waiters[resource]
        self.lock_waiters[resource] = []
        
        for client_id, mode, future in waiters:
            can_grant, reason = await self._can_grant_lock(resource, mode, client_id)
            if can_grant:
                await self._grant_lock(resource, mode, client_id)
                if not future.done():
                    future.set_result({"status": "granted"})
            else:
                
                self.lock_waiters.setdefault(resource, []).append((client_id, mode, future))

    
    
    
    async def _cleanup_expired_locks(self):
        while True:
            try:
                await asyncio.sleep(30)  
                
                expired = []
                for resource, lock in self.locks.items():
                    if lock.is_expired():
                        expired.append(resource)
                
                for resource in expired:
                    logger.info(f"🧹 Cleaning up expired lock: {resource}")
                    await self._release_lock_internal(resource)
                    
            except Exception as e:
                logger.error(f"Error in lock cleanup: {e}")

    
    
    
    async def acquire_handler(self, request):
        body = await request.json()
        resource = body.get("resource")
        mode = body.get("mode", "exclusive")
        client_id = body.get("client_id")
        ttl = body.get("ttl", LOCK_DEFAULT_TTL)
        wait = body.get("wait", False)  

        
        if not resource or not client_id:
            return web.json_response({"error": "resource and client_id required"}, status=400)
        
        if mode not in ["shared", "exclusive"]:
            return web.json_response({"error": "mode must be 'shared' or 'exclusive'"}, status=400)

        
        if self.node.raft.state != "leader":
            leader = self.node.raft.leader_id
            if leader:
                leader_addr = next((addr for addr in self.node.raft.cluster_addrs if leader in addr), None)
                if leader_addr:
                    try:
                        result = await send_json(leader_addr, "/lock/acquire", body)
                        return web.json_response(result)
                    except Exception as e:
                        logger.error(f"Failed to forward to leader: {e}")
            return web.json_response({"error": "no leader available"}, status=503)

        
        can_grant, reason = await self._can_grant_lock(resource, mode, client_id)
        
        if can_grant:
            
            await self._grant_lock(resource, mode, client_id, ttl)
            return web.json_response({
                "status": "granted",
                "resource": resource,
                "mode": mode,
                "client_id": client_id
            })
            
    
        self._add_to_wait_graph(client_id, resource)

        
        if self._detect_deadlock(client_id, resource):
            self.node.metrics.record_lock_acquire(success=False)
            return web.json_response({
                "status": "deadlock_detected",
                "resource": resource,
                "message": "Acquiring this lock would cause a deadlock"
            }, status=409)
        
        
        if not wait:
            
            self.node.metrics.record_lock_acquire(success=False)
            return web.json_response({
                "status": "denied",
                "resource": resource,
                "reason": reason,
                "current_lock": self.locks[resource].to_dict() if resource in self.locks else None
            })
        
        
        logger.info(f"⏳ {client_id} waiting for {resource}")
        
        future = asyncio.Future()
        self.lock_waiters.setdefault(resource, []).append((client_id, mode, future))
        
        try:
            result = await asyncio.wait_for(future, timeout=LOCK_MAX_WAIT_TIME)
            return web.json_response(result)
        except asyncio.TimeoutError:
            
            self._remove_from_wait_graph(client_id, resource)
            
            if resource in self.lock_waiters:
                self.lock_waiters[resource] = [
                    (cid, m, f) for cid, m, f in self.lock_waiters[resource] 
                    if cid != client_id
                ]
            self.node.metrics.record_lock_acquire(success=False)
            return web.json_response({
                "status": "timeout",
                "resource": resource,
                "message": f"Timed out after {LOCK_MAX_WAIT_TIME}s"
            }, status=408)

    async def release_handler(self, request):
        body = await request.json()
        resource = body.get("resource")
        client_id = body.get("client_id")

        
        if self.node.raft.state != "leader":
            leader = self.node.raft.leader_id
            if leader:
                leader_addr = next((addr for addr in self.node.raft.cluster_addrs if leader in addr), None)
                if leader_addr:
                    result = await send_json(leader_addr, "/lock/release", body)
                    return web.json_response(result)
            return web.json_response({"error": "no leader available"}, status=503)

        
        if resource not in self.locks:
            return web.json_response({
                "status": "not_found",
                "resource": resource
            }, status=404)
        
        lock = self.locks[resource]
        if client_id not in lock.owners:
            return web.json_response({
                "status": "not_owner",
                "resource": resource,
                "owners": list(lock.owners)
            }, status=403)

        
        await self._release_lock_internal(resource, client_id)
        
        logger.info(f"🔓 Lock released: {resource} by {client_id}")
        return web.json_response({
            "status": "released",
            "resource": resource,
            "client_id": client_id
        })

    async def force_release_handler(self, request):
        body = await request.json()
        resource = body.get("resource")

        if self.node.raft.state != "leader":
            return web.json_response({"error": "only leader can force release"}, status=403)

        if resource in self.locks:
            await self._release_lock_internal(resource)
            logger.warning(f"⚠️  Force released lock: {resource}")
            return web.json_response({
                "status": "force_released",
                "resource": resource
            })
        
        return web.json_response({
            "status": "not_found",
            "resource": resource
        }, status=404)

    async def status_handler(self, request):
        locks_dict = {res: lock.to_dict() for res, lock in self.locks.items()}
        
        return web.json_response({
            "node_id": self.node.node_id,
            "is_leader": self.node.raft.state == "leader",
            "leader_id": self.node.raft.leader_id,
            "active_locks": len(self.locks),
            "locks": locks_dict,
            "waiting_clients": sum(len(waiters) for waiters in self.lock_waiters.values()),
            "wait_for_graph": {k: list(v) for k, v in self.wait_for_graph.items()}
        })
