# src/nodes/queue_node.py
import asyncio
import hashlib
import json
import time
import uuid
from typing import Dict, List, Optional
from loguru import logger
from aiohttp import web

from src.communication.message_passing import send_json
from src.utils.redis_client import get_redis
from src.utils.config import QUEUE_MAX_SIZE, QUEUE_VIRTUAL_NODES


class Message:
    """Represents a queue message with metadata"""
    def __init__(self, msg_id: str, data: any, queue: str, status: str = "pending", 
                 timestamp: float = None, retry_count: int = 0):
        self.msg_id = msg_id
        self.data = data
        self.queue = queue
        self.status = status  # "pending", "processing", "completed", "failed"
        self.timestamp = timestamp or time.time()
        self.retry_count = retry_count
    
    def to_dict(self):
        return {
            "msg_id": self.msg_id,
            "data": self.data,
            "queue": self.queue,
            "status": self.status,
            "timestamp": self.timestamp,
            "retry_count": self.retry_count
        }
    
    @staticmethod
    def from_dict(data: dict):
        return Message(
            msg_id=data["msg_id"],
            data=data["data"],
            queue=data["queue"],
            status=data.get("status", "pending"),
            timestamp=data.get("timestamp", time.time()),
            retry_count=data.get("retry_count", 0)
        )


class ConsistentHashRing:
    """Consistent hashing implementation for queue distribution"""
    
    def __init__(self, nodes: List[str], virtual_nodes: int = QUEUE_VIRTUAL_NODES):
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
        
        for node in nodes:
            self.add_node(node)
    
    def _hash(self, key: str) -> int:
        """Hash function using MD5"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: str):
        """Add a node to the hash ring with virtual nodes"""
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:{i}"
            hash_val = self._hash(virtual_key)
            self.ring[hash_val] = node
        
        self.sorted_keys = sorted(self.ring.keys())
        logger.debug(f"Added node {node} to hash ring (total: {len(self.sorted_keys)} virtual nodes)")
    
    def remove_node(self, node: str):
        """Remove a node from the hash ring"""
        for i in range(self.virtual_nodes):
            virtual_key = f"{node}:{i}"
            hash_val = self._hash(virtual_key)
            if hash_val in self.ring:
                del self.ring[hash_val]
        
        self.sorted_keys = sorted(self.ring.keys())
        logger.debug(f"Removed node {node} from hash ring")
    
    def get_node(self, key: str) -> Optional[str]:
        """Get the responsible node for a given key"""
        if not self.ring:
            return None
        
        hash_val = self._hash(key)
        
        # Find the first node clockwise from the hash
        for ring_hash in self.sorted_keys:
            if hash_val <= ring_hash:
                return self.ring[ring_hash]
        
        # Wrap around to the first node
        return self.ring[self.sorted_keys[0]]


class QueueNode:
    """
    Distributed Queue System with:
    - Consistent hashing for message distribution
    - Redis persistence
    - At-least-once delivery guarantee
    - Multiple producers/consumers support
    """
    
    def __init__(self, node):
        self.node = node
        self.queues: Dict[str, List[Message]] = {}  # queue_name -> list of messages (in-memory)
        self.hash_ring = ConsistentHashRing(node.raft.cluster_addrs)
        
        # Message tracking for at-least-once delivery
        self.processing_messages: Dict[str, Message] = {}  # msg_id -> Message
        
        # Start background tasks
        asyncio.create_task(self._sync_from_redis())
        asyncio.create_task(self._reprocess_stale_messages())
        
        logger.info("📬 Queue Node initialized with consistent hashing")

    def routes(self):
        """Register HTTP routes"""
        return {
            "/queue/push": self.push_handler,
            "/queue/pop": self.pop_handler,
            "/queue/ack": self.ack_handler,
            "/queue/status": self.status_handler,
        }

    # =========================
    # CONSISTENT HASHING
    # =========================
    def _get_responsible_node(self, queue_name: str) -> str:
        """Get the node responsible for this queue"""
        return self.hash_ring.get_node(queue_name)
    
    def _is_responsible_for_queue(self, queue_name: str) -> bool:
        """Check if this node is responsible for the queue"""
        responsible_node = self._get_responsible_node(queue_name)
        return self.node.node_id in responsible_node if responsible_node else False

    # =========================
    # REDIS PERSISTENCE
    # =========================
    async def _save_message_to_redis(self, message: Message):
        """Save message to Redis for persistence"""
        try:
            redis = await get_redis()
            if redis:
                # Store message details
                key = f"queue:msg:{message.msg_id}"
                await redis.setex(key, 3600, json.dumps(message.to_dict()))  # 1 hour TTL
                
                # Add to queue list
                queue_key = f"queue:list:{message.queue}"
                await redis.rpush(queue_key, message.msg_id)
                
                logger.trace(f"Saved message to Redis: {message.msg_id}")
        except Exception as e:
            logger.warning(f"Failed to save message to Redis: {e}")

    async def _load_queue_from_redis(self, queue_name: str):
        """Load queue messages from Redis"""
        try:
            redis = await get_redis()
            if not redis:
                return
            
            queue_key = f"queue:list:{queue_name}"
            msg_ids = await redis.lrange(queue_key, 0, -1)
            
            messages = []
            for msg_id in msg_ids:
                msg_key = f"queue:msg:{msg_id}"
                data = await redis.get(msg_key)
                if data:
                    message = Message.from_dict(json.loads(data))
                    messages.append(message)
            
            if messages:
                self.queues[queue_name] = messages
                logger.info(f"Loaded {len(messages)} messages from Redis for queue {queue_name}")
        except Exception as e:
            logger.warning(f"Failed to load queue from Redis: {e}")

    async def _delete_message_from_redis(self, message: Message):
        """Delete message from Redis"""
        try:
            redis = await get_redis()
            if redis:
                # Delete message
                msg_key = f"queue:msg:{message.msg_id}"
                await redis.delete(msg_key)
                
                # Remove from queue list
                queue_key = f"queue:list:{message.queue}"
                await redis.lrem(queue_key, 0, message.msg_id)
                
                logger.trace(f"Deleted message from Redis: {message.msg_id}")
        except Exception as e:
            logger.warning(f"Failed to delete message from Redis: {e}")

    # =========================
    # BACKGROUND TASKS
    # =========================
    async def _sync_from_redis(self):
        """Periodic sync from Redis on startup"""
        await asyncio.sleep(2)  # Wait for node to fully start
        
        try:
            redis = await get_redis()
            if not redis:
                return
            
            # Find all queue lists
            pattern = "queue:list:*"
            keys = await redis.keys(pattern)
            
            for key in keys:
                queue_name = key.replace("queue:list:", "")
                if self._is_responsible_for_queue(queue_name):
                    await self._load_queue_from_redis(queue_name)
        except Exception as e:
            logger.error(f"Failed to sync from Redis: {e}")

    async def _reprocess_stale_messages(self):
        """Reprocess messages that are stuck in 'processing' state"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                stale_timeout = 300  # 5 minutes
                now = time.time()
                
                for msg_id, message in list(self.processing_messages.items()):
                    if now - message.timestamp > stale_timeout:
                        logger.warning(f"⚠️  Reprocessing stale message: {msg_id}")
                        
                        # Move back to queue
                        queue_name = message.queue
                        message.status = "pending"
                        message.retry_count += 1
                        
                        if message.retry_count > 3:
                            logger.error(f"❌ Message failed after 3 retries: {msg_id}")
                            message.status = "failed"
                            await self._delete_message_from_redis(message)
                            del self.processing_messages[msg_id]
                        else:
                            if queue_name not in self.queues:
                                self.queues[queue_name] = []
                            self.queues[queue_name].append(message)
                            await self._save_message_to_redis(message)
                            del self.processing_messages[msg_id]
                
            except Exception as e:
                logger.error(f"Error in stale message reprocessing: {e}")

    # =========================
    # QUEUE OPERATIONS
    # =========================
    async def _push_message(self, queue_name: str, data: any) -> Message:
        """Internal push operation"""
        msg_id = str(uuid.uuid4())
        message = Message(msg_id, data, queue_name)
        
        if queue_name not in self.queues:
            self.queues[queue_name] = []
        
        # Check queue size limit
        if len(self.queues[queue_name]) >= QUEUE_MAX_SIZE:
            raise Exception(f"Queue {queue_name} is full (max: {QUEUE_MAX_SIZE})")
        
        self.queues[queue_name].append(message)
        await self._save_message_to_redis(message)
        
        self.node.metrics.record_queue_push(queue_name)
        logger.info(f"📨 Message pushed to {queue_name}: {msg_id}")
        
        return message

    async def _pop_message(self, queue_name: str) -> Optional[Message]:
        """Internal pop operation"""
        if queue_name not in self.queues or not self.queues[queue_name]:
            return None
        
        # Pop from front (FIFO)
        message = self.queues[queue_name].pop(0)
        message.status = "processing"
        message.timestamp = time.time()
        
        # Track for at-least-once delivery
        self.processing_messages[message.msg_id] = message
        await self._save_message_to_redis(message)
        
        self.node.metrics.record_queue_pop(queue_name)
        logger.info(f"📤 Message popped from {queue_name}: {message.msg_id}")
        
        return message

    async def _ack_message(self, msg_id: str) -> bool:
        """Acknowledge message processing completion"""
        if msg_id not in self.processing_messages:
            return False
        
        message = self.processing_messages[msg_id]
        message.status = "completed"
        
        # Remove from tracking
        del self.processing_messages[msg_id]
        await self._delete_message_from_redis(message)
        
        logger.info(f"✅ Message acknowledged: {msg_id}")
        return True

    # =========================
    # HTTP HANDLERS
    # =========================
    async def push_handler(self, request):
        """
        Push message to queue
        
        ---
        summary: Push message to queue
        tags:
          - Queue
        requestBody:
          content:
            application/json:
              schema:
                type: object
                properties:
                  queue:
                    type: string
                    default: default
                  message:
                    type: object
                required:
                  - message
        responses:
          '200':
            description: Message pushed successfully
          '503':
            description: Queue full or unavailable
        """
        body = await request.json()
        queue_name = body.get("queue", "default")
        message_data = body.get("message")

        if not message_data:
            return web.json_response({"error": "message is required"}, status=400)

        # Check which node is responsible using consistent hashing
        responsible_node = self._get_responsible_node(queue_name)
        
        if not self.node.node_id in responsible_node:
            # Forward to responsible node
            logger.debug(f"Forwarding queue push to {responsible_node}")
            try:
                result = await send_json(responsible_node, "/queue/push", body, timeout=5.0)
                if result:
                    return web.json_response(result)
                else:
                    return web.json_response({"error": "failed to forward to responsible node"}, status=503)
            except Exception as e:
                logger.error(f"Failed to forward queue push: {e}")
                return web.json_response({"error": str(e)}, status=503)

        # This node is responsible - process locally
        try:
            message = await self._push_message(queue_name, message_data)
            return web.json_response({
                "status": "queued",
                "queue": queue_name,
                "msg_id": message.msg_id,
                "size": len(self.queues[queue_name])
            })
        except Exception as e:
            logger.error(f"Failed to push message: {e}")
            return web.json_response({"error": str(e)}, status=503)

    async def pop_handler(self, request):
        """
        Pop message from queue
        
        ---
        summary: Pop message from queue
        tags:
          - Queue
        requestBody:
          content:
            application/json:
              schema:
                type: object
                properties:
                  queue:
                    type: string
                    default: default
        responses:
          '200':
            description: Message popped or queue empty
        """
        body = await request.json()
        queue_name = body.get("queue", "default")

        # Check which node is responsible
        responsible_node = self._get_responsible_node(queue_name)
        
        if not self.node.node_id in responsible_node:
            # Forward to responsible node
            try:
                result = await send_json(responsible_node, "/queue/pop", body, timeout=5.0)
                if result:
                    return web.json_response(result)
            except Exception as e:
                logger.error(f"Failed to forward queue pop: {e}")
                return web.json_response({"error": str(e)}, status=503)

        # This node is responsible
        message = await self._pop_message(queue_name)
        
        if message:
            return web.json_response({
                "msg_id": message.msg_id,
                "message": message.data,
                "queue": queue_name,
                "retry_count": message.retry_count,
                "note": "Remember to ACK this message after processing"
            })
        else:
            return web.json_response({
                "message": None,
                "queue": queue_name,
                "status": "empty"
            })

    async def ack_handler(self, request):
        """
        Acknowledge message processing
        
        ---
        summary: Acknowledge message
        tags:
          - Queue
        requestBody:
          content:
            application/json:
              schema:
                type: object
                properties:
                  msg_id:
                    type: string
                required:
                  - msg_id
        responses:
          '200':
            description: Message acknowledged
          '404':
            description: Message not found
        """
        body = await request.json()
        msg_id = body.get("msg_id")

        if not msg_id:
            return web.json_response({"error": "msg_id is required"}, status=400)

        success = await self._ack_message(msg_id)
        
        if success:
            return web.json_response({
                "status": "acknowledged",
                "msg_id": msg_id
            })
        else:
            return web.json_response({
                "status": "not_found",
                "msg_id": msg_id,
                "message": "Message not found in processing queue"
            }, status=404)

    async def status_handler(self, request):
        """
        Get queue status
        
        ---
        summary: Get queue status
        tags:
          - Queue
        responses:
          '200':
            description: Queue statistics
        """
        queue_stats = {}
        for queue_name, messages in self.queues.items():
            queue_stats[queue_name] = {
                "size": len(messages),
                "oldest_message_age": time.time() - messages[0].timestamp if messages else 0,
                "is_responsible": self._is_responsible_for_queue(queue_name)
            }
        
        return web.json_response({
            "node_id": self.node.node_id,
            "total_queues": len(self.queues),
            "total_messages": sum(len(msgs) for msgs in self.queues.values()),
            "processing_messages": len(self.processing_messages),
            "queues": queue_stats,
            "hash_ring_nodes": len(set(self.hash_ring.ring.values())),
            "consistent_hashing": {
                "virtual_nodes": self.hash_ring.virtual_nodes,
                "total_slots": len(self.hash_ring.ring)
            }
        })