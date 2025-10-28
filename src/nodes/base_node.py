# src/nodes/base_node.py
import asyncio
import time
from pathlib import Path
from loguru import logger
from aiohttp import web
from aiohttp_swagger3 import SwaggerFile, SwaggerUiSettings

from src.communication.message_passing import send_json
from src.communication.failure_detector import AdaptiveFailureDetector
from src.consensus.raft import RaftNode
from src.nodes.lock_manager import LockManager
from src.nodes.cache_node import CacheNode
from src.nodes.queue_node import QueueNode
from src.utils.config import (
    NODE_ID, NODE_PORT, NODE_HOST, CLUSTER_ADDRS,
    ENABLE_SWAGGER, DEBUG_MODE
)
from src.utils.metrics import MetricsCollector
from src.utils.redis_client import get_redis, close_redis, check_redis_health


class BaseNode:
    """
    Base node class that integrates all distributed system components:
    - Raft consensus
    - Failure detection
    - Distributed locks
    - Distributed cache
    - Distributed queue
    """

    def __init__(self):
        self.node_id = NODE_ID
        self.port = NODE_PORT
        self.host = NODE_HOST
        self.http_runner = None
        self.app = web.Application()

        # Core components
        self.raft = RaftNode(self.node_id, CLUSTER_ADDRS)
        self.failure_detector = AdaptiveFailureDetector(self)
        self.metrics = MetricsCollector()

        # Distributed data structures
        self.lock_manager = LockManager(self)
        self.cache_node = CacheNode(self)
        self.queue_node = QueueNode(self)

        # Middleware for request tracking
        self.app.middlewares.append(self._request_middleware)

        logger.info(f"🚀 Initializing node {self.node_id}")

    # =========================
    # MIDDLEWARE
    # =========================
    @web.middleware
    async def _request_middleware(self, request, handler):
        """Middleware to track request metrics"""
        start_time = time.time()

        try:
            response = await handler(request)
            latency = time.time() - start_time
            self.metrics.record_request(request.path, latency)

            # Add custom headers
            response.headers['X-Node-Id'] = self.node_id
            response.headers['X-Raft-Term'] = str(self.raft.current_term)
            response.headers['X-Raft-State'] = self.raft.state
            if self.raft.leader_id:
                response.headers['X-Leader-Id'] = self.raft.leader_id

            return response
        except Exception as e:
            logger.error(f"Request error on {request.path}: {e}")
            raise

    # =========================
    # STARTUP & SHUTDOWN
    # =========================
    async def start(self):
        """Start HTTP server and all distributed system components"""

        # Check Redis connection
        redis_healthy = await check_redis_health()
        if not redis_healthy:
            logger.warning("⚠️  Redis is not available. Some features may not work.")

        # Add base routes
        self.app.add_routes([
            web.get("/", self.root_handler),
            web.get("/health", self.health_handler),
            web.post("/ping", self.ping_handler),
            web.get("/cluster/status", self.cluster_status),
            web.get("/metrics", self.metrics_handler),
        ])

        # Add Raft routes
        for path, handler in self.raft.routes().items():
            self.app.router.add_post(path, handler)
            logger.debug(f"Registered Raft route: POST {path}")

        # Add Lock Manager routes
        for path, handler in self.lock_manager.routes().items():
            self.app.router.add_post(path, handler)
            logger.debug(f"Registered Lock route: POST {path}")

        # Add Cache routes
        for path, handler in self.cache_node.routes().items():
            if path.endswith("/get") or path.endswith("/status"):
                self.app.router.add_get(path, handler)
            else:
                self.app.router.add_post(path, handler)
            logger.debug(f"Registered Cache route: {path}")

        # Add Queue routes
        for path, handler in self.queue_node.routes().items():
            if path.endswith("/status"):
                self.app.router.add_get(path, handler)
            else:
                self.app.router.add_post(path, handler)
            logger.debug(f"Registered Queue route: {path}")

        # ---------- Swagger static spec ----------
        if ENABLE_SWAGGER:
            spec_path = Path(__file__).parent.parent / "api" / "openapi_spec.yaml"
            # serve folder-nya biar bisa diakui sebagai /spec/openapi_spec.yaml
            self.app.router.add_static("/spec", spec_path.parent, name="spec")

            SwaggerFile(
                self.app,
                swagger_ui_settings=SwaggerUiSettings(path="/docs"),
                spec_file=str(spec_path),
                validate=False
            )
            logger.success(f"📚 Swagger UI enabled at http://{self.host}:{self.port}/docs")
        # -----------------------------------------

        # Store node_id in app for handlers
        self.app["node_id"] = self.node_id
        self.app["node"] = self

        # Start HTTP server
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        self.http_runner = runner
        logger.success(f"🌐 HTTP server running on {self.host}:{self.port}")

        # Start Raft consensus
        await self.raft.start()

        # Link failure detector to Raft (for cross-references)
        self.raft.failure_detector = self.failure_detector

        # Start failure detector
        await self.failure_detector.start()

        logger.success(f"✅ Node {self.node_id} fully operational")

    async def stop(self):
        """Gracefully stop all components"""
        logger.warning(f"🛑 Stopping node {self.node_id}...")

        # Stop Raft consensus
        await self.raft.stop()

        # Stop failure detector
        await self.failure_detector.stop()

        # Close HTTP server
        if self.http_runner:
            await self.http_runner.cleanup()

        # Close Redis connection
        await close_redis()

        # Log final metrics
        self.metrics.log_summary()

        logger.success(f"✅ Node {self.node_id} stopped gracefully")

    # =========================
    # BASE HANDLERS
    # =========================
    async def root_handler(self, request):
        """Root endpoint with basic info"""
        return web.json_response({
            "service": "Distributed Synchronization System",
            "node_id": self.node_id,
            "version": "1.0.0",
            "status": "operational",
            "endpoints": {
                "health": "/health",
                "metrics": "/metrics",
                "cluster_status": "/cluster/status",
                "docs": "/docs" if ENABLE_SWAGGER else None,
                "raft": {
                    "request_vote": "/raft/request_vote",
                    "append_entries": "/raft/append_entries",
                    "leader_info": "/raft/leader_info",
                    "commit_log": "/raft/commit_log"
                },
                "locks": {
                    "acquire": "/lock/acquire",
                    "release": "/lock/release",
                    "status": "/lock/status"
                },
                "cache": {
                    "get": "/cache/get",
                    "put": "/cache/put",
                    "invalidate": "/cache/invalidate",
                    "status": "/cache/status"
                },
                "queue": {
                    "push": "/queue/push",
                    "pop": "/queue/pop",
                    "status": "/queue/status"
                }
            }
        })

    async def health_handler(self, request):
        """Health check endpoint"""
        redis_status = "connected" if await check_redis_health() else "disconnected"
        data = {
            "node": self.node_id,
            "status": "healthy",
            "uptime_seconds": self.metrics.uptime(),
            "redis": redis_status,
            "raft": {
                "state": self.raft.state,
                "term": self.raft.current_term,
                "leader_id": self.raft.leader_id,
                "log_length": len(self.raft.log),
                "commit_index": self.raft.commit_index
            },
            "failure_detector": {
                "failed_nodes": list(self.failure_detector.failed_nodes),
                "avg_rtt_ms": round(self.metrics.average_rtt() * 1000, 2)
            },
            "timestamp": time.time()
        }
        return web.json_response(data)

    async def ping_handler(self, request):
        """Ping handler for failure detection"""
        try:
            body = await request.json()
        except Exception:
            body = {}
        return web.json_response({
            "status": "alive",
            "node": self.node_id,
            "term": self.raft.current_term,
            "state": self.raft.state,
            "echo": body,
            "timestamp": time.time()
        })

    async def metrics_handler(self, request):
        """Get comprehensive metrics"""
        return web.json_response(self.metrics.summary())

    async def cluster_status(self, request):
        """Get detailed cluster status"""
        nodes_status = {}
        for addr in self.raft.cluster_addrs:
            if self.node_id in addr:
                nodes_status[addr] = {
                    "status": "self",
                    "state": self.raft.state,
                    "term": self.raft.current_term
                }
            else:
                try:
                    result = await send_json(addr, "/health", {}, timeout=2.0)
                    if result:
                        nodes_status[addr] = {
                            "status": "online",
                            "state": result.get("raft", {}).get("state"),
                            "term": result.get("raft", {}).get("term")
                        }
                    else:
                        nodes_status[addr] = {"status": "offline"}
                except Exception:
                    nodes_status[addr] = {"status": "unreachable"}

        data = {
            "node_id": self.node_id,
            "cluster_size": len(self.raft.cluster_addrs),
            "raft": {
                "term": self.raft.current_term,
                "state": self.raft.state,
                "leader": self.raft.leader_id,
                "log_length": len(self.raft.log),
                "commit_index": self.raft.commit_index
            },
            "nodes": nodes_status,
            "failed_nodes": list(self.failure_detector.failed_nodes),
            "uptime": self.metrics.uptime_formatted(),
            "timestamp": time.time()
        }
        return web.json_response(data)