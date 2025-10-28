# src/main.py
import asyncio
import signal
import sys
from loguru import logger

import sys, os
print("Current working dir:", os.getcwd())
print("Python path:", sys.path)


from src.nodes.base_node import BaseNode
from src.utils.config import NODE_ID, NODE_PORT, print_config, DEBUG_MODE


# Global reference for graceful shutdown
node_instance = None


def signal_handler(signum, frame):
    """Handle shutdown signals (SIGINT, SIGTERM)"""
    logger.warning(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    if node_instance:
        # Create task for async shutdown
        asyncio.create_task(shutdown())


async def shutdown():
    """Gracefully shutdown the node"""
    global node_instance
    if node_instance:
        await node_instance.stop()
        node_instance = None
    
    # Give some time for cleanup
    await asyncio.sleep(0.5)
    
    logger.info("👋 Goodbye!")
    sys.exit(0)


async def main():
    """Main entry point for the distributed node"""
    global node_instance
    
    # Print configuration if in debug mode
    if DEBUG_MODE:
        print_config()
    
    logger.info("=" * 60)
    logger.info("🚀 DISTRIBUTED SYNCHRONIZATION SYSTEM")
    logger.info("=" * 60)
    
    # Create and start node
    node = BaseNode()
    node_instance = node
    
    try:
        await node.start()
        logger.info(f"✅ Node {node.node_id} is running on port {node.port}")
        logger.info(f"📊 Metrics: http://localhost:{node.port}/metrics")
        logger.info(f"📚 Swagger UI: http://localhost:{node.port}/docs")
        logger.info(f"🏥 Health: http://localhost:{node.port}/health")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(3600)
            
    except KeyboardInterrupt:
        logger.warning("🛑 Keyboard interrupt received")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
    finally:
        await node.stop()


if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the main async function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        sys.exit(1)