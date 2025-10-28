
import asyncio
import signal
import sys
from loguru import logger


import sys, os
print("Current working dir:", os.getcwd())
print("Python path:", sys.path)


from src.nodes.base_node import BaseNode
from src.utils.config import NODE_ID, NODE_PORT, print_config, DEBUG_MODE



node_instance = None


def signal_handler(signum, frame):
    logger.warning(f"🛑 Received signal {signum}, initiating graceful shutdown...")
    if node_instance:
        
        asyncio.create_task(shutdown())


async def shutdown():
    global node_instance
    if node_instance:
        await node_instance.stop()
        node_instance = None
    
    
    await asyncio.sleep(0.5)
    
    logger.info("👋 Goodbye!")
    sys.exit(0)


async def main():
    global node_instance
    
    
    if DEBUG_MODE:
        print_config()
    
    logger.info("=" * 60)
    logger.info("🚀 DISTRIBUTED SYNCHRONIZATION SYSTEM")
    logger.info("=" * 60)
    
    
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
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown complete")
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        sys.exit(1)
