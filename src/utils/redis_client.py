
import redis.asyncio as redis
from loguru import logger
from src.utils.config import REDIS_URL

_redis = None
_redis_lock = None

async def get_redis():
    global _redis, _redis_lock
    
    if _redis_lock is None:
        import asyncio
        _redis_lock = asyncio.Lock()
    
    async with _redis_lock:
        if _redis is None:
            try:
                _redis = await redis.from_url(
                    REDIS_URL,
                    decode_responses=True,
                    encoding="utf-8",
                    max_connections=10,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    retry_on_timeout=True
                )
                
                await _redis.ping()
                logger.success(f"✅ Redis connected: {REDIS_URL}")
            except redis.ConnectionError as e:
                logger.error(f"❌ Redis connection failed: {e}")
                logger.warning("⚠️  Running without Redis persistence (in-memory only)")
                _redis = None
            except Exception as e:
                logger.error(f"❌ Unexpected Redis error: {e}")
                _redis = None
        
        return _redis

async def close_redis():
    global _redis
    if _redis:
        try:
            await _redis.close()
            logger.info("Redis connection closed")
        except Exception as e:
            logger.error(f"Error closing Redis: {e}")
        finally:
            _redis = None

async def check_redis_health():
    try:
        r = await get_redis()
        if r:
            await r.ping()
            return True
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
    return False



async def redis_set_with_retry(key: str, value: str, ex: int = None, max_retries: int = 3):
    r = await get_redis()
    if not r:
        return False
    
    for attempt in range(max_retries):
        try:
            if ex:
                await r.setex(key, ex, value)
            else:
                await r.set(key, value)
            return True
        except Exception as e:
            logger.warning(f"Redis set failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(0.5 * (attempt + 1))
    return False

async def redis_get_with_retry(key: str, max_retries: int = 3):
    r = await get_redis()
    if not r:
        return None
    
    for attempt in range(max_retries):
        try:
            return await r.get(key)
        except Exception as e:
            logger.warning(f"Redis get failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return None
            await asyncio.sleep(0.5 * (attempt + 1))
    return None

async def redis_delete_with_retry(key: str, max_retries: int = 3):
    r = await get_redis()
    if not r:
        return False
    
    for attempt in range(max_retries):
        try:
            await r.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Redis delete failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(0.5 * (attempt + 1))
    return False
