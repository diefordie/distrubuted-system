
import aiohttp
from loguru import logger

async def send_json(target_url: str, endpoint: str, payload: dict, timeout: float = 3.0):
    url = f"{target_url.rstrip('/')}{endpoint}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=timeout) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.debug(f"Sent {endpoint} to {target_url}, response: {data}")
                    return data
                else:
                    logger.warning(f"Request to {url} failed with status {resp.status}")
    except aiohttp.ClientError as e:
        logger.error(f"Connection error to {url}: {e}")
    except asyncio.TimeoutError:
        logger.error(f"Timeout sending to {url}")

    return None
