
import os
from dotenv import load_dotenv
from loguru import logger


load_dotenv()




NODE_ID = os.getenv("NODE_ID", "node1")
NODE_PORT = int(os.getenv("NODE_PORT", "8001"))
NODE_HOST = os.getenv("NODE_HOST", "0.0.0.0")




CLUSTER_ADDRS_STR = os.getenv("CLUSTER_ADDRS", "http://localhost:8001,http://localhost:8002,http://localhost:8003")
CLUSTER_ADDRS = [addr.strip() for addr in CLUSTER_ADDRS_STR.split(",")]




REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)


REDIS_URL = os.getenv("REDIS_URL")  
if not REDIS_URL:
    if REDIS_PASSWORD:
        REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
    else:
        REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"




RAFT_ELECTION_TIMEOUT_MIN = float(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "3.0"))
RAFT_ELECTION_TIMEOUT_MAX = float(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "5.0"))
RAFT_HEARTBEAT_INTERVAL = float(os.getenv("RAFT_HEARTBEAT_INTERVAL", "1.0"))




PING_INTERVAL = float(os.getenv("PING_INTERVAL", "2.0"))
PING_TIMEOUT = float(os.getenv("PING_TIMEOUT", "2.0"))
RTT_WINDOW = int(os.getenv("RTT_WINDOW", "10"))  
ADAPTIVE_K = float(os.getenv("ADAPTIVE_K", "3.0"))  




CACHE_MAX_SIZE = int(os.getenv("CACHE_MAX_SIZE", "1000"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))  




QUEUE_MAX_SIZE = int(os.getenv("QUEUE_MAX_SIZE", "10000"))
QUEUE_VIRTUAL_NODES = int(os.getenv("QUEUE_VIRTUAL_NODES", "100"))  




LOCK_DEFAULT_TTL = int(os.getenv("LOCK_DEFAULT_TTL", "300"))  
LOCK_MAX_WAIT_TIME = int(os.getenv("LOCK_MAX_WAIT_TIME", "60"))  




LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv("LOG_FORMAT", "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")


logger.remove()  
logger.add(
    sink=lambda msg: print(msg, end=""),
    format=LOG_FORMAT,
    level=LOG_LEVEL,
    colorize=True
)


LOG_FILE = os.getenv("LOG_FILE", None)
if LOG_FILE:
    logger.add(
        LOG_FILE,
        rotation="100 MB",
        retention="7 days",
        format=LOG_FORMAT,
        level=LOG_LEVEL
    )




METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"
METRICS_INTERVAL = int(os.getenv("METRICS_INTERVAL", "60"))  




API_TIMEOUT = float(os.getenv("API_TIMEOUT", "5.0"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES", "3"))




DEBUG_MODE = os.getenv("DEBUG", "false").lower() == "true"
ENABLE_SWAGGER = os.getenv("ENABLE_SWAGGER", "true").lower() == "true"





def validate_config():
    issues = []
    
    
    if len(CLUSTER_ADDRS) < 3:
        issues.append("⚠️  Cluster size < 3. Raft requires at least 3 nodes for fault tolerance.")
    
    
    node_addr = f"http://{NODE_HOST}:{NODE_PORT}"
    if not any(NODE_ID in addr for addr in CLUSTER_ADDRS):
        issues.append(f"⚠️  Node address '{node_addr}' not found in CLUSTER_ADDRS. Inter-node communication may fail.")
    
    
    if RAFT_ELECTION_TIMEOUT_MIN <= RAFT_HEARTBEAT_INTERVAL * 2:
        issues.append("⚠️  Election timeout should be > 2x heartbeat interval to avoid split votes.")
    
    if issues:
        logger.warning("Configuration validation issues:")
        for issue in issues:
            logger.warning(issue)
    else:
        logger.success("✅ Configuration validation passed")

def print_config():
    logger.info("=" * 60)
    logger.info("DISTRIBUTED SYSTEM CONFIGURATION")
    logger.info("=" * 60)
    logger.info(f"Node ID:           {NODE_ID}")
    logger.info(f"Node Address:      {NODE_HOST}:{NODE_PORT}")
    logger.info(f"Cluster Size:      {len(CLUSTER_ADDRS)} nodes")
    logger.info(f"Cluster Addresses: {', '.join(CLUSTER_ADDRS)}")
    logger.info(f"Redis:             {REDIS_URL}")
    logger.info(f"Log Level:         {LOG_LEVEL}")
    logger.info(f"Debug Mode:        {DEBUG_MODE}")
    logger.info(f"Swagger Docs:      {ENABLE_SWAGGER}")
    logger.info("=" * 60)


if not os.getenv("TESTING"):
    if DEBUG_MODE:
        print_config()
    validate_config()
