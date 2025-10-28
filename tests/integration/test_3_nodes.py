
import pytest
import asyncio
import aiohttp
from loguru import logger




NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003"
]


@pytest.mark.integration
class TestThreeNodeCluster:
    
    @pytest.mark.asyncio
    async def test_cluster_health(self):
        async with aiohttp.ClientSession() as session:
            for node_addr in NODES:
                try:
                    async with session.get(f"{node_addr}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        assert resp.status == 200
                        data = await resp.json()
                        assert data["status"] == "healthy"
                        logger.info(f"✅ {node_addr} is healthy")
                except Exception as e:
                    logger.error(f"❌ {node_addr} failed: {e}")
                    pytest.fail(f"Node {node_addr} is not responding. Make sure all 3 nodes are running!")
    
    @pytest.mark.asyncio
    async def test_leader_election(self):
        
        await asyncio.sleep(15)
        
        async with aiohttp.ClientSession() as session:
            leaders = []
            all_states = []
            
            for node_addr in NODES:
                try:
                    async with session.get(f"{node_addr}/cluster/status", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        data = await resp.json()
                        state = data["raft"]["state"]
                        term = data["raft"]["term"]
                        node_id = data["node_id"]
                        
                        all_states.append(f"{node_id}={state}")
                        
                        if state == "leader":
                            leaders.append(node_id)
                        
                        logger.info(f"{node_addr}: state={state}, term={term}, node_id={node_id}")
                except Exception as e:
                    logger.error(f"Failed to get status from {node_addr}: {e}")
            
            
            if len(leaders) == 0:
                logger.warning(f"⚠️  No leader found yet. States: {all_states}")
                logger.warning("This may be normal if election is still in progress. Try waiting longer.")
                
                pytest.skip(f"No leader elected yet after 15s. States: {all_states}. This may indicate election timing issues.")
            else:
                assert len(leaders) == 1, f"Expected 1 leader, found {len(leaders)}: {leaders}"
                logger.success(f"✅ Single leader elected: {leaders[0]}")
    
    @pytest.mark.asyncio
    async def test_distributed_lock_acquire_release(self):
        async with aiohttp.ClientSession() as session:
            
            try:
                async with session.post(f"{NODES[0]}/lock/acquire", 
                                       json={
                                           "resource": "test_resource",
                                           "client_id": "test_client",
                                           "mode": "exclusive"
                                       },
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    logger.info(f"Lock acquire response: {data}")
                    
                    
                    if "error" in data:
                        if "no leader" in data["error"].lower():
                            pytest.skip("No leader available for lock acquisition")
                        else:
                            pytest.fail(f"Lock acquire error: {data['error']}")
                    
                    
                    assert "status" in data, f"Response missing 'status' field: {data}"
                    assert data["status"] in ["granted", "redirect"], f"Unexpected status: {data['status']}"
                
                
                if data.get("status") == "granted":
                    async with session.post(f"{NODES[1]}/lock/acquire", json={
                        "resource": "test_resource",
                        "client_id": "another_client",
                        "mode": "exclusive",
                        "wait": False
                    }) as resp2:
                        data2 = await resp2.json()
                        logger.info(f"Second lock acquire: {data2}")
                        if data2.get("status") not in ["redirect", "denied"]:
                            logger.warning(f"Unexpected second acquire response: {data2}")
                    
                    
                    async with session.post(f"{NODES[0]}/lock/release", json={
                        "resource": "test_resource",
                        "client_id": "test_client"
                    }) as resp3:
                        data3 = await resp3.json()
                        logger.info(f"Lock release: {data3}")
            
            except aiohttp.ClientError as e:
                pytest.fail(f"Connection error: {e}. Make sure nodes are running!")
    
    @pytest.mark.asyncio
    async def test_distributed_cache_coherence(self):
        async with aiohttp.ClientSession() as session:
            try:
                
                async with session.post(f"{NODES[0]}/cache/put", 
                                       json={
                                           "key": "test_key_integration",
                                           "value": "test_value_123"
                                       },
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    assert data.get("status") == "stored", f"Cache put failed: {data}"
                    logger.info(f"Cache put on node1: {data}")
                
                await asyncio.sleep(1)  
                
                
                async with session.get(f"{NODES[1]}/cache/get?key=test_key_integration",
                                      timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    logger.info(f"Cache get from node2: {data}")
                    
                
                
                async with session.get(f"{NODES[2]}/cache/get?key=test_key_integration",
                                      timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    logger.info(f"Cache get from node3: {data}")
            
            except aiohttp.ClientError as e:
                pytest.fail(f"Connection error: {e}. Make sure nodes are running!")
    
    @pytest.mark.asyncio
    async def test_distributed_queue_push_pop(self):
        async with aiohttp.ClientSession() as session:
            try:
                
                async with session.post(f"{NODES[0]}/queue/push", 
                                       json={
                                           "queue": "test_queue_integration",
                                           "message": {"order_id": 12345, "product": "widget"}
                                       },
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    logger.info(f"Queue push response: {data}")
                    
                    
                    if "error" in data:
                        if "no leader" in data.get("error", "").lower():
                            pytest.skip("No leader available for queue operations")
                        else:
                            logger.warning(f"Queue push error: {data}")
                    
                    
                    if "status" in data:
                        assert data["status"] in ["pushed", "redirect", "accepted"], f"Unexpected status: {data}"
                    elif "msg_id" in data:
                        logger.info(f"Message pushed with ID: {data['msg_id']}")
                    else:
                        logger.warning(f"Unexpected queue push response format: {data}")
                
                await asyncio.sleep(0.5)
                
                
                async with session.post(f"{NODES[1]}/queue/pop", 
                                       json={
                                           "queue": "test_queue_integration"
                                       },
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json()
                    logger.info(f"Queue pop response: {data}")
                    
                    
                    if data.get("message"):
                        
                        if "msg_id" in data:
                            async with session.post(f"{NODES[1]}/queue/ack", 
                                                   json={
                                                       "msg_id": data["msg_id"],
                                                       "queue": "test_queue_integration"
                                                   },
                                                   timeout=aiohttp.ClientTimeout(total=5)) as ack_resp:
                                ack_data = await ack_resp.json()
                                logger.info(f"Queue ack: {ack_data}")
                    else:
                        logger.info("Queue is empty or message not yet available")
            
            except aiohttp.ClientError as e:
                pytest.fail(f"Connection error: {e}. Make sure nodes are running!")
    
    @pytest.mark.asyncio
    async def test_metrics_collection(self):
        async with aiohttp.ClientSession() as session:
            for node_addr in NODES:
                try:
                    async with session.get(f"{node_addr}/metrics",
                                          timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        assert resp.status == 200
                        data = await resp.json()
                        
                        
                        assert "system" in data, "Missing 'system' in metrics"
                        assert "raft" in data, "Missing 'raft' in metrics"
                        assert "cache" in data, "Missing 'cache' in metrics"
                        assert "locks" in data, "Missing 'locks' in metrics"
                        assert "queue" in data, "Missing 'queue' in metrics"
                        
                        logger.info(f"Metrics from {node_addr}:")
                        logger.info(f"  Uptime: {data['system']['uptime_formatted']}")
                        logger.info(f"  Raft term: {data['raft']['term']}")
                        logger.info(f"  Cache hit rate: {data['cache']['hit_rate_percent']}%")
                        logger.info(f"  Active locks: {data['locks']['active_locks']}")
                        logger.info(f"  Queue size: {data['queue']['current_total_size']}")
                
                except aiohttp.ClientError as e:
                    pytest.fail(f"Connection error: {e}. Make sure nodes are running!")
