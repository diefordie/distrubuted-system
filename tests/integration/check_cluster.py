#!/usr/bin/env python3
"""
Quick script to check cluster status before running integration tests
"""
import asyncio
import aiohttp

NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003"
]

async def check_cluster():
    print("🔍 Checking cluster status...\n")
    
    async with aiohttp.ClientSession() as session:
        for node_addr in NODES:
            try:
                # Health check
                async with session.get(f"{node_addr}/health", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        print(f"✅ {node_addr} - HEALTHY")
                    else:
                        print(f"⚠️  {node_addr} - Unhealthy (status {resp.status})")
                        continue
                
                # Cluster status
                async with session.get(f"{node_addr}/cluster/status", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    data = await resp.json()
                    raft = data.get("raft", {})
                    print(f"   State: {raft.get('state')} | Term: {raft.get('term')} | Leader: {raft.get('leader_id', 'none')}")
                    print(f"   Log entries: {raft.get('log_length', 0)} | Commit index: {raft.get('commit_index', 0)}")
                    
            except asyncio.TimeoutError:
                print(f"❌ {node_addr} - TIMEOUT (not responding)")
            except aiohttp.ClientError as e:
                print(f"❌ {node_addr} - CONNECTION ERROR: {e}")
            except Exception as e:
                print(f"❌ {node_addr} - ERROR: {e}")
            
            print()
    
    print("=" * 60)
    print("If no leader is elected:")
    print("1. Check CLUSTER_ADDRS in .env matches running nodes")
    print("2. Check election timeout settings (should be > 3s)")
    print("3. Look at node logs for errors")
    print("4. Try restarting all nodes together")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(check_cluster())