
import asyncio
import aiohttp
import time
import csv
import os
from statistics import mean
import matplotlib.pyplot as plt
from loguru import logger




NODES = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]
REQUESTS_PER_SCENARIO = 1000
CONCURRENCY = 100
RESULT_DIR = "benchmark/results"

os.makedirs(RESULT_DIR, exist_ok=True)




async def run_requests(session, url, payload=None, method="GET"):
    start = time.time()
    try:
        if method == "POST":
            async with session.post(url, json=payload or {}) as resp:
                await resp.text()
        else:
            async with session.get(url, params=payload or {}) as resp:
                await resp.text()
        latency = time.time() - start
        return latency
    except Exception as e:
        logger.warning(f"Request failed: {e}")
        return None

async def benchmark_scenario(name, node_urls, operation_fn):
    latencies = []
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(REQUESTS_PER_SCENARIO):
            node_url = node_urls[i % len(node_urls)]
            tasks.append(asyncio.create_task(operation_fn(session, node_url, i)))

            
            if len(tasks) >= CONCURRENCY:
                results = await asyncio.gather(*tasks)
                latencies.extend([r for r in results if r is not None])
                tasks = []

        
        if tasks:
            results = await asyncio.gather(*tasks)
            latencies.extend([r for r in results if r is not None])

    duration = time.time() - start_time
    throughput = len(latencies) / duration
    avg_latency = mean(latencies) if latencies else 0

    logger.success(f"[{name}] Throughput: {throughput:.2f} req/s | Avg Latency: {avg_latency:.4f}s")
    return {
        "scenario": name,
        "requests": len(latencies),
        "throughput": throughput,
        "avg_latency": avg_latency,
    }




async def cache_read_scenario(session, node_url, i):
    key = f"key{i % 50}"
    return await run_requests(session, f"{node_url}/cache/get", {"key": key}, "GET")

async def cache_write_scenario(session, node_url, i):
    key = f"key{i}"
    value = f"value{i}"
    return await run_requests(session, f"{node_url}/cache/put", {"key": key, "value": value}, "POST")

async def queue_scenario(session, node_url, i):
    if i % 2 == 0:
        return await run_requests(session, f"{node_url}/queue/push", {"value": f"item{i}"}, "POST")
    else:
        return await run_requests(session, f"{node_url}/queue/pop", {}, "POST")

async def lock_scenario(session, node_url, i):
    key = "stress_lock"
    if i % 2 == 0:
        return await run_requests(session, f"{node_url}/lock/acquire", {"key": key, "ttl": 5}, "POST")
    else:
        return await run_requests(session, f"{node_url}/lock/release", {"key": key}, "POST")

async def health_check_scenario(session, node_url, i):
    return await run_requests(session, f"{node_url}/health", {}, "GET")




async def main():
    logger.info("🚀 Starting Stress Benchmark...")
    scenarios = [
        ("Single Node Baseline", [NODES[0]], health_check_scenario),
        ("Distributed Cluster", NODES, health_check_scenario),
        ("Cache Heavy Writes", NODES, cache_write_scenario),
        ("Cache Reads Mix", NODES, cache_read_scenario),
        ("Queue High Load", NODES, queue_scenario),
        ("Lock Contention", NODES, lock_scenario),
    ]

    results = []
    for name, node_list, fn in scenarios:
        res = await benchmark_scenario(name, node_list, fn)
        results.append(res)

    
    csv_path = os.path.join(RESULT_DIR, "stress_results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["scenario", "requests", "throughput", "avg_latency"])
        writer.writeheader()
        writer.writerows(results)

    
    plt.figure(figsize=(10, 6))
    scenarios = [r["scenario"] for r in results]
    throughput = [r["throughput"] for r in results]
    latency = [r["avg_latency"] for r in results]

    plt.subplot(2, 1, 1)
    plt.barh(scenarios, throughput)
    plt.xlabel("Requests per second")
    plt.title("Throughput Comparison")

    plt.subplot(2, 1, 2)
    plt.barh(scenarios, latency)
    plt.xlabel("Average Latency (s)")
    plt.title("Average Latency Comparison")

    plt.tight_layout()
    plot_path = os.path.join(RESULT_DIR, "stress_plot.png")
    plt.savefig(plot_path)

    logger.success(f"✅ Benchmark complete! Results saved to:\n  - {csv_path}\n  - {plot_path}")

if __name__ == "__main__":
    asyncio.run(main())
