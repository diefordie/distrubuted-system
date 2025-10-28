
import csv
import os

RESULTS_DIR = "tests/benchmark/results"
os.makedirs(RESULTS_DIR, exist_ok=True)

def log_result(scenario, throughput, latency):
    filepath = os.path.join(RESULTS_DIR, "benchmark_results.csv")
    exists = os.path.exists(filepath)
    with open(filepath, "a", newline="") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["Scenario", "Throughput (req/s)", "Avg Latency (s)"])
        writer.writerow([scenario, throughput, latency])

def summarize_results(results):
    print("\n📊 Summary:")
    print("=" * 50)
    for r in results:
        print(f"{r['scenario']:<35} | "
              f"Throughput: {r['throughput']:.2f} req/s | "
              f"Latency: {r['latency']:.4f}s")
