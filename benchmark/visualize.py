
import matplotlib.pyplot as plt

def plot_results(results):
    scenarios = [r["scenario"] for r in results]
    throughputs = [r["throughput"] for r in results]
    latencies = [r["latency"] for r in results]

    fig, ax1 = plt.subplots()
    ax1.bar(scenarios, throughputs)
    ax1.set_ylabel("Throughput (req/s)")
    ax1.set_title("Benchmark Results")
    ax1.tick_params(axis='x', rotation=30)

    ax2 = ax1.twinx()
    ax2.plot(scenarios, latencies, color="red", marker="o", label="Latency (s)")
    ax2.set_ylabel("Latency (s)")
    fig.tight_layout()
    plt.savefig("tests/benchmark/results/benchmark_plot.png")
    plt.close()
