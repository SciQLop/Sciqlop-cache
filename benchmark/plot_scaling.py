#!/usr/bin/env python3
"""Generate scaling chart from benchmark CSV output.

Usage:
    python benchmark/scaling.py --max-entries 1000000 > results.csv
    python benchmark/plot_scaling.py results.csv -o benchmark/scaling_chart.svg
"""

import argparse
import csv
import sys

import matplotlib.pyplot as plt


def read_csv(path):
    with open(path) as f:
        rows = list(csv.DictReader(f))
    return {
        "entries": [int(r["entries"]) for r in rows],
        "set_us": [float(r["set_us"]) for r in rows],
        "get_hit_us": [float(r["get_hit_us"]) for r in rows],
        "get_miss_us": [float(r["get_miss_us"]) for r in rows],
        "db_size_mb": [float(r["db_size_mb"]) for r in rows],
    }


def plot(data, output):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True,
                                    gridspec_kw={"height_ratios": [3, 1]})
    entries = data["entries"]

    ax1.semilogx(entries, data["set_us"], "o-", color="#e74c3c", label="set()", markersize=4)
    ax1.semilogx(entries, data["get_hit_us"], "s-", color="#2ecc71", label="get() hit", markersize=4)
    ax1.semilogx(entries, data["get_miss_us"], "^-", color="#3498db", label="get() miss", markersize=4)
    ax1.set_ylabel("Latency (\u00b5s / op)")
    ax1.set_title("Sciqlop-cache Scaling: Latency vs Cache Size")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2.semilogx(entries, data["db_size_mb"], "D-", color="#9b59b6", markersize=4)
    ax2.set_xlabel("Cache entries")
    ax2.set_ylabel("DB size (MB)")
    ax2.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file", help="CSV output from scaling.py")
    parser.add_argument("-o", "--output", default="benchmark/scaling_chart.svg")
    args = parser.parse_args()

    data = read_csv(args.csv_file)
    plot(data, args.output)


if __name__ == "__main__":
    main()
