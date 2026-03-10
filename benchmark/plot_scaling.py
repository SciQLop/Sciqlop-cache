#!/usr/bin/env python3
"""Generate scaling charts from benchmark CSV output.

Usage:
    # Summary line chart (from default CSV):
    python benchmark/plot_scaling.py results.csv -o benchmark/scaling_chart.png

    # Violin plot (from --raw CSV):
    python benchmark/plot_scaling.py raw_results.csv --violin -o benchmark/scaling_violin.png
"""

import argparse
import csv
import sys
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def read_summary_csv(path):
    with open(path) as f:
        rows = list(csv.DictReader(f))
    return {
        "entries": [int(r["entries"]) for r in rows],
        "set_us": [float(r["set_us"]) for r in rows],
        "get_hit_us": [float(r["get_hit_us"]) for r in rows],
        "get_miss_us": [float(r["get_miss_us"]) for r in rows],
        "db_size_mb": [float(r["db_size_mb"]) for r in rows],
    }


def read_raw_csv(path):
    by_op = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        for row in csv.DictReader(f):
            by_op[row["operation"]][int(row["entries"])].append(float(row["latency_us"]))
    return by_op


def fmt_entries(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.0f}M" if n % 1_000_000 == 0 else f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K" if n % 1_000 == 0 else f"{n / 1_000:.1f}K"
    return str(n)


def plot_line(data, output):
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


def plot_violin(by_op, output):
    ops = [
        ("set", "#e74c3c", "set()"),
        ("get_hit", "#2ecc71", "get() hit"),
        ("get_miss", "#3498db", "get() miss"),
    ]

    fig, axes = plt.subplots(len(ops), 1, figsize=(12, 9), sharex=True)
    fig.suptitle("Sciqlop-cache Latency Distribution vs Cache Size", fontsize=14)

    for ax, (op, color, label) in zip(axes, ops):
        sizes = sorted(by_op[op].keys())
        data_list = [by_op[op][s] for s in sizes]
        positions = np.arange(len(sizes))

        parts = ax.violinplot(data_list, positions=positions, showmedians=True,
                              showextrema=False, widths=0.8)
        for pc in parts["bodies"]:
            pc.set_facecolor(color)
            pc.set_alpha(0.6)
        parts["cmedians"].set_color("black")

        all_vals = [v for d in data_list for v in d]
        ax.set_ylim(0, np.percentile(all_vals, 99) * 1.2)

        ax.set_ylabel(f"{label} (\u00b5s)")
        ax.set_xticks(positions)
        ax.set_xticklabels([fmt_entries(s) for s in sizes], rotation=45, ha="right")
        ax.grid(True, axis="y", alpha=0.3)

    axes[-1].set_xlabel("Cache entries")
    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file", help="CSV output from scaling.py")
    parser.add_argument("-o", "--output", default="benchmark/scaling_chart.png")
    parser.add_argument("--violin", action="store_true",
                        help="generate violin plot (requires --raw CSV)")
    args = parser.parse_args()

    if args.violin:
        by_op = read_raw_csv(args.csv_file)
        plot_violin(by_op, args.output)
    else:
        data = read_summary_csv(args.csv_file)
        plot_line(data, args.output)


if __name__ == "__main__":
    main()
