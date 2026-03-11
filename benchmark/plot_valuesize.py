#!/usr/bin/env python3
"""Plot value-size and batch benchmarks from bench_valuesize.py CSV output.

Usage:
    python benchmark/plot_valuesize.py benchmark/valuesize_results.csv
"""

import argparse
import csv
import sys
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np


def read_csv(path):
    rows = []
    with open(path) as f:
        for row in csv.DictReader(f):
            rows.append({
                "backend": row["backend"],
                "operation": row["operation"],
                "value_size": int(row["value_size"]),
                "batch_size": int(row["batch_size"]),
                "latency_us": float(row["latency_us"]),
            })
    return rows


def fmt_size(n):
    if n >= 1_048_576:
        return f"{n / 1_048_576:.0f}MB"
    if n >= 1024:
        return f"{n / 1024:.0f}KB"
    return f"{n}B"


COLORS = {"sciqlop": "#e74c3c", "diskcache": "#3498db"}

# Blob-to-file storage thresholds
SCIQLOP_FILE_THRESHOLD = 8192       # 8 KB
DISKCACHE_FILE_THRESHOLD = 32768    # 32 KB


def _storage_mode(backend, value_size):
    threshold = SCIQLOP_FILE_THRESHOLD if backend == "sciqlop" else DISKCACHE_FILE_THRESHOLD
    return "file" if value_size > threshold else "blob"


def _annotate_thresholds(ax):
    """Add vertical lines showing where each backend switches from blob to file storage."""
    ax.axvline(SCIQLOP_FILE_THRESHOLD, color=COLORS["sciqlop"], linestyle=":",
               alpha=0.6, linewidth=1.5)
    ax.axvline(DISKCACHE_FILE_THRESHOLD, color=COLORS["diskcache"], linestyle=":",
               alpha=0.6, linewidth=1.5)
    ymin, ymax = ax.get_ylim()
    label_y = ymax * 0.85
    ax.annotate("sciqlop\nblob→file", xy=(SCIQLOP_FILE_THRESHOLD, label_y),
                fontsize=7, color=COLORS["sciqlop"], alpha=0.8,
                ha="right", va="top",
                xytext=(-4, 0), textcoords="offset points")
    ax.annotate("diskcache\nblob→file", xy=(DISKCACHE_FILE_THRESHOLD, label_y),
                fontsize=7, color=COLORS["diskcache"], alpha=0.8,
                ha="left", va="top",
                xytext=(4, 0), textcoords="offset points")


def _annotate_storage_modes(ax, value_size):
    """Add a subtitle showing which storage mode each backend uses for this value size."""
    sq_mode = _storage_mode("sciqlop", value_size)
    dc_mode = _storage_mode("diskcache", value_size)
    label = f"sciqlop: {sq_mode} (>{fmt_size(SCIQLOP_FILE_THRESHOLD)})  •  diskcache: {dc_mode} (>{fmt_size(DISKCACHE_FILE_THRESHOLD)})"
    ax.set_title(f"Value = {fmt_size(value_size)}\n{label}", fontsize=10)


def plot_single_ops(rows, output):
    fig, (ax_set, ax_get) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Single-op Latency vs Value Size", fontsize=14)

    for op, ax, title in [("single_set", ax_set, "set()"), ("single_get", ax_get, "get()")]:
        for backend in ["sciqlop", "diskcache"]:
            data = [(r["value_size"], r["latency_us"])
                    for r in rows if r["operation"] == op and r["backend"] == backend]
            if not data:
                continue
            data.sort()
            sizes, latencies = zip(*data)
            ax.loglog(sizes, latencies, marker="o", color=COLORS[backend],
                      label=backend, markersize=5)

        ax.set_xlabel("Value size (bytes)")
        ax.set_ylabel("Latency (μs)")
        ax.set_title(title)
        ax.legend()
        ax.grid(True, alpha=0.3)

        xticks = sorted(set(r["value_size"] for r in rows if r["operation"] == op))
        ax.set_xticks(xticks)
        ax.set_xticklabels([fmt_size(s) for s in xticks], rotation=45, ha="right")

        _annotate_thresholds(ax)

    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def plot_batch(rows, output):
    batch_value_sizes = sorted(set(
        r["value_size"] for r in rows if r["operation"] == "batch_set"
    ))

    fig, axes = plt.subplots(1, len(batch_value_sizes), figsize=(5 * len(batch_value_sizes), 5),
                              sharey=False)
    if len(batch_value_sizes) == 1:
        axes = [axes]

    fig.suptitle("Batch set() Latency: Transaction of N ops", fontsize=14)

    for ax, vsz in zip(axes, batch_value_sizes):
        for backend in ["sciqlop", "diskcache"]:
            data = [(r["batch_size"], r["latency_us"])
                    for r in rows
                    if r["operation"] == "batch_set"
                    and r["backend"] == backend
                    and r["value_size"] == vsz]
            if not data:
                continue
            data.sort()
            batch_sizes, latencies = zip(*data)
            ax.plot(batch_sizes, latencies, marker="o", color=COLORS[backend],
                    label=backend, markersize=5)

        ax.set_xlabel("Batch size (ops per transaction)")
        ax.set_ylabel("Transaction latency (μs)")
        _annotate_storage_modes(ax, vsz)
        ax.legend()
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def plot_batch_per_op(rows, output):
    batch_value_sizes = sorted(set(
        r["value_size"] for r in rows if r["operation"] == "batch_set_per_op"
    ))

    fig, axes = plt.subplots(1, len(batch_value_sizes), figsize=(5 * len(batch_value_sizes), 5),
                              sharey=False)
    if len(batch_value_sizes) == 1:
        axes = [axes]

    fig.suptitle("Per-op Cost in Batched Transactions", fontsize=14)

    for ax, vsz in zip(axes, batch_value_sizes):
        for backend in ["sciqlop", "diskcache"]:
            data = [(r["batch_size"], r["latency_us"])
                    for r in rows
                    if r["operation"] == "batch_set_per_op"
                    and r["backend"] == backend
                    and r["value_size"] == vsz]
            if not data:
                continue
            data.sort()
            batch_sizes, latencies = zip(*data)
            ax.plot(batch_sizes, latencies, marker="o", color=COLORS[backend],
                    label=backend, markersize=5)

        ax.set_xlabel("Batch size (ops per transaction)")
        ax.set_ylabel("Per-op latency (μs)")
        _annotate_storage_modes(ax, vsz)
        ax.legend()
        ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file", help="CSV from bench_valuesize.py")
    parser.add_argument("-o", "--output-dir", default="benchmark",
                        help="Directory for output PNGs")
    args = parser.parse_args()

    rows = read_csv(args.csv_file)
    d = args.output_dir

    plot_single_ops(rows, f"{d}/valuesize_chart.png")
    plot_batch(rows, f"{d}/batch_chart.png")
    plot_batch_per_op(rows, f"{d}/batch_per_op_chart.png")


if __name__ == "__main__":
    main()
