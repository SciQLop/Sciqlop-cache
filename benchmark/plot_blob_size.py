#!/usr/bin/env python3
"""Plot the max_blob_size sweep from bench_blob_size.py.

Usage:
    python benchmark/plot_blob_size.py benchmark/blob_size_results.csv -o benchmark/blob_size_chart.png
"""

import argparse
import csv
import sys

import matplotlib.pyplot as plt

PANELS = [
    ("set_us", "set() (μs)"),
    ("first_get_us", "get(), first read of each key (μs)"),
    ("get_us", "get(), same key (μs)"),
    ("db_bytes_per_value", "database + WAL bytes per value"),
]


def fmt_size(n):
    return f"{n // 1048576} MB" if n >= 1048576 else f"{n // 1024} KB"


def read_rows(path):
    with open(path) as f:
        return [{"max_blob_size": int(r["max_blob_size"]), "metric": r["metric"],
                 "value_size": int(r["value_size"]), "value": float(r["value"])}
                for r in csv.DictReader(f)]


def plot(rows, output, machine):
    thresholds = sorted({r["max_blob_size"] for r in rows})
    fig, axes = plt.subplots(1, len(PANELS), figsize=(5.5 * len(PANELS), 4.8))
    title = "Where values go: max_blob_size"
    fig.suptitle(f"{title} ({machine})" if machine else title, fontsize=14)
    for ax, (metric, label) in zip(axes, PANELS):
        for threshold in thresholds:
            data = sorted((r["value_size"], r["value"]) for r in rows
                          if r["metric"] == metric and r["max_blob_size"] == threshold)
            if not data:
                continue
            sizes, values = zip(*data)
            style = "-" if threshold == 8192 else "--"
            ax.plot(sizes, values, marker="o", markersize=4, linestyle=style,
                    label=f"max_blob_size = {fmt_size(threshold)}" + (" (default)" if threshold == 8192 else ""))
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xticks(sorted({r["value_size"] for r in rows}))
        ax.set_xticklabels([fmt_size(s) for s in sorted({r["value_size"] for r in rows})], rotation=45)
        ax.set_xlabel("Value size")
        ax.set_title(label, fontsize=11)
        ax.grid(True, which="both", alpha=0.3)
    axes[0].legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_file")
    parser.add_argument("-o", "--output", default="benchmark/blob_size_chart.png")
    parser.add_argument("--machine", default="", help="appended to the chart title")
    args = parser.parse_args()
    plot(read_rows(args.csv_file), args.output, args.machine)


if __name__ == "__main__":
    main()
