#!/usr/bin/env python3
"""Plot bench_arrays.py CSV output: one row of panels per dataset (get, set, disk), and
the thread sweep in a second chart next to it (<output>_threads.png).

Usage:
    python benchmark/plot_arrays.py benchmark/arrays_results.csv -o benchmark/arrays_chart.png
"""

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

STYLE = {
    "diskcache": dict(color="#3498db", marker="s"),
    "sciqlop pickle": dict(color="#f39c12", marker="^"),
    "sciqlop pickle-oob": dict(color="#e74c3c", marker="o"),
}
DATASETS = [
    ("images", "CIFAR-10 images (uint8)"),
    ("table", "NYC taxi trips (DataFrame)"),
    ("fields", "Hurricane ISABEL fields (float32)"),
    ("sensor", "household power readings (time + float64)"),
    ("noise", "random noise (incompressible)"),
]
SIZE_PANELS = [("get", "get (ms)"), ("set", "set (ms)"), ("disk", "disk / data size")]


def read(path):
    series = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            dataset = row.get("dataset", "noise")  # CSVs from before the datasets
            series[(dataset, row["sweep"], row["operation"], row["backend"])].append(
                (float(row["x"]), float(row["value"])))
    return series


def plot_sizes(series, output, machine):
    datasets = [(d, label) for d, label in DATASETS
                if any(key[0] == d and key[1] == "size" for key in series)]
    fig, axes = plt.subplots(len(datasets), 3, figsize=(13, 2.9 * len(datasets)), squeeze=False)
    for row, (dataset, label) in zip(axes, datasets):
        for ax, (op, ylabel) in zip(row, SIZE_PANELS):
            for backend, style in STYLE.items():
                points = sorted(series[(dataset, "size", op, backend)])
                xs = [x / 1e6 for x, _ in points]
                ys = [y / (x / 1e6) if op == "disk" else y for x, y in points]
                ax.plot(xs, ys, label=backend, linewidth=1.8, markersize=4, **style)
            ax.set_xscale("log")
            if op == "disk":
                ax.set_ylim(0, 1.05)  # one scale for every dataset: 1 = no saving
            else:
                ax.set_yscale("log")
            ax.set_ylabel(ylabel)
            ax.grid(True, which="both", alpha=0.3)
        row[0].set_title(label, loc="left", fontsize=11, fontweight="bold")
    for ax in axes[-1]:
        ax.set_xlabel("value size (MB)")
    axes[0][0].legend(fontsize=8)
    title = "Values from public datasets, 100 KB to 1 GB"
    fig.suptitle(f"{title} ({machine})" if machine else title)
    fig.tight_layout()
    fig.savefig(output, dpi=110)
    print(f"wrote {output}")


def plot_threads(series, output, machine):
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for ax, op in zip(axes, ("get", "set")):
        for backend, style in STYLE.items():
            points = sorted(p for (d, sweep, o, b), pts in series.items()
                            if sweep == "threads" and o == op and b == backend for p in pts)
            ax.plot([x for x, _ in points], [y for _, y in points], label=backend, linewidth=2, **style)
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 2, 4, 8, 16], labels=["1", "2", "4", "8", "16"])
        ax.set_title(f"{op} throughput, 24 MB values")
        ax.set_xlabel("threads")
        ax.set_ylabel("GB/s (all threads)")
        ax.grid(True, which="both", alpha=0.3)
    axes[0].legend()
    title = "Threads: Hurricane ISABEL fields"
    fig.suptitle(f"{title} ({machine})" if machine else title)
    fig.tight_layout()
    fig.savefig(output, dpi=110)
    print(f"wrote {output}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    parser.add_argument("-o", "--output", default="benchmark/arrays_chart.png")
    parser.add_argument("--machine", default="",
                        help="machine name appended to the chart titles, e.g. 'Apple M2, macOS'")
    args = parser.parse_args()
    series = read(args.csv)
    plot_sizes(series, args.output, args.machine)
    out = Path(args.output)
    plot_threads(series, out.with_name(out.stem + "_threads" + out.suffix), args.machine)


if __name__ == "__main__":
    main()
