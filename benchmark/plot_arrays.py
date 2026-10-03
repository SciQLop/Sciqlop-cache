#!/usr/bin/env python3
"""Plot bench_arrays.py CSV output.

Usage:
    python benchmark/plot_arrays.py benchmark/arrays_results.csv -o benchmark/arrays_chart.png
"""

import argparse
import csv
from collections import defaultdict

import matplotlib.pyplot as plt

# Appended to every chart title (--machine), so charts from two machines can't be mixed up.
MACHINE = ""


def titled(text):
    return f"{text} ({MACHINE})" if MACHINE else text


STYLE = {
    "diskcache": dict(color="#3498db", marker="s"),
    "sciqlop pickle": dict(color="#f39c12", marker="^"),
    "sciqlop pickle-oob": dict(color="#e74c3c", marker="o"),
}
PANELS = [
    ("size", "get", "get latency vs value size", "value size (MB)", "ms per get", True),
    ("size", "set", "set latency vs value size", "value size (MB)", "ms per set", True),
    ("threads", "get", "get throughput, 24 MB values", "threads", "GB/s (all threads)", False),
    ("threads", "set", "set throughput, 24 MB values", "threads", "GB/s (all threads)", False),
]


def read(path):
    series = defaultdict(list)
    with open(path) as f:
        for row in csv.DictReader(f):
            series[(row["sweep"], row["operation"], row["backend"])].append(
                (float(row["x"]), float(row["value"])))
    return series


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    parser.add_argument("-o", "--output", default="benchmark/arrays_chart.png")
    parser.add_argument("--machine", default="",
                        help="machine name appended to the chart titles, e.g. 'Apple M2, macOS'")
    args = parser.parse_args()
    global MACHINE
    MACHINE = args.machine
    series = read(args.csv)

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    for ax, (sweep, op, title, xlabel, ylabel, loglog) in zip(axes.flat, PANELS):
        for backend, style in STYLE.items():
            points = sorted(series[(sweep, op, backend)])
            xs = [x / 1e6 if sweep == "size" else x for x, _ in points]
            ax.plot(xs, [y for _, y in points], label=backend, linewidth=2, **style)
        if loglog:
            ax.set_xscale("log")
            ax.set_yscale("log")
        else:
            ax.set_xscale("log", base=2)
            ax.set_xticks([1, 2, 4, 8, 16], labels=["1", "2", "4", "8", "16"])
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(True, which="both", alpha=0.3)
    axes[0][0].legend()
    fig.suptitle(titled("numpy measurement arrays: 4 x float32 + datetime64 time axis per sample"))
    fig.tight_layout()
    fig.savefig(args.output, dpi=110)
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
