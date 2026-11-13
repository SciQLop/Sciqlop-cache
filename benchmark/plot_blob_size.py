#!/usr/bin/env python3
"""Plot the max_blob_size sweep from bench_blob_size.py.

Filled markers: the value is stored in the database. Hollow markers: in a file.

Usage:
    python benchmark/plot_blob_size.py benchmark/blob_size_results.csv -o benchmark/blob_size_chart.png
"""

import argparse
import csv
import sys

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

DEFAULT = 8192
COLORS = {8192: "#c0392b", 32768: "#e67e22", 131072: "#27ae60", 1048576: "#2980b9"}

# (metric, panel title, y label, y transform taking (value, value_size))
PANELS = [
    ("set_us", "Writing a value: set()", "µs per call", lambda v, sz: v),
    ("first_get_us", "Reading a value for the first time: get()", "µs per call", lambda v, sz: v),
    ("get_us", "Reading the same value again: get()", "µs per call", lambda v, sz: v),
    ("disk_bytes_per_value", "Disk space used per byte of data\n(database + value files, 1 = no overhead)",
     "bytes on disk / bytes stored", lambda v, sz: v / sz),
]

# Notes drawn on a panel: (metric, text, value size and threshold of the point it points at, text offset)
NOTES = [
    ("set_us", "creating a file costs more\nthan a database write", 16384, DEFAULT, (-95, 35)),
    ("get_us", "files are memory-mapped once;\na database value is copied\non every read", 65536, 131072, (-150, 20)),
    ("disk_bytes_per_value", "SQLite gives each value\nof 1-4 KB a whole 4 KB page", 1024, DEFAULT, (15, -30)),
    ("disk_bytes_per_value", "a file also fills\nits last 4 KB block", 16384, DEFAULT, (10, 25)),
]


def fmt_size(n):
    return f"{n // 1048576} MB" if n >= 1048576 else f"{n // 1024} KB"


def read_rows(path):
    with open(path) as f:
        return [{"max_blob_size": int(r["max_blob_size"]), "metric": r["metric"],
                 "value_size": int(r["value_size"]), "value": float(r["value"])}
                for r in csv.DictReader(f)]


def lookup(rows):
    return {(r["max_blob_size"], r["metric"], r["value_size"]): r["value"] for r in rows}


def plot_panel(ax, table, thresholds, sizes, metric, title, ylabel, transform):
    for threshold in thresholds:
        points = [(sz, transform(table[(threshold, metric, sz)], sz)) for sz in sizes
                  if (threshold, metric, sz) in table]
        if not points:
            continue
        xs, ys = zip(*points)
        color = COLORS.get(threshold, "gray")
        ax.plot(xs, ys, color=color, linewidth=2.2 if threshold == DEFAULT else 1.4,
                linestyle="-" if threshold == DEFAULT else "--")
        for x, y in points:
            in_file = table.get((threshold, "files_per_value", x), 0) > 0
            ax.plot(x, y, marker="o", markersize=6, color=color,
                    markerfacecolor="white" if in_file else color)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(sizes)
    ax.set_xticklabels([fmt_size(s) for s in sizes], rotation=35, ha="right")
    plain = FuncFormatter(lambda v, _: f"{v:g}")
    ax.yaxis.set_major_formatter(plain)
    ax.yaxis.set_minor_formatter(plain)
    ax.set_xlabel("value size")
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=11)
    ax.grid(True, which="both", alpha=0.25)


def annotate(ax, table, metric, transform, text, size, threshold, offset):
    if (threshold, metric, size) not in table:
        return
    y = transform(table[(threshold, metric, size)], size)
    ax.annotate(text, xy=(size, y), xytext=offset, textcoords="offset points", fontsize=8.5,
                arrowprops=dict(arrowstyle="->", color="0.35"), color="0.2",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.8"))


def legend_handles(thresholds):
    handles = [Line2D([], [], color=COLORS.get(t, "gray"), linewidth=2.2 if t == DEFAULT else 1.4,
                      linestyle="-" if t == DEFAULT else "--",
                      label=f"max_blob_size = {fmt_size(t)}" + (" (default)" if t == DEFAULT else ""))
               for t in thresholds]
    handles += [Line2D([], [], marker="o", color="0.3", linestyle="", label="value stored in the database"),
                Line2D([], [], marker="o", color="0.3", markerfacecolor="white", linestyle="",
                       label="value stored as a file")]
    return handles


def plot(rows, output, machine):
    table = lookup(rows)
    thresholds = sorted({r["max_blob_size"] for r in rows})
    sizes = sorted({r["value_size"] for r in rows})
    fig, axes = plt.subplots(2, 2, figsize=(13, 9.5))
    title = "Choosing max_blob_size: values up to it are stored in the database, bigger ones as files"
    fig.suptitle(f"{title}\n({machine})" if machine else title, fontsize=13)
    for ax, (metric, panel_title, ylabel, transform) in zip(axes.flat, PANELS):
        plot_panel(ax, table, thresholds, sizes, metric, panel_title, ylabel, transform)
        for note_metric, text, size, threshold, offset in NOTES:
            if note_metric == metric:
                annotate(ax, table, metric, transform, text, size, threshold, offset)
    fig.legend(handles=legend_handles(thresholds), loc="lower center", ncol=3, fontsize=9.5,
               frameon=False, bbox_to_anchor=(0.5, -0.04))
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(output, dpi=130, bbox_inches="tight")
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
