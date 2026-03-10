#!/usr/bin/env python3
"""Generate scaling charts from benchmark CSV output.

Usage:
    # Summary line chart:
    python benchmark/plot_scaling.py results.csv -o benchmark/scaling_chart.png

    # Violin plot with comparison:
    python benchmark/plot_scaling.py raw.csv --violin -o benchmark/scaling_violin.png
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

    by_backend = defaultdict(lambda: defaultdict(list))
    for r in rows:
        b = r.get("backend", "sciqlop")
        by_backend[b]["entries"].append(int(r["entries"]))
        by_backend[b]["set_us"].append(float(r["set_us"]))
        by_backend[b]["get_hit_us"].append(float(r["get_hit_us"]))
        by_backend[b]["get_miss_us"].append(float(r["get_miss_us"]))
    return dict(by_backend)


def read_raw_csv(path):
    # by_backend -> by_op -> by_size -> [latencies]
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    with open(path) as f:
        for row in csv.DictReader(f):
            b = row.get("backend", "sciqlop")
            data[b][row["operation"]][int(row["entries"])].append(float(row["latency_us"]))
    return data


def fmt_entries(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.0f}M" if n % 1_000_000 == 0 else f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K" if n % 1_000 == 0 else f"{n / 1_000:.1f}K"
    return str(n)


BACKEND_STYLES = {
    "sciqlop": {"marker": "o", "ls": "-"},
    "diskcache": {"marker": "x", "ls": "--"},
}

OP_COLORS = {
    "set": "#e74c3c",
    "get_hit": "#2ecc71",
    "get_miss": "#3498db",
}

OP_LABELS = {
    "set": "set()",
    "get_hit": "get() hit",
    "get_miss": "get() miss",
}


def plot_line(by_backend, output):
    fig, ax = plt.subplots(figsize=(10, 5))

    for backend, data in by_backend.items():
        style = BACKEND_STYLES.get(backend, {"marker": "o", "ls": "-"})
        for op, col in [("set", "set_us"), ("get_hit", "get_hit_us"), ("get_miss", "get_miss_us")]:
            ax.semilogx(data["entries"], data[col], marker=style["marker"],
                        linestyle=style["ls"], color=OP_COLORS[op],
                        label=f"{backend} {OP_LABELS[op]}", markersize=4)

    ax.set_ylabel("Latency (\u00b5s / op)")
    ax.set_xlabel("Cache entries")
    ax.set_title("Sciqlop-cache vs diskcache: Latency Scaling")
    ax.legend(fontsize=9, ncol=2)
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    fig.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Wrote {output}", file=sys.stderr)


def plot_violin(data, output):
    backends = sorted(data.keys())
    ops = [("set", "set()"), ("get_hit", "get() hit"), ("get_miss", "get() miss")]
    n_backends = len(backends)

    all_sizes = sorted(set(
        s for b in backends for op_key, _ in ops for s in data[b][op_key].keys()
    ))

    fig, axes = plt.subplots(len(ops), 1, figsize=(14, 10), sharex=True)
    fig.suptitle("Latency Distribution: sciqlop-cache vs diskcache", fontsize=14)

    backend_colors = {"sciqlop": "#e74c3c", "diskcache": "#3498db"}
    width = 0.35

    for ax, (op_key, op_label) in zip(axes, ops):
        for bi, backend in enumerate(backends):
            by_size = data[backend][op_key]
            sizes_present = [s for s in all_sizes if s in by_size]
            data_list = [by_size[s] for s in sizes_present]
            positions = [all_sizes.index(s) + (bi - (n_backends - 1) / 2) * width
                         for s in sizes_present]

            if not data_list:
                continue

            parts = ax.violinplot(data_list, positions=positions, showmedians=True,
                                  showextrema=False, widths=width * 0.9)
            color = backend_colors.get(backend, "#999")
            for pc in parts["bodies"]:
                pc.set_facecolor(color)
                pc.set_alpha(0.6)
            parts["cmedians"].set_color("black")

        all_vals = [v for b in backends for s in all_sizes
                    if s in data[b].get(op_key, {}) for v in data[b][op_key][s]]
        if all_vals:
            ax.set_ylim(bottom=0, top=np.percentile(all_vals, 99) * 1.3)

        ax.set_ylabel(f"{op_label} (\u00b5s)")
        ax.set_xticks(range(len(all_sizes)))
        ax.set_xticklabels([fmt_entries(s) for s in all_sizes], rotation=45, ha="right")
        ax.grid(True, axis="y", alpha=0.3)

    # legend
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=backend_colors.get(b, "#999"), alpha=0.6, label=b)
                       for b in backends]
    axes[0].legend(handles=legend_elements, loc="upper left")

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
        data = read_raw_csv(args.csv_file)
        plot_violin(data, args.output)
    else:
        by_backend = read_summary_csv(args.csv_file)
        plot_line(by_backend, args.output)


if __name__ == "__main__":
    main()
