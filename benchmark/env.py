#!/usr/bin/env python3
"""Print the benchmark setup as a Markdown table (paste into README).

Run with the same interpreter, PYTHONPATH and TMPDIR as the benchmarks:
    TMPDIR=/dev/shm PYTHONPATH=build python benchmark/env.py
"""

import os
import platform
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path

import diskcache


def cpu_model():
    text = Path("/proc/cpuinfo").read_text()
    return re.search(r"model name\s*:\s*(.+)", text).group(1)


def ram_gib():
    kib = int(re.search(r"MemTotal:\s*(\d+)", Path("/proc/meminfo").read_text()).group(1))
    return f"{kib / 2**20:.0f} GiB"


def filesystem(path):
    """Type of the mount holding `path` (longest matching mount point wins)."""
    mounts = [line.split()[1:3] for line in Path("/proc/mounts").read_text().splitlines()]
    matching = [(mnt, fs) for mnt, fs in mounts if path == mnt or path.startswith(mnt.rstrip("/") + "/")]
    mnt, fs = max(matching, key=lambda m: len(m[0]))
    return f"`{fs}` ({path})"


def vendored_sqlite():
    wrap = Path(__file__).resolve().parent.parent / "subprojects" / "sqlite3.wrap"
    digits = re.search(r"sqlite-amalgamation-(\d+)", wrap.read_text()).group(1)
    major, minor, patch = int(digits[0]), int(digits[1:3]), int(digits[3:5])
    return f"{major}.{minor}.{patch}"


def git_describe():
    repo = Path(__file__).resolve().parent.parent
    return subprocess.run(["git", "describe", "--tags", "--always", "--dirty"], cwd=repo,
                          capture_output=True, text=True).stdout.strip() or (repo / "version.txt").read_text().strip()


ROWS = [
    ("CPU", cpu_model()),
    ("RAM", ram_gib()),
    ("OS / kernel", f"{platform.freedesktop_os_release().get('PRETTY_NAME', '?')} / {platform.release()}"),
    ("Storage", filesystem(os.path.realpath(tempfile.gettempdir()))),
    ("Python", platform.python_version()),
    ("sciqlop-cache", f"{git_describe()}, bundled SQLite {vendored_sqlite()}"),
    ("diskcache", f"{diskcache.__version__}, Python's SQLite {sqlite3.sqlite_version}"),
]

print("| | |\n|---|---|")
for name, value in ROWS:
    print(f"| {name} | {value} |")
