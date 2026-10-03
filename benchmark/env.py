#!/usr/bin/env python3
"""Print the benchmark setup as a Markdown table (paste into README).

Run with the same interpreter, PYTHONPATH and TMPDIR as the benchmarks:
    TMPDIR=/dev/shm PYTHONPATH=build python benchmark/env.py
Works on Linux and macOS.
"""

import os
import platform
import re
import sqlite3
import subprocess
import tempfile
from pathlib import Path

import diskcache


MACOS = platform.system() == "Darwin"


def sysctl(name):
    return subprocess.run(["sysctl", "-n", name], capture_output=True, text=True).stdout.strip()


def cpu_model():
    if MACOS:
        perf, eff = sysctl("hw.perflevel0.logicalcpu"), sysctl("hw.perflevel1.logicalcpu")
        cores = f" ({perf} performance + {eff} efficiency cores)" if eff else ""
        return sysctl("machdep.cpu.brand_string") + cores
    text = Path("/proc/cpuinfo").read_text()
    return re.search(r"model name\s*:\s*(.+)", text).group(1)


def ram_gib():
    if MACOS:
        return f"{int(sysctl('hw.memsize')) / 2**30:.0f} GiB"
    kib = int(re.search(r"MemTotal:\s*(\d+)", Path("/proc/meminfo").read_text()).group(1))
    return f"{kib / 2**20:.0f} GiB"


def os_name():
    if MACOS:
        return f"macOS {platform.mac_ver()[0]} / Darwin {platform.release()}"
    return f"{platform.freedesktop_os_release().get('PRETTY_NAME', '?')} / {platform.release()}"


def _mounts():
    """(mount point, file system type) pairs."""
    if MACOS:
        # "/dev/disk3s1 on /Volumes/x (apfs, local, nodev, ...)"
        lines = subprocess.run(["mount"], capture_output=True, text=True).stdout.splitlines()
        return [m.groups() for m in (re.match(r".+? on (.+) \((\w+)", l) for l in lines) if m]
    return [line.split()[1:3] for line in Path("/proc/mounts").read_text().splitlines()]


def filesystem(path):
    """Type of the mount holding `path` (longest matching mount point wins)."""
    mounts = _mounts()
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
    ("OS / kernel", os_name()),
    ("Storage", filesystem(os.path.realpath(tempfile.gettempdir()))),
    ("Python", platform.python_version()),
    ("sciqlop-cache", f"{git_describe()}, bundled SQLite {vendored_sqlite()}"),
    ("diskcache", f"{diskcache.__version__}, Python's SQLite {sqlite3.sqlite_version}"),
]

print("| | |\n|---|---|")
for name, value in ROWS:
    print(f"| {name} | {value} |")
