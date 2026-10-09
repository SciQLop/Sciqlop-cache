"""Stores still referenced from outside Python at interpreter exit get closed.

An embedding host (Julia via PythonCall, speasy PR #402) can hold a reference
to a store past Py_Finalize. The store then outlives its extension module and
its SQLite connection is never closed (the -wal file stays behind). nanobind
still reports the live instance as leaked: only the holder can release it.
Py_IncRef from ctypes plays the host here.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest

CHILD = textwrap.dedent("""
    import atexit, ctypes, os, sys
    import pysciqlop_cache as sc

    root = sys.argv[1]
    stores = [cls(os.path.join(root, cls.__name__))
              for cls in (sc.Cache, sc.Index, sc.FanoutCache, sc.FanoutIndex)]
    for store in stores:
        store["k"] = b"v" * 100_000
        ctypes.pythonapi.Py_IncRef(ctypes.py_object(store))

    def late_writer():
        for store in stores:
            store["late"] = 1

    atexit.register(late_writer)
""")


def _wal_files(root):
    return [os.path.join(d, f) for d, _, files in os.walk(root) for f in files if f.endswith("-wal")]


class CloseAtExit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.child = subprocess.run(
            [sys.executable, "-c", CHILD, self.tmp],
            capture_output=True, text=True, timeout=60, cwd=self.tmp,  # keep the source tree off sys.path
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_child_exits_cleanly(self):
        self.assertEqual(self.child.returncode, 0, self.child.stderr)

    def test_connections_closed(self):
        self.assertEqual(_wal_files(self.tmp), [])

    def test_atexit_hooks_registered_later_still_write(self):
        import pysciqlop_cache as sc

        for cls in (sc.Cache, sc.Index, sc.FanoutCache, sc.FanoutIndex):
            with self.subTest(cls=cls.__name__):
                store = cls(os.path.join(self.tmp, cls.__name__))
                self.assertEqual(store.get("late"), 1)
                store.close()


if __name__ == "__main__":
    unittest.main()
