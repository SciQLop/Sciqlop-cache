import gc
import sys

import pytest


@pytest.fixture(autouse=True)
def _collect_garbage_on_pypy():
    """PyPy frees objects only when its GC runs, not when the last reference
    goes. A sqlite3 connection (ours in helpers, or diskcache's) left by an
    earlier test is then finalized inside a forked child, which segfaults on
    macOS. Collecting before each test finalizes it in the parent."""
    if sys.implementation.name == "pypy":
        gc.collect()
    yield
