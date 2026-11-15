import gc
import shutil
import sys

import pytest

_PYPY = sys.implementation.name == "pypy"


def _collect():
    # PyPy frees a C extension object (a Buffer's file mapping) only on the
    # second collection: the first runs its finalizer.
    gc.collect()
    gc.collect()


@pytest.fixture(autouse=True)
def _collect_garbage_on_pypy():
    """PyPy frees objects only when its GC runs, not when the last reference
    goes. A sqlite3 connection (diskcache's, or a test helper's) left by an
    earlier test is then finalized inside a forked child, which segfaults on
    macOS. Collecting before each test finalizes it in the parent."""
    if _PYPY:
        _collect()
    yield


if _PYPY:
    _rmtree = shutil.rmtree

    def _rmtree_after_collecting(*args, **kwargs):
        """Windows can't delete a file that a not-yet-collected Buffer maps.
        TemporaryDirectory cleans up through shutil.rmtree too."""
        _collect()
        return _rmtree(*args, **kwargs)

    shutil.rmtree = _rmtree_after_collecting
