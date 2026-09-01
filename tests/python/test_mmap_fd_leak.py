"""Reproducer for the Buffer.memoryview() fd/mmap leak (production incident
2026-09-01, speasy-proxy).

Buffer.memoryview() used to hand PyBuffer_FillInfo a heap-allocated capsule
as the exporter, then build the memoryview via PyMemoryView_FromBuffer.
CPython's PyMemoryView_FromBuffer clears the managed buffer's `obj` after
FillInfo has already INCREFed the capsule, so PyBuffer_Release never DECREFs
it back — the capsule (and the Buffer copy holding the mmap's shared_ptr)
leaks with refcount 1 forever. Every distinct file-backed key read through
get() leaked one fd and one mmap, permanently: no amount of releasing the
memoryview, gc.collect(), or destroying the Cache object could free it.

This test is deterministic RED before the fix (fd/map counts grow well past
the fixed-size cached-handle allowance) and GREEN after (fixing the buffer
protocol lets releasing the memoryview and the Cache tear down the mmap).
"""

import gc
import os
import shutil
import tempfile
import unittest

from pysciqlop_cache import Cache

N_KEYS = 64
VALUE_SIZE = 16 * 1024  # >8KB => file-backed
# DiskStorage keeps an LRU of up to 128 open mmap handles per store BY DESIGN;
# 64 keys stays under that cap, but those cached handles belong to the Cache
# object itself, so the real assertion must run only after the Cache (and
# therefore its DiskStorage) has been destroyed.
SLACK = 8


def _fd_count():
    return len(os.listdir("/proc/self/fd"))


def _map_count():
    with open("/proc/self/maps") as f:
        return sum(1 for _ in f)


@unittest.skipUnless(os.path.exists("/proc/self/fd"), "requires /proc (Linux)")
class MmapFdLeak(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_get_of_many_file_backed_keys_does_not_leak_fds_or_mmaps(self):
        cache = Cache(self.tmp)
        values = {}
        for i in range(N_KEYS):
            key = f"key-{i}"
            value = bytes([i % 256]) * VALUE_SIZE
            cache.set(key, value)
            values[key] = value

        gc.collect()
        baseline_fds = _fd_count()
        baseline_maps = _map_count()

        for key, expected in values.items():
            got = cache.get(key)
            self.assertEqual(got, expected, f"value for {key} did not round-trip")

        del cache
        gc.collect()

        after_fds = _fd_count()
        after_maps = _map_count()

        self.assertLessEqual(
            after_fds - baseline_fds, SLACK,
            f"leaked file descriptors: baseline={baseline_fds}, after={after_fds}",
        )
        self.assertLessEqual(
            after_maps - baseline_maps, SLACK,
            f"leaked memory mappings: baseline={baseline_maps}, after={after_maps}",
        )


if __name__ == "__main__":
    unittest.main()
