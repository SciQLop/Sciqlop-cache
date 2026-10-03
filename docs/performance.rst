===========
Performance
===========

These benchmarks compare SciQLop Cache with `diskcache
<https://grantjenks.com/docs/diskcache/>`_. Each library runs **with its default
settings**, so the gaps come from the implementations.

**Held identical for both libraries:**

- The same machine, Python interpreter and benchmark process.
- The same storage: a RAM filesystem, so disk speed doesn't hide the library's own
  cost (``tmpfs`` on Linux, an APFS RAM disk on macOS; see below for the one exception).
- The same keys, values and number of operations, through the public Python API.
- SQLite in WAL mode with ``synchronous=NORMAL`` for both.

**Left at each library's default (the differences being measured):**

.. list-table::
    :header-rows: 1

    * -
      - SciQLop Cache
      - diskcache
    * - Engine
      - C++ core, nanobind bindings
      - pure Python on ``sqlite3``
    * - Values stored as files above
      - 8 KB
      - 32 KB
    * - Size limit, eviction
      - unlimited; LRU when a limit is set
      - 1 GiB, least-recently-stored

Every chart has a tab for each machine; switching one switches them all.

.. list-table::
    :header-rows: 1

    * -
      - Linux, x86-64
      - macOS, Apple M2
    * - CPU
      - AMD Ryzen 7 5800X (8 cores, 16 threads)
      - Apple M2 (4 performance + 4 efficiency cores)
    * - RAM
      - 63 GiB
      - 16 GiB
    * - OS
      - Fedora Linux 44
      - macOS 26.6
    * - Storage
      - ``tmpfs``
      - APFS RAM disk (``hdiutil``); internal SSD for the numpy arrays
    * - Python, diskcache
      - 3.14 (GIL build), 5.6.3
      - 3.14.7 (GIL build), 5.6.3

Scaling with the number of entries
==================================

100 to 1 M entries of 256 bytes. Latency stays flat as the cache grows:

.. tab-set::
    :sync-group: platform

    .. tab-item:: Linux, x86-64
        :sync: linux

        .. image:: ../benchmark/scaling_chart.png
            :alt: set and get latency from 100 to 1M entries (Linux, AMD Ryzen 7 5800X)

    .. tab-item:: macOS, Apple M2
        :sync: macos

        .. image:: ../benchmark/macos-m2/scaling_chart.png
            :alt: set and get latency from 100 to 1M entries (macOS, Apple M2)

.. tab-set::
    :sync-group: platform

    .. tab-item:: Linux, x86-64
        :sync: linux

        .. image:: ../benchmark/scaling_violin.png
            :alt: latency distribution (Linux, AMD Ryzen 7 5800X)

    .. tab-item:: macOS, Apple M2
        :sync: macos

        .. image:: ../benchmark/macos-m2/scaling_violin.png
            :alt: latency distribution (macOS, Apple M2)

Latency vs value size
=====================

Raw ``bytes`` values from 64 B to 1 MB:

.. tab-set::
    :sync-group: platform

    .. tab-item:: Linux, x86-64
        :sync: linux

        .. image:: ../benchmark/valuesize_chart.png
            :alt: set and get latency vs value size (Linux, AMD Ryzen 7 5800X)

    .. tab-item:: macOS, Apple M2
        :sync: macos

        .. image:: ../benchmark/macos-m2/valuesize_chart.png
            :alt: set and get latency vs value size (macOS, Apple M2)

Values from 8 KB to 32 KB are the one range where diskcache is faster. SciQLop Cache stores
them as files, while diskcache keeps them in SQLite up to 32 KB.

- **Writes:** 130 µs against 37 µs on the M2, 75–81 µs against 52–60 µs on the Ryzen. A file
  costs ~100 µs more than a blob on the M2 (~50 µs on the Ryzen): on APFS, ``open`` with
  ``O_CREAT`` alone takes 32 µs and ``close`` 10 µs.
- **First read of a value:** 12–15 µs against 7 µs on the Ryzen, the cost of opening the
  file. From 64 KB, where diskcache uses files too, both are level.
- **Reading the same value again** is faster than diskcache from 8 KB up, 1.5x at 16 KB
  and 3x at 64 KB on the Ryzen: SciQLop Cache keeps the last 128 values it loaded from
  files in memory, so it doesn't open the file again.

The M2 chart predates the "first read" measurement, so it has no third panel.

Batched transactions
====================

Writing many values in one ``transact()`` block spreads the commit cost over the batch.
The per-operation cost drops sharply, especially for small values:

.. tab-set::
    :sync-group: platform

    .. tab-item:: Linux, x86-64
        :sync: linux

        .. image:: ../benchmark/batch_per_op_chart.png
            :alt: per-operation latency vs batch size (Linux, AMD Ryzen 7 5800X)

    .. tab-item:: macOS, Apple M2
        :sync: macos

        .. image:: ../benchmark/macos-m2/batch_per_op_chart.png
            :alt: per-operation latency vs batch size (macOS, Apple M2)

numpy arrays
============

Here the values are measurement series: float32 samples (4 per row) plus a datetime64
time axis, from 100 KB to 100 MB each. All three store them with pickle protocol 5.
diskcache and the default serializer keep the array bytes inside the pickle;
:class:`~pysciqlop_cache.PickleOOBSerializer` stores them next to it, compressed when that
pays off (see :doc:`serializers`).

.. tab-set::
    :sync-group: platform

    .. tab-item:: Linux, x86-64
        :sync: linux

        .. image:: ../benchmark/arrays_chart.png
            :alt: numpy arrays: diskcache vs pickle vs pickle-oob (Linux, AMD Ryzen 7 5800X)

    .. tab-item:: macOS, Apple M2
        :sync: macos

        .. image:: ../benchmark/macos-m2/arrays_chart.png
            :alt: numpy arrays: diskcache vs pickle vs pickle-oob (macOS, Apple M2)

- **Writes:** pickle-oob is the fastest from 400 KB up, and ~5x faster than diskcache at
  100 MB. The arrays go to disk with no copy into a pickle buffer.
- **Reads, one thread:** below 1 MB, both SciQLop Cache serializers are 3x to 14x faster
  than diskcache. The benchmark reads the same key again, and SciQLop Cache keeps the
  last 128 files it loaded open, so only the unpickling is left. Between 1 and 10 MB,
  pickle-oob is a little slower than the default serializer: decompressing the time axis
  costs more than copying it. From 25 MB it is the fastest, 3.4x at 100 MB.
  ``PickleOOBSerializer(compress=False)`` skips the decompression.
- **Reads, many threads:** the array bytes are copied without the GIL, so throughput
  keeps growing with the thread count: 9.6 GB/s at 16 threads, against 7.9 GB/s for
  diskcache (its file reads release the GIL) and 4.8 GB/s for the default serializer (it
  copies with the GIL held).
- **Writes, many threads:** SQLite takes one writer at a time, so no library scales
  here. pickle-oob sustains 2.6 to 3.9 GB/s, 2.2x to 3.8x diskcache.
- **Disk:** noisy float values don't compress, but the time axis does. From 1.6 MB up,
  each value takes ~29 % less space with pickle-oob.

On the Apple M2 the picture is the same, with two differences:

- **Reads are much faster**, for every library: the M2's memory moves a 100 MB value in
  2.8 ms with pickle-oob (14.5 ms for diskcache), and 8 threads read 36 GB/s with
  pickle-oob, against 16 GB/s for diskcache (11 and 7 GB/s on the Ryzen).
- **Writes go to the internal SSD.** The thread sweep overwrites 768 MB per round, and
  replaced files are deleted 5 s later, so it needs several GB of free space: more than a
  RAM disk on a 16 GB machine can hold. All three libraries then write at 0.9 to 1.7 GB/s,
  the speed of the SSD, so these numbers can't be compared with the Linux ``tmpfs`` ones.

Both runs were on lightly loaded machines (load average 2–4), not idle ones.

Reproducing the benchmarks
==========================

Use a release build and a RAM filesystem. ``TMPDIR`` decides where both libraries put
their cache directories. ``-Db_ndebug=if-release`` builds like the wheels do: without it,
macOS's libc++ checks the bounds of every ``std::span`` access.

.. code-block:: console

    $ meson setup build --buildtype=release -Db_ndebug=if-release && meson compile -C build
    $ export TMPDIR=/dev/shm PYTHONPATH=build

On macOS, make a 2 GB RAM disk first, and run ``bench_arrays.py`` with ``TMPDIR`` on the
internal SSD (it needs more space than that). Write the results and charts to
``benchmark/macos-m2/``, and pass ``--machine "Apple M2, macOS"`` to the ``plot_*.py``
scripts:

.. code-block:: console

    $ diskutil erasevolume APFS sqcbench $(hdiutil attach -nomount ram://4194304)
    $ export TMPDIR=/Volumes/sqcbench PYTHONPATH=build

    $ python benchmark/env.py            # prints the environment table

    $ python benchmark/scaling.py --max-entries 1000000 --backend both > benchmark/scaling_results.csv
    $ python benchmark/plot_scaling.py benchmark/scaling_results.csv -o benchmark/scaling_chart.png
    $ python benchmark/scaling.py --max-entries 1000000 --raw --backend both > benchmark/scaling_raw.csv
    $ python benchmark/plot_scaling.py benchmark/scaling_raw.csv --violin -o benchmark/scaling_violin.png

    $ python benchmark/bench_valuesize.py > benchmark/valuesize_results.csv
    $ python benchmark/plot_valuesize.py benchmark/valuesize_results.csv -o benchmark

    $ python benchmark/bench_arrays.py > benchmark/arrays_results.csv
    $ python benchmark/plot_arrays.py benchmark/arrays_results.csv -o benchmark/arrays_chart.png
