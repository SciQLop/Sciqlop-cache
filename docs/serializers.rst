===========
Serializers
===========

A serializer turns your Python values into bytes and back. Each cache uses one:

.. list-table::
    :header-rows: 1
    :widths: 30 70

    * - Serializer
      - Use it for
    * - :class:`~pysciqlop_cache.PickleSerializer` (default)
      - Anything picklable. The safe default.
    * - :class:`~pysciqlop_cache.PickleOOBSerializer`
      - Values that carry big numpy arrays, especially in programs with several
        threads. See below.
    * - :class:`~pysciqlop_cache.MsgspecSerializer`
      - Plain structured data: dicts, lists, numbers, small arrays. Faster and safer
        than pickle. Needs ``msgspec``.

.. code-block:: python

    from pysciqlop_cache import Cache, PickleOOBSerializer

    arrays = Cache("arrays-cache", serializer=PickleOOBSerializer())

The choice is recorded in the cache. Reopening it without a serializer uses the recorded
one. Asking for a different one raises, instead of silently misreading your data:

.. code-block:: python

    from pysciqlop_cache import PickleSerializer

    print(Cache("arrays-cache").serializer.name)
    # pickle-oob
    try:
        Cache("arrays-cache", serializer=PickleSerializer())
    except ValueError as error:
        print(error)

The one exception is ``pickle`` → ``pickle-oob``: the new serializer reads plain pickle
entries too, so an existing pickle cache can switch in place. The reverse still raises.

Only load caches you wrote: unpickling runs code. ``MsgspecSerializer`` only builds plain
data types.

Big numpy arrays: ``PickleOOBSerializer``
=========================================

.. code-block:: python

    import numpy as np

    samples = 1_000_000
    measurements = {
        "time": np.arange(samples).astype("datetime64[ms]"),
        "values": np.random.default_rng(0).standard_normal((samples, 4), dtype=np.float32),
        "units": "nT",
    }
    arrays["sensor-42/2020-01-01"] = measurements

    back = arrays["sensor-42/2020-01-01"]
    back["values"][0, 0] = np.nan      # normal, writable numpy arrays
    print(back["time"].dtype, back["values"].shape)
    # datetime64[ms] (1000000, 4)

The problem it solves
---------------------

Plain pickle copies every array byte into the pickle stream when writing, and out of it
when reading. Python runs that copy while holding the GIL. For a 33 MB array, that is
15 ms per write and 1 ms per read during which no other Python thread can run. When
several threads load data at once, like the fetch threads of a GUI, they end up waiting
on each other.

What it does differently
------------------------

It uses pickle protocol 5 and keeps the arrays *out* of the pickle stream:

1. The pickle only holds the small parts: the object structure, metadata, dtypes and
   shapes.
2. The array bytes are stored next to it, one block per array.
3. ``set`` writes those blocks straight from the arrays' memory, in C++, without the GIL.
4. ``get`` copies them into fresh numpy arrays, also in C++ and without the GIL. The
   arrays you get are normal and writable, and don't depend on the cache staying open.
5. Blocks of 256 KiB or more are compressed with `blosc2 <https://www.blosc.org/>`_
   (lz4 + shuffle, on all cores) when that makes them at least twice smaller. Time axes
   shrink about 15x. Noisy float data is detected on a small sample in ~6 µs and stored
   raw.

Where it shines
---------------

- **Big arrays.** Anything holding numpy arrays of 64 KiB or more: time series,
  spectrograms, images, any measurement data.
- **Many threads.** A GUI or a server with several threads reading or writing the
  cache. The array copies no longer block the other threads.
- **Heavy writes.** Writing a big value is several times faster: there is no pickle
  buffer to grow and copy into.
- **Regular data.** Timestamps, counters, masks and other smooth arrays are stored
  compressed, so more of them fit under a ``max_size`` limit.

Where it doesn't help
---------------------

- Values without big arrays (dicts, strings, small arrays) are written as plain pickle.
  They cost the same as with ``PickleSerializer``, within a few percent.
- Object arrays and non-numpy containers also go through plain pickle.
- In a single-threaded script nothing waits on the GIL, so reads are about as fast as
  plain pickle. Between 1 and 10 MB they can even be a little slower, because
  decompressing a time axis costs more than copying it. The gains there are faster
  writes and less disk.

Numbers
-------

One day of sensor measurements (33 MB: 1.4 M samples of 4 float32 values plus a
datetime64 time axis):

.. list-table::
    :header-rows: 1

    * -
      - ``pickle``
      - ``pickle-oob``
    * - ``set``
      - 22.1 ms
      - 3.6 ms
    * - GIL held during ``set``
      - 15.6 ms
      - 0 ms
    * - ``get``
      - 2.0 ms
      - 1.6 ms
    * - GIL held during ``get``
      - 1.0 ms
      - 0 ms
    * - 8 days, read by 4 threads
      - 45–55 ms
      - 23 ms
    * - Disk per day
      - 31.6 MiB
      - 22.4 MiB

See :doc:`performance` for the full size and thread sweeps against diskcache.

Options
-------

.. code-block:: python

    raw = PickleOOBSerializer(compress=False)     # never compress
    strict = PickleOOBSerializer(min_ratio=4.0)   # keep a block compressed only if 4x smaller

Reads decode any block, whatever these options are. The wheels bundle blosc2. A source
build with ``-Dwith_blosc2=false`` leaves it out and refuses compressed values with a
clear error.
