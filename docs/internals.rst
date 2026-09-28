=============
How it works
=============

This page is for readers who want to know *why* they can trust the cache. You don't
need it to use the library.

The big picture
===============

.. mermaid::

    flowchart TB
        subgraph PY["Python layer (pysciqlop_cache)"]
            API["Cache / Index / FanoutCache<br/>dict API, memoize, locks, serializers"]
        end
        subgraph NB["nanobind bindings"]
            GIL["GIL released around every call"]
        end
        subgraph CORE["C++ core (header-only)"]
            STORE["_Store&lt;DiskStorage, Policies...&gt;<br/>one mutex per instance"]
            FS["FanoutStore (N shards)"]
        end
        subgraph DISK["On disk (one directory per store)"]
            DB[("SQLite database, WAL mode<br/>keys, metadata, small values")]
            BLOBS["ab/cd/&lt;uuid&gt;-&lt;pid&gt;<br/>value files (values &gt; 8 KB)"]
        end
        API --> NB --> STORE --> DB
        STORE --> BLOBS
        FS --> STORE

One store is one directory: a SQLite database plus zero or more value files. Python
never touches the disk directly. Every operation goes through the C++ ``_Store``, which
is assembled at compile time from policies (``WithExpiration``, ``WithEviction``,
``WithTags``, ``WithStats``). ``Index`` is ``_Store`` with no policy, so it pays nothing
for features it doesn't use.

How values are stored
=====================

Values take one of two paths, chosen by size:

.. mermaid::

    flowchart LR
        S["set(key, value)"] --> Q{value ≤ 8 KB?}
        Q -->|yes| B["stored inside the<br/>database row"]
        Q -->|no| F["written to a new value file<br/>(exclusive create, uuid+pid name)"]
        F --> R["the row stores the file's<br/>relative path and size"]

- **Small values (≤ 8 KB)** live inside the SQLite row: one transaction, nothing else
  on disk.
- **Large values** get their own file, named ``<uuid>-<pid>`` and spread over a
  two-level directory tree (``ab/cd/…``). The row keeps the *relative* path, so the
  whole cache directory can be moved or copied.
- **Reads** of large values are memory-mapped: the bytes are not copied until your code
  uses them. The last 128 mappings stay open to avoid mapping churn.

Why not put everything in SQLite? Every write goes through SQLite's write-ahead log, so
large values would churn it, and every read would copy them. Files and ``mmap`` avoid
both.

The write path
--------------

.. mermaid::

    sequenceDiagram
        participant U as Your code
        participant S as _Store
        participant D as DiskStorage
        participant Q as SQLite (WAL)

        U->>S: set(key, big_value)
        S->>S: lock, BEGIN EXCLUSIVE
        S->>D: store(value)
        D->>D: create ab/cd/<uuid>-<pid> exclusively, write, close
        D-->>S: relative path
        S->>Q: REPLACE INTO cache (key, path, size, ...)
        Note over S,Q: a trigger queues the old file<br/>in the trash table, same transaction
        S->>Q: COMMIT

Three properties follow from this order:

1. **A row only ever points at a complete file.** The file is written and closed
   before the row that references it commits.
2. **A replaced or deleted file is never removed right away.** A trigger queues its path
   in a ``trash`` table, in the same transaction as the row change. The background
   thread deletes it 5 seconds later. A reader in another process that read the old
   path just before the commit can still open it. And if a user transaction rolls back,
   the trash entry rolls back too, so the file survives with its row.
3. **File names are unique per store.** A random UUID plus the process id, created with
   an exclusive open, so a name clash can never overwrite live data.

A value that can't be written (full disk, quota, permissions) raises :class:`OSError`
with the real error, and nothing is stored.

Thread safety
=============

- Each store instance has one mutex, and every public operation takes it. Sharing one
  ``Cache`` between threads is safe.
- The bindings **release the GIL** around every call into C++. A thread waiting for the
  store never blocks your other Python threads.
- Key iteration takes a snapshot of the keys under the lock, then iterates without it:
  iterating never blocks writers.
- The background thread never takes the store mutex.

Process safety
==============

Several processes can open the same cache directory:

- SQLite runs in **WAL mode**, with a 600 s busy timeout. Readers never block writers,
  and writers take turns.
- Every write that reads before it writes (replacing an entry, ``delete``, ``pop``,
  ``incr``, tag eviction) runs inside a ``BEGIN EXCLUSIVE`` transaction, so it is atomic
  across processes.
- The entry count, total size and file size are kept in the database by triggers, in the
  same transaction as each row change. They are exact across processes, and ``len()``
  is O(1).

Fork safety
-----------

``fork()`` only copies the calling thread: a naive child would inherit locked mutexes
and a SQLite connection it must not use. Every live store registers ``pthread_atfork``
handlers:

- **before the fork**, the background thread is stopped and the store mutex is taken, so
  the fork happens in a quiet state;
- **in the parent**, everything resumes;
- **in the child**, the mutex is reset, the SQLite connection is reopened, the background
  thread restarts, and the random generator for file names is reseeded. Without that,
  parent and children would generate the same file names.

One-shot ``fork → work → exit`` patterns and long-lived forked worker pools are both
covered by regression tests.

Self-healing
============

The design assumes crashes and races *will* happen, and makes them cheap:

.. mermaid::

    flowchart TB
        subgraph automatic["Automatic"]
            R1["get() can't load a file: retry,<br/>re-reading the row's current path"]
            R2["row confirmed dangling (file gone)<br/>→ the row is deleted"]
            R3["background thread: checkpoint the WAL,<br/>evict expired and LRU entries,<br/>drain the trash"]
        end
        subgraph ondemand["On demand: cache.check(fix=True)"]
            C1["SQLite integrity check"]
            C2["dangling rows → deleted"]
            C3["orphaned files → removed"]
            C4["size mismatches → corrected"]
            C5["counters recomputed"]
        end

- **A crash in the middle of a write?** The row never committed, so it never pointed at
  the file. ``check()`` finds the orphaned file, and it doesn't count in ``volume()``.
- **A crash after a commit, before the trash is drained?** The trash entry is in the
  database. The next process's background thread finishes the job.
- **A transient error never becomes data loss.** If a file exists but can't be opened
  (out of file descriptors, permissions), ``get()`` reports a miss and keeps the row.

The background thread
=====================

Each store runs a background thread that, about once per second:

1. checkpoints the WAL without blocking writers (under heavy writes, the writer's own
   automatic checkpoint keeps the WAL around 16 MB);
2. deletes expired entries and, when ``max_size`` is exceeded, evicts the
   least-recently-used entries;
3. drains the trash table.

Durability settings
===================

Every connection runs with ``journal_mode=WAL``, ``synchronous=NORMAL``,
``wal_autocheckpoint=4000``, ``cache_size=10000``, ``temp_store=MEMORY``,
``mmap_size=256MB``, ``recursive_triggers=ON`` and a 600 s busy timeout.
``synchronous=NORMAL`` trades the durability of the very last transaction on a power
loss for much higher throughput. A process crash or a clean exit loses nothing.

Tests
=====

The suite includes multi-process stress tests, fork-safety reproducers for every past
bug, torture tests, property-based (hypothesis) tests, and sanitizer builds (address,
undefined behaviour, threads). A failure mode that ever shipped comes back as a
regression test.
