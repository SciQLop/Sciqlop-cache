===
FAQ
===

Do I need to call ``close()``?
==============================

No. A cache closes itself when it is garbage-collected. ``close()`` exists for code
written for diskcache. Closing is final: using the cache afterwards raises
:class:`RuntimeError`.

Can I move or copy a cache directory?
=====================================

Yes, while no process has it open. Rows store the paths of value files relative to the
cache directory, so a moved or copied cache still works.

Can I put a cache on a network filesystem?
==========================================

No. The cache relies on SQLite's WAL mode, which needs a local filesystem: its shared
memory and locks don't work over NFS or SMB. The processes sharing a cache must run on
the same machine.

What happens when the disk is full?
===================================

The ``set()`` or ``add()`` that can't write raises :class:`OSError` (with the real
error, for instance ``ENOSPC``), and nothing is stored. The cache stays usable.

How do I empty a cache, or delete it?
=====================================

``cache.clear()`` removes every entry. To delete the cache entirely, close it (or let it
be garbage-collected) and remove its directory.

How large can a value be?
=========================

As large as your disk allows. Values above 8 KB live in their own file and are read
through ``mmap``, so large values don't go through SQLite.

Is it safe to load a cache someone else wrote?
==============================================

Not with the pickle-based serializers: unpickling can run code. Only load caches you
(or code you trust) wrote. :class:`~pysciqlop_cache.MsgspecSerializer` only builds plain
data types.

Why does ``len()`` count an entry that just expired?
====================================================

``len()`` reads a counter kept in the database, which is O(1). An entry that expired but
hasn't been deleted yet by the background thread still counts, for about a second.
``get()`` never returns it. diskcache's ``len()`` behaves the same.

Does it work on Windows?
========================

Yes. One difference: Windows can't delete a file while a value still maps it. Such files
are deleted later, once the value is released.

Which store should I use?
=========================

See :doc:`stores`. In short: ``Cache`` by default, ``Index`` for a plain persistent
dict, and the ``Fanout`` variants only when many processes write heavily at the same
time.
