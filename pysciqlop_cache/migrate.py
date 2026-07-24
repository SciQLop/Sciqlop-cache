"""Migrate a diskcache.Cache, FanoutCache, or Index into sciqlop-cache.

Usage:
    python -m pysciqlop_cache.migrate /path/to/diskcache /path/to/sciqlop-cache
    python -m pysciqlop_cache.migrate --drop /old/cache /new/cache
    python -m pysciqlop_cache.migrate --type index /old/index /new/index
"""

import argparse
import shutil
import time
import sys
from pathlib import Path

# Sentinel yielded by _iter_diskcache_entries for a key whose value could not be
# deserialized (e.g. pickled by an incompatible library version -- a different numpy
# major version is the most common real-world case). Least-astonishment principle:
# one unreadable legacy entry should not abort migration of every other entry, it
# should be warned about and skipped.
_UNREADABLE = object()


class InsufficientDiskSpaceError(OSError):
    """Not enough free disk space to safely attempt a migration."""


def _dir_size(path):
    """Total size in bytes of every file under path (recursively). 0 if path
    doesn't exist."""
    path = Path(path)
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob('*') if f.is_file())


def _largest_file_size(path):
    """Size in bytes of the single largest file under path (recursively). 0 if
    path doesn't exist or has no files."""
    path = Path(path)
    if not path.exists():
        return 0
    sizes = [f.stat().st_size for f in path.rglob('*') if f.is_file()]
    return max(sizes) if sizes else 0


def _existing_ancestor(path):
    """The nearest existing ancestor of path (path itself if it already exists) --
    shutil.disk_usage needs a path that actually exists."""
    path = Path(path)
    while not path.exists():
        parent = path.parent
        if parent == path:  # reached the filesystem root without finding one
            break
        path = parent
    return path


def _ensure_enough_disk_space(src_path, dst_path, move=False, margin=1.2):
    """Raise InsufficientDiskSpaceError if the destination's filesystem doesn't have
    enough free space to safely attempt the migration (plus a safety margin for
    format overhead). Checked upfront, before anything is written, so a migration
    that can't possibly fit doesn't get abandoned half-written instead -- predictable
    failure beats a disk filling up mid-migration.

    In copy mode (move=False), the whole source is preserved untouched throughout
    migration, so a full second copy of its data must fit alongside it. In move
    mode (move=True, i.e. called with drop=True), each source entry is deleted as
    soon as it's written to the destination, so at most one entry's worth of
    duplication ever exists at a time -- the estimate is based on the single
    largest entry instead of the whole source.
    """
    needed = (_largest_file_size(src_path) if move else _dir_size(src_path)) * margin
    free = shutil.disk_usage(_existing_ancestor(dst_path)).free
    if free < needed:
        raise InsufficientDiskSpaceError(
            f"Not enough free disk space to migrate {src_path} to {dst_path}: "
            f"need ~{needed / 1e6:.1f} MB (with a {margin}x safety margin), only "
            f"{free / 1e6:.1f} MB free."
        )


def _remaining_ttl(expire_time):
    """Convert diskcache absolute expire_time to remaining seconds, or None."""
    if expire_time is None:
        return None
    remaining = expire_time - time.time()
    return remaining if remaining > 0 else 0


def _iter_diskcache_entries(dc):
    """Yield (key_str, value_bytes, ttl_secs_or_none, tag_or_none) from a diskcache.

    A key whose value fails to deserialize yields ``_UNREADABLE`` as its value
    instead of raising, so one bad entry doesn't abort every entry after it.
    """
    # diskcache.Index wraps a ._cache; use it for _sql access
    sql_source = dc._cache if hasattr(dc, '_cache') else dc
    for key in list(dc):
        key_str = str(key) if not isinstance(key, str) else key
        try:
            value = dc.get(key) if hasattr(dc, 'get') else dc[key]
        except Exception as e:
            print(f"  warning: could not read entry {key_str!r}, skipping ({e})", file=sys.stderr)
            yield key_str, _UNREADABLE, None, None
            continue
        if value is None:
            continue
        row = sql_source._sql(
            "SELECT expire_time, tag FROM Cache WHERE key=?", (key,)
        ).fetchone()
        ttl = _remaining_ttl(row[0]) if row else None
        tag = row[1] if row else None
        # Skip already-expired entries
        if ttl is not None and ttl <= 0:
            continue
        yield key_str, value, ttl, tag


def _iter_fanout_entries(fc):
    """Yield entries from a diskcache.FanoutCache (iterates each shard)."""
    for shard in fc._shards:
        yield from _iter_diskcache_entries(shard)


def migrate(src_path, dst_path, *, drop=False, shard_count=None, store_type="cache"):
    """Migrate entries from a diskcache directory into sciqlop-cache.

    Parameters
    ----------
    src_path : str or Path
        Path to the source diskcache directory.
    dst_path : str or Path
        Path to the destination sciqlop-cache directory.
    drop : bool
        Move instead of copy: if True, delete each entry from the source
        immediately after it's successfully written to the destination, rather
        than preserving the whole source untouched. Uses much less peak disk
        space (at most one entry's worth of duplication at a time, instead of a
        full second copy of everything) at the cost of not being able to fall
        back to an intact source if something turns out wrong with the
        destination afterwards.
    shard_count : int or None
        If set, create a FanoutCache/FanoutIndex with this many shards.
        Auto-detected from source if source is a FanoutCache.
    store_type : str
        "cache" (default) or "index". Controls destination type and whether
        expire/tag metadata is preserved.

    Returns
    -------
    dict with keys: migrated, skipped, errors, elapsed_secs

    Raises
    ------
    InsufficientDiskSpaceError
        If the destination's filesystem doesn't have enough free space for a full
        copy of src_path's data. Raised before anything is written to dst_path.
    """
    import diskcache as dc
    from pysciqlop_cache import Cache, FanoutCache, Index, FanoutIndex

    src_path = Path(src_path)
    dst_path = Path(dst_path)
    is_index = store_type == "index"

    _ensure_enough_disk_space(src_path, dst_path, move=drop)

    # Detect source type
    shard_dbs = list(src_path.glob("*/cache.db"))
    is_fanout = len(shard_dbs) > 0
    if is_fanout:
        detected_shards = len(shard_dbs)
        src = dc.FanoutCache(str(src_path), shards=detected_shards)
        if shard_count is None:
            shard_count = detected_shards
        entries = _iter_fanout_entries(src)
    else:
        if is_index:
            src = dc.Index(str(src_path))
        else:
            src = dc.Cache(str(src_path))
        entries = _iter_diskcache_entries(src)

    # Create destination
    if is_index:
        if shard_count is not None:
            dst = FanoutIndex(str(dst_path), shard_count=shard_count)
        else:
            dst = Index(str(dst_path))
    else:
        if shard_count is not None:
            dst = FanoutCache(str(dst_path), shard_count=shard_count)
        else:
            dst = Cache(str(dst_path))

    migrated = 0
    skipped = 0
    errors = 0
    t0 = time.monotonic()

    for key_str, value, ttl, tag in entries:
        if value is _UNREADABLE:
            skipped += 1
            continue
        try:
            if is_index:
                dst.set(key_str, value)
            else:
                kwargs = {}
                if ttl is not None:
                    kwargs["expire"] = ttl
                if tag is not None:
                    kwargs["tag"] = tag
                dst.set(key_str, value, **kwargs)
            if drop:
                try:
                    if is_index and not is_fanout:
                        del src[key_str]
                    else:
                        src.delete(key_str)
                except Exception:
                    pass  # best-effort deletion
            migrated += 1
        except Exception as e:
            print(f"  error migrating {key_str!r}: {e}", file=sys.stderr)
            errors += 1

    elapsed = time.monotonic() - t0
    if hasattr(src, 'close'):
        src.close()

    return {
        "migrated": migrated,
        "skipped": skipped,
        "errors": errors,
        "elapsed_secs": round(elapsed, 2),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Migrate diskcache entries to sciqlop-cache"
    )
    parser.add_argument("src", help="Source diskcache directory")
    parser.add_argument("dst", help="Destination sciqlop-cache directory")
    parser.add_argument(
        "--drop", action="store_true",
        help="Move instead of copy: delete each entry from source as soon as it's "
             "migrated, using much less peak disk space at the cost of the source "
             "no longer being an intact fallback afterwards",
    )
    parser.add_argument(
        "--shards", type=int, default=None,
        help="Number of shards for destination FanoutCache/FanoutIndex (auto-detected from source)",
    )
    parser.add_argument(
        "--type", choices=["cache", "index"], default="cache",
        help="Source/destination store type: 'cache' (default) or 'index'",
    )
    args = parser.parse_args()

    print(f"Migrating ({args.type}): {args.src} -> {args.dst}")
    if args.drop:
        print("  (dropping entries from source after migration)")

    result = migrate(args.src, args.dst, drop=args.drop, shard_count=args.shards,
                     store_type=args.type)

    print(f"Done: {result['migrated']} migrated, {result['errors']} errors "
          f"in {result['elapsed_secs']}s")
    if result["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
