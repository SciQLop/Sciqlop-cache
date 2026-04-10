"""Migrate a diskcache.Cache or diskcache.FanoutCache into sciqlop-cache.

Usage:
    python -m pysciqlop_cache.migrate /path/to/diskcache /path/to/sciqlop-cache
    python -m pysciqlop_cache.migrate --drop /old/cache /new/cache
"""

import argparse
import time
import sys
from pathlib import Path


def _remaining_ttl(expire_time):
    """Convert diskcache absolute expire_time to remaining seconds, or None."""
    if expire_time is None:
        return None
    remaining = expire_time - time.time()
    return remaining if remaining > 0 else 0


def _iter_diskcache_entries(dc):
    """Yield (key_str, value_bytes, ttl_secs_or_none, tag_or_none) from a diskcache."""
    for key in list(dc):
        value = dc.get(key)
        if value is None:
            continue
        key_str = str(key) if not isinstance(key, str) else key
        row = dc._sql(
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


def migrate(src_path, dst_path, *, drop=False, shard_count=None):
    """Migrate entries from a diskcache directory into sciqlop-cache.

    Parameters
    ----------
    src_path : str or Path
        Path to the source diskcache directory.
    dst_path : str or Path
        Path to the destination sciqlop-cache directory.
    drop : bool
        If True, delete each entry from the source after successful migration.
    shard_count : int or None
        If set, create a FanoutCache with this many shards. Otherwise creates
        a plain Cache. Auto-detected from source if source is a FanoutCache.

    Returns
    -------
    dict with keys: migrated, skipped, errors, elapsed_secs
    """
    import diskcache as dc
    from pysciqlop_cache import Cache, FanoutCache

    src_path = Path(src_path)
    dst_path = Path(dst_path)

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
        src = dc.Cache(str(src_path))
        entries = _iter_diskcache_entries(src)

    # Create destination
    if shard_count is not None:
        dst = FanoutCache(str(dst_path), shard_count=shard_count)
    else:
        dst = Cache(str(dst_path))

    migrated = 0
    skipped = 0
    errors = 0
    t0 = time.monotonic()

    for key_str, value, ttl, tag in entries:
        try:
            kwargs = {}
            if ttl is not None:
                kwargs["expire"] = ttl
            if tag is not None:
                kwargs["tag"] = tag
            dst.set(key_str, value, **kwargs)
            if drop:
                try:
                    src.delete(key_str)
                except Exception:
                    pass  # best-effort deletion
            migrated += 1
        except Exception as e:
            print(f"  error migrating {key_str!r}: {e}", file=sys.stderr)
            errors += 1

    elapsed = time.monotonic() - t0
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
        help="Delete entries from source after migration",
    )
    parser.add_argument(
        "--shards", type=int, default=None,
        help="Number of shards for destination FanoutCache (auto-detected from source)",
    )
    args = parser.parse_args()

    print(f"Migrating: {args.src} -> {args.dst}")
    if args.drop:
        print("  (dropping entries from source after migration)")

    result = migrate(args.src, args.dst, drop=args.drop, shard_count=args.shards)

    print(f"Done: {result['migrated']} migrated, {result['errors']} errors "
          f"in {result['elapsed_secs']}s")
    if result["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
