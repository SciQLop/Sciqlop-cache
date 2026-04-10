"""Migrate a diskcache.Cache, FanoutCache, or Index into sciqlop-cache.

Usage:
    python -m pysciqlop_cache.migrate /path/to/diskcache /path/to/sciqlop-cache
    python -m pysciqlop_cache.migrate --drop /old/cache /new/cache
    python -m pysciqlop_cache.migrate --type index /old/index /new/index
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
    # diskcache.Index wraps a ._cache; use it for _sql access
    sql_source = dc._cache if hasattr(dc, '_cache') else dc
    for key in list(dc):
        value = dc.get(key) if hasattr(dc, 'get') else dc[key]
        if value is None:
            continue
        key_str = str(key) if not isinstance(key, str) else key
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
        If True, delete each entry from the source after successful migration.
    shard_count : int or None
        If set, create a FanoutCache/FanoutIndex with this many shards.
        Auto-detected from source if source is a FanoutCache.
    store_type : str
        "cache" (default) or "index". Controls destination type and whether
        expire/tag metadata is preserved.

    Returns
    -------
    dict with keys: migrated, skipped, errors, elapsed_secs
    """
    import diskcache as dc
    from pysciqlop_cache import Cache, FanoutCache, Index, FanoutIndex

    src_path = Path(src_path)
    dst_path = Path(dst_path)
    is_index = store_type == "index"

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
        help="Delete entries from source after migration",
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
