# diskcache API compatibility review

Status 2026-09-16: items 1, 2, 3, 5, 6, 7, 12 and `set()` returning `True` (part of 13) are
implemented (branch `fix/touch-diskcache-parity`, tests in `tests/python/test_diskcache_compat.py`).
`Timeout` is real: the busy timeout is configurable via `timeout=` and raised as
`pysciqlop_cache.Timeout`. Item 4 (`read=`) raises `NotImplementedError` instead of `TypeError`.
The rest of this document is the original audit, kept as the backlog for the remaining items.

Date: 2026-09-15. Compared `pysciqlop_cache` (branch `fix/touch-diskcache-parity`, build/) against
diskcache 5.6.3 (and 5.4.0, identical on every probe). Method: `inspect`-based signature diff of
`Cache`, `FanoutCache`, `Index` plus ~75 side-by-side behavioural probes executed against both
libraries. Every finding below was observed, not inferred. Issues #11-#14 were all of this class;
the ranking is by how likely a diskcache user is to hit each one.

## Tier 1: common idioms that raise today

| # | diskcache idiom | Ours | Fix cost |
|---|---|---|---|
| 1 | Any hashable key: `cache[42]`, `cache[("a", 1)]`, `cache[b"k"]` | `TypeError` (keys must be `str`) | Medium. Encode non-str keys reversibly in the wrapper (diskcache's `Disk.put` does int/float/bytes/pickle). `keys()`/iteration must decode. |
| 2 | `retry=True` on `set/get/add/delete/pop/incr/decr/touch/expire/clear/check/transact` | `TypeError: unexpected keyword` | Trivial. Accept and ignore in the wrapper; there is no timeout concept here. |
| 3 | `get(key, tag=True)`, `get(key, expire_time=True)` return `(value, meta)`; `pop` likewise | `TypeError` | Medium. C++ `get` must return expire/tag alongside the value. |
| 4 | `get(key, read=True)` / `set(key, fileobj, read=True)` | `TypeError` | Small. Wrap the memoryview in `io.BytesIO`; read the file object on set. |
| 5 | Constructor: `Cache(directory=...)`, `size_limit=`, `eviction_policy=`, `statistics=`, `cull_limit=`, `timeout=`, `disk=`, `tag_index=`; `FanoutCache(shards=, timeout=)` | `TypeError` for every one; ours are `cache_path`, `max_size`, `shard_count`, `serializer` | Small. Alias `directory`/`size_limit`/`shards`; accept and ignore the sqlite/disk tuning keys; reject `eviction_policy` other than LRS/LRU with a clear message. |
| 6 | `Index` is a `MutableMapping`: `items()`, `values()`, `update()`, `setdefault()`, `popitem()`, `peekitem()`, `Index(dir, mapping, **kw)` | `AttributeError` for all | Trivial. Inherit `collections.abc.MutableMapping`; fix `__getitem__` to raise `KeyError` (see 7) and the mixins come for free. |
| 7 | `cache[missing]`, `del cache[missing]`, `Index[missing]`, `Index.pop(missing)` raise `KeyError` | Return `None` / silent | Trivial. This is the same silent-`None` shape as issue #12. |
| 8 | `with fanout.transact():` (no key, spans all shards) | `TypeError: missing 'key'` | Hard (cross-shard transaction). Already bit Speasy (backlog). At minimum accept no key and document the non-atomic fallback. |
| 9 | `cache.evict(tag)` removes by tag; `cache.cull()` runs the size policy | `evict()` takes no arg and is the LRU cull; tag removal is `evict_tag(tag)`; `cull` missing | Small. `evict(tag)` overload delegating to `evict_tag`, `cull()` alias of the no-arg `evict()`. |
| 10 | `hits, misses = cache.stats()`; `stats(enable=True)`, `stats(reset=True)` | Returns a dict, so tuple-unpacking silently yields the strings `'hits'`,`'misses'`; kwargs raise | Small. Return a tuple; map `reset=` to `reset_stats()`. |
| 11 | `pickle.dumps(cache)` (passing a cache to worker processes) | `TypeError: cannot pickle` | Small. `__reduce__` -> `(Cache, (path, max_size, serializer))`. |
| 12 | `except diskcache.Timeout:` | `AttributeError` | Trivial. Export a `Timeout` exception class, never raised. |

## Tier 2: silent semantic differences (no error, different result)

| # | diskcache | Ours |
|---|---|---|
| 13 | `set()` returns `True`; `clear()`/`expire()`/`evict()`/`cull()` return counts | `None` everywhere. `assert cache.set(k, v)` fails. C++ `clear`/`expire` need to return counts. |
| 14 | `Cache()` with no args creates a fresh temp dir | Uses `.cache/` in the cwd, shared across calls. |
| 15 | Default `size_limit` is 1 GiB, policy least-recently-stored | Unbounded by default, LRU only. A migrated cache silently stops evicting. |
| 16 | `len(cache)` counts expired-but-unevicted rows | Excludes them (backlog T2-B). |
| 17 | Iteration is insertion order (rowid) | Key order (`['a','b','c']` for inserts `b,a,c`). |
| 18 | `memoize(name=None, typed=False, expire=None, tag=None, ignore=())`, `__cache_key__` returns a tuple | `memoize(expire=None, tag=None, typed=False, version_aware=False)`. `@cache.memoize("name")` passes the name as `expire` and dies inside `set()` with a misleading `TypeError`; `ignore=` unsupported; `__cache_key__` returns a hashed `str`. |
| 19 | `close()` is cheap and lazily reopens | One-way shutdown, `RuntimeError` on later use. Documented in README (issues #11/#12); keep, but it is the one remaining divergence users of `with cache:` patterns can still trip on. |
| 20 | `incr(key, default=None)` on a missing key raises `KeyError` | `TypeError` from `None + 1`. |
| 21 | `reversed(cache)` | No `__reversed__`; Python falls back to the sequence protocol and calls `__getitem__(int)`, which raises `TypeError` on a non-empty cache. |

## Tier 3: missing surface (documented in the backlog, low traffic)

`Deque`, `RLock`, `BoundedSemaphore`, `throttle`, `barrier`, `memoize_stampede`, `Averager`,
`Disk`/`JSONDisk`, `ENOVAL`, `DEFAULT_SETTINGS`; `Cache.read()`, `push/pull/peek/peekitem`,
`create_tag_index/drop_tag_index`, `reset(key, value)`, `directory`/`timeout`/`disk` properties
(`path()` exists), `iterkeys(reverse=True)`, `FanoutCache.cache(name)/index(name)/deque(name)`.

## Where ours is a superset (fine)

`expire=` accepts `timedelta` (diskcache raises), `evict_tag`, `keys()` on `Cache`, `exists()`,
`count()`, `size()`, `set_meta/get_meta`, `serializer`, `lock()` method, `version_aware` memoize,
`Lock.release()` returning a lost-lock bool.

## Suggested order

1. Wrapper-only batch, one PR, no C++ change: items 2, 5, 6, 7, 9, 10, 11, 12, 18 (signature
   only), 20, 21, 13 for `set()`. Covers most of the next dozen issues.
2. Small C++ additions: 13 (counts from `clear`/`expire`), 3 (expire/tag out of `get`), 4,
   `iterkeys(reverse)`.
3. Design decisions to take explicitly: 1 (key encoding), 8 (cross-shard transact), 14/15
   (defaults), 17 (order), 19 (close).

Probe scripts: `docs/compat-probes/{apidiff,behav,behav2}.py`; re-run them
after each batch to track the delta.
