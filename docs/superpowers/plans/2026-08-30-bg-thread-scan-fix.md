# Plan: remove the background thread's O(N) scans and bound the WAL

Spec: `/home/jeandet/.claude/projects/-var-home-jeandet-Documents-prog-Sciqlop-cache/memory/bg_thread_oN_scan.md`
(measurements + design rationale). Branch: `fix/bg-thread-scan`.

## Problem (measured)

`_checkpoint_loop` in `include/sciqlop_cache/store.hpp` wakes every 1 s and,
holding `_mtx` (the mutex every user write takes), runs full-table scans:
`_resync_counters` (`SELECT COUNT(*), SUM(size) FROM cache`, every store type)
and, for `Cache`, `_bg_evict` (`WHERE expire <= now` scan, `ORDER BY last_use`
scan — neither column indexed). Writers stall for the scan's duration: 2 s per
second at 390k rows. Separately, the bg PASSIVE checkpoint never resets the WAL
under a continuous writer (WAL grew to 8.9 GB with a 3.5 GB DB).

## Design

1. Counters live in the DB, maintained by **incremental** triggers
   (diskcache's design). Note: the triggers this codebase had before `8fc905e`
   recomputed `SUM(size)` on every insert — O(N) per write — which is why they
   were replaced by atomics. The new ones are `value + NEW.size`: O(1).
   Cross-process correct by construction, so `_resync_counters` and the whole
   atomic-counter machinery go away, and with it the reason `_mtx` is taken in
   the bg loop (T1-B).
2. Partial index on `expire`, index on `last_use`.
3. Checkpoint escalation (Litestream pattern): PASSIVE every tick; if the WAL
   still exceeds a page threshold, TRUNCATE with a short busy timeout, giving
   up silently on BUSY.
4. `_mtx` is no longer taken in the bg loop.

## Global Constraints

- **Environment**: `/usr/bin/python3` (3.13) crashes on import of the extension
  (known nanobind issue). The `build/` dir is configured against python3.14 via
  a PATH shim. Every meson/python command must be run as:
  `export PATH=/tmp/claude-6516/-var-home-jeandet-Documents-prog-Sciqlop-cache/47675fdf-2469-4e1f-ad09-c9fa125b1c7b/scratchpad/pybin:$PATH`
  first, and from the repo root `/var/home/jeandet/Documents/prog/Sciqlop-cache`
  (use absolute paths; never `cd build`).
- Build: `meson compile -C build`. Tests: `meson test -C build --timeout-multiplier 4`
  (torture suite exceeds the default 300 s). Baseline before this work: 20/20
  pass with the multiplier. **Never run two builds/test invocations
  concurrently, never background them.**
- Single suite: `meson test -C build sciqlop-cache:<name>`; suites: `database`,
  `basic`, `basic_index`, `intermediate`, `multithreads`, `fanout`, `check`,
  `fork_safety`, `torture`, `concurrency_bugs`, python `test_*`.
- Tests are Catch2 BDD (`SCENARIO/GIVEN/WHEN/THEN`), helpers in
  `tests/common.hpp` (`AutoCleanDirectory`). Types: `Cache`, `Index`,
  `FanoutCache`, `FanoutIndex` from `sciqlop_cache.hpp`.
- Repo has pre-existing untracked files (`.idea/`, `patch.patch`, `benchmark/*.csv`,
  `.coverage`, `docs/superpowers/specs/…`, `docs/superpowers/plans/2026-03-10-*`).
  **Commit with explicit pathspecs only.** Never `git add -A`/`git add .`.
- Public API (C++ and Python) is unchanged: `size()`, `count()`, `volume()`,
  `check()` keep their signatures and meaning. `count()` on types with
  `WithExpiration` keeps querying the DB (deliberate, see T2-B).
- Code style: KISS, no comment-decorated blocks, comments only for "why".
  Match the surrounding style in `store.hpp`.
- Commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01CRTgvEQxqXSLhbvUr5fq1q`

---

## Task 1 — Reproducer tests (must fail before the fix)

New suite `tests/checkpoint/main.cpp`, registered in `meson.build` by adding
`'checkpoint'` to the `foreach test_name:[...]` list at line ~109.

### 1a. Counters are shared across instances immediately

```cpp
#include <catch2/catch_test_macros.hpp>
#include <sciqlop_cache.hpp>
#include "../common.hpp"
#include <string>
#include <thread>
#include <chrono>

SCENARIO("size() and count() are consistent across instances on the same directory without waiting", "[counters]")
{
    GIVEN("two Cache instances opened on the same directory")
    {
        AutoCleanDirectory dir("checkpoint_shared_counters");
        Cache a { dir.path() };
        Cache b { dir.path() };
        WHEN("one instance writes")
        {
            const std::string v(100, 'x');
            REQUIRE(a.set("k1", v));
            REQUIRE(a.set("k2", v));
            THEN("the other sees the new size and count immediately")
            {
                REQUIRE(b.count() == 2);
                REQUIRE(b.size() == 200);
            }
        }
        WHEN("one instance overwrites and deletes")
        {
            REQUIRE(a.set("k1", std::string(100, 'x')));
            REQUIRE(a.set("k1", std::string(50, 'y')));
            REQUIRE(a.set("k2", std::string(100, 'x')));
            REQUIRE(a.del("k2"));
            THEN("size reflects the replace and the delete")
            {
                REQUIRE(b.count() == 1);
                REQUIRE(b.size() == 50);
            }
        }
    }
}
```
Add the same scenario for `Index` (name `"[counters][index]"`). These fail
today because the second instance's atomics only refresh on its 1 s resync.

### 1b. WAL stays bounded under a saturating writer

```cpp
SCENARIO("WAL does not grow without bound under a continuous writer", "[wal]")
{
    GIVEN("a Cache written to continuously for several seconds")
    {
        AutoCleanDirectory dir("checkpoint_wal_bound");
        std::size_t written = 0;
        {
            Cache cache { dir.path() };
            const std::string v(8000, 'x');
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
            std::size_t i = 0;
            while (std::chrono::steady_clock::now() < deadline)
            {
                REQUIRE(cache.set("key-" + std::to_string(i++), v));
                written += v.size();
            }
            THEN("the WAL file is a fraction of what was written (it has been reset)")
            {
                auto wal = dir.path() / "sciqlop-cache.db-wal";
                REQUIRE(std::filesystem::exists(wal));
                REQUIRE(std::filesystem::file_size(wal) < written / 2);
            }
        }
    }
}
```
Rationale for the bound: with the fix the WAL holds at most ~1 tick (1 s) of
writes out of 6 s; today it holds all of them. Measure inside the scope, before
the destructor's final checkpoint.

Run `meson test -C build sciqlop-cache:checkpoint` and confirm **both fail**
(1a on the count/size assertions, 1b on the file_size assertion). Commit the
failing tests: `git commit tests/checkpoint/main.cpp meson.build`.

---

## Task 2 — Trigger-maintained counters, delete the atomic counter machinery

All in `include/sciqlop_cache/store.hpp` unless noted.

### Schema (`_schema_sql`)
Append to the existing schema string (after the `meta` insert of `'size'`):
```sql
INSERT OR IGNORE INTO meta (key, value) VALUES ('count', 0);
CREATE TRIGGER IF NOT EXISTS cache_count_insert AFTER INSERT ON cache BEGIN
  UPDATE meta SET value = value + 1 WHERE key = 'count'; END;
CREATE TRIGGER IF NOT EXISTS cache_count_delete AFTER DELETE ON cache BEGIN
  UPDATE meta SET value = value - 1 WHERE key = 'count'; END;
CREATE TRIGGER IF NOT EXISTS cache_size_insert AFTER INSERT ON cache BEGIN
  UPDATE meta SET value = value + NEW.size WHERE key = 'size'; END;
CREATE TRIGGER IF NOT EXISTS cache_size_update AFTER UPDATE OF size ON cache BEGIN
  UPDATE meta SET value = value + NEW.size - OLD.size WHERE key = 'size'; END;
CREATE TRIGGER IF NOT EXISTS cache_size_delete AFTER DELETE ON cache BEGIN
  UPDATE meta SET value = value - OLD.size WHERE key = 'size'; END;
```
`REPLACE INTO` fires the delete trigger for the displaced row then the insert
trigger (`PRAGMA recursive_triggers=ON` is already set) — so REPLACE paths need
no special handling.

### Migration (`_migrate_schema`)
- Remove the three `DROP TRIGGER IF EXISTS cache_insert_meta/cache_delete_meta/cache_update_size` lines.
- Add a one-time reconciliation for DBs created before this change, guarded by
  a marker row so it runs once per DB, not once per open:
```cpp
if (!_db.exec<std::string>("SELECT value FROM meta WHERE key = 'counters_v';"))
{
    (void)_db.exec("BEGIN IMMEDIATE;"
        "UPDATE meta SET value = (SELECT COUNT(*) FROM cache) WHERE key = 'count';"
        "UPDATE meta SET value = (SELECT COALESCE(SUM(size), 0) FROM cache) WHERE key = 'size';"
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('counters_v', '1');"
        "COMMIT;");
}
```
(Check `Database::exec` templates in `database.hpp` for the exact call shape;
adapt if `exec<std::string>` on a missing row returns an empty optional vs
throws.) This is the last O(N) scan and it runs exactly once per existing DB.

### Reads
- Add compiled statements `META_SIZE_STMT { "SELECT value FROM meta WHERE key = 'size';" }`
  and `META_COUNT_STMT { "SELECT value FROM meta WHERE key = 'count';" }`.
- `size()` → `db()->exec<std::size_t>(META_SIZE_STMT)` (0 if empty).
- `count()` non-expiration branch → `META_COUNT_STMT`. Expiration branch unchanged.
- `_bg_evict` `current_size` → read `'size'` from `bg_db` with a plain
  `sqlite3_prepare_v2` on `"SELECT value FROM meta WHERE key = 'size';"`.
- `_check_counters` → compare meta values against recomputed `COUNT(*)`/`SUM(size)`;
  with `fix`, `UPDATE meta` to the recomputed values. Keep `counters_consistent` semantics.

### Delete
- `_total_size`, `_total_count` members and **every** `fetch_add/fetch_sub/store/load`
  on them (≈30 sites: `_set_impl`, `add`, `del`, `pop`, `incr`, `expire`,
  `evict`, `evict_tag`, `clear`, `check`'s size-mismatch fix, `_bg_evict`).
- `_resync_counters` entirely (both call sites: `_bg_evict` tail and `_checkpoint_loop`).
- `_load_counters`' COUNT/SUM part — **keep** its `last_use`/`_access_seq`
  seeding (the reopen-LRU fix); rename to `_seed_access_seq` if only that remains.
- Grep the whole `include/` tree for `_total_size|_total_count|_resync_counters`
  afterwards: zero hits.

### Verify
- `meson test -C build sciqlop-cache:checkpoint` → 1a scenarios pass (1b still fails).
- Full suite with multiplier passes. The `check` suite exercises
  `counters_consistent`; python `test_python_interface` exercises `size()/len()`.
- Commit `include/sciqlop_cache/store.hpp` (+ any test adjustments) only.

---

## Task 3 — Indexes for `expire` and `last_use`

`include/sciqlop_cache/policies.hpp`:
- `WithExpiration::extra_indexes()` →
  `"CREATE INDEX IF NOT EXISTS idx_cache_expire ON cache(expire) WHERE expire IS NOT NULL;"`
- `WithEviction::extra_indexes()` →
  `"CREATE INDEX IF NOT EXISTS idx_cache_last_use ON cache(last_use);"`

`_schema_sql` already appends `_extra_schema_indexes()` and runs on every open
(`IF NOT EXISTS` → idempotent, so existing DBs get the indexes on next open).
Verify with a small test in `tests/checkpoint/main.cpp`: open a `Cache`, then
`SELECT name FROM sqlite_master WHERE type='index'` via a raw `sqlite3_open`
on `dir/sciqlop-cache.db` contains both names. Full suite passes. Commit.

---

## Task 4 — Checkpoint loop: no `_mtx`, WAL reset escalation

`_checkpoint_loop` in `store.hpp`:

1. After `cp_db` opens successfully: run
   `sqlite3_exec(cp_db, "SELECT count(*) FROM sqlite_master;", nullptr, nullptr, nullptr);`
   with a comment: *a connection that has never read the DB has no WAL
   handle, and `sqlite3_wal_checkpoint_v2` on it is a silent no-op returning
   SQLITE_OK.* Then `sqlite3_busy_timeout(cp_db, 100);`.
2. Per tick replace the single PASSIVE call with:
```cpp
int wal_pages = 0;
sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, &wal_pages, nullptr);
if (wal_pages > WAL_TRUNCATE_THRESHOLD_PAGES)
    sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr);
```
   with `static constexpr int WAL_TRUNCATE_THRESHOLD_PAGES = 4000;` (≈16 MB at
   the default 4 KB page) and a "why" comment: *a PASSIVE checkpoint backfills
   but can only reset the WAL when no writer is active; under a continuous
   writer the file grows without bound (checkpoint starvation, sqlite.org/wal.html).
   TRUNCATE takes the writer lock briefly; on SQLITE_BUSY (100 ms) we simply
   retry next tick.*
3. Remove the `std::lock_guard mtx_guard(_mtx);` and its comment block from the
   loop — nothing left in the loop touches process-local state (`_bg_evict`
   works on `bg_db` and `storage`, which has its own mutex).
4. Keep the shutdown PASSIVE checkpoint; also run a TRUNCATE there so a closed
   store leaves a small WAL.

Verify: `sciqlop-cache:checkpoint` fully passes (1b now green). Full suite
passes including `fork_safety`, `multithreads`, `concurrency_bugs`,
`test_concurrency_bugs` (the `CounterBgResyncRace` reproducer must still pass —
the race it guards against is now structurally impossible). Commit.

---

## Task 5 — Benchmark target + docs

- Add `tests/bench_bgscan/main.cpp` from
  `/tmp/claude-6516/-var-home-jeandet-Documents-prog-Sciqlop-cache/47675fdf-2469-4e1f-ad09-c9fa125b1c7b/scratchpad/bench_bgscan.cpp`
  (copy verbatim; fix the include to `<sciqlop_cache.hpp>` if needed), registered in
  `meson.build` as `benchmark('bgscan', bench_bgscan_exe, args: ['--n', '100000'], timeout: 600)`
  next to the existing test registrations (no Google Benchmark dependency).
- Run it once for 400k rows before/after (checkout `main` build not required:
  the numbers from the spec are the "before"). Record the after-numbers in the
  report file: total time, max stall, WAL size at 400k.
- `CLAUDE.md`: in *Key Design Decisions*, replace the bullets about in-memory
  atomic counters / BG resync with: counters are `meta` rows kept by
  incremental triggers (cross-process exact, O(1) read); bg thread does
  PASSIVE + threshold TRUNCATE checkpoints and eviction only, never takes `_mtx`.
  Update the *Available test suites* list with `sciqlop-cache:checkpoint`.
- Commit `tests/bench_bgscan/main.cpp meson.build CLAUDE.md`.
