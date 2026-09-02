#pragma once

#include "database.hpp"
#include "disk_storage.hpp"
#include "policies.hpp"
#include "utils/concepts.hpp"
#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <optional>
#include <span>
#include <sqlite3.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <cstdint>
#include <new>
#ifdef _WIN32
#include <process.h>
inline int _sq_getpid() { return _getpid(); }
using _sq_pid_t = int;
#else
#include <pthread.h>
#include <unistd.h>
inline pid_t _sq_getpid() { return getpid(); }
using _sq_pid_t = pid_t;
#endif

// Classification query for DiskStorage's collision-healing path (the
// is_referenced callback in _Store::_set_impl): decides whether an existing
// blob path is owned by a live cache row (genuine collision -> regenerate)
// or is an orphaned leftover (safe to overwrite and reuse). It must cover the
// trash table too: a displaced path spends 5 s there pending deferred
// deletion, and classifying it as orphaned would let the heal reuse a file
// that _drain_trash then unlinks — orphaning the new row. Defined at
// namespace scope so tests exercise the exact production SQL.
inline constexpr std::string_view IS_PATH_REFERENCED_SQL
    = "SELECT 1 FROM cache WHERE path = ? UNION ALL "
      "SELECT 1 FROM trash WHERE path = ? LIMIT 1;";

// --- Fork safety -----------------------------------------------------------
// A live Store owns a background checkpoint thread that periodically holds the
// store's mutex AND allocates / runs SQLite. POSIX fork() clones only the
// calling thread, so a child forked while that thread is active inherits not
// just the store mutex but libc-level locks (malloc, the WAL shared-memory
// connection) in a locked state with no thread to release them — touching the
// inherited store would then deadlock. A child cannot reset libc's locks, so
// recovery-after-the-fact is impossible; instead we quiesce the store *before*
// every fork via pthread_atfork handlers and rebuild it afterwards.
//
// All live stores register in a process-global set. The handlers run for every
// fork in the process (the cost is paid only at fork time):
//   prepare (parent, pre-fork): stop+join each checkpoint thread (closing its
//       connection) and take each store mutex, so the forking thread is the
//       only one running and no background lock is held.
//   parent  (post-fork): release each mutex and restart each checkpoint thread.
//   child   (post-fork): reset each mutex, reopen each SQLite connection and
//       restart each checkpoint thread on a clean slate.
class _ForkAware
{
public:
    virtual ~_ForkAware() = default;
    virtual void _fork_prepare() = 0;
    virtual void _fork_parent() = 0;
    virtual void _fork_child() = 0;
};

inline std::mutex& _fork_registry_mutex()
{
    static std::mutex m;
    return m;
}

inline std::unordered_set<_ForkAware*>& _fork_registry()
{
    static std::unordered_set<_ForkAware*> set;
    return set;
}

inline void _ensure_atfork_registered()
{
#ifndef _WIN32
    static std::once_flag once;
    std::call_once(once, [] {
        pthread_atfork(
            [] { _fork_registry_mutex().lock();
                 for (auto* s : _fork_registry()) s->_fork_prepare(); },
            [] { for (auto* s : _fork_registry()) s->_fork_parent();
                 _fork_registry_mutex().unlock(); },
            [] { for (auto* s : _fork_registry()) s->_fork_child();
                 _fork_registry_mutex().unlock(); });
    });
#endif
}

inline void _register_fork_aware(_ForkAware* s)
{
    _ensure_atfork_registered();
    std::lock_guard<std::mutex> g(_fork_registry_mutex());
    _fork_registry().insert(s);
}

inline void _unregister_fork_aware(_ForkAware* s)
{
    std::lock_guard<std::mutex> g(_fork_registry_mutex());
    _fork_registry().erase(s);
}

template <typename Storage, typename... Policies>
class _Store : private Policies..., private _ForkAware
{
    static constexpr bool has_expiration = has_policy_v<WithExpiration, Policies...>;
    static constexpr bool has_eviction = has_policy_v<WithEviction, Policies...>;
    static constexpr bool has_tags = has_policy_v<WithTags, Policies...>;
    static constexpr bool has_stats = has_policy_v<WithStats, Policies...>;

    std::filesystem::path cache_path;
    size_t max_size;
    std::unique_ptr<Storage> storage;
    std::size_t _file_size_threshold = 8 * 1024;

    std::thread _checkpoint_thread;
    std::atomic<bool> _stop_checkpoint { false };
    std::mutex _checkpoint_mutex;
    std::condition_variable _checkpoint_cv;
    _sq_pid_t _owner_pid;
    mutable Database _db;
    mutable std::recursive_mutex _mtx;
    // Depth counter for nested transactions. Only the outermost level
    // (depth 0→1) issues an actual SQLite BEGIN/COMMIT; nested levels are
    // logical no-ops. Always read/written under _mtx.
    std::size_t _txn_depth = 0;

    // --- SQL building helpers (derived from policy fold expressions) ---

    static std::string _where_valid()
    {
        return (Policies::where_valid() + ... + std::string {});
    }

    static std::string _extra_schema_columns()
    {
        return (Policies::extra_columns() + ... + std::string {});
    }

    static std::string _extra_schema_indexes()
    {
        return (Policies::extra_indexes() + ... + std::string {});
    }

    static std::string _insert_extra_cols()
    {
        return (Policies::insert_columns() + ... + std::string {});
    }

    static std::string _insert_extra_placeholders()
    {
        return (Policies::insert_placeholders() + ... + std::string {});
    }

    // --- Compiled statements (SQL built from policies) ---

    CompiledStatement KEYS_STMT {
        std::string("SELECT key FROM cache WHERE 1=1") + _where_valid() + ";"
    };
    CompiledStatement EXISTS_STMT {
        std::string("SELECT 1 FROM cache WHERE key = ?") + _where_valid() + " LIMIT 1;"
    };
    CompiledStatement GET_STMT {
        std::string("SELECT value, path FROM cache WHERE key = ?") + _where_valid() + ";"
    };
    CompiledStatement GET_PATH_SIZE_STMT { "SELECT path, size FROM cache WHERE key = ?;" };
    CompiledStatement REPLACE_VALUE_STMT {
        std::string("REPLACE INTO cache (key, value, size") + _insert_extra_cols()
        + ", path) VALUES (?, ?, ?" + _insert_extra_placeholders() + ", NULL);"
    };
    CompiledStatement REPLACE_PATH_STMT {
        std::string("REPLACE INTO cache (key, path, size") + _insert_extra_cols()
        + ", value) VALUES (?, ?, ?" + _insert_extra_placeholders() + ", NULL);"
    };
    CompiledStatement INSERT_VALUE_STMT {
        std::string("INSERT OR IGNORE INTO cache (key, value, size") + _insert_extra_cols()
        + ") VALUES (?, ?, ?" + _insert_extra_placeholders() + ");"
    };
    CompiledStatement INSERT_PATH_STMT {
        std::string("INSERT OR IGNORE INTO cache (key, path, size") + _insert_extra_cols()
        + ") VALUES (?, ?, ?" + _insert_extra_placeholders() + ");"
    };
    CompiledStatement DELETE_STMT { "DELETE FROM cache WHERE key = ?;" };
    CompiledStatement SET_META_STMT { "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?);" };
    CompiledStatement GET_META_STMT { "SELECT value FROM meta WHERE key = ?;" };
    CompiledStatement META_SIZE_STMT { "SELECT value FROM meta WHERE key = 'size';" };
    CompiledStatement META_COUNT_STMT { "SELECT value FROM meta WHERE key = 'count';" };
    CompiledStatement META_FILE_SIZE_STMT { "SELECT value FROM meta WHERE key = 'file_size';" };

    // Incr/decr statements
    CompiledStatement INCR_GET_STMT {
        std::string("SELECT value FROM cache WHERE key = ?") + _where_valid() + ";"
    };
    static std::string _incr_update_sql()
    {
        std::string sql = "UPDATE cache SET value = ?, size = ?";
        if constexpr (has_eviction) sql += ", last_use = ?";
        sql += ", path = NULL WHERE key = ?;";
        return sql;
    }
    CompiledStatement INCR_UPDATE_STMT { _incr_update_sql() };

    // Placeholder for conditional statements — accepts any initializer, does nothing
    struct NoStmt
    {
        constexpr NoStmt() = default;
        constexpr NoStmt(const char*) {}
        constexpr NoStmt(const std::string&) {}
        bool compile(sqlite3*) { return true; }
        bool finalize() { return true; }
    };

    [[no_unique_address]] std::conditional_t<has_expiration, CompiledStatement, NoStmt>
        TOUCH_STMT { "UPDATE cache SET expire = ? WHERE key = ?;" };
    [[no_unique_address]] std::conditional_t<has_expiration, CompiledStatement, NoStmt>
        EXPIRE_STMT { "SELECT path, size FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');" };
    [[no_unique_address]] std::conditional_t<has_expiration, CompiledStatement, NoStmt>
        EVICT_EXPIRED_STMT { "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');" };

    [[no_unique_address]] std::conditional_t<has_eviction, CompiledStatement, NoStmt>
        UPDATE_LAST_USE_STMT {
            "UPDATE cache SET last_use = ?, "
            "access_count_since_last_update = access_count_since_last_update + 1 "
            "WHERE key = ?;"
        };
    [[no_unique_address]] std::conditional_t<has_eviction, CompiledStatement, NoStmt>
        EVICT_LRU_STMT { "SELECT key, path, size FROM cache ORDER BY last_use ASC;" };

    [[no_unique_address]] std::conditional_t<has_tags, CompiledStatement, NoStmt>
        EVICT_TAG_PATH_STMT { "SELECT path FROM cache WHERE tag = ?;" };
    [[no_unique_address]] std::conditional_t<has_tags, CompiledStatement, NoStmt>
        EVICT_TAG_STMT { "DELETE FROM cache WHERE tag = ?;" };

    auto _all_statements()
    {
        std::vector<CompiledStatement*> stmts = {
            &KEYS_STMT, &EXISTS_STMT, &GET_STMT,
            &GET_PATH_SIZE_STMT,
            &REPLACE_VALUE_STMT, &REPLACE_PATH_STMT,
            &INSERT_VALUE_STMT, &INSERT_PATH_STMT, &DELETE_STMT,
            &SET_META_STMT, &GET_META_STMT, &META_SIZE_STMT, &META_COUNT_STMT,
            &META_FILE_SIZE_STMT,
            &INCR_GET_STMT, &INCR_UPDATE_STMT
        };
        if constexpr (has_expiration)
        {
            stmts.push_back(&TOUCH_STMT);
            stmts.push_back(&EXPIRE_STMT);
            stmts.push_back(&EVICT_EXPIRED_STMT);
        }
        if constexpr (has_eviction)
        {
            stmts.push_back(&UPDATE_LAST_USE_STMT);
            stmts.push_back(&EVICT_LRU_STMT);
        }
        if constexpr (has_tags)
        {
            stmts.push_back(&EVICT_TAG_PATH_STMT);
            stmts.push_back(&EVICT_TAG_STMT);
        }
        return stmts;
    }

    inline bool _compile_statements() const
    {
        auto& self = const_cast<_Store&>(*this);
        bool result = true;
        for (auto* stmt : self._all_statements())
            result &= stmt->compile(_db.get());
        return result;
    }

    inline bool _finalize_statements()
    {
        bool result = true;
        for (auto* stmt : _all_statements())
            result &= stmt->finalize();
        return result;
    }

    // --- Schema ---

    static std::string _schema_sql()
    {
        return std::string(
            "CREATE TABLE IF NOT EXISTS cache ("
            "key TEXT PRIMARY KEY NOT NULL,"
            "path TEXT DEFAULT NULL,"
            "value BLOB DEFAULT NULL,"
            "size INT NOT NULL DEFAULT 0")
            + _extra_schema_columns()
            + ") WITHOUT ROWID;"
            + " CREATE TABLE IF NOT EXISTS meta ("
              "key TEXT PRIMARY KEY, value);"
              // Files displaced by a row change (REPLACE of a file-backed
              // value, incr() rewriting one to a blob) are never unlinked
              // inline: a reader in another process may already hold the old
              // path between its "SELECT path" and open() (see
              // docs/known-issues/pool-reuse-fork-safety-gap.md). They are
              // queued here in the same transaction as the row change and
              // unlinked by the background thread after a grace period.
              " CREATE TABLE IF NOT EXISTS trash ("
              "path TEXT PRIMARY KEY NOT NULL,"
              "ts INT NOT NULL) WITHOUT ROWID;"
              " INSERT OR IGNORE INTO meta (key, value) VALUES ('size', 0);"
              " INSERT OR IGNORE INTO meta (key, value) VALUES ('count', 0);"
              " INSERT OR IGNORE INTO meta (key, value) VALUES ('file_size', 0);"
              " CREATE TRIGGER IF NOT EXISTS cache_count_insert AFTER INSERT ON cache BEGIN"
              "   UPDATE meta SET value = value + 1 WHERE key = 'count'; END;"
              " CREATE TRIGGER IF NOT EXISTS cache_count_delete AFTER DELETE ON cache BEGIN"
              "   UPDATE meta SET value = value - 1 WHERE key = 'count'; END;"
              " CREATE TRIGGER IF NOT EXISTS cache_size_insert AFTER INSERT ON cache BEGIN"
              "   UPDATE meta SET value = value + NEW.size WHERE key = 'size'; END;"
              " CREATE TRIGGER IF NOT EXISTS cache_size_update AFTER UPDATE OF size ON cache BEGIN"
              "   UPDATE meta SET value = value + NEW.size - OLD.size WHERE key = 'size'; END;"
              " CREATE TRIGGER IF NOT EXISTS cache_size_delete AFTER DELETE ON cache BEGIN"
              "   UPDATE meta SET value = value - OLD.size WHERE key = 'size'; END;"
              " CREATE TRIGGER IF NOT EXISTS cache_fsize_insert AFTER INSERT ON cache"
              "   WHEN NEW.path IS NOT NULL BEGIN"
              "   UPDATE meta SET value = value + NEW.size WHERE key = 'file_size'; END;"
              " CREATE TRIGGER IF NOT EXISTS cache_fsize_delete AFTER DELETE ON cache"
              "   WHEN OLD.path IS NOT NULL BEGIN"
              "   UPDATE meta SET value = value - OLD.size WHERE key = 'file_size'; END;"
              // No WHEN guard: this must also catch the blob<->file
              // transitions of a real UPDATE (e.g. incr() rewriting a
              // file-backed entry to path = NULL via INCR_UPDATE_STMT).
              // REPLACE (used by set()) never reaches this trigger — with
              // recursive_triggers=ON it fires as DELETE+INSERT, handled by
              // the WHEN-guarded pair above.
              " CREATE TRIGGER IF NOT EXISTS cache_fsize_update AFTER UPDATE OF size, path ON cache BEGIN"
              "   UPDATE meta SET value = value"
              "     - (CASE WHEN OLD.path IS NOT NULL THEN OLD.size ELSE 0 END)"
              "     + (CASE WHEN NEW.path IS NOT NULL THEN NEW.size ELSE 0 END)"
              "   WHERE key = 'file_size'; END;"
            + _extra_schema_indexes();
    }

    static inline constexpr auto _PRAGMA_SQL =
        R"(
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            -- The background thread's PASSIVE checkpoint backfills frames but
            -- cannot reset the WAL while this connection keeps writing
            -- (checkpoint starvation). Letting the writer connection run its
            -- own autocheckpoint once the WAL exceeds 4000 pages (~16 MB)
            -- finishes the small remainder and lets the next transaction
            -- restart the WAL. Measured: WAL bounded at ~18 MB and flat
            -- throughput vs an 8.9 GB WAL before.
            PRAGMA wal_autocheckpoint=4000;
            PRAGMA cache_size=10000;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=268435456;
            PRAGMA analysis_limit=1000;
            PRAGMA busy_timeout=600000;
            PRAGMA recursive_triggers=ON;
        )";

    void _init_db()
    {
        auto init_stmts = { std::string(_PRAGMA_SQL), _schema_sql() };
        for (int attempt = 0; attempt < 5; ++attempt)
        {
            try
            {
                _db.open(this->cache_path / db_fname, init_stmts);
                _compile_statements();
                _migrate_schema();
                _seed_access_seq();
                return;
            }
            catch (const std::runtime_error&)
            {
                _db.close();
                if (attempt == 4) throw;
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * (1 << attempt)));
            }
        }
    }

    void _seed_access_seq()
    {
        // last_use is the monotonic access counter. Resume it above the
        // persisted maximum so entries written after a reopen are ranked
        // more-recently-used than older ones (otherwise the counter restarts
        // at 0 and LRU eviction order is inverted across restarts).
        if constexpr (has_eviction)
        {
            if (auto r = _db.exec<std::size_t>(
                    "SELECT COALESCE(MAX(last_use), -1) + 1 FROM cache;"))
                WithEviction::_access_seq.store(*r, std::memory_order_relaxed);
        }
    }

    void _migrate_schema()
    {
        // These are trigger names from a pre-0.1 schema that recomputed
        // SUM(size) per write; if they survived on a DB opened with an old
        // binary they would double-count alongside the new incremental
        // triggers above, so drop them unconditionally on every open.
        sqlite3_exec(_db.get(), "DROP TRIGGER IF EXISTS cache_insert_meta;", nullptr, nullptr, nullptr);
        sqlite3_exec(_db.get(), "DROP TRIGGER IF EXISTS cache_delete_meta;", nullptr, nullptr, nullptr);
        sqlite3_exec(_db.get(), "DROP TRIGGER IF EXISTS cache_update_size;", nullptr, nullptr, nullptr);
        if constexpr (has_tags)
        {
            sqlite3_exec(_db.get(),
                "ALTER TABLE cache ADD COLUMN tag TEXT DEFAULT NULL;",
                nullptr, nullptr, nullptr);
            sqlite3_exec(_db.get(),
                "CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag) WHERE tag IS NOT NULL;",
                nullptr, nullptr, nullptr);
        }
        // One-time reconciliation for DBs whose maintained counters predate
        // the current counters_v (guarded by that marker row so each
        // reconciliation runs once per DB, not once per open): recompute
        // every counter from the cache table directly. This is the only O(N)
        // scan; every write after this point is kept current incrementally
        // by the triggers in _schema_sql. v1 -> v2 added 'file_size'
        // (0.1.6 DBs have counters_v='1' but no file_size row yet), so a
        // missing marker or one below the current version both trigger it.
        auto counters_v = _db.exec<std::size_t>("SELECT value FROM meta WHERE key = 'counters_v';");
        if (!counters_v || *counters_v < 2)
        {
            (void)_db.exec("BEGIN IMMEDIATE;"
                "UPDATE meta SET value = (SELECT COUNT(*) FROM cache) WHERE key = 'count';"
                "UPDATE meta SET value = (SELECT COALESCE(SUM(size), 0) FROM cache) WHERE key = 'size';"
                "UPDATE meta SET value = (SELECT COALESCE(SUM(size), 0) FROM cache WHERE path IS NOT NULL) "
                "WHERE key = 'file_size';"
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('counters_v', '2');"
                "COMMIT;");
        }
    }

    // --- Fork safety (pthread_atfork hooks; see _ForkAware above) ---

    void _stop_checkpoint_thread()
    {
        _stop_checkpoint.store(true, std::memory_order_relaxed);
        _checkpoint_cv.notify_one();
        if (_checkpoint_thread.joinable())
            _checkpoint_thread.join();
    }

    void _start_checkpoint_thread()
    {
        _stop_checkpoint.store(false, std::memory_order_relaxed);
        _checkpoint_thread = std::thread(&_Store::_checkpoint_loop, this);
    }

    // prepare: stop the checkpoint thread (it closes its own connection) and
    // take _mtx, so at fork the forking thread is the only one running and no
    // background lock — store, libc malloc, or WAL — is held by a vanished
    // thread.
    void _fork_prepare() override
    {
        _stop_checkpoint_thread();
        _mtx.lock();
    }

    // parent: undo prepare — release _mtx and resume checkpointing.
    void _fork_parent() override
    {
        _mtx.unlock();
        _start_checkpoint_thread();
    }

    // child: the forking thread holds _mtx, but a recursive_mutex records the
    // owning thread id, which differs in the child — it cannot be unlocked, so
    // reinitialise it in place (no destructor: that is undefined while locked).
    // The inherited SQLite connection is reopened on a clean slate; prepare
    // already closed the checkpoint connection, so nothing else is open here.
    void _fork_child() override
    {
        new (&_mtx) std::recursive_mutex();
        _txn_depth = 0;
        _owner_pid = _sq_getpid();
        // fork() cloned the blob-filename PRNG state verbatim; without a
        // reseed, parent and children generate lockstep-identical UUID
        // sequences and file-backed values for different keys collide on the
        // same on-disk path. (The pid in the filename is the structural
        // backstop; this removes the root cause.)
        storage->reseed();
        _finalize_statements();
        _db.close();
        _init_db();
        _start_checkpoint_thread();
    }

    // --- Background checkpoint / eviction ---

    void _bg_evict([[maybe_unused]] sqlite3* bg_db)
    {
        if constexpr (has_expiration)
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(bg_db,
                "SELECT path FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');",
                -1, &stmt, nullptr);
            std::vector<std::filesystem::path> files;
            while (stmt && sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                if (p && p[0])
                    files.emplace_back(p);
            }
            if (stmt) sqlite3_finalize(stmt);
            auto rc = sqlite3_exec(bg_db,
                "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');",
                nullptr, nullptr, nullptr);
            // Only unlink files whose rows are confirmed deleted — a DELETE
            // that failed (e.g. SQLITE_BUSY against the writer's own
            // BEGIN EXCLUSIVE) must not orphan the row by removing its file.
            if (rc == SQLITE_OK)
                for (auto& f : files)
                    storage->remove(f);
        }

        if constexpr (has_eviction)
        {
            if (max_size > 0)
            {
                std::size_t current_size = 0;
                {
                    sqlite3_stmt* stmt = nullptr;
                    sqlite3_prepare_v2(bg_db, "SELECT value FROM meta WHERE key = 'size';",
                                       -1, &stmt, nullptr);
                    if (stmt && sqlite3_step(stmt) == SQLITE_ROW)
                        current_size = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
                    if (stmt) sqlite3_finalize(stmt);
                }
                if (current_size > max_size)
                {
                    auto target = max_size * 9 / 10;
                    sqlite3_stmt* stmt = nullptr;
                    sqlite3_prepare_v2(bg_db, "SELECT key, path, size FROM cache ORDER BY last_use ASC;",
                                       -1, &stmt, nullptr);
                    struct Entry { std::string key; std::filesystem::path path; };
                    std::vector<Entry> to_evict;
                    while (stmt && current_size > target && sqlite3_step(stmt) == SQLITE_ROW)
                    {
                        auto k = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                        auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                        auto sz = static_cast<std::size_t>(sqlite3_column_int64(stmt, 2));
                        to_evict.push_back({ k ? k : "", p ? p : "" });
                        current_size -= std::min(current_size, sz);
                    }
                    if (stmt) sqlite3_finalize(stmt);

                    stmt = nullptr;
                    sqlite3_prepare_v2(bg_db, "DELETE FROM cache WHERE key = ?;", -1, &stmt, nullptr);
                    for (auto& [key, path] : to_evict)
                    {
                        // Only unlink a key's file once its row is confirmed
                        // deleted — a DELETE that lost the busy-timeout race
                        // against the writer's own BEGIN EXCLUSIVE leaves the
                        // row untouched, and removing the file anyway would
                        // orphan it.
                        bool gone = false;
                        if (stmt)
                        {
                            sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
                            gone = sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(bg_db) > 0;
                            sqlite3_reset(stmt);
                        }
                        if (gone && !path.empty())
                            storage->remove(path);
                    }
                    if (stmt) sqlite3_finalize(stmt);
                }
            }
        }
    }

    void _checkpoint_loop()
    {
        auto db_path = (cache_path / db_fname).string();
        sqlite3* cp_db = nullptr;

        while (!_stop_checkpoint.load(std::memory_order_relaxed))
        {
            if (sqlite3_open_v2(db_path.c_str(), &cp_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX,
                                nullptr)
                == SQLITE_OK)
                break;
            if (cp_db)
            {
                sqlite3_close(cp_db);
                cp_db = nullptr;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        if (!cp_db)
            return;

        // A connection that has never read the DB has no WAL handle, and
        // sqlite3_wal_checkpoint_v2 on it is a silent no-op returning
        // SQLITE_OK (with *pnLog left at -1).
        sqlite3_exec(cp_db, "SELECT count(*) FROM sqlite_master;", nullptr, nullptr, nullptr);

        // _bg_evict's DELETEs contend with the writer's BEGIN EXCLUSIVE; a
        // short bounded wait avoids an immediate SQLITE_BUSY (which would
        // otherwise leave the row in place while we go on to unlink its
        // file). Never applies to the writer's own connection/path.
        sqlite3_busy_timeout(cp_db, 250);

        while (!_stop_checkpoint.load(std::memory_order_relaxed))
        {
            std::unique_lock lock(_checkpoint_mutex);
            _checkpoint_cv.wait_for(lock, std::chrono::seconds(1));
            if (_stop_checkpoint.load(std::memory_order_relaxed))
                break;
            sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
            if constexpr (has_expiration || has_eviction)
                _bg_evict(cp_db);
            _drain_trash(cp_db);
        }

        // Final drain so a short-lived process doesn't leave past-grace trash
        // behind; still-in-grace entries are left for the next open's thread.
        _drain_trash(cp_db);
        sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
        sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, nullptr, nullptr);
        sqlite3_close(cp_db);
    }

    // --- DbGuard ---

    struct DbGuard
    {
        Database& ref;
        std::unique_lock<std::recursive_mutex> lock;
        Database* operator->() { return &ref; }
        Database& operator*() { return ref; }
    };

    DbGuard db() const { return { _db, std::unique_lock(_mtx) }; }

    // RAII helper for internal C++ paths that need an EXCLUSIVE transaction.
    // At depth 0 it issues a real BEGIN EXCLUSIVE; at depth>0 it's a logical
    // no-op (the surrounding outermost txn is the boundary). commit() and
    // rollback() only hit SQLite at the outermost level. Always constructed
    // under _mtx (callers hold the DbGuard).
    struct _NestedTxn
    {
        _Store& store;
        std::optional<Transaction> txn;
        bool finished = false;
        bool outermost;

        explicit _NestedTxn(_Store& s)
            : store(s), outermost(s._txn_depth == 0)
        {
            if (outermost)
                txn.emplace(s._db.get(), true);
            ++store._txn_depth;
        }
        _NestedTxn(const _NestedTxn&) = delete;
        _NestedTxn& operator=(const _NestedTxn&) = delete;
        ~_NestedTxn()
        {
            if (!finished)
            {
                if (store._txn_depth > 0) --store._txn_depth;
                // ~Transaction will rollback if not committed.
            }
        }
        void commit()
        {
            if (finished) return;
            finished = true;
            if (store._txn_depth > 0) --store._txn_depth;
            if (outermost && txn) txn->commit();
        }
        void rollback() noexcept
        {
            if (finished) return;
            finished = true;
            if (store._txn_depth > 0) --store._txn_depth;
            if (outermost && txn) (void)txn->rollback();
        }
    };

public:
    // Snapshot iterator: pulls all keys into memory under the store mutex at
    // construction, then iterates lock-free. Trade-off: O(N) memory at start,
    // but no lock contention for other threads while the user iterates.
    // Snapshot semantics: keys added after iter starts are not seen — same as
    // calling keys() then iterating the returned vector.
    class KeyCursor
    {
        std::vector<std::string> _keys;
        std::size_t _pos = 0;

    public:
        KeyCursor(std::recursive_mutex& mtx, sqlite3* db, const std::string& sql)
        {
            std::lock_guard lk { mtx };
            sqlite3_stmt* stmt = nullptr;
            if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK)
            {
                std::string err = sqlite3_errmsg(db);
                if (stmt) sqlite3_finalize(stmt);
                throw std::runtime_error("Failed to prepare key iterator: " + err);
            }
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                _keys.emplace_back(v ? v : "");
            }
            sqlite3_finalize(stmt);
        }

        KeyCursor(const KeyCursor&) = delete;
        KeyCursor& operator=(const KeyCursor&) = delete;
        KeyCursor(KeyCursor&&) noexcept = default;

        std::optional<std::string> next()
        {
            if (_pos >= _keys.size()) return std::nullopt;
            return std::move(_keys[_pos++]);
        }
    };

    // User-facing transaction guard. Reentrant: nested same-thread
    // begin_user_transaction() is allowed (depth-counted); only the
    // outermost level issues a real SQLite BEGIN/COMMIT. Inner commit()
    // appears to succeed but is logically owned by the outer scope (an
    // outer rollback discards inner work — same semantics as diskcache,
    // which uses an RLock + depth, not real savepoints).
    class TransactionGuard
    {
        _Store& _store;
        std::unique_lock<std::recursive_mutex> _lock;
        std::optional<Transaction> _txn;
        bool _finished = false;
        bool _outermost;

    public:
        TransactionGuard(_Store& store)
            : _store(store)
            , _lock(store._mtx)
            , _outermost(store._txn_depth == 0)
        {
            if (_outermost)
                _txn.emplace(store._db.get(), true);
            ++store._txn_depth;
        }

        TransactionGuard(const TransactionGuard&) = delete;
        TransactionGuard& operator=(const TransactionGuard&) = delete;
        TransactionGuard(TransactionGuard&& other) noexcept
            : _store(other._store)
            , _lock(std::move(other._lock))
            , _txn(std::move(other._txn))
            , _finished(other._finished)
            , _outermost(other._outermost)
        {
            other._finished = true;
        }

        ~TransactionGuard()
        {
            if (!_finished)
                rollback();
        }

        bool commit()
        {
            if (_finished) return false;
            _finished = true;
            if (_store._txn_depth > 0) --_store._txn_depth;
            if (_outermost && _txn)
                return _txn->commit();
            return true;
        }

        bool rollback()
        {
            if (_finished) return false;
            _finished = true;
            if (_store._txn_depth > 0) --_store._txn_depth;
            if (_outermost && _txn)
                return _txn->rollback();
            return true;
        }
    };

private:

    // --- Bind helpers for policy-aware INSERT/REPLACE ---

    void _bind_core_and_policies(sqlite3_stmt* stmt, const std::string& col1,
                                 const Bytes auto& col2, std::size_t sz,
                                 [[maybe_unused]] std::optional<double> abs_exp,
                                 [[maybe_unused]] std::size_t seq,
                                 [[maybe_unused]] const std::optional<std::string>& tag) const
    {
        int i = 1;
        sql_bind(stmt, i++, col1);
        sql_bind(stmt, i++, col2);
        sql_bind(stmt, i++, sz);
        if constexpr (has_expiration) sql_bind(stmt, i++, abs_exp);
        if constexpr (has_eviction) sql_bind(stmt, i++, seq);
        if constexpr (has_tags) sql_bind(stmt, i++, tag);
    }

    void _bind_core_and_policies(sqlite3_stmt* stmt, const std::string& col1,
                                 const std::string& col2, std::size_t sz,
                                 [[maybe_unused]] std::optional<double> abs_exp,
                                 [[maybe_unused]] std::size_t seq,
                                 [[maybe_unused]] const std::optional<std::string>& tag) const
    {
        int i = 1;
        sql_bind(stmt, i++, col1);
        sql_bind(stmt, i++, col2);
        sql_bind(stmt, i++, sz);
        if constexpr (has_expiration) sql_bind(stmt, i++, abs_exp);
        if constexpr (has_eviction) sql_bind(stmt, i++, seq);
        if constexpr (has_tags) sql_bind(stmt, i++, tag);
    }

    // --- deferred file deletion (trash) ---

    // Grace period before a displaced file is really unlinked. A reader's
    // exposure window (row read → mmap open) is microseconds; 5 s gives a
    // >10^5 margin while keeping displaced files from lingering on disk.
    static constexpr int _trash_grace_secs = 5;

    // Queue a displaced file for deferred deletion. Must be called inside
    // the transaction that displaces it, so the row change and the trash
    // entry commit (or roll back) atomically — this also closes the old
    // crash window where a commit-then-crash-before-unlink orphaned the file.
    void _trash_file(DbGuard& db, const std::filesystem::path& p)
    {
        auto path_str = p.string();
        db->exec("INSERT OR REPLACE INTO trash (path, ts) VALUES (?, unixepoch('now'));",
                 path_str);
    }

    // Unlink trashed files once their grace period has passed. Runs on the
    // background connection without _mtx. DELETE-first: several processes'
    // background threads can race on the same row; only the one whose
    // DELETE reports a change unlinks the file.
    void _drain_trash(sqlite3* bg_db)
    {
        static const std::string select_sql
            = "SELECT path FROM trash WHERE ts <= unixepoch('now') - "
            + std::to_string(_trash_grace_secs) + ";";
        sqlite3_stmt* stmt = nullptr;
        sqlite3_prepare_v2(bg_db, select_sql.c_str(), -1, &stmt, nullptr);
        std::vector<std::string> paths;
        while (stmt && sqlite3_step(stmt) == SQLITE_ROW)
        {
            auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            if (p && p[0])
                paths.emplace_back(p);
        }
        if (stmt) sqlite3_finalize(stmt);
        if (paths.empty())
            return;

        stmt = nullptr;
        sqlite3_prepare_v2(bg_db, "DELETE FROM trash WHERE path = ?;", -1, &stmt, nullptr);
        for (auto& p : paths)
        {
            bool won = false;
            if (stmt)
            {
                sqlite3_bind_text(stmt, 1, p.c_str(), -1, SQLITE_TRANSIENT);
                won = sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(bg_db) > 0;
                sqlite3_reset(stmt);
            }
            if (won)
                storage->remove(p);
        }
        if (stmt) sqlite3_finalize(stmt);
    }

    // --- set/add implementation ---

    inline bool _set_impl(const std::string& key, const Bytes auto& value,
                           [[maybe_unused]] std::optional<double> expires_secs,
                           [[maybe_unused]] std::optional<std::string> tag = std::nullopt)
    {
        auto db = this->db();
        std::size_t seq = 0;
        if constexpr (has_eviction)
            seq = WithEviction::_access_seq.fetch_add(1, std::memory_order_relaxed);

        std::optional<double> abs_exp;
        if constexpr (has_expiration)
            abs_exp = _abs_expire(expires_secs);

        auto new_size = std::size(value);

        // BEGIN EXCLUSIVE here covers BOTH branches: the SELECT of the old
        // entry and the REPLACE must see the same DB state. Without it a
        // concurrent process can swap the entry between our SELECT and our
        // REPLACE — we'd then `storage->remove(old_filepath_we_read)` while
        // the file actually attached to the row now is the *other* process's
        // path, which we leak. (See test_no_orphans_after_concurrent_mixed_size_set.)
        _NestedTxn txn(*this);

        // Single query to get old path and size (saves a round-trip vs separate queries)
        auto old_entry = db->template exec<std::filesystem::path, std::size_t>(
            GET_PATH_SIZE_STMT, key);
        std::filesystem::path old_filepath;
        if (old_entry)
            old_filepath = std::get<0>(*old_entry);

        if (new_size <= _file_size_threshold)
        {
            auto binded = REPLACE_VALUE_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, value, new_size, abs_exp, seq, tag);
            sqlite3_step(binded.get());
            if (!old_filepath.empty())
                _trash_file(db, old_filepath);
            txn.commit();
            return true;
        }

        // Tells DiskStorage's collision-healing path whether an existing blob
        // file is owned by a live row (regenerate) or is an orphaned leftover
        // (safe to overwrite and reuse). Only ever runs on EEXIST — the happy
        // path never queries. Safe to check-then-overwrite here: this function
        // holds BEGIN EXCLUSIVE plus _mtx, so no concurrent writer can race.
        auto is_referenced = [&db](const std::filesystem::path& p) {
            auto path = p.string();
            return db->template exec<bool>(
                std::string(IS_PATH_REFERENCED_SQL), path, path).has_value();
        };
        auto new_filepath = storage->store(value, is_referenced);
        if (!new_filepath)
        {
            txn.rollback();
            return false;
        }
        {
            auto path_str = new_filepath->string();
            auto binded = REPLACE_PATH_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, path_str, new_size,
                                    abs_exp, seq, tag);
            sqlite3_step(binded.get());
        }
        try
        {
            if (!old_filepath.empty())
                _trash_file(db, old_filepath);
            txn.commit();
        }
        catch (const std::runtime_error&)
        {
            txn.rollback();
            // The new file was never visible to any reader (its row never
            // committed), so an inline remove is safe here.
            storage->remove(*new_filepath);
            throw;
        }
        return true;
    }

    inline bool _add_impl(const std::string& key, const Bytes auto& value,
                           [[maybe_unused]] std::optional<double> expires_secs,
                           [[maybe_unused]] std::optional<std::string> tag = std::nullopt)
    {
        auto db = this->db();
        std::size_t seq = 0;
        if constexpr (has_eviction)
            seq = WithEviction::_access_seq.fetch_add(1, std::memory_order_relaxed);

        std::optional<double> abs_exp;
        if constexpr (has_expiration)
            abs_exp = _abs_expire(expires_secs);

        auto new_size = std::size(value);

        if (new_size <= _file_size_threshold)
        {
            auto binded = INSERT_VALUE_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, value, new_size, abs_exp, seq, tag);
            sqlite3_step(binded.get());
            return sqlite3_changes(db->get()) > 0;
        }

        auto file_path = storage->store(value);
        if (!file_path)
            return false;

        {
            auto path_str = file_path->string();
            auto binded = INSERT_PATH_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, path_str, new_size,
                                    abs_exp, seq, tag);
            sqlite3_step(binded.get());
        }
        if (sqlite3_changes(db->get()) == 0)
        {
            storage->remove(*file_path);
            return false;
        }
        return true;
    }

    static std::optional<double> _abs_expire(std::optional<double> offset_secs)
    {
        if (!offset_secs) return std::nullopt;
        auto now = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        return static_cast<double>(now) + *offset_secs;
    }

public:
    static constexpr std::string_view db_fname = "sciqlop-cache.db";

    explicit _Store(const std::filesystem::path& cache_path = ".cache/",
                    size_t max_size = 0)
            : cache_path(cache_path)
            , max_size(max_size)
            , storage(std::make_unique<Storage>(cache_path))
            , _owner_pid(_sq_getpid())
    {
        _init_db();
        _checkpoint_thread = std::thread(&_Store::_checkpoint_loop, this);
        // Register last, once fully built, so a concurrent fork's handlers only
        // ever see a complete store.
        _register_fork_aware(this);
    }

    ~_Store()
    {
        // Deregister first so fork handlers cannot touch a store mid-teardown;
        // this also serialises against an in-progress fork via the registry lock.
        _unregister_fork_aware(this);
        if (_sq_getpid() != _owner_pid)
        {
            if (_checkpoint_thread.joinable())
                (void)new std::thread(std::move(_checkpoint_thread));
            return;
        }
        _stop_checkpoint.store(true, std::memory_order_relaxed);
        _checkpoint_cv.notify_one();
        if (_checkpoint_thread.joinable())
            _checkpoint_thread.join();
        try { close(); } catch (...) {}
    }

    [[nodiscard]] inline bool opened() const { return db()->opened(); }

    // Reentrant on the same thread: the TransactionGuard constructor takes
    // _mtx (recursive_mutex re-entry on same thread). Cross-thread callers
    // serialize on _mtx and become outermost in turn.
    TransactionGuard begin_user_transaction()
    {
        return TransactionGuard(*this);
    }

    inline bool close()
    {
        auto g = db();
        return _finalize_statements() & g->close();
    }

    [[nodiscard]] inline std::filesystem::path path() { return cache_path; }

    [[nodiscard]] inline size_t max_cache_size()
        requires (has_eviction)
    { return max_size; }

    inline void set_max_cache_size(size_t value)
        requires (has_eviction)
    { max_size = value; }

    [[nodiscard]] inline std::size_t file_size_threshold() { return _file_size_threshold; }

    [[nodiscard]] inline std::size_t count()
    {
        // The maintained counter can briefly include entries that are
        // already expired but not yet evicted — the background thread
        // sweeps those every ~1 s. Same semantics as Python diskcache's
        // len(), and O(1) instead of the full-table scan a WHERE-filtered
        // COUNT(*) would need.
        if (auto r = db()->template exec<std::size_t>(META_COUNT_STMT))
            return *r;
        return 0;
    }

    [[nodiscard]] inline size_t size()
    {
        if (auto r = db()->template exec<std::size_t>(META_SIZE_STMT))
            return *r;
        return 0;
    }

    // O(1): page_count*page_size for the DB file, plus the trigger-maintained
    // 'file_size' counter for on-disk values. Counts live values only — a
    // file on disk with no matching row (orphaned, e.g. after a crash mid-
    // write) contributes nothing here; check() reports those separately.
    [[nodiscard]] inline size_t volume()
    {
        auto g = db();
        std::size_t db_size = 0;
        if (auto pc = g->template exec<std::size_t>("PRAGMA page_count;"))
            if (auto ps = g->template exec<std::size_t>("PRAGMA page_size;"))
                db_size = *pc * *ps;
        std::size_t file_size = 0;
        if (auto fs = g->template exec<std::size_t>(META_FILE_SIZE_STMT))
            file_size = *fs;
        return db_size + file_size;
    }

    [[nodiscard]] inline std::vector<std::string> keys()
    {
        if (auto r = db()->template exec<std::vector<std::string>>(KEYS_STMT))
            return *r;
        return {};
    }

    [[nodiscard]] inline KeyCursor iterkeys()
    {
        auto sql = std::string("SELECT key FROM cache WHERE 1=1") + _where_valid() + ";";
        return KeyCursor(_mtx, _db.get(), sql);
    }

    [[nodiscard]] inline bool exists(const std::string& key)
    {
        if (auto r = db()->template exec<bool>(EXISTS_STMT, key))
            return *r;
        return false;
    }

    // --- set() overloads ---

    inline bool set(const std::string& key, const Bytes auto& value)
    {
        return _set_impl(key, value, std::optional<double> {});
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
        requires (has_expiration)
    {
        return _set_impl(key, value,
            std::optional<double> { static_cast<double>(
                std::chrono::duration_cast<std::chrono::seconds>(expire).count()) });
    }

    inline bool set(const std::string& key, const Bytes auto& value, const std::string& tag)
        requires (has_tags)
    {
        return _set_impl(key, value, std::optional<double> {}, std::optional<std::string> { tag });
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire,
                    const std::string& tag)
        requires (has_expiration && has_tags)
    {
        return _set_impl(key, value,
            std::optional<double> { static_cast<double>(
                std::chrono::duration_cast<std::chrono::seconds>(expire).count()) },
            std::optional<std::string> { tag });
    }

    // --- get() ---

    inline std::optional<Buffer> get(const std::string& key)
    {
        auto db = this->db();
        if (auto values = db->template exec<std::vector<char>, std::filesystem::path>(GET_STMT, key))
        {
            if constexpr (has_stats)
                WithStats::_hits.fetch_add(1, std::memory_order_relaxed);

            if constexpr (has_eviction)
            {
                if (max_size > 0)
                    db->exec(UPDATE_LAST_USE_STMT,
                             WithEviction::_access_seq.fetch_add(1, std::memory_order_relaxed), key);
            }

            const auto& [_, path] = *values;
            if (!path.empty())
            {
                if (auto result = storage->load(path))
                    return result;

                // A concurrent REPLACE on this key can remove `path` right
                // after we read it: _set_impl commits the row pointing at
                // its new file *before* removing the old one, so re-reading
                // the row after a failed load either shows the same
                // (genuinely gone) path or a newer one whose file is
                // guaranteed complete. A few bounded retries close the
                // window against back-to-back REPLACEs instead of surfacing
                // a spurious miss for a key that was never actually
                // deleted; the loop stops as soon as the path stops moving.
                auto last_path = path;
                for (int attempt = 0; attempt < 4; ++attempt)
                {
                    auto retry_path = db->template exec<std::filesystem::path>(
                        "SELECT path FROM cache WHERE key = ?;", key);
                    if (!retry_path || retry_path->empty() || *retry_path == last_path)
                        break;
                    if (auto result = storage->load(*retry_path))
                        return result;
                    last_path = *retry_path;
                }

                // Only delete the row once the file is confirmed absent from
                // disk. A transient open failure (fd exhaustion, permission
                // error, ...) on a file that is still present must be a soft
                // miss, not row deletion — deleting here previously turned a
                // fleeting open failure into a permanent data loss, and the
                // blob file itself was never unlinked either (it just became
                // orphaned on disk).
                if (!storage->file_exists(path))
                {
                    // Path-aware cleanup. Without this, a concurrent process
                    // that swapped the entry between our SELECT and this
                    // fallback would have its file removed by the bare del()
                    // (del re-reads the row, finds the new path, removes the
                    // wrong file). DELETE WHERE key=? AND path=? only fires
                    // if the row still references the path we just failed to
                    // load.
                    auto path_str = path.string();
                    db->exec("DELETE FROM cache WHERE key = ? AND path = ?;",
                             key, path_str);
                    std::cerr << "Error loading file for key: " << key << ", deleting entry."
                              << std::endl;
                }
                else
                {
                    std::cerr << "Error loading file for key: " << key
                              << ", file present, keeping entry." << std::endl;
                }
                return std::nullopt;
            }
            return Buffer(std::move(std::get<0>(*values)));
        }

        if constexpr (has_stats)
            WithStats::_misses.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    // --- add() overloads ---

    inline bool add(const std::string& key, const Bytes auto& value)
    {
        return _add_impl(key, value, std::optional<double> {});
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
        requires (has_expiration)
    {
        return _add_impl(key, value,
            std::optional<double> { static_cast<double>(
                std::chrono::duration_cast<std::chrono::seconds>(expire).count()) });
    }

    inline bool add(const std::string& key, const Bytes auto& value, const std::string& tag)
        requires (has_tags)
    {
        return _add_impl(key, value, std::optional<double> {}, std::optional<std::string> { tag });
    }

    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire,
                    const std::string& tag)
        requires (has_expiration && has_tags)
    {
        return _add_impl(key, value,
            std::optional<double> { static_cast<double>(
                std::chrono::duration_cast<std::chrono::seconds>(expire).count()) },
            std::optional<std::string> { tag });
    }

    // --- del / pop ---

    inline bool del(const std::string& key)
    {
        // BEGIN EXCLUSIVE so the SELECT-of-old-entry and the DELETE see the
        // same row. Without it, a concurrent set() between our SELECT and our
        // DELETE leaves us removing the wrong file (the freshly-written one
        // instead of the one we actually displaced).
        auto db = this->db();
        _NestedTxn txn(*this);
        auto old_entry = db->template exec<std::filesystem::path, std::size_t>(
            GET_PATH_SIZE_STMT, key);
        if (!db->exec(DELETE_STMT, key))
        {
            txn.rollback();
            return false;
        }
        if (sqlite3_changes(db->get()) == 0)
        {
            txn.rollback();
            return false;
        }
        txn.commit();
        if (old_entry && !std::get<0>(*old_entry).empty())
            storage->remove(std::get<0>(*old_entry));
        return true;
    }

    inline std::optional<Buffer> pop(const std::string& key)
    {
        // Hold _mtx for the whole op so concurrent threads can't slip a set()
        // between get and del. Wrap in BEGIN EXCLUSIVE so concurrent processes
        // can't either (multi-process safety via SQLite reserved locks). The
        // _NestedTxn handles depth so a containing user transaction folds
        // this in transparently.
        auto db = this->db();
        _NestedTxn txn(*this);
        auto result = get(key);
        if (result)
            del(key);
        txn.commit();
        return result;
    }

    // --- Expiration-specific ---

    inline bool touch(const std::string& key, DurationConcept auto expire)
        requires (has_expiration)
    {
        auto expire_secs = static_cast<double>(
            std::chrono::duration_cast<std::chrono::seconds>(expire).count());
        auto abs_exp = _abs_expire(std::optional<double> { expire_secs });
        return db()->exec(TOUCH_STMT, abs_exp, key);
    }

    inline void expire()
        requires (has_expiration)
    {
        auto db = this->db();
        {
            auto binded = EXPIRE_STMT.bind_all();
            while (auto file_path = db->template step<std::filesystem::path>(binded))
            {
                if (!file_path->empty() && !storage->remove(*file_path))
                    std::cerr << "Failed to delete file: " << *file_path << std::endl;
            }
        }
        db->exec(EVICT_EXPIRED_STMT);
    }

    // --- Eviction-specific ---

    inline std::size_t evict()
        requires (has_eviction)
    {
        if (max_size == 0)
            return 0;

        if constexpr (has_expiration)
            expire();

        auto current_size = size();
        if (current_size <= max_size)
            return 0;

        auto target = max_size * 9 / 10;
        auto db = this->db();

        struct Entry { std::string key; std::filesystem::path path; std::size_t entry_size; };
        std::vector<Entry> to_evict;
        {
            auto binded = EVICT_LRU_STMT.bind_all();
            while (current_size > target)
            {
                auto r = db->template step<std::string, std::filesystem::path, std::size_t>(binded);
                if (!r) break;
                auto& [key, path, entry_size] = *r;
                to_evict.push_back({ std::move(key), std::move(path), entry_size });
                current_size -= std::min(current_size, entry_size);
            }
        }

        for (auto& entry : to_evict)
        {
            db->exec(DELETE_STMT, entry.key);
            if (!entry.path.empty())
                storage->remove(entry.path);
        }

        return to_evict.size();
    }

    // --- Tag-specific ---

    inline std::size_t evict_tag(const std::string& tag)
        requires (has_tags)
    {
        // Wrap the whole op in BEGIN EXCLUSIVE so the SELECT-paths and DELETE
        // see the same DB snapshot. The per-row DELETE triggers keep meta
        // 'size'/'count' correct as part of the same transaction, including
        // for rows written by other processes.
        auto db = this->db();
        _NestedTxn txn(*this);

        std::vector<std::filesystem::path> files;
        {
            auto binded = EVICT_TAG_PATH_STMT.bind_all(tag);
            while (auto r = db->template step<std::filesystem::path>(binded))
            {
                if (!r->empty())
                    files.push_back(std::move(*r));
            }
        }
        db->exec(EVICT_TAG_STMT, tag);
        auto evicted = static_cast<std::size_t>(sqlite3_changes(db->get()));
        txn.commit();

        for (auto& f : files)
            storage->remove(f);
        return evicted;
    }

    // --- incr / decr ---

    inline int64_t incr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        auto db = this->db();
        _NestedTxn txn(*this);

        int64_t current = default_value;
        if (auto blob = db->template exec<std::vector<char>>(INCR_GET_STMT, key))
        {
            if (blob->size() == sizeof(int64_t))
                std::memcpy(&current, blob->data(), sizeof(int64_t));
        }

        // A file-backed row is rewritten to a blob below (path = NULL);
        // its file is displaced and must go through the trash like any
        // REPLACE-displaced file (it used to be silently leaked).
        std::filesystem::path old_filepath;
        if (auto old_entry = db->template exec<std::filesystem::path, std::size_t>(
                GET_PATH_SIZE_STMT, key))
            old_filepath = std::get<0>(*old_entry);

        int64_t new_value = current + delta;
        std::size_t seq = 0;
        if constexpr (has_eviction)
            seq = WithEviction::_access_seq.fetch_add(1, std::memory_order_relaxed);

        std::array<char, sizeof(int64_t)> buf;
        std::memcpy(buf.data(), &new_value, sizeof(int64_t));
        auto data = std::span<const char>(buf.data(), buf.size());

        {
            auto binded = INCR_UPDATE_STMT.bind_all();
            int i = 1;
            sql_bind(binded.get(), i++, data);
            sql_bind(binded.get(), i++, sizeof(int64_t));
            if constexpr (has_eviction) sql_bind(binded.get(), i++, seq);
            sql_bind(binded.get(), i++, key);
            sqlite3_step(binded.get());
        }
        if (sqlite3_changes(db->get()) == 0)
        {
            auto binded = REPLACE_VALUE_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, data, sizeof(int64_t),
                                    std::optional<double> {}, seq, std::optional<std::string> {});
            sqlite3_step(binded.get());
        }
        if (!old_filepath.empty())
            _trash_file(db, old_filepath);

        txn.commit();
        return new_value;
    }

    inline int64_t decr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return incr(key, -delta, default_value);
    }

    // --- clear ---

    inline void clear()
    {
        auto db = this->db();
        // Trash rows go too: the whole file tree is removed below, so keeping
        // them would only make later drains chase files that no longer exist.
        sqlite3_exec(db->get(), "DELETE FROM cache; DELETE FROM trash;", nullptr, nullptr, nullptr);
        // Drop every mmap handle BEFORE removing files. On Linux removing an
        // mmap'd file succeeds (the inode lingers), but on Windows the file
        // can't be removed while a mapping exists — clear() would silently
        // leave files behind. Linux-side this is also a small leak: stale
        // shared_ptr<MemoryMappedFile> entries pointing at deleted inodes.
        storage->clear_mmap_cache();
        if (std::filesystem::exists(cache_path) && std::filesystem::is_directory(cache_path))
        {
            for (const auto& entry : std::filesystem::directory_iterator(cache_path))
            {
                auto fname = entry.path().filename().string();
                if (fname != db_fname && !fname.starts_with(std::string(db_fname)))
                    std::filesystem::remove_all(entry);
            }
        }
    }

    // --- meta ---

    inline void set_meta(const std::string& key, const std::string& value)
    {
        if (key == "size" || key == "count" || key == "counters_v" || key == "file_size")
            throw std::runtime_error("set_meta: reserved key");
        db()->exec(SET_META_STMT, key, value);
    }

    [[nodiscard]] inline std::optional<std::string> get_meta(const std::string& key)
    {
        return db()->template exec<std::string>(GET_META_STMT, key);
    }

    struct CheckResult
    {
        bool ok = true;
        std::size_t orphaned_files = 0;
        std::size_t dangling_rows = 0;
        std::size_t size_mismatches = 0;
        bool counters_consistent = true;
        bool sqlite_integrity_ok = true;

        explicit operator bool() const { return ok; }
    };

    CheckResult check(bool fix = false)
    {
        auto db = this->db();
        CheckResult result;

        result.sqlite_integrity_ok = _check_sqlite_integrity(db);
        result.dangling_rows = _check_dangling_rows(db, fix);
        result.size_mismatches = _check_size_mismatches(db, fix);
        result.orphaned_files = _check_orphaned_files(db, fix);
        result.counters_consistent = _check_counters(db, fix);

        result.ok = result.sqlite_integrity_ok
                 && result.dangling_rows == 0
                 && result.size_mismatches == 0
                 && result.orphaned_files == 0
                 && result.counters_consistent;
        return result;
    }

    // --- Stats (only with WithStats) ---

    struct Stats
    {
        uint64_t hits;
        uint64_t misses;
    };

    Stats stats() const
        requires (has_stats)
    {
        return { WithStats::_hits.load(std::memory_order_relaxed),
                 WithStats::_misses.load(std::memory_order_relaxed) };
    }

    void reset_stats()
        requires (has_stats)
    {
        WithStats::_hits.store(0, std::memory_order_relaxed);
        WithStats::_misses.store(0, std::memory_order_relaxed);
    }

    bool _check_sqlite_integrity(DbGuard& db)
    {
        if (auto r = db->template exec<std::string>("PRAGMA integrity_check;"))
            return *r == "ok";
        return false;
    }

    std::size_t _check_dangling_rows(DbGuard& db, bool fix)
    {
        std::size_t count = 0;
        std::vector<std::string> to_fix;

        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db->get(),
                "SELECT key, path FROM cache WHERE path IS NOT NULL;",
                -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                if (path && !std::filesystem::exists(storage->abs_path(path)))
                {
                    ++count;
                    if (fix)
                    {
                        auto key = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                        to_fix.emplace_back(key);
                    }
                }
            }
            sqlite3_finalize(stmt);
        }

        if (fix)
        {
            for (auto& key : to_fix)
                db->exec(DELETE_STMT, key);
        }

        return count;
    }

    std::size_t _check_size_mismatches(DbGuard& db, bool fix)
    {
        std::size_t count = 0;

        struct Mismatch { std::string key; std::size_t file_size; };
        std::vector<Mismatch> to_fix;

        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db->get(),
                "SELECT key, path, size FROM cache WHERE path IS NOT NULL;",
                -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                auto path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                auto abs = path ? storage->abs_path(path) : std::filesystem::path {};
                if (!path || !std::filesystem::exists(abs))
                    continue; // dangling row — handled separately

                auto db_size = static_cast<std::size_t>(sqlite3_column_int64(stmt, 2));
                auto file_size = std::filesystem::file_size(abs);
                if (db_size != file_size)
                {
                    ++count;
                    if (fix)
                    {
                        auto key = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                        to_fix.push_back({ key, file_size });
                    }
                }
            }
            sqlite3_finalize(stmt);
        }

        // UPDATE ... SET size = ... fires cache_size_update, which keeps
        // meta 'size' correct from the (old size, new size) delta.
        if (fix)
        {
            for (auto& [key, file_size] : to_fix)
                db->exec("UPDATE cache SET size = ? WHERE key = ?;", file_size, key);
        }

        return count;
    }

    std::size_t _check_orphaned_files(DbGuard& db, bool fix)
    {
        // Collect all known file paths from DB. Pending trash entries are
        // known too: they are displaced files inside their grace window, and
        // unlinking them early would reopen the reader TOCTOU the trash
        // mechanism exists to close — the background drain owns their removal.
        std::unordered_set<std::string> known_paths;
        for (auto sql : { "SELECT path FROM cache WHERE path IS NOT NULL;",
                          "SELECT path FROM trash;" })
        {
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(db->get(), sql, -1, &stmt, nullptr);
            while (sqlite3_step(stmt) == SQLITE_ROW)
            {
                if (auto p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)))
                    known_paths.insert(storage->abs_path(p).lexically_normal().string());
            }
            sqlite3_finalize(stmt);
        }

        std::size_t count = 0;

        if (!std::filesystem::exists(cache_path))
            return 0;

        for (auto& entry : std::filesystem::recursive_directory_iterator(cache_path))
        {
            if (!entry.is_regular_file())
                continue;

            auto fname = entry.path().filename().string();
            // Skip database files (db, WAL, SHM, journal)
            if (fname == db_fname || fname.starts_with(std::string(db_fname)))
                continue;

            auto path_str = entry.path().lexically_normal().string();
            if (known_paths.find(path_str) == known_paths.end())
            {
                ++count;
                if (fix)
                    std::filesystem::remove(entry.path());
            }
        }

        return count;
    }

    // Cross-checks the trigger-maintained meta counters against a direct
    // recomputation from the cache table. These should always agree — the
    // triggers update meta in the same transaction as the row change — so a
    // mismatch here means something wrote to `cache` without going through
    // the triggers (e.g. manual DB surgery).
    bool _check_counters(DbGuard& db, bool fix)
    {
        // The three ground-truth queries, the three meta reads, and (when
        // fixing) the three meta writes must all see one consistent snapshot
        // — otherwise a concurrent writer between them can make fix=true
        // overwrite meta with an already-stale value.
        _NestedTxn txn(*this);

        auto db_size = db->template exec<std::size_t>(
            "SELECT COALESCE(SUM(size), 0) FROM cache;");
        auto db_count = db->template exec<std::size_t>(
            "SELECT COUNT(*) FROM cache;");
        auto db_file_size = db->template exec<std::size_t>(
            "SELECT COALESCE(SUM(size), 0) FROM cache WHERE path IS NOT NULL;");

        if (!db_size || !db_count || !db_file_size)
        {
            txn.rollback();
            return false;
        }

        auto meta_size = db->template exec<std::size_t>(META_SIZE_STMT);
        auto meta_count = db->template exec<std::size_t>(META_COUNT_STMT);
        auto meta_file_size = db->template exec<std::size_t>(META_FILE_SIZE_STMT);

        bool consistent = meta_size && meta_count && meta_file_size
                        && *meta_size == *db_size && *meta_count == *db_count
                        && *meta_file_size == *db_file_size;

        if (!consistent && fix)
        {
            db->exec(SET_META_STMT, std::string("size"), *db_size);
            db->exec(SET_META_STMT, std::string("count"), *db_count);
            db->exec(SET_META_STMT, std::string("file_size"), *db_file_size);
        }

        txn.commit();
        return consistent;
    }
};
