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
#include <unistd.h>

template <typename Storage, typename... Policies>
class _Store : private Policies...
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
    pid_t _owner_pid;
    mutable Database _db;
    mutable std::recursive_mutex _mtx;
    bool _in_transaction = false;

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

    CompiledStatement COUNT_STMT {
        std::string("SELECT COUNT(*) FROM cache WHERE 1=1") + _where_valid() + ";"
    };
    CompiledStatement KEYS_STMT {
        std::string("SELECT key FROM cache WHERE 1=1") + _where_valid() + ";"
    };
    CompiledStatement EXISTS_STMT {
        std::string("SELECT 1 FROM cache WHERE key = ?") + _where_valid() + " LIMIT 1;"
    };
    CompiledStatement GET_STMT {
        std::string("SELECT value, path FROM cache WHERE key = ?") + _where_valid() + ";"
    };
    CompiledStatement GET_PATH_SIMPLE_STMT { "SELECT path FROM cache WHERE key = ?;" };
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
        EXPIRE_STMT { "SELECT path FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');" };
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
            &COUNT_STMT, &KEYS_STMT, &EXISTS_STMT, &GET_STMT,
            &GET_PATH_SIMPLE_STMT, &REPLACE_VALUE_STMT, &REPLACE_PATH_STMT,
            &INSERT_VALUE_STMT, &INSERT_PATH_STMT, &DELETE_STMT,
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
              " INSERT OR IGNORE INTO meta (key, value) VALUES ('size', '0');"
            + _extra_schema_indexes();
    }

    static inline constexpr auto _PRAGMA_SQL =
        R"(
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA wal_autocheckpoint=0;
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
            if (_db.open(this->cache_path / db_fname, init_stmts) && _compile_statements())
            {
                _migrate_schema();
                return;
            }
            _db.close();
            std::this_thread::sleep_for(std::chrono::milliseconds(50 * (1 << attempt)));
        }
        throw std::runtime_error("Failed to initialize database schema.");
    }

    void _migrate_schema()
    {
        if constexpr (has_tags)
        {
            sqlite3_exec(_db.get(),
                "ALTER TABLE cache ADD COLUMN tag TEXT DEFAULT NULL;",
                nullptr, nullptr, nullptr);
            sqlite3_exec(_db.get(),
                "CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag) WHERE tag IS NOT NULL;",
                nullptr, nullptr, nullptr);
        }
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
            sqlite3_exec(bg_db,
                "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= unixepoch('now');",
                nullptr, nullptr, nullptr);
            for (auto& f : files)
                storage->remove(f);
        }

        if constexpr (has_eviction)
        {
            if (max_size == 0)
                return;

            sqlite3_exec(bg_db,
                "UPDATE meta SET value = (SELECT COALESCE(SUM(size), 0) FROM cache) WHERE key = 'size';",
                nullptr, nullptr, nullptr);

            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(bg_db, "SELECT value FROM meta WHERE key = 'size';", -1, &stmt, nullptr);
            std::size_t current_size = 0;
            if (stmt && sqlite3_step(stmt) == SQLITE_ROW)
                current_size = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
            if (stmt) sqlite3_finalize(stmt);

            if (current_size <= max_size)
                return;

            auto target = max_size * 9 / 10;
            stmt = nullptr;
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
                if (stmt)
                {
                    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
                    sqlite3_step(stmt);
                    sqlite3_reset(stmt);
                }
                if (!path.empty())
                    storage->remove(path);
            }
            if (stmt) sqlite3_finalize(stmt);
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

        while (!_stop_checkpoint.load(std::memory_order_relaxed))
        {
            std::unique_lock lock(_checkpoint_mutex);
            _checkpoint_cv.wait_for(lock, std::chrono::seconds(1));
            if (_stop_checkpoint.load(std::memory_order_relaxed))
                break;
            sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
            if constexpr (has_expiration || has_eviction)
                _bg_evict(cp_db);
        }

        sqlite3_wal_checkpoint_v2(cp_db, nullptr, SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
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

public:
    class TransactionGuard
    {
        _Store& _store;
        std::unique_lock<std::recursive_mutex> _lock;
        Transaction _txn;
        bool _finished = false;

    public:
        TransactionGuard(_Store& store)
            : _store(store)
            , _lock(store._mtx)
            , _txn(store._db.get(), true)
        {
        }

        TransactionGuard(const TransactionGuard&) = delete;
        TransactionGuard& operator=(const TransactionGuard&) = delete;
        TransactionGuard(TransactionGuard&& other) noexcept
            : _store(other._store)
            , _lock(std::move(other._lock))
            , _txn(std::move(other._txn))
            , _finished(other._finished)
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
            _store._in_transaction = false;
            return _txn.commit();
        }

        bool rollback()
        {
            if (_finished) return false;
            _finished = true;
            _store._in_transaction = false;
            return _txn.rollback();
        }
    };

private:

    // --- Bind helpers for policy-aware INSERT/REPLACE ---

    void _bind_core_and_policies(sqlite3_stmt* stmt, const std::string& col1,
                                 const Bytes auto& col2, std::size_t sz,
                                 [[maybe_unused]] std::optional<double> abs_exp,
                                 [[maybe_unused]] std::size_t seq,
                                 [[maybe_unused]] std::optional<std::string> tag) const
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
                                 [[maybe_unused]] std::optional<std::string> tag) const
    {
        int i = 1;
        sql_bind(stmt, i++, col1);
        sql_bind(stmt, i++, col2);
        sql_bind(stmt, i++, sz);
        if constexpr (has_expiration) sql_bind(stmt, i++, abs_exp);
        if constexpr (has_eviction) sql_bind(stmt, i++, seq);
        if constexpr (has_tags) sql_bind(stmt, i++, tag);
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

        if (std::size(value) <= _file_size_threshold)
        {
            auto binded = REPLACE_VALUE_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, value, std::size(value), abs_exp, seq, tag);
            sqlite3_step(binded.get());
            return true;
        }

        auto transaction = db->begin_transaction(true);
        auto filepath = db->template exec<std::filesystem::path>(GET_PATH_SIMPLE_STMT, key);
        auto new_filepath = storage->store(value);
        if (!new_filepath)
        {
            (void)transaction.rollback();
            return false;
        }
        {
            auto path_str = new_filepath->string();
            auto binded = REPLACE_PATH_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, path_str, std::size(value),
                                    abs_exp, seq, tag);
            sqlite3_step(binded.get());
        }
        if (!transaction.commit())
        {
            (void)transaction.rollback();
            storage->remove(*new_filepath);
            return false;
        }
        if (filepath && !filepath->empty())
            storage->remove(*filepath);
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

        if (std::size(value) <= _file_size_threshold)
        {
            auto binded = INSERT_VALUE_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, value, std::size(value), abs_exp, seq, tag);
            sqlite3_step(binded.get());
            if (sqlite3_changes(db->get()) > 0)
                return true;
            return false;
        }

        auto file_path = storage->store(value);
        if (!file_path)
            return false;

        {
            auto path_str = file_path->string();
            auto binded = INSERT_PATH_STMT.bind_all();
            _bind_core_and_policies(binded.get(), key, path_str, std::size(value),
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
            , _owner_pid(getpid())
    {
        _init_db();
        _checkpoint_thread = std::thread(&_Store::_checkpoint_loop, this);
    }

    ~_Store()
    {
        if (getpid() != _owner_pid)
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

    TransactionGuard begin_user_transaction()
    {
        std::unique_lock lock(_mtx);
        if (_in_transaction)
            throw std::runtime_error("Nested transactions are not supported");
        _in_transaction = true;
        lock.unlock();
        try {
            return TransactionGuard(*this);
        } catch (...) {
            std::lock_guard g(_mtx);
            _in_transaction = false;
            throw;
        }
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
        if (auto r = db()->template exec<std::size_t>(COUNT_STMT))
            return *r;
        return 0;
    }

    [[nodiscard]] inline size_t size()
    {
        if (auto r = db()->template exec<size_t>("SELECT COALESCE(SUM(size), 0) FROM cache;"))
            return *r;
        return 0;
    }

    [[nodiscard]] inline std::vector<std::string> keys()
    {
        if (auto r = db()->template exec<std::vector<std::string>>(KEYS_STMT))
            return *r;
        return {};
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
                else
                {
                    del(key);
                    std::cerr << "Error loading file for key: " << key << ", deleting entry."
                              << std::endl;
                    return std::nullopt;
                }
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
        auto db = this->db();
        auto filepath = db->template exec<std::filesystem::path>(GET_PATH_SIMPLE_STMT, key);
        if (!db->exec(DELETE_STMT, key))
            return false;
        if (sqlite3_changes(db->get()) == 0)
            return false;
        if (filepath && !filepath->empty())
            storage->remove(*filepath);
        return true;
    }

    inline std::optional<Buffer> pop(const std::string& key)
    {
        auto result = get(key);
        if (result)
            del(key);
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
            while (auto r = db->template step<std::filesystem::path>(binded))
            {
                const auto file_path = *r;
                if (!storage->remove(file_path))
                    std::cerr << "Failed to delete file: " << file_path << std::endl;
            }
        }
        if (!db->exec(EVICT_EXPIRED_STMT))
            std::cerr << "Error deleting expired items from cache database." << std::endl;
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

        for (auto& [key, path, _] : to_evict)
        {
            db->exec(DELETE_STMT, key);
            if (!path.empty())
                storage->remove(path);
        }

        return to_evict.size();
    }

    // --- Tag-specific ---

    inline std::size_t evict_tag(const std::string& tag)
        requires (has_tags)
    {
        auto db = this->db();
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
        auto count = static_cast<std::size_t>(sqlite3_changes(db->get()));
        for (auto& f : files)
            storage->remove(f);
        return count;
    }

    // --- incr / decr ---

    inline int64_t incr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        auto db = this->db();
        auto transaction = db->begin_transaction(true);

        int64_t current = default_value;
        if (auto blob = db->template exec<std::vector<char>>(INCR_GET_STMT, key))
        {
            if (blob->size() == sizeof(int64_t))
                std::memcpy(&current, blob->data(), sizeof(int64_t));
        }

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

        transaction.commit();
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
        sqlite3_exec(db->get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
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
        auto db = this->db();
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db->get(),
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?);", -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    [[nodiscard]] inline std::optional<std::string> get_meta(const std::string& key)
    {
        auto db = this->db();
        sqlite3_stmt* stmt;
        sqlite3_prepare_v2(db->get(),
            "SELECT value FROM meta WHERE key = ?;", -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        std::optional<std::string> result;
        if (sqlite3_step(stmt) == SQLITE_ROW)
            result = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        sqlite3_finalize(stmt);
        return result;
    }

    inline bool check()
    {
        auto db = this->db();
        const char* sql = "SELECT COUNT(*) FROM cache;";
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db->get(), sql, -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::cerr << "Error preparing statement: " << sqlite3_errmsg(db->get()) << std::endl;
            return false;
        }
        bool valid = (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) >= 0);
        sqlite3_finalize(stmt);
        return valid;
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
};
