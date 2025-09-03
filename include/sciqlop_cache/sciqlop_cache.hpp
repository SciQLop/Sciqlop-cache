/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include "database.hpp"
#include "disk_storage.hpp"
#include "utils/concepts.hpp"
#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <optional>
#include <span>
#include <sqlite3.h>
#include <string>

template <typename Storage>
class _Cache
{
    std::filesystem::path cache_path;
    size_t max_size;
    bool auto_clean = false;
    Database db;
    std::unique_ptr<Storage> storage;
    std::size_t _file_size_threshold = 8 * 1024;


    CompiledStatement COUNT_STMT {
        "SELECT COUNT(*) FROM cache WHERE (expire IS NULL OR expire > strftime('%s', 'now'));"
    };
    CompiledStatement KEYS_STMT {
        "SELECT key FROM cache WHERE (expire IS NULL OR expire > strftime('%s', 'now'));"
    };
    CompiledStatement EXISTS_STMT {
        "SELECT 1 FROM cache WHERE key = ? AND (expire IS NULL OR expire > strftime('%s', 'now')) LIMIT 1;"
    };
    CompiledStatement GET_STMT {
        "SELECT value, path FROM cache WHERE key = ? AND (expire IS NULL OR expire > strftime('%s', 'now'));"
    };
    CompiledStatement GET_PATH_STMT {
        "SELECT path FROM cache WHERE key = ? AND (expire IS NULL OR expire > strftime('%s', 'now'));"
    };
    CompiledStatement REPLACE_VALUE_STMT {
        R"(
            REPLACE INTO cache (key, value, expire, size)
            VALUES (?, ?, (strftime('%s', 'now') + ?), ?);
        )"
    };
    CompiledStatement REPLACE_PATH_STMT {
        R"(
            REPLACE INTO cache (key, path, expire, size)
            VALUES (?, ?, (strftime('%s', 'now') + ?), ?);
        )"
    };
    CompiledStatement INSERT_VALUE_STMT {
        "INSERT INTO cache (key, value, expire, size) VALUES (?, ?, (strftime('%s', 'now') + ?), ?);"
    };
    CompiledStatement INSERT_PATH_STMT {
        "INSERT INTO cache (key, path, expire, size) VALUES (?, ?, (strftime('%s', 'now') + ?), ?);"
    };
    CompiledStatement DELETE_STMT { "DELETE FROM cache WHERE key = ?;" };
    CompiledStatement TOUCH_STMT {
        "UPDATE cache SET last_update = strftime('%s', 'now'), expire = strftime('%s', 'now') + ?, last_use = strftime('%s', 'now') WHERE key = ?;"
    };
    CompiledStatement EXPIRE_STMT {
        "SELECT path FROM cache WHERE expire IS NOT NULL AND expire <= strftime('%s', 'now');"
    };
    CompiledStatement EVICT_STMT {
        "DELETE FROM cache WHERE expire IS NOT NULL AND expire <= strftime('%s', 'now');"
    };

    CompiledStatement* statements[13] = { &COUNT_STMT,      &KEYS_STMT,       &EXISTS_STMT,
                           &GET_STMT,        &GET_PATH_STMT,   &REPLACE_VALUE_STMT,
                           &REPLACE_PATH_STMT, &INSERT_VALUE_STMT, &INSERT_PATH_STMT,
                           &DELETE_STMT,     &TOUCH_STMT,      &EXPIRE_STMT,
                           &EVICT_STMT };


    static inline constexpr auto _INIT_STMTS = {
        R"(
            -- Use Write-Ahead Logging for better concurrency
            PRAGMA journal_mode=TRUNCATE;
            -- Set synchronous mode to NORMAL for performance
            PRAGMA synchronous=NORMAL;
            -- Set the number of cache pages
            PRAGMA cache_size=10000;
            -- Store temporary tables in memory
            PRAGMA temp_store=MEMORY;
            -- Set memory-mapped I/O size for performance
            PRAGMA mmap_size=268435456;
            -- Limit the number of rows analyzed for query planning
            PRAGMA analysis_limit=1000;
            PRAGMA busy_timeout=600000;
            )",

        R"(
            CREATE TABLE IF NOT EXISTS cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                path TEXT DEFAULT NULL,
                value BLOB DEFAULT NULL,
                expire REAL DEFAULT NULL,
                last_update REAL NOT NULL DEFAULT (strftime('%s', 'now')),
                last_use REAL NOT NULL DEFAULT (strftime('%s', 'now')),
                access_count_since_last_update INT NOT NULL DEFAULT 0,
                size INT NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_key ON cache (key);

            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value
            );

            INSERT OR IGNORE INTO meta (key, value) VALUES ('size', '0');

            -- Trigger for INSERT
            CREATE TRIGGER IF NOT EXISTS cache_size_insert
            AFTER INSERT ON cache
            BEGIN
                UPDATE meta SET value = COALESCE((SELECT SUM(size) FROM cache), 0) WHERE key = 'size';
            END;

            -- Trigger for DELETE
            CREATE TRIGGER IF NOT EXISTS cache_size_delete
            AFTER DELETE ON cache
            BEGIN
                UPDATE meta SET value = COALESCE((SELECT SUM(size) FROM cache), 0) WHERE key = 'size';
            END;

            -- Trigger for UPDATE OF size
            CREATE TRIGGER IF NOT EXISTS cache_size_update
            AFTER UPDATE OF size ON cache
            BEGIN
                UPDATE meta SET value = COALESCE((SELECT SUM(size) FROM cache), 0) WHERE key = 'size';
            END;
)"

    };

    inline bool _compile_statements()
    {
        bool result = true;
        for (auto& stmt : statements)
            result &= stmt->compile(db.get());
        return result;
    }

    inline bool _finalize_statements()
    {
        bool result = true;
        for (auto& stmt : statements)
            result &=stmt->finalize();
        return result;
    }


public:
    static constexpr std::string_view db_fname = "sciqlop-cache.db";

    _Cache(const std::filesystem::path& cache_path = ".cache/", size_t max_size = 1000)
            : cache_path(cache_path)
            , max_size(max_size)
            , storage(std::make_unique<Storage>(cache_path))
    {
        if (!db.open(this->cache_path / db_fname, _INIT_STMTS) || !_compile_statements())
        {
            throw std::runtime_error("Failed to initialize database schema.");
        }
    }

    ~_Cache() { close(); }

    [[nodiscard]] inline bool opened() const { return db.opened(); }

    inline bool close()
    {
        return _finalize_statements() & db.close();
    }

    [[nodiscard]] inline std::filesystem::path path() { return cache_path; }

    [[nodiscard]] inline size_t max_cache_size() { return max_size; }

    [[nodiscard]] inline std::size_t file_size_threshold() { return _file_size_threshold; }

    [[nodiscard]] inline std::size_t count()
    {
        if (auto r = db.exec<std::size_t>(COUNT_STMT))
            return *r;
        return 0;
    }

    [[nodiscard]] inline size_t size()
    {
        if (auto r = db.exec<size_t>("SELECT value FROM meta WHERE key = 'size';"))
            return *r;
        return 0;
    }

    [[nodiscard]] inline std::vector<std::string> keys()
    {
        if (auto r = db.exec<std::vector<std::string>>(KEYS_STMT))
            return *r;
        return {};
    }

    [[nodiscard]] inline bool exists(const std::string& key)
    {
        if (auto r = db.exec<bool>(EXISTS_STMT, key))
            return *r;
        return false;
    }

    inline bool set(const std::string& key, const Bytes auto& value)
    {
        return set(key, value, std::chrono::seconds { 3600 });
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        const double expires_secs
            = std::chrono::duration_cast<std::chrono::seconds>(expire).count();

        if (std::size(value) <= _file_size_threshold)
            return db.exec(REPLACE_VALUE_STMT, key, value, expires_secs,
                           std::size(value));

        if (const auto file_path = storage->store(value))
        {
            const auto file_path_str = file_path->string();
            if (!db.exec(REPLACE_PATH_STMT, key, file_path_str, expires_secs,
                         std::size(value)))
                return false;
        }
        else
        {
            std::cerr << "Error storing file for key: " << key << std::endl;
            return false;
        }
        return true;
    }

    inline std::optional<Buffer> get(const std::string& key)
    {
        if (auto values
            = db.exec<std::vector<char>, std::filesystem::path>(GET_STMT, key))
        {
            const auto& [_, path] = *values;

            if (!path.empty())
            {
                if (auto result = storage->load(path))
                    return result;
                else
                    del(key);
            }
            return Buffer(std::move(std::get<0>(*values)));
        }
        return std::nullopt;
    }

    inline bool add(const std::string& key, const Bytes auto& value)
    {
        return add(key, value, std::chrono::seconds { 3600 });
    }

    // Add a value if the key doesn't already exist
    inline bool add(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        const double expires_secs
            = std::chrono::duration_cast<std::chrono::seconds>(expire).count();

        if (auto filepath = db.exec<std::filesystem::path>(GET_PATH_STMT, key))
        {
            if (!storage->remove(cache_path / *filepath))
                std::cerr << "Error deleting existing file for key: " << key << std::endl;
        }

        if (std::size(value) <= _file_size_threshold)
            return db.exec(INSERT_VALUE_STMT, key, value, expires_secs,
                           std::size(value));

        if (const auto file_path = storage->store(value))
        {
            const auto file_path_str = file_path->string();
            if (!db.exec(INSERT_PATH_STMT, key, file_path_str, expires_secs,
                         std::size(value)))
                return false;
        }
        else
        {
            std::cerr << "Error storing file for key: " << key << std::endl;
            return false;
        }
        return true;
    }

    inline bool del(const std::string& key)
    {
        using namespace std;
        if (!exists(key))
        {
            std::cerr << "Key not found: " << key << std::endl;
            return false;
        }
        if (auto filepath = db.exec<filesystem::path>(GET_PATH_STMT, key))
        {
            if (filesystem::exists(*filepath))
                filesystem::remove(*filepath);
        }
        if (db.exec(DELETE_STMT, key))
        {
            return true;
        }
        else
        {
            std::cerr << "Error deleting key: " << key << std::endl;
            return false;
        }
    }

    inline std::optional<Buffer> pop(const std::string& key)
    {
        std::optional<Buffer> result = get(key);

        if (!del(key))
            std::cerr << "Error deleting key: " << key << std::endl;
        return result;
    }

    // Touch a key to update its expiration time
    inline bool touch(const std::string& key, DurationConcept auto expire)
    {
        auto expire_secs = std::chrono::duration_cast<std::chrono::seconds>(expire).count();
        return db.exec(TOUCH_STMT, expire_secs, key);
    }

    inline void expire()
    {
        {
            auto binded = EXPIRE_STMT.bind_all();
            while (auto r = db.step<std::filesystem::path>(binded))
            {
                const auto file_path = *r;
                if (!storage->remove(file_path))
                {
                    std::cerr << "Failed to delete file: " << file_path << std::endl;
                }
            }
        }

        if (!db.exec(EVICT_STMT))
            std::cerr << "Error deleting expired items from cache database." << std::endl;
    }

    // Delete items based on policy
    inline void evict() { ; }

    inline void clear()
    {
        sqlite3_exec(db.get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
        if (std::filesystem::exists(cache_path) && std::filesystem::is_directory(cache_path))
        {
            for (const auto& entry : std::filesystem::directory_iterator(cache_path))
            {
                if (entry != cache_path / db_fname)
                    std::filesystem::remove(entry);
            }
        }
    }

    // Check the cache statistics
    // CacheStats stats()
    // {
    //     ;
    // }

    // Check if the cache is valid
    // need explaination and actually test everything
    inline bool check()
    {
        const char* sql = "SELECT COUNT(*) FROM cache;";
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db.get(), sql, -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::cerr << "Error preparing statement: " << sqlite3_errmsg(db.get()) << std::endl;
            return false;
        }

        bool valid = (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_int(stmt, 0) >= 0);
        sqlite3_finalize(stmt);
        return valid;
    }
};

using Cache = _Cache<DiskStorage>;
