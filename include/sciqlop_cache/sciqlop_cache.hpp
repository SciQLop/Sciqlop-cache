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
#include "utils/time.hpp"
#include <cpp_utils/io/memory_mapped_file.hpp>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <sqlite3.h>
#include <string>

template <typename Storage>
class _Cache
{
    std::filesystem::path cache_path;
    size_t max_size;
    std::mutex global_mutex;
    bool auto_clean = false;
    Database db;
    std::unique_ptr<Storage> storage;
    std::size_t _file_size_threshold = 8 * 1024;

    CompiledStatement statements[11]
        = { CompiledStatement { "SELECT COUNT(*) FROM cache;" },
            CompiledStatement { "SELECT key FROM cache;" },
            CompiledStatement { "SELECT 1 FROM cache WHERE key = ? LIMIT 1;" },
            CompiledStatement { "SELECT value, path FROM cache WHERE key = ? AND (expire IS NULL OR expire > strftime('%s', 'now'));" },
            CompiledStatement { "SELECT path FROM cache WHERE key = ? AND (expire IS NULL OR expire > strftime('%s', 'now'));" },
            CompiledStatement {
                "REPLACE INTO cache (key, value, expire, size) VALUES (?, ?, ?, ?);" },
            CompiledStatement {
                "REPLACE INTO cache (key, path, expire, size) VALUES (?, ?, ?, ?);" },
            CompiledStatement {
                "INSERT INTO cache (key, value, expire, size) VALUES (?, ?, ?, ?);" },
            CompiledStatement {
                "INSERT INTO cache (key, path, expire, size) VALUES (?, ?, ?, ?);" },
            CompiledStatement { "DELETE FROM cache WHERE key = ?;" },
            CompiledStatement { "UPDATE cache SET last_update = ?, expire = ? WHERE key = ?;" } };

    static inline constexpr auto _COUNT_STMT = 0;
    static inline constexpr auto _KEYS_STMT = 1;
    static inline constexpr auto _EXISTS_STMT = 2;
    static inline constexpr auto _GET_STMT = 3;
    static inline constexpr auto _GET_PATH_STMT = 4;
    static inline constexpr auto _REPLACE_VALUE_STMT = 5;
    static inline constexpr auto _REPLACE_PATH_STMT = 6;
    static inline constexpr auto _INSERT_VALUE_STMT = 7;
    static inline constexpr auto _INSERT_PATH_STMT = 8;
    static inline constexpr auto _DELETE_STMT = 9;
    static inline constexpr auto _TOUCH_STMT = 10;


public:
    static constexpr std::string_view db_fname = "sciqlop-cache.db";

    _Cache(const std::filesystem::path& cache_path = ".cache/", size_t max_size = 1000)
            : cache_path(cache_path)
            , max_size(max_size)
            , storage(std::make_unique<Storage>(cache_path))
    {
        if (db.open(cache_path / db_fname) != SQLITE_OK || !init())
            throw std::runtime_error("Failed to initialize database schema.");
    }

    ~_Cache()
    {
        if (db.valid())
            db.close();
    }

    [[nodiscard]] inline bool init()
    {
        bool result = db.exec(R"(
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=10000;
            PRAGMA temp_store=MEMORY;
            PRAGMA mmap_size=268435456;
            PRAGMA analysis_limit=1000;)"
            );
        if (!result)
            std::cerr << "PRAGMA exec failed in init()." << std::endl;

        result &= db.exec(R"(
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
            );

        if (!result)
            std::cerr << "Table creation failed in init()." << std::endl;

        result &= db.exec(R"(PRAGMA wal_checkpoint(TRUNCATE);)");

        if (!result)
            std::cerr << "WAL checkpoint failed in init()." << std::endl;

        for (auto& stmt : statements)
            stmt.compile(db.get());

        if (!result)
        {
            std::cerr << "SQL exec failed in init()." << std::endl;
        }
        return result;
    }

    [[nodiscard]] inline std::filesystem::path path() { return cache_path; }

    [[nodiscard]] inline size_t max_cache_size() { return max_size; }

    [[nodiscard]] inline std::size_t file_size_threshold() { return _file_size_threshold; }

    [[nodiscard]] inline std::size_t count()
    {
        if (auto r = db.exec<std::size_t>(statements[_COUNT_STMT]))
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
        if (auto r = db.exec<std::vector<std::string>>(statements[_KEYS_STMT]))
            return *r;
        return {};
    }

    [[nodiscard]] inline bool exists(const std::string& key)
    {
        if (auto r = db.exec<bool>(statements[_EXISTS_STMT], key))
            return *r;
        return false;
    }

    inline bool set(const std::string& key, const Bytes auto& value)
    {
        return set(key, value, std::chrono::seconds { 3600 });
    }

    inline bool set(const std::string& key, const Bytes auto& value, DurationConcept auto expire)
    {
        const auto now = std::chrono::system_clock::now();

        if (std::size(value) <= _file_size_threshold)
            return db.exec(statements[_REPLACE_VALUE_STMT], key, value, now + expire, std::size(value));

        if (const auto file_path = storage->store(value))
        {
            const auto file_path_str = file_path->string();
            if (!db.exec(statements[_REPLACE_PATH_STMT], key, file_path_str, now + expire, std::size(value)))
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
            = db.exec<std::vector<char>, std::filesystem::path>(statements[_GET_STMT], key))
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
        const auto now = std::chrono::system_clock::now();

        if (auto filepath = db.exec<std::filesystem::path>(statements[_GET_PATH_STMT], key))
        {
            if (!storage->remove(cache_path / *filepath))
                std::cerr << "Error deleting existing file for key: " << key << std::endl;
        }

        if (std::size(value) <= _file_size_threshold)
            return db.exec(statements[_INSERT_VALUE_STMT], key, value, now + expire, std::size(value));

        if (const auto file_path = storage->store(value))
        {
            const auto file_path_str = file_path->string();
            if (!db.exec(statements[_INSERT_PATH_STMT], key, file_path_str, now + expire, std::size(value)))
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
        if (auto filepath = db.exec<filesystem::path>(statements[_GET_PATH_STMT], key))
        {
            if (filesystem::exists(*filepath))
                filesystem::remove(*filepath);
        }
        if (db.exec(statements[_DELETE_STMT], key))
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
        return db.exec(statements[_TOUCH_STMT], std::chrono::system_clock::now(),
                       std::chrono::system_clock::now() + expire, key);
    }

    inline void expire()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        using namespace std::chrono_literals;
        const auto now = std::chrono::system_clock::now();
        double now_ = time_point_to_epoch(now);
        const char* sql = "SELECT id, path FROM cache WHERE expire IS NOT NULL AND expire <= ?";
        sqlite3_stmt* stmt = nullptr;

        if (sqlite3_prepare_v2(db.get(), sql, -1, &stmt, nullptr) != SQLITE_OK)
            return;

        sqlite3_bind_double(stmt, 1, now_);
        std::vector<int> expired_ids;

        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            int id = sqlite3_column_int(stmt, 0);
            const char* path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));

            if (path && sizeof(path) > 0)
            {
                std::filesystem::path file_path = std::filesystem::path(cache_path) / path;
                std::error_code ec;
                if (!std::filesystem::remove(file_path, ec) && ec)
                {
                    std::cerr << "Failed to delete file: " << file_path << " (" << ec.message()
                              << ")\n";
                }
            }

            expired_ids.push_back(id);
        }
        sqlite3_finalize(stmt);

        const char* delete_sql = "DELETE FROM cache WHERE id = ?;";
        if (sqlite3_prepare_v2(db.get(), delete_sql, -1, &stmt, nullptr) != SQLITE_OK)
        {
            std::cerr << "Failed to prepare DELETE statement: " << sqlite3_errmsg(db.get())
                      << std::endl;
            return;
        }

        for (int id : expired_ids)
        {
            sqlite3_bind_int(stmt, 1, id);
            if (sqlite3_step(stmt) != SQLITE_DONE)
            {
                std::cerr << "Failed to delete row with id " << id << ": "
                          << sqlite3_errmsg(db.get()) << std::endl;
            }
            sqlite3_reset(stmt);
        }
        sqlite3_finalize(stmt);
    }

    // Delete items based on policy
    inline void evict() { ; }

    inline void clear()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_exec(db.get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
        if (std::filesystem::exists(cache_path) && std::filesystem::is_directory(cache_path))
        {
            for (const auto& entry : std::filesystem::directory_iterator(cache_path))
            {
                if (entry != cache_path / db_fname)
                    std::filesystem::remove_all(entry);
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
        std::lock_guard<std::mutex> lock(global_mutex);
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
