/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

//#include "database.hpp"
#include "cache_by_files.hpp"
#include "utils/time.hpp"
#include <bitset>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <mutex>
#include <optional>
#include <sqlite3.h>
#include <string>
#include <cstdio>

class Cache
{
private:
    size_t max_size;
    sqlite3_stmt* stmt;
    std::mutex global_mutex;
    bool auto_clean = false;
    Database db;

public:
    Cache(const std::string& db_path = "sciqlop-cache.db", size_t max_size_ = 1000)
            : max_size(max_size_), stmt(nullptr)
    {
        if (db.open(db_path) != SQLITE_OK || !init())
            throw std::runtime_error("Failed to initialize database schema.");
    }

    bool init()
    {
        // const std::string sql = R"(
        //     CREATE TABLE IF NOT EXISTS cache (
        //         key TEXT PRIMARY KEY,
        //         value TEXT,
        //         expire REAL,
        //         last_update REAL
        //     );
        // )";
        const std::string sql = R"(
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT NOT NULL,
                path TEXT DEFAULT NULL,
                value BLOB DEFAULT NULL,
                expire REAL DEFAULT NULL,
                last_update REAL NOT NULL DEFAULT 0,
                last_use REAL NOT NULL DEFAULT 0,
                access_count_since_last_update INT NOT NULL DEFAULT 0
            );
        )";
        bool result = db.exec(sql);

        if (!result)
        {
            std::cerr << "SQL exec failed in init(). SQL: " << sql << std::endl;
        }
        return result;
    }

    [[nodiscard]] std::size_t count()
    {
        return db.exec<std::size_t>("SELECT COUNT(*) FROM cache;");
    }

    [[nodiscard]] std::vector<std::string> keys()
    {
        return db.exec<std::vector<std::string>>("SELECT key FROM cache;");
    }

    bool exists(const std::string& key)
    {
        auto exists = db.exec<bool>("SELECT 1 FROM cache WHERE key = ? LIMIT 1;", key);
        return exists;
    }

    inline bool set(const std::string& key, const Bytes auto & value)
    {
        return set(key, value, std::chrono::seconds { 3600 });
    }

    bool set(const std::string& key, const Bytes auto & value, Duration auto expire)
    {
        const auto now = std::chrono::system_clock::now();

        if (std::size(value) <= 500)
            return db.exec("REPLACE INTO cache (key, value, expire, last_update) VALUES (?, ?, ?, ?);", key, value, now + expire, now);
        db.exec("REPLACE INTO cache (key, path, expire, last_update) VALUES (?, ?, ?, ?);", key, key, now + expire, now);
        bool result = storeBytes(key, value, true);
        return result;
    }

    std::optional<std::vector<char>> get(const std::string& key)
    {
        auto values = db.exec<std::vector<char>, std::string>("SELECT value, path FROM cache WHERE key = ?;", key);
        const auto &[value, path] = values;

        if (!value.empty())
            return value;
        else if (!path.empty())
            return getBytes(path);
        else {
            std::cerr << "Key not found: " << key << std::endl;
            return std::nullopt;
        }
    }

    inline bool add(const std::string& key, const Bytes auto & value)
    {
        return add(key, value, std::chrono::seconds { 3600 });
    }

    // Add a value if the key doesn't already exist
    bool add(const std::string& key, const Bytes auto & value, Duration auto expire)
    {
        const auto now = std::chrono::system_clock::now();

        if (std::size(value) <= 500)
            return db.exec("INSERT INTO cache (key, value, expire, last_update) VALUES (?, ?, ?, ?);", key, value, now + expire, now);
        bool result = db.exec("INSERT INTO cache (key, path, expire, last_update) VALUES (?, ?, ?, ?);", key, key, now + expire, now);
        if (!result)
            return false;
        result = storeBytes(key, value, false);
        return result;
    }

    bool del(const std::string& key)
    {
        if (!exists(key)) {
            std::cerr << "Key not found: " << key << std::endl;
            return false;
        }

        db.exec("DELETE FROM cache WHERE key = ?;", key);
        auto success = true;
        if (success) {
            return true;
        } else {
            std::cerr << "Error deleting key: " << key << std::endl;
            return false;
        }
    }

    std::optional<std::vector<char>> pop(const std::string& key)
    {
        std::optional<std::vector<char>> result = get(key);

        if (!del(key))
            std::cerr << "Error deleting key: " << key << std::endl;
        return result;
    }

    // Touch a key to update its expiration time
    bool touch(const std::string& key, Duration auto expire)
    {
        db.exec("UPDATE cache SET last_update = ?, expire = ? WHERE key = ?;",
            std::chrono::system_clock::now(), std::chrono::system_clock::now() + expire, key);
        return true;
    }

    void expire()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        using namespace std::chrono_literals;
        const auto now = std::chrono::system_clock::now();
        double now_ = time_point_to_epoch(now);
        // const auto now_seconds = duration_cast<seconds>(now.time_since_epoch()).count();
        const char* sql = "SELECT id, path FROM cache WHERE expire IS NOT NULL AND expire <= ?";
        stmt = nullptr;

        if (sqlite3_prepare_v2(db.get(), sql, -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_double(stmt, 1, static_cast<double>(now_));

            while (sqlite3_step(stmt) == SQLITE_ROW) {
                int id = sqlite3_column_int(stmt, 0);
                const char* path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));

                if (path) std::remove(path);
                sqlite3_stmt* deleteStmt = nullptr;
                if (sqlite3_prepare_v2(db.get(), "DELETE FROM cache WHERE id = ?", -1, &deleteStmt, nullptr) == SQLITE_OK) {
                    sqlite3_bind_int(deleteStmt, 1, id);
                    sqlite3_step(deleteStmt);
                }
                sqlite3_finalize(deleteStmt);
            }
        }
        sqlite3_finalize(stmt);
        // const std::string sql = "DELETE FROM cache WHERE expire <= " + std::to_string(now_) + ";";
        // sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, nullptr);
    }

    // Delete items based on policy
    void evict() { ; }

    void clear()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_exec(db.get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
    }

    // Check the cache statistics
    // CacheStats stats()
    // {
    //     ;
    // }

    // Check if the cache is valid
    // need explaination and actually test everything
    bool check()
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
