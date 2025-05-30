/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include <iostream>
#include <string>
#include <bitset>
#include <unordered_map>
#include <memory>
#include <vector>
#include <sqlite3.h>
#include <optional>
#include <stdexcept>
#include <mutex>

/*
use shared_mutex for thread safety
std::atomic<int> for frequency tracking
Optionally consider lock-free structures (like concurrent_hash_map from TBB) if perf is critical.
*/

struct CacheStats {
    public:
        size_t byte_size;
        size_t item_nb;
        // std::bitset<8> values;
        // std::string key;
        // int frequency;
        // std::chrono::time_point<std::chrono::steady_clock> last_access;
};

// Base Cache class
class Cache {
    public:
        sqlite3 *db;
        sqlite3_stmt* stmt;
        std::mutex global_mutex;
        size_t current_size;
        size_t max_size;
        int check_;

        Cache(const std::string &db_path, size_t max_size = 1000)
        {
            check_ = open();

            if (!check_)
                exit(84);
        }

        /*Cache::Cache(const std::string& db_path, size_t max_size) : db(nullptr), current_size(0), max_size(max_size) {
            if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
                std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
                db = nullptr;
            } else {
                const char *create_table_sql = R"(
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    );
                )";
                sqlite3_exec(db, create_table_sql, nullptr, nullptr, nullptr);
            }
        }*/

        ~Cache() {;}

        int open()
        {
            int check = sqlite3_open("cache.db", &db);

            if (check)
                std::cerr << "Error opening database: " << sqlite3_errmsg(db) << std::endl;
            else
                std::cout << "Database opened successfully." << std::endl;
            return check;
        }

        void close()
        {
            if (db)
                sqlite3_close(db);
        }

        std::optional<std::string> get(const std::string& key)
        {
            std::lock_guard<std::mutex> lock(global_mutex);

            const char *sql = "SELECT value FROM cache WHERE key = ?;";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
                return std::nullopt;

            if (sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC) != SQLITE_OK) {
                sqlite3_finalize(stmt);
                return std::nullopt;
            }

            std::optional<std::string> result;
            if (sqlite3_step(stmt) == SQLITE_ROW)
                result = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));

            sqlite3_finalize(stmt);
            return result;
        }

        bool set(const std::string& key, const std::string& value, int expire = 3600)
        {
            std::lock_guard<std::mutex> lock(global_mutex);

            const char *sql = "REPLACE INTO cache (key, value, expire) VALUES (?, ?, ?);";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
                return false;

            sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_STATIC);

            bool success = (sqlite3_step(stmt) == SQLITE_DONE);
            sqlite3_finalize(stmt);
            return success;
        }

        // Add a value if the key doesn't already exist
        bool add(const std::string& key, const std::string& value)
        {
            return false;
        }

        bool delete(const std::string& key)
        {
            std::lock_guard<std::mutex> lock(global_mutex);

            const char *sql = "DELETE FROM cache WHERE key = ?;";
            sqlite3_stmt *stmt;
            sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
            sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

            bool success = (sqlite3_step(stmt) == SQLITE_DONE);
            sqlite3_finalize(stmt);
            return success;
        }

        std::optional<std::string> pop(const std::string& key)
        {
            std::lock_guard<std::mutex> lock(global_mutex);

            const char *sql = "SELECT value FROM cache WHERE key = ?;";
            sqlite3_stmt *stmt;
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
                return std::nullopt;
            sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

            std::optional<std::string> result;
            if (sqlite3_step(stmt) == SQLITE_ROW)
                result = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));

            const char *sql = "DELETE FROM cache WHERE key = ?;";
            sqlite3_stmt *stmt;
            sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
            sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

            sqlite3_finalize(stmt);
            return result;
        }

        // Touch a key to update its expiration time
        bool touch(const std::string& key, int expire)
        {
            return false;
        }

        // Delete expired items
        void expire()
        {
            std::lock_guard<std::mutex> lock(global_mutex);

            const char *sql = "DELETE FROM cache WHERE expire < datetime('now');";
            sqlite3_exec(db, sql, nullptr, nullptr, nullptr);
        }

        // Delete items based on policy
        void evict()
        {
            ;
        }

        // Get the cache directory
        void clear()
        {
            std::lock_guard<std::mutex> lock(global_mutex);
            sqlite3_exec(db, "DELETE FROM cache;", nullptr, nullptr, nullptr);
        }

        // Check the cache statistics
        CacheStats stats()
        {
            ;
        }

        // Check if the cache is valid
        bool check()
        {
            return false;
        }

        // Remove old items or least recently used items
        bool cull()
        {
            return false;
        }
};
