/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** class Cache used to store and retrieve data
** often used by the user
*/

#pragma once

#include "database.hpp"
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
#include <filesystem>
#include <cpp_utils/io/memory_mapped_file.hpp>

using namespace cpp_utils::io;
class Buffer
{
    using shared_mmf = std::shared_ptr<memory_mapped_file>;
    std::variant<shared_mmf, std::vector<char>> _data;
    const char *data_ptr = nullptr;
    std::size_t size_ = 0;

    void init_from_variant()
    {
        if (std::holds_alternative<shared_mmf>(_data)) {
            auto& mmf = std::get<shared_mmf>(_data);
            data_ptr = mmf->data();
            size_ = mmf->size();
        } else if (std::holds_alternative<std::vector<char>>(_data)) {
            auto& v = std::get<std::vector<char>>(_data);
            data_ptr = v.data();
            size_ = v.size();
        }
    }

public:

    Buffer(const std::filesystem::path& path) : _data(std::make_shared<memory_mapped_file>(path.string()))
    {
        auto& mmf = std::get<shared_mmf>(_data);
        data_ptr = mmf->data();
        size_ = mmf->size();
    }

    Buffer(std::vector<char>&& vec) : _data(std::move(vec))
    {
        auto& v = std::get<std::vector<char>>(_data);
        data_ptr = v.data();
        size_ = v.size();
    }

    Buffer(const Buffer& other)
        : _data(other._data)
    {
        init_from_variant();
    }

    Buffer(Buffer&& other) noexcept
        : _data(std::move(other._data)), data_ptr(other.data_ptr), size_(other.size_)
    {
        other.data_ptr = nullptr;
        other.size_ = 0;
    }

    ~Buffer() = default;

    Buffer& operator=(const Buffer& other)
    {
        if (this != &other) {
            _data = other._data;
            init_from_variant();
        }
        return *this;
    }

    Buffer& operator=(Buffer&& other) noexcept
    {
        if (this != &other) {
            _data = std::move(other._data);
            data_ptr = other.data_ptr;
            size_ = other.size_;
            other.data_ptr = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    [[nodiscard]]inline operator bool() const noexcept { return data_ptr != nullptr; }
    [[nodiscard]]inline const char* data() const noexcept { return data_ptr; }
    [[nodiscard]]inline size_t size() const noexcept { return size_; }

    [[nodiscard]]inline std::vector<char> to_vector() const
    {
        if (std::holds_alternative<std::vector<char>>(_data)) {
            return std::get<std::vector<char>>(_data);
        } else if (std::holds_alternative<shared_mmf>(_data)) {
            return std::vector<char>(data_ptr, data_ptr + size_);
        }
        return {};
    }
};

class Cache
{
    std::filesystem::path cache_path;
    size_t max_size;
    sqlite3_stmt* stmt;
    std::mutex global_mutex;
    bool auto_clean = false;
    Database db;

public:
    static constexpr std::string_view db_fname = "sciqlop-cache.db";

    Cache(const std::filesystem::path &cache_path = ".cache/", size_t max_size = 1000)
            : cache_path(cache_path), max_size(max_size), stmt(nullptr)
    {
        std::filesystem::create_directories(cache_path);
        if (db.open(cache_path/db_fname) != SQLITE_OK || !init())
            throw std::runtime_error("Failed to initialize database schema.");
    }

    [[nodiscard]]bool init()
    {
        const std::string sql = R"(
            CREATE TABLE IF NOT EXISTS cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
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
        if (auto r = db.exec<std::size_t>("SELECT COUNT(*) FROM cache;"))
            return *r;
        return 0;
    }

    [[nodiscard]] std::vector<std::string> keys()
    {
        if (auto r = db.exec<std::vector<std::string>>("SELECT key FROM cache;"))
            return *r;
        return {};
    }

    [[nodiscard]]bool exists(const std::string& key)
    {
        if(auto r = db.exec<bool>("SELECT 1 FROM cache WHERE key = ? LIMIT 1;", key))
            return *r;
        return false;
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

        const auto filename = generate_random_filename();
        const auto file_path = cache_path / filename.substr(0, 2) / filename.substr(2, 2) / filename;
        if (!db.exec("REPLACE INTO cache (key, path, expire, last_update) VALUES (?, ?, ?, ?);", key, file_path, now + expire, now))
            return false;
        return storeBytes(file_path, value);
    }

    std::optional<Buffer> get(const std::string& key)
    {
        if(auto values = db.exec<std::vector<char>, std::filesystem::path>("SELECT value, path FROM cache WHERE key = ?;", key)) {
            const auto &[_, path] = *values;

            if (!path.empty()) {
                auto result = Buffer(path);
                if (result)
                    return result;
                else
                    del(key);
            }
            return Buffer(std::move(std::get<0>(*values)));
        }
        return std::nullopt;
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

        const auto filename = generate_random_filename();
        const auto file_path = cache_path / filename.substr(0, 2) / filename.substr(2, 2) / filename;
        if (!db.exec("INSERT INTO cache (key, path, expire, last_update) VALUES (?, ?, ?, ?);", key, file_path, now + expire, now))
            return false;
        return storeBytes(file_path, value);
    }

    bool del(const std::string& key)
    {
        using namespace std::filesystem;
        if (!exists(key)) {
            std::cerr << "Key not found: " << key << std::endl;
            return false;
        }

        auto success = db.exec("DELETE FROM cache WHERE key = ?;", key);
        if (success) {
            if (exists(cache_path / key))
                remove(cache_path / key);
            return true;
        } else {
            std::cerr << "Error deleting key: " << key << std::endl;
            return false;
        }
    }

    std::optional<Buffer> pop(const std::string& key)
    {
        std::optional<Buffer> result = get(key);

        if (!del(key))
            std::cerr << "Error deleting key: " << key << std::endl;
        return result;
    }

    // Touch a key to update its expiration time
    bool touch(const std::string& key, Duration auto expire)
    {
        return db.exec("UPDATE cache SET last_update = ?, expire = ? WHERE key = ?;",
            std::chrono::system_clock::now(), std::chrono::system_clock::now() + expire, key);
    }

    void expire()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        using namespace std::chrono_literals;
        const auto now = std::chrono::system_clock::now();
        double now_ = time_point_to_epoch(now);
        const char* sql = "SELECT id, path FROM cache WHERE expire IS NOT NULL AND expire <= ?";
        stmt = nullptr;

        if (sqlite3_prepare_v2(db.get(), sql, -1, &stmt, nullptr) != SQLITE_OK)
            return;

        sqlite3_bind_double(stmt, 1, now_);
        std::vector<int> expired_ids;

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int id = sqlite3_column_int(stmt, 0);
            const char* path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));

            if (path && sizeof(path) > 0) {
                std::filesystem::path file_path = std::filesystem::path(cache_path) / path;
                std::error_code ec;
                if (!std::filesystem::remove(file_path, ec) && ec) {
                    std::cerr << "Failed to delete file: " << file_path << " (" << ec.message() << ")\n";
                }
            }

            expired_ids.push_back(id);
        }
        sqlite3_finalize(stmt);

        const char* delete_sql = "DELETE FROM cache WHERE id = ?;";
        if (sqlite3_prepare_v2(db.get(), delete_sql, -1, &stmt, nullptr) != SQLITE_OK) {
            std::cerr << "Failed to prepare DELETE statement: " << sqlite3_errmsg(db.get()) << std::endl;
            return;
        }

        for (int id : expired_ids) {
            sqlite3_bind_int(stmt, 1, id);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                std::cerr << "Failed to delete row with id " << id << ": " << sqlite3_errmsg(db.get()) << std::endl;
            }
            sqlite3_reset(stmt);
        }
        sqlite3_finalize(stmt);
    }

    // Delete items based on policy
    void evict() { ; }

    void clear()
    {
        std::lock_guard<std::mutex> lock(global_mutex);
        sqlite3_exec(db.get(), "DELETE FROM cache;", nullptr, nullptr, nullptr);
        if (std::filesystem::exists(cache_path) && std::filesystem::is_directory(cache_path)) {
            for (const auto& entry : std::filesystem::directory_iterator(cache_path))
            {
                if (entry!=cache_path/db_fname)
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
