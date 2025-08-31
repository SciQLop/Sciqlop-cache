/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include "../utils/time.hpp"
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>
#include <cpp_utils/types/detectors.hpp>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <sqlite3.h>
#include <string>
#include <tuple>
#include <vector>

struct SQLiteDeleter
{
    void operator()(sqlite3* db) const
    {
        if (db)
            sqlite3_close(db);
    }
};

template <typename T>
concept Bytes = requires(T t) {
    { std::size(t) } -> std::convertible_to<std::size_t>;
    { std::data(t) } -> std::convertible_to<const char*>;
};

void sql_bind(const auto& stmt, int col, TimePoint auto&& value)
{
    sqlite3_bind_double(stmt, col, time_point_to_epoch(value));
}

void sql_bind(const auto& stmt, int col, Bytes auto&& value)
{
    sqlite3_bind_blob(stmt, col, std::data(value), static_cast<int>(std::size(value)),
                      SQLITE_STATIC);
}

void sql_bind(const auto& stmt, int col, const std::string& value)
{
    sqlite3_bind_text(stmt, col, value.c_str(), -1, SQLITE_STATIC);
}

void sql_bind_all(const auto& stm, auto&&... values)
{
    int i = 1;
    ((sql_bind(stm, i++, std::forward<decltype(values)>(values))), ...);
}

template <typename rtype>
auto sql_get(const auto& stmt, int col)
{
    static_assert(cpp_utils::types::detectors::is_any_of_v<rtype, std::vector<char>, std::string,
                                                           std::filesystem::path, bool, std::size_t,
                                                           std::vector<std::string>>
                      || TimePoint<rtype>,
                  "Unsupported return type for sql_get");

    if constexpr (std::is_same_v<rtype, std::vector<char>>)
    {
        const void* blob = sqlite3_column_blob(stmt, col);
        int size = sqlite3_column_bytes(stmt, col);
        if (blob && size > 0)
        {
            const char* start = static_cast<const char*>(blob);
            return std::vector<char>(start, start + size);
        }
        else
        {
            return std::vector<char> {};
        }
    }
    else if constexpr (std::is_same_v<rtype, std::string>)
    {
        const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
        if (v)
            return std::string(v);
        else
            return std::string {};
    }
    else if constexpr (std::is_same_v<rtype, std::filesystem::path>)
    {
        const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
        if (v)
            return std::filesystem::path(v);
        else
            return std::filesystem::path {};
    }
    else if constexpr (TimePoint<rtype>)
    {
        double v = sqlite3_column_double(stmt, col);
        return epoch_to_time_point(v);
    }
    else if constexpr (std::is_same_v<rtype, bool>)
    {
        return true;
    }
    else if constexpr (std::is_same_v<rtype, std::size_t>)
    {
        return static_cast<std::size_t>(sqlite3_column_int64(stmt, col));
    }
    else if constexpr (std::is_same_v<rtype, std::vector<std::string>>)
    {
        std::vector<std::string> result;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            const char* v = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
            if (v)
                result.emplace_back(v);
        }
        return result;
    }
    else
        return nullptr;
}

template <typename... rtypes, std::size_t... Is>
std::tuple<rtypes...> _sql_get_all(const auto& stmt, std::index_sequence<Is...>)
{
    return std::tuple { sql_get<rtypes>(stmt, Is)... };
}

template <typename... rtypes>
auto sql_get_all(const auto& stm)
{
    return _sql_get_all<rtypes...>(stm, std::index_sequence_for<rtypes...> {});
}

class Database
{
private:
    std::unique_ptr<sqlite3, SQLiteDeleter> db;
    std::mutex db_mutex;

public:
    Database() : db(nullptr, SQLiteDeleter()) { ; }

    ~Database()
    {
        if (db)
        {
            sqlite3_close(db.get());
        }
    }

    int open(const std::filesystem::path& db_path)
    {
        std::lock_guard<std::mutex> lock(db_mutex);

        sqlite3* tmp_db = nullptr;
        auto db_path_str = db_path.string();
        int check = sqlite3_open_v2(
            db_path_str.c_str(), &tmp_db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr);

        sqlite3_busy_timeout(tmp_db, 1000 * 60 * 15); // set busy timeout to 10 minutes

        if (check)
        {
            std::cerr << "Error opening database: " << sqlite3_errmsg(tmp_db) << std::endl;
            if (tmp_db)
                sqlite3_close(tmp_db);
        }
        else
        {
            std::cout << "Database opened successfully." << std::endl;
            db.reset(tmp_db);
        }
        return check;
    }

    void close()
    {
        if (db)
            sqlite3_close(db.get());
    }

    sqlite3* get() const { return db.get(); }

    std::mutex& mutex() { return db_mutex; }

    bool valid() const { return db != nullptr; }

    bool exec(const std::string& sql)
    {
        std::lock_guard<std::mutex> lock(db_mutex);
        char* errMsg = nullptr;

        int rc = sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK)
        {
            std::cerr << "SQL error: " << (errMsg ? errMsg : "unknown error") << std::endl;
            sqlite3_free(errMsg);
            return false;
        }
        return true;
    }

    template <typename... rtypes>
    consteval auto exec_return_type()
    {
        if constexpr (sizeof...(rtypes) == 0)
            return bool {};
        else if constexpr (sizeof...(rtypes) == 1)
            return std::optional<rtypes...> {};
        else
            return std::optional<std::tuple<rtypes...>> {};
    }

    template <typename... rtypes>
    auto exec(const std::string& sql, const auto&... values)
        -> decltype(exec_return_type<rtypes...>())
    {
        using namespace cpp_utils::lifetime;
        std::lock_guard<std::mutex> lock(db_mutex);
        sqlite3_stmt* stmt;

        if (sqlite3_prepare_v2(db.get(), sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK)
        {
            sql_bind_all(stmt, values...);
            auto cpp_utils_scope_guard = scope_leaving_guard<sqlite3_stmt, sqlite3_finalize>(stmt);
            int rc = sqlite3_step(stmt);
            if (rc == SQLITE_ROW)
            {
                if constexpr (sizeof...(rtypes) == 1)
                {
                    return sql_get<rtypes...>(stmt, 0);
                }
                if constexpr (sizeof...(rtypes) > 1)
                {
                    return sql_get_all<rtypes...>(stmt);
                }
            }
            else if (rc == SQLITE_CONSTRAINT)
            { // handle when trying to insert a duplicate key
                if constexpr (sizeof...(rtypes) == 0)
                    return false;
                else
                    return std::nullopt;
            }
            else if (rc == SQLITE_DONE)
            {
                if constexpr (sizeof...(rtypes) == 0)
                    return true;
                else
                    return std::nullopt;
            }
        }
        if constexpr (sizeof...(rtypes) == 0)
            return false;
        else
            return std::nullopt;
    }
};
