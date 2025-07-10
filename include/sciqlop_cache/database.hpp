/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** time related functions
*/

#pragma once

#include "utils/time.hpp"
#include <iostream>
#include <string>
#include <memory>
#include <sqlite3.h>
#include <optional>
#include <mutex>
#include <tuple>
#include <vector>
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>

struct SQLiteDeleter {
    void operator()(sqlite3* db) const {
        if (db)
            sqlite3_close(db);
    }
};

template <typename T>
concept Bytes = requires(T t) {
    { std::size(t) } -> std::convertible_to<std::size_t>;
    { std::data(t) } -> std::convertible_to<const char*>;
};


void sql_bind(const auto& stmt,int col, const TimePoint auto value)
{
    sqlite3_bind_double(stmt, col, time_point_to_epoch(value));
}

void sql_bind(const auto& stmt, int col, const Bytes auto& value)
{
    sqlite3_bind_blob(stmt, col, std::data(value), static_cast<int>(std::size(value)), SQLITE_STATIC);
}

void sql_bind(const auto& stmt, int col, const std::string& value)
{
    sqlite3_bind_text(stmt, col, value.c_str(), -1, SQLITE_STATIC);
}

void sql_bind_all(const auto& stm, const auto&... values)
{
    int i = 1;
    ((sql_bind(stm, i++, values)), ...);
}

template <typename rtype>
auto sql_get(const auto& stmt, int col)
{
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
            return std::vector<char>{};
        }
    }
    if constexpr (std::is_same_v<rtype, bool>)
    {
        //std::cout << "bool "<< col  << std::endl;
        return true;
    }
}

template<typename... rtypes, std::size_t... Is>
std::tuple<rtypes...> _sql_get_all(const auto& stmt, std::index_sequence<Is...>)
{
    return std::tuple{sql_get<rtypes>(stmt, Is) ...};
}

template<typename... rtypes>
auto sql_get_all(const auto& stm)
{
    return _sql_get_all<rtypes... >(stm,std::index_sequence_for<rtypes...>{});
}


class Database {
    private:
    std::unique_ptr<sqlite3, SQLiteDeleter> db;
    std::mutex db_mutex;

    public:
    Database()
        : db(nullptr, SQLiteDeleter())
    {
        ;
    }

    ~Database()
    {
        if (db) {
            sqlite3_close(db.get());
        }
    }

    int open(const std::string &db_path)
    {
        std::lock_guard<std::mutex> lock(db_mutex);

        const char *buffer = db_path.c_str();
        sqlite3 *tmp_db = db.get();
        int check = sqlite3_open(buffer, &tmp_db);

        if (check) {
            std::cerr << "Error opening database: " << sqlite3_errmsg(tmp_db) << std::endl;
            sqlite3_close(tmp_db);
        } else {
            std::cout << "Database opened successfully." << std::endl;
            db.reset(tmp_db);
        }
        std::cout << "check: " << check << std::endl;
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

        std::cout << "db pointer: " << db.get() << std::endl;
        int rc = sqlite3_exec(db.get(), sql.c_str(), nullptr, nullptr, &errMsg);
        if (rc != SQLITE_OK) {
            std::cerr << "SQL error: " << (errMsg ? errMsg : "unknown error") << std::endl;
            sqlite3_free(errMsg);
            return false;
        }
        return true;
    }

    template<typename... rtypes>
    auto exec(const std::string& sql, const auto&... values)
    {
        std::lock_guard<std::mutex> lock(db_mutex);
        char* errMsg = nullptr;
        sqlite3_stmt* stmt;

        if (sqlite3_prepare_v2(db.get(), sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK)
        {
            sql_bind_all(stmt, values...);
            auto cpp_utils_scope_guard = cpp_utils::lifetime::scope_leaving_guard<sqlite3_stmt, sqlite3_finalize>(stmt);
            if (sqlite3_step(stmt) == SQLITE_ROW)
            {
                if constexpr (sizeof...(rtypes))
               {
                       return sql_get_all<rtypes...>(stmt);
               }
            }
        }
        if constexpr (sizeof...(rtypes) == 0)
        {
            return;
        }
        else
        {
            return std::tuple<rtypes...>{};
        }
    }


};
