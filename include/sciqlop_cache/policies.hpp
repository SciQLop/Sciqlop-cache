#pragma once

#include <atomic>
#include <algorithm>
#include <cstdint>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <unordered_map>

template <typename Policy, typename... Policies>
inline constexpr bool has_policy_v = (std::is_same_v<Policy, Policies> || ...);

// Each policy that contributes columns to INSERT/REPLACE must define
// insert_columns() and insert_placeholders(). The fold expression in _Store
// concatenates them in Policies... declaration order, which must match
// extra_columns() order. This eliminates the hardcoded ordering trap.

struct WithExpiration
{
    static std::string where_valid() { return " AND (expire IS NULL OR expire > unixepoch('now'))"; }
    static std::string extra_columns() { return ", expire REAL DEFAULT NULL"; }
    static std::string extra_indexes()
    {
        return "CREATE INDEX IF NOT EXISTS idx_cache_expire ON cache(expire) WHERE expire IS NOT NULL;";
    }
    static std::string insert_columns() { return ", expire"; }
    static std::string insert_placeholders() { return ", ?"; }
};

struct WithEviction
{
    using Uses = std::unordered_map<std::string, std::size_t>;

    std::atomic<std::size_t> _access_seq { 0 };
    // get() hits wait here for the background thread to write them out once
    // per tick: an inline UPDATE made every hit a write transaction.
    std::mutex _uses_mtx;
    Uses _pending_uses;

    void _note_use(const std::string& key)
    {
        auto seq = _access_seq.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard lock(_uses_mtx);
        _pending_uses[key] = seq;
    }

    Uses _take_uses()
    {
        std::lock_guard lock(_uses_mtx);
        return std::exchange(_pending_uses, {});
    }

    void _restore_uses(const Uses& uses)
    {
        std::lock_guard lock(_uses_mtx);
        for (const auto& [key, seq] : uses)
            _pending_uses[key] = std::max(_pending_uses[key], seq);
    }

    static std::string where_valid() { return ""; }
    static std::string extra_columns()
    {
        return ", last_update REAL NOT NULL DEFAULT 0"
               ", last_use REAL NOT NULL DEFAULT 0"
               ", access_count_since_last_update INT NOT NULL DEFAULT 0";
    }
    static std::string extra_indexes()
    {
        return "CREATE INDEX IF NOT EXISTS idx_cache_last_use ON cache(last_use);";
    }
    static std::string insert_columns() { return ", last_use"; }
    static std::string insert_placeholders() { return ", ?"; }
};

struct WithTags
{
    static std::string where_valid() { return ""; }
    static std::string extra_columns() { return ", tag TEXT DEFAULT NULL"; }
    static std::string extra_indexes()
    {
        return "CREATE INDEX IF NOT EXISTS idx_cache_tag ON cache(tag) WHERE tag IS NOT NULL;";
    }
    static std::string insert_columns() { return ", tag"; }
    static std::string insert_placeholders() { return ", ?"; }
};

struct WithStats
{
    std::atomic<uint64_t> _hits { 0 };
    std::atomic<uint64_t> _misses { 0 };

    static std::string where_valid() { return ""; }
    static std::string extra_columns() { return ""; }
    static std::string extra_indexes() { return ""; }
    static std::string insert_columns() { return ""; }
    static std::string insert_placeholders() { return ""; }
};
