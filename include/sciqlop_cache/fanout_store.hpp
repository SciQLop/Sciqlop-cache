#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fmt/format.h>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <vector>

#define XXH_INLINE_ALL
#include <xxhash.h>

// Stores created before v0.4 named their shards "00".. and routed keys with
// std::hash, which differs between libstdc++, libc++ and MSVC (issue #26).
// Such stores keep that routing; new ones use XXH3, whose output is frozen
// across platforms. The new shard names keep an old library version from
// misrouting keys of a new store: it sees no "00" and starts an empty one.
enum class ShardHash
{
    legacy_std,
    xxh3
};

inline ShardHash detect_shard_hash(const std::filesystem::path& path)
{
    if (std::filesystem::exists(path / "00") && !std::filesystem::exists(path / "xxh3_00"))
        return ShardHash::legacy_std;
    return ShardHash::xxh3;
}

inline std::string shard_dir_name(ShardHash hash, std::size_t index)
{
    if (hash == ShardHash::xxh3)
        return fmt::format("xxh3_{:02d}", index);
    return fmt::format("{:02d}", index);
}

inline std::uint64_t shard_hash_of(ShardHash hash, const std::string& key)
{
    if (hash == ShardHash::xxh3)
        return XXH3_64bits(key.data(), key.size());
    return std::hash<std::string> {}(key);
}

template <typename StoreType>
class FanoutStore
{
    std::vector<std::unique_ptr<StoreType>> _shards;
    ShardHash _hash = ShardHash::xxh3;

    std::size_t _shard_for(const std::string& key) const
    {
        return shard_hash_of(_hash, key) % _shards.size();
    }

    StoreType& _shard(const std::string& key) { return *_shards[_shard_for(key)]; }

    template <typename F>
    void _for_each_shard(F&& f)
    {
        for (auto& s : _shards)
            f(*s);
    }

public:
    FanoutStore(const FanoutStore&) = delete;
    FanoutStore& operator=(const FanoutStore&) = delete;
    FanoutStore(FanoutStore&&) = default;
    FanoutStore& operator=(FanoutStore&&) = default;

    explicit FanoutStore(const std::filesystem::path& path,
                         std::size_t shard_count = 8,
                         std::size_t max_size = 0,
                         int busy_timeout_ms = 600'000)
        : _hash { detect_shard_hash(path) }
    {
        _shards.reserve(shard_count);
        for (std::size_t i = 0; i < shard_count; ++i)
        {
            auto shard_path = path / shard_dir_name(_hash, i);
            std::filesystem::create_directories(shard_path);
            _shards.push_back(std::make_unique<StoreType>(shard_path, max_size, busy_timeout_ms));
        }
    }

    [[nodiscard]] std::size_t shard_count() const { return _shards.size(); }

    [[nodiscard]] std::size_t file_size_threshold() const { return _shards[0]->file_size_threshold(); }

    void set_file_size_threshold(std::size_t bytes)
    {
        _for_each_shard([bytes](auto& s) { s.set_file_size_threshold(bytes); });
    }

    [[nodiscard]] bool exists(const std::string& key) { return _shard(key).exists(key); }

    inline bool del(const std::string& key) { return _shard(key).del(key); }

    [[nodiscard]] inline std::optional<Buffer> get(const std::string& key)
    {
        return _shard(key).get(key);
    }

    // One snapshot per shard; shards share no transaction, so two keys on
    // different shards may come from different moments.
    [[nodiscard]] std::vector<std::optional<Buffer>> get_many(const std::vector<std::string>& keys)
    {
        std::vector<std::vector<std::size_t>> positions(_shards.size());
        for (std::size_t i = 0; i < keys.size(); ++i)
            positions[_shard_for(keys[i])].push_back(i);

        std::vector<std::optional<Buffer>> values(keys.size());
        for (std::size_t s = 0; s < _shards.size(); ++s)
        {
            if (positions[s].empty())
                continue;
            std::vector<std::string> shard_keys;
            shard_keys.reserve(positions[s].size());
            for (auto i : positions[s])
                shard_keys.push_back(keys[i]);
            auto shard_values = _shards[s]->get_many(shard_keys);
            for (std::size_t j = 0; j < positions[s].size(); ++j)
                values[positions[s][j]] = std::move(shard_values[j]);
        }
        return values;
    }

    template <typename Items>
    void _for_each_shard_batch(const Items& items, auto&& write)
    {
        using Item = std::ranges::range_value_t<Items>;
        std::vector<std::vector<std::reference_wrapper<const Item>>> batches(_shards.size());
        for (const auto& item : items)
            batches[_shard_for(std::get<0>(item))].push_back(std::cref(item));
        for (std::size_t s = 0; s < _shards.size(); ++s)
            if (!batches[s].empty())
                write(*_shards[s], batches[s] | std::views::transform([](const Item& i) -> const Item& { return i; }));
    }

    [[nodiscard]] inline std::optional<Buffer> pop(const std::string& key)
    {
        return _shard(key).pop(key);
    }

    // --- set() overloads ---

    inline bool set(const std::string& key, const Payload auto& value)
    {
        return _shard(key).set(key, value);
    }

    inline bool set(const std::string& key, const Payload auto& value, DurationConcept auto expire)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e) { s.set(k, v, e); }
    {
        return _shard(key).set(key, value, expire);
    }

    inline bool set(const std::string& key, const Payload auto& value, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, const std::string& t) { s.set(k, v, t); }
    {
        return _shard(key).set(key, value, tag);
    }

    inline bool set(const std::string& key, const Payload auto& value,
                    DurationConcept auto expire, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e, const std::string& t) { s.set(k, v, e, t); }
    {
        return _shard(key).set(key, value, expire, tag);
    }

    // --- set_many(): one transaction per shard. Shards share none, so a failing
    // batch may leave the items of shards already written. ---

    void set_many(const auto& items)
    {
        _for_each_shard_batch(items, [](StoreType& shard, const auto& batch) { shard.set_many(batch); });
    }

    void set_many(const auto& items, std::optional<std::chrono::seconds> expire,
                  const std::optional<std::string>& tag)
        requires requires(StoreType& s) { s.set_many(std::vector<std::pair<std::string, std::string>> {}, expire, tag); }
    {
        _for_each_shard_batch(items, [&](StoreType& shard, const auto& batch)
                             { shard.set_many(batch, expire, tag); });
    }

    // --- add() overloads ---

    inline bool add(const std::string& key, const Payload auto& value)
    {
        return _shard(key).add(key, value);
    }

    inline bool add(const std::string& key, const Payload auto& value, DurationConcept auto expire)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e) { s.add(k, v, e); }
    {
        return _shard(key).add(key, value, expire);
    }

    inline bool add(const std::string& key, const Payload auto& value, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, const std::string& t) { s.add(k, v, t); }
    {
        return _shard(key).add(key, value, tag);
    }

    inline bool add(const std::string& key, const Payload auto& value,
                    DurationConcept auto expire, const std::string& tag)
        requires requires(StoreType& s, const std::string& k, const decltype(value)& v, decltype(expire) e, const std::string& t) { s.add(k, v, e, t); }
    {
        return _shard(key).add(key, value, expire, tag);
    }

    // --- Aggregated operations ---

    [[nodiscard]] std::size_t count()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.count(); });
        return total;
    }

    [[nodiscard]] std::size_t size()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.size(); });
        return total;
    }

    [[nodiscard]] std::size_t volume()
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.volume(); });
        return total;
    }

    [[nodiscard]] std::vector<std::string> keys()
    {
        std::vector<std::string> all;
        _for_each_shard([&](auto& s) {
            auto k = s.keys();
            all.insert(all.end(), std::make_move_iterator(k.begin()),
                       std::make_move_iterator(k.end()));
        });
        return all;
    }

    class KeyCursor
    {
        std::vector<std::unique_ptr<StoreType>>& _shards;
        std::size_t _shard_idx = 0;
        std::optional<typename StoreType::KeyCursor> _cursor;

        void _advance_shard()
        {
            _cursor.reset();
            while (_shard_idx < _shards.size())
            {
                _cursor.emplace(_shards[_shard_idx]->iterkeys());
                auto val = _cursor->next();
                if (val)
                {
                    _pending = std::move(*val);
                    return;
                }
                _cursor.reset();
                ++_shard_idx;
            }
        }

        std::optional<std::string> _pending;

    public:
        explicit KeyCursor(std::vector<std::unique_ptr<StoreType>>& shards)
            : _shards(shards)
        {
            _advance_shard();
        }

        KeyCursor(const KeyCursor&) = delete;
        KeyCursor& operator=(const KeyCursor&) = delete;
        KeyCursor(KeyCursor&&) = default;

        std::optional<std::string> next()
        {
            if (!_pending) return std::nullopt;
            auto result = std::move(*_pending);
            _pending.reset();
            if (_cursor)
            {
                auto val = _cursor->next();
                if (val)
                    _pending = std::move(*val);
                else
                {
                    _cursor.reset();
                    ++_shard_idx;
                    _advance_shard();
                }
            }
            return result;
        }
    };

    [[nodiscard]] KeyCursor iterkeys()
    {
        return KeyCursor(_shards);
    }

    void clear()
    {
        _for_each_shard([](auto& s) { s.clear(); });
    }

    bool close()
    {
        bool ok = true;
        _for_each_shard([&](auto& s) { ok &= s.close(); });
        return ok;
    }

    using CheckResult = typename StoreType::CheckResult;

    CheckResult check(bool fix = false)
    {
        CheckResult combined;
        _for_each_shard([&](auto& s) {
            auto r = s.check(fix);
            combined.orphaned_files += r.orphaned_files;
            combined.dangling_rows += r.dangling_rows;
            combined.size_mismatches += r.size_mismatches;
            if (!r.counters_consistent) combined.counters_consistent = false;
            if (!r.sqlite_integrity_ok) combined.sqlite_integrity_ok = false;
        });
        combined.ok = combined.sqlite_integrity_ok
                   && combined.dangling_rows == 0
                   && combined.size_mismatches == 0
                   && combined.orphaned_files == 0
                   && combined.counters_consistent;
        return combined;
    }

    [[nodiscard]] std::filesystem::path path() const
    {
        return _shards[0]->path().parent_path();
    }

    // --- Expiration ---

    inline bool touch(const std::string& key, DurationConcept auto expire)
        requires requires(StoreType& s, const std::string& k, decltype(expire) e) { s.touch(k, e); }
    {
        return _shard(key).touch(key, expire);
    }

    inline bool touch(const std::string& key)
        requires requires(StoreType& s, const std::string& k) { s.touch(k); }
    {
        return _shard(key).touch(key);
    }

    inline auto expire_and_tag(const std::string& key)
        requires requires(StoreType& s, const std::string& k) { s.expire_and_tag(k); }
    {
        return _shard(key).expire_and_tag(key);
    }

    inline void expire()
        requires requires(StoreType& s) { s.expire(); }
    {
        _for_each_shard([](auto& s) { s.expire(); });
    }

    // --- Eviction ---

    inline std::size_t evict()
        requires requires(StoreType& s) { s.evict(); }
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.evict(); });
        return total;
    }

    inline void set_max_cache_size(std::size_t value)
        requires requires(StoreType& s) { s.set_max_cache_size(value); }
    {
        _for_each_shard([value](auto& s) { s.set_max_cache_size(value); });
    }

    [[nodiscard]] inline std::size_t max_cache_size()
        requires requires(StoreType& s) { s.max_cache_size(); }
    {
        return _shards[0]->max_cache_size();
    }

    // --- Tags ---

    inline std::size_t evict_tag(const std::string& tag)
        requires requires(StoreType& s) { s.evict_tag(tag); }
    {
        std::size_t total = 0;
        _for_each_shard([&](auto& s) { total += s.evict_tag(tag); });
        return total;
    }

    [[nodiscard]] std::vector<std::string> keys_with_tag(const std::string& tag)
        requires requires(StoreType& s) { s.keys_with_tag(tag); }
    {
        std::vector<std::string> all;
        _for_each_shard([&](auto& s) {
            auto k = s.keys_with_tag(tag);
            all.insert(all.end(), std::make_move_iterator(k.begin()),
                       std::make_move_iterator(k.end()));
        });
        return all;
    }

    // --- Stats ---

    struct Stats { uint64_t hits; uint64_t misses; };

    auto stats()
        requires requires(StoreType& s) { s.stats(); }
    {
        uint64_t h = 0, m = 0;
        _for_each_shard([&](auto& s) {
            auto st = s.stats();
            h += st.hits;
            m += st.misses;
        });
        return Stats { h, m };
    }

    void reset_stats()
        requires requires(StoreType& s) { s.reset_stats(); }
    {
        _for_each_shard([](auto& s) { s.reset_stats(); });
    }

    // --- incr / decr ---

    inline int64_t incr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return _shard(key).incr(key, delta, default_value);
    }

    inline int64_t decr(const std::string& key, int64_t delta = 1, int64_t default_value = 0)
    {
        return _shard(key).decr(key, delta, default_value);
    }

    // --- meta (per-shard, keyed) ---

    inline void set_meta(const std::string& key, const std::string& value)
    {
        _shard(key).set_meta(key, value);
    }

    [[nodiscard]] inline std::optional<std::string> get_meta(const std::string& key)
    {
        return _shard(key).get_meta(key);
    }

    // --- transact (scoped to shard for key) ---

    auto begin_user_transaction(const std::string& key)
    {
        return _shard(key).begin_user_transaction();
    }
};
