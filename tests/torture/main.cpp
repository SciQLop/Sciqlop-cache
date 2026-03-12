#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

using namespace std::chrono_literals;

static constexpr int NUM_KEYS = 200;
static constexpr int MIN_VALUE_SIZE = 50;
static constexpr int MAX_VALUE_SIZE = 64 * 1024; // crosses 8KB blob/file threshold
static constexpr auto TORTURE_DURATION = 10s;

static std::vector<char> random_value(std::mt19937& rng, int min_size, int max_size)
{
    auto dist = std::uniform_int_distribution<int>(min_size, max_size);
    auto size = dist(rng);
    std::vector<char> v(size);
    std::generate(v.begin(), v.end(), [&] { return static_cast<char>(rng()); });
    return v;
}

// --------------------------------------------------------------------------
// Multi-thread stress: many threads doing random ops on the same store
// --------------------------------------------------------------------------

template <typename StoreFactory>
void multithread_stress(StoreFactory make_store, const std::filesystem::path& path)
{
    // Pre-create schema
    { auto s = make_store(path); }

    const int n_threads = std::max(4, static_cast<int>(std::thread::hardware_concurrency()) * 2);
    std::atomic<std::size_t> total_ops { 0 };

    auto worker = [&](int seed) {
        std::mt19937 rng(seed);
        auto store = make_store(path);
        auto key_dist = std::uniform_int_distribution<int>(0, NUM_KEYS - 1);
        auto op_dist = std::uniform_real_distribution<double>(0.0, 1.0);
        auto expire_dist = std::uniform_int_distribution<int>(10, 3600);
        auto tag_dist = std::uniform_int_distribution<int>(0, 5);
        std::size_t ops = 0;
        const auto deadline = std::chrono::steady_clock::now() + TORTURE_DURATION;

        while (std::chrono::steady_clock::now() < deadline)
        {
            auto key = "k_" + std::to_string(key_dist(rng));
            auto op = op_dist(rng);
            try
            {
                if (op < 0.40)
                {
                    auto value = random_value(rng, MIN_VALUE_SIZE, MAX_VALUE_SIZE);
                    if constexpr (requires { store.set(key, value, 60s, std::string("t")); })
                    {
                        bool use_expire = op_dist(rng) < 0.2;
                        bool use_tag = op_dist(rng) < 0.3;
                        if (use_expire && use_tag)
                            store.set(key, value, std::chrono::seconds(expire_dist(rng)),
                                      "tag_" + std::to_string(tag_dist(rng)));
                        else if (use_expire)
                            store.set(key, value, std::chrono::seconds(expire_dist(rng)));
                        else if (use_tag)
                            store.set(key, value, "tag_" + std::to_string(tag_dist(rng)));
                        else
                            store.set(key, value);
                    }
                    else
                    {
                        store.set(key, value);
                    }
                }
                else if (op < 0.70)
                {
                    (void)store.get(key);
                }
                else if (op < 0.90)
                {
                    store.del(key);
                }
                else
                {
                    (void)store.pop(key);
                }
            }
            catch (const std::exception&)
            {
                // Transient errors under heavy contention are acceptable
            }
            ++ops;
        }
        total_ops += ops;
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i)
        threads.emplace_back(worker, i * 12345);

    for (auto& t : threads)
        t.join();

    REQUIRE(total_ops > 0);

    // Verify integrity: fix pass then clean check
    auto store = make_store(path);
    store.check(true);
    auto cr = store.check();
    REQUIRE(cr.ok);

    // All remaining keys must be readable without crash
    for (const auto& key : store.keys())
        (void)store.get(key);
}

// --------------------------------------------------------------------------
// Data integrity: single-thread ground-truth verification
// --------------------------------------------------------------------------

template <typename StoreFactory>
void data_integrity(StoreFactory make_store, const std::filesystem::path& path)
{
    auto store = make_store(path);
    std::mt19937 rng(42);
    std::unordered_map<std::string, std::vector<char>> ground_truth;
    auto key_dist = std::uniform_int_distribution<int>(0, NUM_KEYS - 1);
    auto op_dist = std::uniform_real_distribution<double>(0.0, 1.0);
    const auto deadline = std::chrono::steady_clock::now() + TORTURE_DURATION;

    while (std::chrono::steady_clock::now() < deadline)
    {
        auto key = "k_" + std::to_string(key_dist(rng));
        auto op = op_dist(rng);

        if (op < 0.50)
        {
            auto value = random_value(rng, MIN_VALUE_SIZE, MAX_VALUE_SIZE);
            store.set(key, value);
            ground_truth[key] = value;
        }
        else if (op < 0.80)
        {
            auto result = store.get(key);
            if (ground_truth.count(key))
            {
                REQUIRE(result.has_value());
                REQUIRE(result->size() == ground_truth[key].size());
                REQUIRE(std::memcmp(result->data(), ground_truth[key].data(),
                                    ground_truth[key].size())
                        == 0);
            }
        }
        else
        {
            store.del(key);
            ground_truth.erase(key);
        }
    }

    // Final full verification
    for (const auto& [key, expected] : ground_truth)
    {
        auto result = store.get(key);
        REQUIRE(result.has_value());
        REQUIRE(result->size() == expected.size());
        REQUIRE(std::memcmp(result->data(), expected.data(), expected.size()) == 0);
    }

    auto cr = store.check();
    REQUIRE(cr.ok);
}

// --------------------------------------------------------------------------
// Store factories
// --------------------------------------------------------------------------

auto make_cache = [](const std::filesystem::path& p) { return Cache(p); };
auto make_index = [](const std::filesystem::path& p) { return Index(p); };
auto make_fanout_cache = [](const std::filesystem::path& p) {
    return FanoutCache(p, 4);
};
auto make_fanout_index = [](const std::filesystem::path& p) {
    return FanoutIndex(p, 4);
};

// --------------------------------------------------------------------------
// Test cases
// --------------------------------------------------------------------------

SCENARIO("Multi-thread stress: Cache", "[torture][stress]")
{
    AutoCleanDirectory dir { "TortureStressCache" };
    multithread_stress(make_cache, dir.path());
}

SCENARIO("Multi-thread stress: Index", "[torture][stress]")
{
    AutoCleanDirectory dir { "TortureStressIndex" };
    multithread_stress(make_index, dir.path());
}

SCENARIO("Multi-thread stress: FanoutCache", "[torture][stress]")
{
    AutoCleanDirectory dir { "TortureStressFanoutCache" };
    multithread_stress(make_fanout_cache, dir.path());
}

SCENARIO("Multi-thread stress: FanoutIndex", "[torture][stress]")
{
    AutoCleanDirectory dir { "TortureStressFanoutIndex" };
    multithread_stress(make_fanout_index, dir.path());
}

SCENARIO("Data integrity: Cache", "[torture][integrity]")
{
    AutoCleanDirectory dir { "TortureIntegrityCache" };
    data_integrity(make_cache, dir.path());
}

SCENARIO("Data integrity: Index", "[torture][integrity]")
{
    AutoCleanDirectory dir { "TortureIntegrityIndex" };
    data_integrity(make_index, dir.path());
}

SCENARIO("Data integrity: FanoutCache", "[torture][integrity]")
{
    AutoCleanDirectory dir { "TortureIntegrityFanoutCache" };
    data_integrity(make_fanout_cache, dir.path());
}

SCENARIO("Data integrity: FanoutIndex", "[torture][integrity]")
{
    AutoCleanDirectory dir { "TortureIntegrityFanoutIndex" };
    data_integrity(make_fanout_index, dir.path());
}
