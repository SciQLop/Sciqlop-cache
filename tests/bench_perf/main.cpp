#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

static void BM_Size(benchmark::State& state)
{
    auto n_entries = state.range(0);
    AutoCleanDirectory dir { "BenchSize" };
    Cache cache(dir.path());
    std::vector<char> value(200, 'x');
    for (int64_t i = 0; i < n_entries; ++i)
        cache.set("k" + std::to_string(i), value);

    for (auto _ : state)
        benchmark::DoNotOptimize(cache.size());
}
BENCHMARK(BM_Size)->Arg(100)->Arg(1000)->Arg(5000)->Arg(10000);

static void BM_Set(benchmark::State& state)
{
    AutoCleanDirectory dir { "BenchSet" };
    Cache cache(dir.path());
    std::vector<char> value(200, 'x');
    int64_t i = 0;
    for (auto _ : state)
        cache.set("k" + std::to_string(i++), value);
    state.SetItemsProcessed(i);
}
BENCHMARK(BM_Set);

static void BM_Get(benchmark::State& state)
{
    AutoCleanDirectory dir { "BenchGet" };
    Cache cache(dir.path());
    std::vector<char> value(200, 'x');
    int n = 5000;
    for (int i = 0; i < n; ++i)
        cache.set("k" + std::to_string(i), value);

    int64_t ops = 0;
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(cache.get("k" + std::to_string(ops % n)));
        ++ops;
    }
    state.SetItemsProcessed(ops);
}
BENCHMARK(BM_Get);

static void BM_SetGetCycle(benchmark::State& state)
{
    AutoCleanDirectory dir { "BenchCycle" };
    Cache cache(dir.path());
    std::vector<char> value(200, 'x');
    int64_t i = 0;
    for (auto _ : state)
    {
        auto key = "k" + std::to_string(i++);
        cache.set(key, value);
        benchmark::DoNotOptimize(cache.get(key));
    }
    state.SetItemsProcessed(i);
}
BENCHMARK(BM_SetGetCycle);

static void BM_Del(benchmark::State& state)
{
    AutoCleanDirectory dir { "BenchDel" };
    Cache cache(dir.path());
    std::vector<char> value(200, 'x');
    int n = 100000;
    for (int i = 0; i < n; ++i)
        cache.set("k" + std::to_string(i), value);

    int64_t i = 0;
    for (auto _ : state)
    {
        cache.del("k" + std::to_string(i % n));
        ++i;
    }
    state.SetItemsProcessed(i);
}
BENCHMARK(BM_Del);

static void BM_Count(benchmark::State& state)
{
    auto n_entries = state.range(0);
    AutoCleanDirectory dir { "BenchCount" };
    Cache cache(dir.path());
    std::vector<char> value(200, 'x');
    for (int64_t i = 0; i < n_entries; ++i)
        cache.set("k" + std::to_string(i), value);

    for (auto _ : state)
        benchmark::DoNotOptimize(cache.count());
}
BENCHMARK(BM_Count)->Arg(100)->Arg(1000)->Arg(5000);

static void BM_IndexCount(benchmark::State& state)
{
    auto n_entries = state.range(0);
    AutoCleanDirectory dir { "BenchIdxCount" };
    Index idx(dir.path());
    std::vector<char> value(200, 'x');
    for (int64_t i = 0; i < n_entries; ++i)
        idx.set("k" + std::to_string(i), value);

    for (auto _ : state)
        benchmark::DoNotOptimize(idx.count());
}
BENCHMARK(BM_IndexCount)->Arg(100)->Arg(1000)->Arg(5000);

static void BM_IndexSize(benchmark::State& state)
{
    auto n_entries = state.range(0);
    AutoCleanDirectory dir { "BenchIdxSize" };
    Index idx(dir.path());
    std::vector<char> value(200, 'x');
    for (int64_t i = 0; i < n_entries; ++i)
        idx.set("k" + std::to_string(i), value);

    for (auto _ : state)
        benchmark::DoNotOptimize(idx.size());
}
BENCHMARK(BM_IndexSize)->Arg(100)->Arg(1000)->Arg(5000);

// Large value (file-backed) benchmarks — measures open/mmap/munmap/close overhead
static void BM_GetLargeValue(benchmark::State& state)
{
    auto value_size = state.range(0);
    AutoCleanDirectory dir { "BenchGetLarge" };
    Cache cache(dir.path());
    std::vector<char> value(value_size, 'x');
    cache.set("bigkey", value);

    int64_t ops = 0;
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(cache.get("bigkey"));
        ++ops;
    }
    state.SetItemsProcessed(ops);
    state.SetBytesProcessed(ops * value_size);
}
// 16KB, 256KB, 1MB, 4MB — all above 8KB threshold, stored as files
BENCHMARK(BM_GetLargeValue)->Arg(16 * 1024)->Arg(256 * 1024)->Arg(1024 * 1024)->Arg(4 * 1024 * 1024);

// Same key read N times — hot-read pattern (SciQLop plot panning)
static void BM_GetLargeValueRepeat(benchmark::State& state)
{
    auto value_size = state.range(0);
    AutoCleanDirectory dir { "BenchGetLargeRepeat" };
    Cache cache(dir.path());
    std::vector<char> value(value_size, 'x');
    cache.set("hotkey", value);

    // Pre-warm: read once so page cache is hot, isolating mmap overhead
    { auto _ = cache.get("hotkey"); }

    int64_t ops = 0;
    for (auto _ : state)
    {
        benchmark::DoNotOptimize(cache.get("hotkey"));
        ++ops;
    }
    state.SetItemsProcessed(ops);
    state.SetBytesProcessed(ops * value_size);
}
BENCHMARK(BM_GetLargeValueRepeat)->Arg(256 * 1024)->Arg(1024 * 1024)->Arg(4 * 1024 * 1024);

BENCHMARK_MAIN();
