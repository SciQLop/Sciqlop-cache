#pragma once

// Process-wide blosc2 state for pickle-oob: worker threads for blosc2's
// parallel jobs, and reusable compression contexts. Both are fork-safe.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <blosc2.h>
#include <map>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <pthread.h>
#endif

// Persistent worker threads that run blosc2's parallel jobs (through
// blosc2_set_threads_callback). Without it every compress/decompress starts
// and joins its own threads: ~0.35 ms per call on 16 cores, more than
// decompressing 10 MB. blosc2's own shared pool would avoid that, but it is
// torn down with its last context and does not survive fork().
class JobPool
{
    // One parallel-for: jobs are claimed by index, by the caller and by any
    // idle worker. blosc2 jobs never wait on each other, so any number of
    // threads (even just the caller) completes a batch.
    struct Batch
    {
        void (*dojob)(void*);
        char* jobdata;
        std::size_t elsize;
        int count;
        std::atomic<int> next { 0 };
        int done = 0;
        std::mutex m;
        std::condition_variable finished;

        bool exhausted() const { return next.load() >= count; }

        void run_some()
        {
            int ran = 0;
            for (int i = next.fetch_add(1); i < count; i = next.fetch_add(1), ++ran)
                dojob(jobdata + static_cast<std::size_t>(i) * elsize);
            if (ran == 0)
                return;
            std::lock_guard lk { m };
            done += ran;
            if (done == count)
                finished.notify_all();
        }
    };

    std::mutex _m;
    std::condition_variable _wake;
    std::deque<std::shared_ptr<Batch>> _queue;
    std::vector<std::thread> _workers;
    bool _stop = false;

    std::shared_ptr<Batch> _next_batch()
    {
        std::unique_lock lk { _m };
        for (;;)
        {
            while (!_queue.empty() && _queue.front()->exhausted())
                _queue.pop_front();
            if (_stop)
                return nullptr;
            if (!_queue.empty())
                return _queue.front();
            _wake.wait(lk);
        }
    }

    void _work()
    {
        while (auto batch = _next_batch())
            batch->run_some();
    }

public:
    explicit JobPool(unsigned workers)
    {
        for (unsigned i = 0; i < workers; ++i)
            _workers.emplace_back([this] { _work(); });
    }

    JobPool(const JobPool&) = delete;
    JobPool& operator=(const JobPool&) = delete;

    ~JobPool()
    {
        {
            std::lock_guard lk { _m };
            _stop = true;
        }
        _wake.notify_all();
        for (auto& t : _workers)
            t.join();
    }

    void run(void (*dojob)(void*), int count, std::size_t elsize, void* jobdata)
    {
        auto batch = std::make_shared<Batch>();
        batch->dojob = dojob;
        batch->jobdata = static_cast<char*>(jobdata);
        batch->elsize = elsize;
        batch->count = count;
        {
            std::lock_guard lk { _m };
            _queue.push_back(batch);
        }
        _wake.notify_all();
        batch->run_some();
        std::unique_lock lk { batch->m };
        batch->finished.wait(lk, [&] { return batch->done == batch->count; });
    }
};

namespace blosc2_runtime
{

// Guards the pool pointer and the idle contexts. Held across fork() so the
// child never inherits it locked by another thread.
inline std::mutex& _mutex()
{
    static std::mutex* m = new std::mutex; // never destroyed: used until exit
    return *m;
}

// A forked child only has the thread that called fork(), so it drops the
// parent's pool (leaked on purpose: destroying it would join threads that do
// not exist) and starts a fresh one when it needs it.
inline JobPool*& _pool()
{
    static JobPool* pool = nullptr;
    return pool;
}

inline unsigned thread_count()
{
    return std::max(1u, std::thread::hardware_concurrency());
}

inline JobPool& pool()
{
    std::lock_guard lk { _mutex() };
    if (!_pool())
        _pool() = new JobPool(thread_count() - 1); // the caller is the last worker
    return *_pool();
}

// Compression contexts are reused: a new one faults in fresh scratch buffers
// for every job (~8 MiB on 16 cores, ~7 ms, 20x the compression itself).
// With the callback backend a context owns no threads, so idle ones stay
// valid in a forked child.
// simplify: at most 2 idle contexts per item size are kept (~16 MiB each);
// more concurrent writers than that pay for a new context. Raise the cap, or
// shrink blosc2's blocksize, if many threads compress at once.
inline constexpr std::size_t idle_contexts_per_typesize = 2;

inline std::map<int, std::vector<blosc2_context*>>& _idle()
{
    static auto* idle = new std::map<int, std::vector<blosc2_context*>>;
    return *idle;
}

inline blosc2_context* _new_compression_ctx(int typesize)
{
    blosc2_cparams params = BLOSC2_CPARAMS_DEFAULTS;
    params.compcode = BLOSC_LZ4;
    params.clevel = 5;
    params.typesize = typesize;
    params.nthreads = static_cast<int16_t>(std::min<unsigned>(thread_count(), INT16_MAX));
    params.filters[BLOSC2_MAX_FILTERS - 1] = BLOSC_SHUFFLE;
    return blosc2_create_cctx(params);
}

// A compression context (lz4 + shuffle, all cores) for `typesize`-byte
// items, returned to the idle set when the lease ends.
class CompressionLease
{
    int _typesize;
    blosc2_context* _ctx;

public:
    explicit CompressionLease(int typesize) : _typesize(typesize), _ctx(nullptr)
    {
        {
            std::lock_guard lk { _mutex() };
            auto& idle = _idle()[typesize];
            if (!idle.empty())
            {
                _ctx = idle.back();
                idle.pop_back();
            }
        }
        if (!_ctx)
            _ctx = _new_compression_ctx(typesize);
        if (!_ctx)
            throw std::runtime_error("blosc2: could not create a compression context");
    }

    CompressionLease(const CompressionLease&) = delete;
    CompressionLease& operator=(const CompressionLease&) = delete;

    ~CompressionLease()
    {
        {
            std::lock_guard lk { _mutex() };
            auto& idle = _idle()[_typesize];
            if (idle.size() < idle_contexts_per_typesize)
            {
                idle.push_back(_ctx);
                return;
            }
        }
        blosc2_free_ctx(_ctx);
    }

    blosc2_context* get() const { return _ctx; }
};

// blosc2 picks its SIMD shuffle kernels lazily, on first use, without a
// memory barrier (shuffle.c: "shouldn't matter"). Fine on x86, but on ARM a
// thread could see the flag before the kernel table. Running one shuffle
// here, at import, before any other thread uses blosc2, removes the race.
inline void _init_shuffle_kernels()
{
    blosc2_cparams params = BLOSC2_CPARAMS_DEFAULTS;
    params.typesize = 8;
    blosc2_context* ctx = blosc2_create_cctx(params);
    if (!ctx)
        return;
    std::int64_t src[256] = {};
    char dst[sizeof(src) + BLOSC2_MAX_OVERHEAD];
    blosc2_compress_ctx(ctx, src, sizeof(src), dst, sizeof(dst));
    blosc2_free_ctx(ctx);
}

inline void _install()
{
    blosc2_init();
    _init_shuffle_kernels();
#ifndef _WIN32
    // In the child, the thread that locked the mutex before fork() is the
    // one that returns from it, so it can unlock it.
    pthread_atfork([] { _mutex().lock(); }, [] { _mutex().unlock(); },
                   []
                   {
                       _pool() = nullptr;
                       _mutex().unlock();
                   });
#endif
    blosc2_set_threads_callback(
        [](void*, void (*dojob)(void*), int count, std::size_t elsize, void* jobdata)
        { pool().run(dojob, count, elsize, jobdata); },
        nullptr);
}

// Call before any other blosc2 use. Idempotent: a second module init (e.g.
// in a subinterpreter) must not register the fork handlers twice, or the
// second prepare handler would deadlock on the mutex the first one holds.
inline void install()
{
    static std::once_flag once;
    std::call_once(once, _install);
}

} // namespace blosc2_runtime
