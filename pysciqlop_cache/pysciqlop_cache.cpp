#include "sciqlop_cache/sciqlop_cache.hpp"
#include "buffer_codecs.hpp"

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/chrono.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>

#include <fmt/ranges.h>

#include <cstring>
#include <span>
#include <tuple>

using namespace std::chrono_literals;

namespace nb = nanobind;
using namespace nb::literals;

using OptDuration = std::optional<std::chrono::system_clock::duration>;
using OptString = std::optional<std::string>;

// Buffer-protocol views acquired with the GIL held and released together
// when this goes out of scope (also with the GIL held). What they point to
// stays valid in between, so it can be read or written with the GIL released.
class PyBufferViews
{
    std::vector<Py_buffer> _views;

public:
    PyBufferViews() = default;
    PyBufferViews(const PyBufferViews&) = delete;
    PyBufferViews& operator=(const PyBufferViews&) = delete;
    ~PyBufferViews()
    {
        for (auto& view : _views)
            PyBuffer_Release(&view);
    }

    std::span<char> acquire(nb::handle obj, int flags)
    {
        Py_buffer view;
        if (PyObject_GetBuffer(obj.ptr(), &view, flags) != 0)
            throw nb::python_error();
        _views.push_back(view);
        return { static_cast<char*>(view.buf), static_cast<std::size_t>(view.len) };
    }
};

// A value is either bytes or a list/tuple of buffer objects (e.g. a pickle
// header plus out-of-band array buffers), written as their concatenation
// without joining them first. Built and destroyed with the GIL held; visit()
// runs with the GIL released, so set/add never hold the GIL while blocking on
// the store mutex or writing the value.
class PyPayload
{
    PyBufferViews _views;
    std::optional<std::span<const char>> _bytes;
    ByteChunks _chunks;

public:
    explicit PyPayload(nb::handle value)
    {
        if (PyBytes_Check(value.ptr()))
        {
            _bytes.emplace(PyBytes_AS_STRING(value.ptr()),
                           static_cast<std::size_t>(PyBytes_GET_SIZE(value.ptr())));
            return;
        }
        if (!PyList_Check(value.ptr()) && !PyTuple_Check(value.ptr()))
            throw nb::type_error("value must be bytes or a list of buffer objects");
        for (nb::handle item : nb::borrow<nb::sequence>(value))
            _chunks.parts.push_back(_views.acquire(item, PyBUF_ANY_CONTIGUOUS));
    }

    decltype(auto) visit(auto&& fn) const
    {
        if (_bytes)
            return fn(*_bytes);
        return fn(_chunks);
    }
};

struct DecodeJob
{
    BufferCodec codec;
    std::span<const char> stored;
    std::size_t raw_size;
    std::span<char> dst = {};
};

static DecodeJob _checked_decode_job(std::span<const char> src, nb::handle job)
{
    auto [codec, offset, stored_size, raw_size]
        = nb::cast<std::tuple<int, std::size_t, std::size_t, std::size_t>>(job);
    if (offset > src.size() || stored_size > src.size() - offset)
        throw nb::value_error("pickle-oob buffer lies outside the stored value");
    DecodeJob checked { static_cast<BufferCodec>(codec), src.subspan(offset, stored_size), raw_size };
    if (auto error = check_stored_buffer(checked.codec, checked.stored, raw_size))
        throw nb::value_error(fmt::format("pickle-oob buffer codec {}: {}", codec, *error).c_str());
    return checked;
}

// Decodes each (codec, offset, stored_size, raw_size) job of `src` (pickle-oob
// loads) into a fresh buffer from `alloc(raw_size)`, and returns them. Every
// job is validated first, with the GIL held and before anything is
// allocated, so a corrupt value raises instead of allocating a bogus size or
// touching memory out of bounds; the copies then run with the GIL released.
static nb::list decode_buffers(nb::handle src_obj, nb::sequence jobs, nb::callable alloc)
{
    PyBufferViews views;
    auto src = views.acquire(src_obj, PyBUF_ANY_CONTIGUOUS);
    std::vector<DecodeJob> checked;
    for (nb::handle job : jobs)
        checked.push_back(_checked_decode_job(src, job));
    nb::list buffers;
    for (auto& job : checked)
    {
        nb::object dst = alloc(job.raw_size);
        job.dst = views.acquire(dst, PyBUF_WRITABLE | PyBUF_ANY_CONTIGUOUS);
        if (job.dst.size() != job.raw_size)
            throw nb::value_error("alloc() returned a buffer of the wrong size");
        buffers.append(dst);
    }
    std::optional<std::string> error;
    {
        nb::gil_scoped_release release;
        for (const auto& job : checked)
            if ((error = decode_buffer(job.codec, job.stored, job.dst)))
                break;
    }
    if (error)
        throw nb::value_error(("pickle-oob buffer: " + *error).c_str());
    return buffers;
}

// Returns the blosc2-compressed bytes of `src` as a Buffer, or None when
// blosc2 is not built in or compressing would not make `src` at least
// `min_ratio` times smaller. Runs with the GIL released.
static nb::object compress_buffer([[maybe_unused]] nb::handle src_obj,
                                  [[maybe_unused]] int typesize, double min_ratio)
{
    if (!(min_ratio > 0.0))
        throw nb::value_error("min_ratio must be > 0");
#ifdef SCIQLOP_CACHE_WITH_BLOSC2
    PyBufferViews views;
    auto src = views.acquire(src_obj, PyBUF_ANY_CONTIGUOUS);
    std::optional<std::vector<char>> compressed;
    {
        nb::gil_scoped_release release;
        compressed = blosc2_compress(src, typesize, min_ratio);
    }
    if (compressed)
        return nb::cast(Buffer(std::move(*compressed)));
#endif
    return nb::none();
}

template <typename T>
inline void _set_item_impl(T& c, const std::string& key, nb::handle value,
                            OptDuration expire = std::nullopt,
                            OptString tag = std::nullopt)
{
    PyPayload payload(value);
    nb::gil_scoped_release release;
    payload.visit([&](const auto& data) {
        if (expire && tag)
            c.set(key, data, *expire, *tag);
        else if (expire)
            c.set(key, data, *expire);
        else if (tag)
            c.set(key, data, *tag);
        else
            c.set(key, data);
    });
}

template <typename T>
inline bool _add_item_impl(T& c, const std::string& key, nb::handle value,
                            OptDuration expire = std::nullopt,
                            OptString tag = std::nullopt)
{
    PyPayload payload(value);
    nb::gil_scoped_release release;
    return payload.visit([&](const auto& data) {
        if (expire && tag)
            return c.add(key, data, *expire, *tag);
        else if (expire)
            return c.add(key, data, *expire);
        else if (tag)
            return c.add(key, data, *tag);
        return c.add(key, data);
    });
}

template <typename T>
inline bool _touch_impl(T& c, const std::string& key, OptDuration expire)
{
    nb::gil_scoped_release release;
    if (expire)
        return c.touch(key, *expire);
    return c.touch(key);
}

template <typename T>
inline void _simple_set_item(T& s, const std::string& key, nb::handle value)
{
    PyPayload payload(value);
    nb::gil_scoped_release release;
    payload.visit([&](const auto& data) { s.set(key, data); });
}

template <typename T>
inline bool _simple_add_item(T& s, const std::string& key, nb::handle value)
{
    PyPayload payload(value);
    nb::gil_scoped_release release;
    return payload.visit([&](const auto& data) { return s.add(key, data); });
}

template <typename CursorType>
void bind_key_cursor(nb::module_& m, const char* name)
{
    nb::class_<CursorType>(m, name)
        .def("__iter__", [](nb::handle self) { return self; })
        // Release GIL so other threads can run between cursor steps. Without
        // this, an iter loop monopolizes the GIL while holding the store mutex
        // → any other thread calling into C++ deadlocks.
        .def("__next__", [](CursorType& c) -> std::string {
            std::optional<std::string> val;
            {
                nb::gil_scoped_release release;
                val = c.next();
            }
            if (!val) throw nb::stop_iteration();
            return *val;
        });
}

// CPython buffer-protocol getbuffer slot for Buffer. Exporting the protocol
// on the type itself (rather than hand-rolling a Py_buffer + capsule and
// building the memoryview via PyMemoryView_FromBuffer) keeps the INCREF/DECREF
// of the exporter object balanced: PyBuffer_FillInfo INCREFs `exporter` into
// view->obj, and PyBuffer_Release (called when the memoryview is released or
// collected) DECREFs it back. The old capsule-based approach lost this
// balance because PyMemoryView_FromBuffer clears the managed buffer's `obj`
// after FillInfo already bumped its refcount, so the capsule (and the mmap
// it kept alive) leaked forever — one fd + one mapping per distinct
// file-backed key, immune to release()/gc.collect()/Cache destruction.
static int Buffer_getbuffer(PyObject* exporter, Py_buffer* view, int flags)
{
    Buffer* b = nullptr;
    if (!nb::try_cast<Buffer*>(nb::handle(exporter), b, false) || !b || !*b)
    {
        PyErr_SetString(PyExc_BufferError, "Cannot create memory view of invalid buffer");
        view->obj = nullptr;
        return -1;
    }
    return PyBuffer_FillInfo(view, exporter, const_cast<char*>(b->data()),
                              (Py_ssize_t) b->size(), 1, flags);
}

static constexpr const char* close_doc
    = "Close the cache's SQLite connection and stop its background thread.\n\n"
      "Not required: this already happens automatically when the object is\n"
      "garbage-collected. Provided for diskcache-compatible cleanup code\n"
      "(an explicit close() in a finally: block). `with cache:` does NOT\n"
      "call this. Safe to call more than once. Any operation on the cache\n"
      "after calling this raises RuntimeError.";

NB_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

#ifdef SCIQLOP_CACHE_WITH_BLOSC2
    blosc2_runtime::install();
#endif
    m.attr("BLOSC2_AVAILABLE") = blosc2_available;
    m.def("decode_buffers", &decode_buffers, "src"_a, "jobs"_a, "alloc"_a,
          "Decode each (codec, offset, stored_size, raw_size) job of src into alloc(raw_size).");
    m.def("compress_buffer", &compress_buffer, "src"_a, "typesize"_a, "min_ratio"_a,
          "blosc2 (lz4 + shuffle) bytes of src as a Buffer, or None if not worth it.");

    bind_key_cursor<Cache::KeyCursor>(m, "CacheKeyCursor");
    bind_key_cursor<Index::KeyCursor>(m, "IndexKeyCursor");
    bind_key_cursor<FanoutCache::KeyCursor>(m, "FanoutCacheKeyCursor");
    bind_key_cursor<FanoutIndex::KeyCursor>(m, "FanoutIndexKeyCursor");

    static PyType_Slot buffer_slots[] = {
        { Py_bf_getbuffer, (void*) Buffer_getbuffer },
        { 0, nullptr },
    };

    nb::class_<Buffer>(m, "Buffer", nb::type_slots(buffer_slots))
        .def("memoryview",
             [](nb::handle self) -> nb::object
             {
                 Buffer& b = nb::cast<Buffer&>(self);

                 if (!b) {
                     throw std::runtime_error("Cannot create memory view of invalid buffer");
                 }

                 if (b.data() == nullptr && b.size() == 0) {
                     return nb::steal(PyMemoryView_FromMemory(nullptr, 0, PyBUF_READ));
                 }

                 return nb::steal(PyMemoryView_FromObject(self.ptr()));
             });


    nb::class_<Cache::CheckResult>(m, "CacheCheckResult")
        .def_ro("ok", &Cache::CheckResult::ok)
        .def_ro("orphaned_files", &Cache::CheckResult::orphaned_files)
        .def_ro("dangling_rows", &Cache::CheckResult::dangling_rows)
        .def_ro("size_mismatches", &Cache::CheckResult::size_mismatches)
        .def_ro("counters_consistent", &Cache::CheckResult::counters_consistent)
        .def_ro("sqlite_integrity_ok", &Cache::CheckResult::sqlite_integrity_ok);

    nb::class_<Index::CheckResult>(m, "IndexCheckResult")
        .def_ro("ok", &Index::CheckResult::ok)
        .def_ro("orphaned_files", &Index::CheckResult::orphaned_files)
        .def_ro("dangling_rows", &Index::CheckResult::dangling_rows)
        .def_ro("size_mismatches", &Index::CheckResult::size_mismatches)
        .def_ro("counters_consistent", &Index::CheckResult::counters_consistent)
        .def_ro("sqlite_integrity_ok", &Index::CheckResult::sqlite_integrity_ok);

    nb::class_<Cache::TransactionGuard>(m, "CacheTransactionGuard")
        .def("commit", &Cache::TransactionGuard::commit,
             nb::call_guard<nb::gil_scoped_release>())
        .def("rollback", &Cache::TransactionGuard::rollback,
             nb::call_guard<nb::gil_scoped_release>());

    nb::class_<Index::TransactionGuard>(m, "IndexTransactionGuard")
        .def("commit", &Index::TransactionGuard::commit,
             nb::call_guard<nb::gil_scoped_release>())
        .def("rollback", &Index::TransactionGuard::rollback,
             nb::call_guard<nb::gil_scoped_release>());

    nb::exception<busy_error>(m, "Timeout", PyExc_RuntimeError);

    // nanobind maps std::system_error to RuntimeError; OSError(errno, msg)
    // keeps the errno and lets Python pick the subclass (PermissionError...).
    nb::register_exception_translator(
        [](const std::exception_ptr& p, void*)
        {
            try
            {
                std::rethrow_exception(p);
            }
            catch (const std::system_error& e)
            {
                PyObject* args = Py_BuildValue("(is)", e.code().value(), e.what());
                PyErr_SetObject(PyExc_OSError, args);
                Py_XDECREF(args);
            }
        });

    nb::class_<Cache>(m, "Cache")
        .def(
            "__init__",
            [](Cache* self, const std::string& cache_path, size_t max_size, double timeout)
            { new (self) Cache(cache_path, max_size, static_cast<int>(timeout * 1000.0)); },
            "cache_path"_a = ".cache/", "max_size"_a = 0, "timeout"_a = 600.0)
        .def("count", &Cache::count, nb::call_guard<nb::gil_scoped_release>())
        .def("__len__", &Cache::count, nb::call_guard<nb::gil_scoped_release>())
        .def("set", _set_item_impl<Cache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def(
            "__setitem__", [](Cache& c, const std::string& key, nb::handle buffer)
            { _set_item_impl(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &Cache::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("__getitem__", &Cache::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("keys", &Cache::keys, nb::call_guard<nb::gil_scoped_release>())
        .def("iterkeys", &Cache::iterkeys, nb::keep_alive<0, 1>(),
             nb::call_guard<nb::gil_scoped_release>())
        .def("exists", &Cache::exists, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("add", _add_item_impl<Cache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def("delete", &Cache::del, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("pop", &Cache::pop, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("touch", _touch_impl<Cache>, nb::arg("key"), nb::arg("expire") = nb::none())
        .def("expire_and_tag", &Cache::expire_and_tag, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("expire", &Cache::expire, nb::call_guard<nb::gil_scoped_release>())
        .def("evict", &Cache::evict, nb::call_guard<nb::gil_scoped_release>())
        .def("evict_tag", &Cache::evict_tag, nb::arg("tag"), nb::call_guard<nb::gil_scoped_release>())
        .def("incr", &Cache::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("decr", &Cache::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("clear", &Cache::clear, nb::call_guard<nb::gil_scoped_release>())
        .def("check", &Cache::check, nb::arg("fix") = false, nb::call_guard<nb::gil_scoped_release>())
        .def("set_meta", &Cache::set_meta, nb::arg("key"), nb::arg("value"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("get_meta", &Cache::get_meta, nb::arg("key"), nb::call_guard<nb::gil_scoped_release>())
        .def("size", &Cache::size, nb::call_guard<nb::gil_scoped_release>())
        .def("volume", &Cache::volume, nb::call_guard<nb::gil_scoped_release>())
        .def("set_max_cache_size", &Cache::set_max_cache_size, nb::arg("value"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("path", [](Cache& c) { return c.path().string(); })
        .def("stats", [](Cache& c) {
            auto s = [&] { nb::gil_scoped_release release; return c.stats(); }();
            nb::dict d;
            d["hits"] = s.hits;
            d["misses"] = s.misses;
            return d;
        })
        .def("reset_stats", &Cache::reset_stats, nb::call_guard<nb::gil_scoped_release>())
        .def("close", &Cache::close, close_doc, nb::call_guard<nb::gil_scoped_release>())
        .def("begin_user_transaction", &Cache::begin_user_transaction,
             nb::call_guard<nb::gil_scoped_release>());

    nb::class_<Index>(m, "Index")
        .def(nb::init<const std::string&>(), "path"_a = ".index/")
        .def("count", &Index::count, nb::call_guard<nb::gil_scoped_release>())
        .def("__len__", &Index::count, nb::call_guard<nb::gil_scoped_release>())
        .def("set", _simple_set_item<Index>, nb::arg("key"), nb::arg("value"))
        .def("__setitem__", _simple_set_item<Index>, nb::arg("key"), nb::arg("value"))
        .def("get", &Index::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("__getitem__", &Index::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("keys", &Index::keys, nb::call_guard<nb::gil_scoped_release>())
        .def("iterkeys", &Index::iterkeys, nb::keep_alive<0, 1>(),
             nb::call_guard<nb::gil_scoped_release>())
        .def("exists", &Index::exists, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("add", _simple_add_item<Index>, nb::arg("key"), nb::arg("value"))
        .def("delete", &Index::del, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("pop", &Index::pop, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("incr", &Index::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("decr", &Index::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("clear", &Index::clear, nb::call_guard<nb::gil_scoped_release>())
        .def("check", &Index::check, nb::arg("fix") = false, nb::call_guard<nb::gil_scoped_release>())
        .def("size", &Index::size, nb::call_guard<nb::gil_scoped_release>())
        .def("volume", &Index::volume, nb::call_guard<nb::gil_scoped_release>())
        .def("set_meta", &Index::set_meta, nb::arg("key"), nb::arg("value"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("get_meta", &Index::get_meta, nb::arg("key"), nb::call_guard<nb::gil_scoped_release>())
        .def("path", [](Index& idx) { return idx.path().string(); })
        .def("close", &Index::close, close_doc, nb::call_guard<nb::gil_scoped_release>())
        .def("begin_user_transaction", &Index::begin_user_transaction,
             nb::call_guard<nb::gil_scoped_release>());

    nb::class_<FanoutCache>(m, "FanoutCache")
        .def(
            "__init__",
            [](FanoutCache* self, const std::string& cache_path, std::size_t shard_count,
               std::size_t max_size, double timeout)
            {
                new (self) FanoutCache(cache_path, shard_count, max_size,
                                       static_cast<int>(timeout * 1000.0));
            },
            "cache_path"_a = ".cache/", "shard_count"_a = 8, "max_size"_a = 0,
            "timeout"_a = 600.0)
        .def("count", &FanoutCache::count, nb::call_guard<nb::gil_scoped_release>())
        .def("__len__", &FanoutCache::count, nb::call_guard<nb::gil_scoped_release>())
        .def("set", _set_item_impl<FanoutCache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def(
            "__setitem__", [](FanoutCache& c, const std::string& key, nb::handle buffer)
            { _set_item_impl(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &FanoutCache::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("__getitem__", &FanoutCache::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("keys", &FanoutCache::keys, nb::call_guard<nb::gil_scoped_release>())
        .def("iterkeys", &FanoutCache::iterkeys, nb::keep_alive<0, 1>(),
             nb::call_guard<nb::gil_scoped_release>())
        .def("exists", &FanoutCache::exists, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("add", _add_item_impl<FanoutCache>, nb::arg("key"), nb::arg("value"),
             nb::arg("expire") = nb::none(), nb::arg("tag") = nb::none())
        .def("delete", &FanoutCache::del, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("pop", &FanoutCache::pop, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("touch", _touch_impl<FanoutCache>, nb::arg("key"), nb::arg("expire") = nb::none())
        .def("expire_and_tag", &FanoutCache::expire_and_tag, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("expire", &FanoutCache::expire, nb::call_guard<nb::gil_scoped_release>())
        .def("evict", &FanoutCache::evict, nb::call_guard<nb::gil_scoped_release>())
        .def("evict_tag", &FanoutCache::evict_tag, nb::arg("tag"), nb::call_guard<nb::gil_scoped_release>())
        .def("incr", &FanoutCache::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("decr", &FanoutCache::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("clear", &FanoutCache::clear, nb::call_guard<nb::gil_scoped_release>())
        .def("check", &FanoutCache::check, nb::arg("fix") = false, nb::call_guard<nb::gil_scoped_release>())
        .def("set_meta", &FanoutCache::set_meta, nb::arg("key"), nb::arg("value"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("get_meta", &FanoutCache::get_meta, nb::arg("key"), nb::call_guard<nb::gil_scoped_release>())
        .def("size", &FanoutCache::size, nb::call_guard<nb::gil_scoped_release>())
        .def("volume", &FanoutCache::volume, nb::call_guard<nb::gil_scoped_release>())
        .def("shard_count", &FanoutCache::shard_count)
        .def("set_max_cache_size", &FanoutCache::set_max_cache_size, nb::arg("value"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("path", [](FanoutCache& c) { return c.path().string(); })
        .def("stats", [](FanoutCache& c) {
            auto s = [&] { nb::gil_scoped_release release; return c.stats(); }();
            nb::dict d;
            d["hits"] = s.hits;
            d["misses"] = s.misses;
            return d;
        })
        .def("reset_stats", &FanoutCache::reset_stats, nb::call_guard<nb::gil_scoped_release>())
        .def("close", &FanoutCache::close, close_doc, nb::call_guard<nb::gil_scoped_release>())
        .def("begin_user_transaction", &FanoutCache::begin_user_transaction, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>());

    nb::class_<FanoutIndex>(m, "FanoutIndex")
        .def(nb::init<const std::string&, std::size_t>(),
             "path"_a = ".index/", "shard_count"_a = 8)
        .def("count", &FanoutIndex::count, nb::call_guard<nb::gil_scoped_release>())
        .def("__len__", &FanoutIndex::count, nb::call_guard<nb::gil_scoped_release>())
        .def("set", _simple_set_item<FanoutIndex>, nb::arg("key"), nb::arg("value"))
        .def("__setitem__", _simple_set_item<FanoutIndex>, nb::arg("key"), nb::arg("value"))
        .def("get", &FanoutIndex::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("__getitem__", &FanoutIndex::get, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("keys", &FanoutIndex::keys, nb::call_guard<nb::gil_scoped_release>())
        .def("iterkeys", &FanoutIndex::iterkeys, nb::keep_alive<0, 1>(),
             nb::call_guard<nb::gil_scoped_release>())
        .def("exists", &FanoutIndex::exists, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("add", _simple_add_item<FanoutIndex>, nb::arg("key"), nb::arg("value"))
        .def("delete", &FanoutIndex::del, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("pop", &FanoutIndex::pop, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("incr", &FanoutIndex::incr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("decr", &FanoutIndex::decr, nb::arg("key"), nb::arg("delta") = 1,
             nb::arg("default_value") = 0,
             nb::call_guard<nb::gil_scoped_release>())
        .def("clear", &FanoutIndex::clear, nb::call_guard<nb::gil_scoped_release>())
        .def("check", &FanoutIndex::check, nb::arg("fix") = false, nb::call_guard<nb::gil_scoped_release>())
        .def("size", &FanoutIndex::size, nb::call_guard<nb::gil_scoped_release>())
        .def("volume", &FanoutIndex::volume, nb::call_guard<nb::gil_scoped_release>())
        .def("shard_count", &FanoutIndex::shard_count)
        .def("set_meta", &FanoutIndex::set_meta, nb::arg("key"), nb::arg("value"),
             nb::call_guard<nb::gil_scoped_release>())
        .def("get_meta", &FanoutIndex::get_meta, nb::arg("key"), nb::call_guard<nb::gil_scoped_release>())
        .def("path", [](FanoutIndex& idx) { return idx.path().string(); })
        .def("close", &FanoutIndex::close, close_doc, nb::call_guard<nb::gil_scoped_release>())
        .def("begin_user_transaction", &FanoutIndex::begin_user_transaction, nb::arg("key"),
             nb::call_guard<nb::gil_scoped_release>());
}
