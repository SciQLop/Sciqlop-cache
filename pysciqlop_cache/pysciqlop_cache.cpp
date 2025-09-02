#include "sciqlop_cache/sciqlop_cache.hpp"

#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/chrono.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/string.h>

#include <fmt/ranges.h>

#include "include/sciqlop_cache/sciqlop_cache.hpp"
using namespace std::chrono_literals;

namespace nb = nanobind;
using namespace nb::literals;

inline void _set_item(Cache& c, const std::string& key, nb::bytes& buffer,
                      std::chrono::system_clock::duration* expire = nullptr)
{
    if (expire == nullptr)
    {
        c.set(key, std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size()));
    }
    else
    {
        c.set(key, std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size()),
              *expire);
    }
}

inline void _add_item(Cache& c, const std::string& key, nb::bytes& buffer,
                      std::chrono::system_clock::duration* expire = nullptr)
{
    if (expire == nullptr)
    {
        c.add(key, std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size()));
    }
    else
    {
        c.add(key, std::span<const char>(static_cast<const char*>(buffer.data()), buffer.size()),
              *expire);
    }
}

NB_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

    nb::class_<Buffer>(m, "Buffer")
        .def("memoryview",
             [](Buffer& b) -> nb::object
             {
                 // Check if the buffer is valid
                 if (!b) {
                     throw std::runtime_error("Cannot create memory view of invalid buffer");
                 }

                 // Handle the case where data is null but size is zero
                 if (b.data() == nullptr && b.size() == 0) {
                     return nb::steal(PyMemoryView_FromMemory(nullptr, 0, PyBUF_READ));
                 }

                 Py_buffer view;

                 if (PyBuffer_FillInfo(&view, NULL, const_cast<char*>(b.data()),
                                       b.size(), 1, PyBUF_SIMPLE) == -1) {
                     throw std::runtime_error("Failed to create memory view");
                 }

                 PyObject* memview = PyMemoryView_FromBuffer(&view);
                 if (!memview) {
                     throw std::runtime_error("Failed to create memory view object");
                 }

                 return nb::borrow(memview);
             })
        .def_prop_ro("size", &Buffer::size)
        .def_prop_ro("valid", &Buffer::operator bool)
        .def_prop_ro("address", [](const Buffer& b) { return reinterpret_cast<std::uintptr_t>(b.data()); });


    nb::class_<Cache>(m, "Cache")
        .def(nb::init<const std::string&, size_t>(), "cache_path"_a = ".cache/",
             "max_size"_a = 1000)
        .def("count", &Cache::count)
        .def("__len__", &Cache::count)
        .def("set", _set_item, nb::arg("key"), nb::arg("value"), nb::arg("expire") = 3600s)
        .def(
            "__setitem__", [](Cache& c, const std::string& key, nb::bytes& buffer)
            { return _set_item(c, key, buffer); }, nb::arg("key"), nb::arg("value"))
        .def("get", &Cache::get, nb::arg("key"))
        .def("__getitem__", &Cache::get, nb::arg("key"))
        .def("keys", &Cache::keys)
        .def("exists", &Cache::exists, nb::arg("key"))
        .def("add", _add_item, nb::arg("key"), nb::arg("value"),
             nb::arg("expire").none(true) = 3600s)
        .def("delete", &Cache::del, nb::arg("key"))
        .def("pop", &Cache::pop, nb::arg("key"))
        .def(
            "touch",
            [&](Cache& c, const std::string& key, std::chrono::system_clock::duration expire)
            { return c.touch(key, expire); }, nb::arg("key"), nb::arg("expire").none(true) = 3600s)
        .def("expire", &Cache::expire)
        .def("evict", &Cache::evict)
        .def("clear", &Cache::clear)
        .def("check", &Cache::check);
}
