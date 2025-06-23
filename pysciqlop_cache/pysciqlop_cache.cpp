#include "sciqlop_cache/sciqlop_cache.hpp"

#include <pybind11/chrono.h>
#include <pybind11/iostream.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <fmt/ranges.h>

#include "include/sciqlop_cache/sciqlop_cache.hpp"

namespace py = pybind11;

PYBIND11_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

    py::class_<Cache>(m, "Cache")
        .def(py::init<const std::string &, size_t>(), py::arg("db_path") = "sciclop-cache.db", py::arg("max_size") = 1000)
        .def("open", &Cache::open, py::arg("db_path") = "sciclop-cache.db")
        .def("close", &Cache::close)
        .def("set", &Cache::set, py::arg("key"), py::arg("value"), py::arg("expire") = 3600)
        .def("get", &Cache::get, py::arg("key"))
        .def("add", &Cache::add, py::arg("key"), py::arg("value"), py::arg("expire") = 3600)
        .def("del", &Cache::del, py::arg("key"))
        .def("pop", &Cache::pop, py::arg("key"))
        .def("touch", &Cache::touch, py::arg("key"), py::arg("expire") = 3600)
        .def("expire", &Cache::expire)
        .def("evict", &Cache::evict)
        .def("clear", &Cache::clear)
        .def("check", &Cache::check);
}
