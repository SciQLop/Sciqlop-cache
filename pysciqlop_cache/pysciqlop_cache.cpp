#include "sciqlop_cache/sciqlop_cache.hpp"

#include <pybind11/chrono.h>
#include <pybind11/iostream.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <fmt/ranges.h>

namespace py = pybind11;

PYBIND11_MODULE(_pysciqlop_cache, m)
{
    m.doc() = R"pbdoc(
        _pysciqlop_cache
        ----------------

    )pbdoc";

    m.def("test", test);
    m.def("sum",
        [](py::bytes& buffer)
        {
            py::buffer_info info(py::buffer(buffer).request());
            return sum(static_cast<char*>(info.ptr), static_cast<std::size_t>(info.size));
        });
}
