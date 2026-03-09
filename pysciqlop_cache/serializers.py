from __future__ import annotations

import pickle
from typing import Any, Protocol, runtime_checkable

__all__ = ["Serializer", "PickleSerializer", "MsgspecSerializer"]


@runtime_checkable
class Serializer(Protocol):
    name: str

    def dumps(self, value: Any) -> bytes: ...
    def loads(self, data: bytes | memoryview) -> Any: ...


class PickleSerializer:
    name = "pickle"

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL):
        self._protocol = protocol

    def dumps(self, value: Any) -> bytes:
        return pickle.dumps(value, self._protocol)

    def loads(self, data: bytes | memoryview) -> Any:
        return pickle.loads(data)


# Ext type codes for msgspec
_EXT_NUMPY = 1
_EXT_OBJECT = 2


def _enc_hook(obj: Any) -> Any:
    import msgspec.msgpack

    try:
        import numpy as np
        if isinstance(obj, np.ndarray):
            order = "F" if obj.flags["F_CONTIGUOUS"] else "C"
            header = msgspec.msgpack.encode({
                "dtype": str(obj.dtype),
                "shape": list(obj.shape),
                "order": order,
            })
            header_len = len(header).to_bytes(4, "little")
            return msgspec.msgpack.Ext(_EXT_NUMPY, header_len + header + obj.tobytes(order=order))
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
    except ImportError:
        pass

    if hasattr(obj, "__dict__"):
        return msgspec.msgpack.Ext(
            _EXT_OBJECT,
            msgspec.msgpack.encode(
                {
                    "__type__": f"{type(obj).__module__}.{type(obj).__qualname__}",
                    "__data__": obj.__dict__,
                },
                enc_hook=_enc_hook,
            ),
        )

    raise TypeError(f"Cannot serialize {type(obj)}")


def _import_type(type_path: str) -> type:
    module_path, _, qualname = type_path.rpartition(".")
    import importlib
    mod = importlib.import_module(module_path)
    obj = mod
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return obj


def _ext_hook(code: int, data: memoryview) -> Any:
    import msgspec.msgpack

    if code == _EXT_NUMPY:
        import numpy as np
        header_len = int.from_bytes(data[:4], "little")
        header = msgspec.msgpack.decode(data[4:4 + header_len])
        buf = bytes(data[4 + header_len:])
        return np.frombuffer(buf, dtype=np.dtype(header["dtype"])).reshape(
            header["shape"], order=header.get("order", "C")
        )

    if code == _EXT_OBJECT:
        info = msgspec.msgpack.decode(data, type=dict, ext_hook=_ext_hook)
        type_path = info["__type__"]
        cls = _import_type(type_path)
        obj = cls.__new__(cls)
        for k, v in info["__data__"].items():
            setattr(obj, k, v)
        return obj

    raise ValueError(f"Unknown ext type code: {code}")


class MsgspecSerializer:
    name = "msgspec"

    def dumps(self, value: Any) -> bytes:
        import msgspec.msgpack
        return msgspec.msgpack.encode(value, enc_hook=_enc_hook)

    def loads(self, data: bytes | memoryview) -> Any:
        import msgspec.msgpack
        return msgspec.msgpack.decode(data, ext_hook=_ext_hook)


_SERIALIZERS: dict[str, type[PickleSerializer | MsgspecSerializer]] = {
    "pickle": PickleSerializer,
    "msgspec": MsgspecSerializer,
}


def get_serializer_by_name(name: str) -> Serializer:
    cls = _SERIALIZERS.get(name)
    if cls is None:
        raise ValueError(f"Unknown serializer: {name!r}. Available: {list(_SERIALIZERS)}")
    return cls()
