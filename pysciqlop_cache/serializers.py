from __future__ import annotations

import io
import pickle
import struct
import sys
from typing import Any, Protocol, runtime_checkable

__all__ = ["Serializer", "PickleSerializer", "MsgspecSerializer", "PickleOOBSerializer"]


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


def _datetime_from_int64(ints: Any, dtype: Any) -> Any:
    return ints.view(dtype)


class _OOBPickler(pickle.Pickler):
    # numpy only exports plain numeric dtypes as PickleBuffer; datetime64 and
    # timedelta64 would silently fall back in-band (a full copy under the GIL).
    def reducer_override(self, obj: Any) -> Any:
        np = sys.modules.get("numpy")
        if np is not None and type(obj) is np.ndarray and obj.dtype.kind in "mM":
            return _datetime_from_int64, (obj.view("i8"), obj.dtype)
        return NotImplemented


def _copy_raw(stored: memoryview, raw_size: int) -> Any:
    import numpy as np

    return np.frombuffer(stored, np.uint8).copy()


_CODEC_RAW = 0
# codec id -> (stored bytes, raw size) -> writable buffer. Ids are persisted:
# never reuse one, only add.
_BUFFER_DECODERS = {_CODEC_RAW: _copy_raw}
_PREAMBLE = struct.Struct("<IQ")  # buffer count, header size
_ENTRY = struct.Struct("<BQQ")  # codec, stored size, raw size


def _decode_buffer(codec: int, stored: memoryview, raw_size: int) -> Any:
    decoder = _BUFFER_DECODERS.get(codec)
    if decoder is None:
        raise pickle.UnpicklingError(
            f"pickle-oob buffer codec {codec} is not supported by this pysciqlop_cache version"
        )
    return decoder(stored, raw_size)


class PickleOOBSerializer:
    """Pickle protocol 5 with the array buffers stored out-of-band.

    Layout: MAGIC | u32 count | u64 header size | count x (u8 codec, u64 stored
    size, u64 raw size) | header pickle | buffers. The per-buffer codec lets a
    later version compress some buffers (e.g. blosc2 on time axes) without a
    format change; today every buffer is raw. On load each buffer is copied by numpy, which
    releases the GIL, instead of by pickle, which holds it. Values without
    buffers are written as plain pickle, and plain pickle entries still load,
    so a "pickle" cache can switch to this serializer in place.
    Same trust model as PickleSerializer: only load caches you wrote.
    See https://peps.python.org/pep-0574/
    """

    name = "pickle-oob"
    MAGIC = b"\x00SQCOOB1"
    # Below this a buffer stays in-band: copying it holds the GIL for less than
    # ~10 us, and the per-buffer bookkeeping would cost more than it saves.
    MIN_OOB_BUFFER = 64 * 1024

    def dumps(self, value: Any) -> bytes:
        buffers: list[pickle.PickleBuffer] = []

        def keep_in_band(buffer: pickle.PickleBuffer) -> bool:
            if buffer.raw().nbytes < self.MIN_OOB_BUFFER:
                return True
            buffers.append(buffer)
            return False

        out = io.BytesIO()
        _OOBPickler(out, protocol=5, buffer_callback=keep_in_band).dump(value)
        header = out.getvalue()
        if not buffers:
            return header
        raws = [b.raw() for b in buffers]
        entries = b"".join(_ENTRY.pack(_CODEC_RAW, r.nbytes, r.nbytes) for r in raws)
        return b"".join([self.MAGIC, _PREAMBLE.pack(len(raws), len(header)), entries, header, *raws])

    def loads(self, data: bytes | memoryview) -> Any:
        if data[0] != self.MAGIC[0]:
            return pickle.loads(data)
        view = memoryview(data)
        if view[: len(self.MAGIC)] != self.MAGIC:
            raise pickle.UnpicklingError("unknown pickle-oob header")
        count, header_size = _PREAMBLE.unpack_from(view, len(self.MAGIC))
        entries_at = len(self.MAGIC) + _PREAMBLE.size
        header_at = entries_at + count * _ENTRY.size
        offset = header_at + header_size
        buffers = []
        for codec, stored_size, raw_size in _ENTRY.iter_unpack(view[entries_at:header_at]):
            buffers.append(_decode_buffer(codec, view[offset : offset + stored_size], raw_size))
            offset += stored_size
        return pickle.loads(view[header_at : header_at + header_size], buffers=buffers)


_SERIALIZERS: dict[str, type[PickleSerializer | MsgspecSerializer | PickleOOBSerializer]] = {
    "pickle": PickleSerializer,
    "msgspec": MsgspecSerializer,
    "pickle-oob": PickleOOBSerializer,
}

# stored -> requested switches allowed on an existing cache: the requested
# serializer must read every entry the stored one wrote.
SERIALIZER_UPGRADES = frozenset({("pickle", "pickle-oob")})


def get_serializer_by_name(name: str) -> Serializer:
    cls = _SERIALIZERS.get(name)
    if cls is None:
        raise ValueError(f"Unknown serializer: {name!r}. Available: {list(_SERIALIZERS)}")
    return cls()
