"""Public datasets for the array benchmark, each made into a value of a requested size.

Files are downloaded once into SQC_BENCH_DATA (default ~/.cache/sciqlop-cache-bench),
from the SQC_BENCH_MIRROR mirror if set, else from the original source, checked against
a pinned checksum either way, and never committed. Sources (cite them when
publishing results):

- images:  CIFAR-10, binary version (Krizhevsky 2009), https://www.cs.toronto.edu/~kriz/cifar.html
           The binary version, not the "python version": that one is pickled, and
           unpickling a downloaded file runs whatever code it holds.
- table:   NYC TLC yellow taxi trips, 2024-01 to 2024-03 (Parquet),
           https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- fields:  SDRBench Hurricane ISABEL, 100x500x500 float32, one time step, 13 raw fields,
           https://sdrbench.github.io/datasets.html
- sensor:  UCI Individual household electric power consumption (Hebrail & Berard 2006,
           CC BY 4.0), https://doi.org/10.24432/C58K54
- noise:   random normal float32 with a datetime64 axis: the incompressible worst case.

A value bigger than its dataset is the dataset tiled; tiled data can compress better
than unique data of the same size.
"""

import hashlib
import io
import os
import tarfile
import urllib.request
import zipfile
from pathlib import Path

import numpy as np

DATA_DIR = Path(os.environ.get("SQC_BENCH_DATA", "~/.cache/sciqlop-cache-bench")).expanduser()
# Optional mirror with the same layout as DATA_DIR/downloads (<name>/<file>), tried first.
MIRROR = os.environ.get(
    "SQC_BENCH_MIRROR", "https://hephaistos.lpp.polytechnique.fr/data/mirrors/sciqlop-cache-bench").rstrip("/")

# name: (url, algorithm, digest). None = not pinned: fetch() prints the digest.
FILES = {
    "cifar": ("https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz",
              "md5", "c32a1d4ab5d03f1284b67883e8d87530"),
    "taxi-2024-01": ("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
                     "sha256", "c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510"),
    "taxi-2024-02": ("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
                     "sha256", "c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20"),
    "taxi-2024-03": ("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
                     "sha256", "2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823"),
    "hurricane": ("https://g-d0cd3f.fd635.8443.data.globus.org/raw-data/Hurricane-ISABEL/"
                  "SDRBENCH-Hurricane-ISABEL-100x500x500.tar.gz", "sha256", "ca5f8003ab4a650ea2d546eeb7ad6efd55b6e14281b361b365add41d0e7fa5ac"),
    "power": ("https://archive.ics.uci.edu/static/public/235/"
              "individual+household+electric+power+consumption.zip", "sha256", "9f84b46ade8a2d8e1286ec4b2b6c2987a45a755c59f263be3b3b3d10dfbda3ff"),
}


def _digest(path, algorithm):
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def fetch(name):
    """Local path of a dataset file, downloaded and checked on first use."""
    url, algorithm, expected = FILES[name]
    path = DATA_DIR / "downloads" / name / url.rsplit("/", 1)[-1]
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_suffix(path.suffix + ".part")
        sources = ([f"{MIRROR}/{name}/{path.name}"] if MIRROR else []) + [url]
        for source in sources:
            try:
                urllib.request.urlretrieve(source, partial)
                break
            except OSError as error:
                print(f"  {name}: {source} failed ({error})")
        else:
            raise RuntimeError(f"{name}: no source could be downloaded")
        partial.rename(path)
    actual = _digest(path, algorithm)
    if expected is None:
        print(f"  {name}: {algorithm} {actual} (not pinned)")
    elif actual != expected:
        raise RuntimeError(f"{name}: {algorithm} mismatch, got {actual}; delete {path} to retry")
    return path


def _tile(array, n):
    """The first n rows of `array` repeated as often as needed."""
    reps = -(-n // len(array))
    return np.concatenate([array] * reps)[:n] if reps > 1 else array[:n]


def _rows_for(nbytes, row_bytes):
    return max(1, round(nbytes / row_bytes))


class Images:
    """CIFAR-10 images as one uint8 array (n, 32, 32, 3): an ML pipeline's batch."""

    def __init__(self):
        with tarfile.open(fetch("cifar")) as tar:
            batches = [tar.extractfile(m).read() for m in sorted(tar.getmembers(), key=lambda m: m.name)
                       if m.name.endswith(".bin") and "batch" in m.name]
        records = np.frombuffer(b"".join(batches), np.uint8).reshape(-1, 1 + 3072)
        self.images = records[:, 1:].reshape(-1, 3, 32, 32).transpose(0, 2, 3, 1).copy()

    def value(self, nbytes):
        return _tile(self.images, _rows_for(nbytes, 3072))


class Table:
    """NYC taxi trips as a pandas DataFrame: timestamps, floats, ints and strings."""

    def __init__(self):
        import pandas as pd
        months = [pd.read_parquet(fetch(f"taxi-2024-0{m}")) for m in (1, 2, 3)]
        self.df = pd.concat(months, ignore_index=True)
        self.row_bytes = self.df.memory_usage(deep=True).sum() / len(self.df)

    def value(self, nbytes):
        import pandas as pd
        n = _rows_for(nbytes, self.row_bytes)
        reps = -(-n // len(self.df))
        df = pd.concat([self.df] * reps, ignore_index=True) if reps > 1 else self.df
        return df.iloc[:n].reset_index(drop=True)


class Fields:
    """Hurricane ISABEL simulation fields, float32 (fields, 100, 500, 500)."""

    def __init__(self):
        with tarfile.open(fetch("hurricane")) as tar:
            # The .log10 files are derived copies of the same fields: skip them.
            members = sorted((m for m in tar.getmembers() if m.isfile() and m.name.endswith(".f32")
                              and ".log10." not in m.name), key=lambda m: m.name)
            self.fields = np.stack([np.frombuffer(tar.extractfile(m).read(), np.float32)
                                    for m in members])

    def value(self, nbytes):
        flat = self.fields.reshape(-1)
        return _tile(flat, _rows_for(nbytes, 4))


class Sensor:
    """Household power readings: one-minute datetime64 axis plus 7 float64 channels."""

    def __init__(self):
        import pandas as pd
        with zipfile.ZipFile(fetch("power")) as z:
            text = z.read("household_power_consumption.txt")
        df = pd.read_csv(io.BytesIO(text), sep=";", na_values="?", low_memory=False)
        self.time = pd.to_datetime(df.pop("Date") + " " + df.pop("Time"),
                                   format="%d/%m/%Y %H:%M:%S").to_numpy()
        self.values = df.to_numpy(np.float64)

    def value(self, nbytes):
        n = _rows_for(nbytes, 8 + 7 * 8)
        return {"time": _tile(self.time, n), "values": _tile(self.values, n),
                "meta": {"units": "kW, V, A, Wh", "sampling": "1 min"}}


class Noise:
    """Random normal float32 x4 with a regular datetime64 axis: incompressible values."""

    def __init__(self):
        self.rng = np.random.default_rng(0)

    def value(self, nbytes):
        n = _rows_for(nbytes, 24)
        time = (np.arange(n, dtype=np.int64) * 62_500_000).astype("datetime64[ns]")
        return {"time": time, "values": self.rng.standard_normal((n, 4), dtype=np.float32)}


DATASETS = {"images": Images, "table": Table, "fields": Fields, "sensor": Sensor, "noise": Noise}


def value_nbytes(value):
    """In-memory size of a benchmark value."""
    if isinstance(value, np.ndarray):
        return value.nbytes
    if isinstance(value, dict):
        return sum(v.nbytes for v in value.values() if isinstance(v, np.ndarray))
    return int(value.memory_usage(deep=True).sum())


if __name__ == "__main__":
    for name in FILES:
        print(name, fetch(name))
