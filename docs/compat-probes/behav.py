import sys, tempfile, pickle, os, traceback
sys.path.insert(0, "/home/jeandet/Documents/prog/SciQLop/.venv/lib/python3.14/site-packages")
sys.path.insert(0, "/home/jeandet/Documents/prog/Sciqlop-cache/build")
import diskcache, pysciqlop_cache

def run(label, fn):
    try:
        r = fn()
        return f"{label}: OK -> {r!r}"[:160]
    except Exception as e:
        return f"{label}: {type(e).__name__}: {str(e).splitlines()[0][:110]}"

def probe(mod, name):
    C = mod.Cache; F = mod.FanoutCache; I = mod.Index
    d = tempfile.mkdtemp(); c = C(d); c.set("k", "v")
    f = F(tempfile.mkdtemp()); f.set("k", "v")
    ix = I(tempfile.mkdtemp()); ix["a"] = 1
    out = []
    out.append(run("set returns", lambda: c.set("k2", "v")))
    out.append(run("cache[missing]", lambda: c["missing"]))
    out.append(run("del cache[missing]", lambda: c.__delitem__("missing")))
    out.append(run("delete(missing)", lambda: c.delete("missing")))
    out.append(run("int key", lambda: (c.set(42, "x"), c.get(42))))
    out.append(run("tuple key", lambda: (c.set(("a", 1), "x"), c.get(("a", 1)))))
    out.append(run("bytes key", lambda: (c.set(b"bk", "x"), c.get(b"bk"))))
    out.append(run("bytes value roundtrip type", lambda: (c.set("bv", b"raw"), type(c.get("bv")).__name__)))
    out.append(run("set retry=True", lambda: c.set("k", "v", retry=True)))
    out.append(run("get retry=True", lambda: c.get("k", retry=True)))
    out.append(run("get tag=True", lambda: c.get("k", tag=True)))
    out.append(run("get expire_time=True", lambda: c.get("k", expire_time=True)))
    out.append(run("get read=True", lambda: type(c.get("k", read=True)).__name__))
    out.append(run("pop missing", lambda: c.pop("nope")))
    out.append(run("stats()", lambda: c.stats()))
    out.append(run("stats(enable=True)", lambda: c.stats(enable=True)))
    out.append(run("clear() returns", lambda: c.clear()))
    out.append(run("expire() returns", lambda: c.expire()))
    out.append(run("evict('tag')", lambda: c.evict("tag")))
    out.append(run("cull()", lambda: c.cull()))
    out.append(run("check() type", lambda: type(c.check()).__name__))
    out.append(run("volume()", lambda: c.volume()))
    out.append(run("iterkeys(reverse=True)", lambda: list(c.iterkeys(reverse=True))))
    out.append(run("reversed(cache)", lambda: list(reversed(c))))
    out.append(run("directory attr", lambda: c.directory))
    out.append(run("pickle roundtrip", lambda: type(pickle.loads(pickle.dumps(c))).__name__))
    out.append(run("Cache() no args dir", lambda: C().directory if hasattr(C(), "directory") else C().path()))
    out.append(run("Cache('~/...') expands", lambda: C(os.path.join("~", ".sciqlop-probe-tmp")).__class__.__name__ and os.path.isdir(os.path.expanduser("~/.sciqlop-probe-tmp"))))
    out.append(run("Cache(size_limit=)", lambda: C(tempfile.mkdtemp(), size_limit=2**20).__class__.__name__))
    out.append(run("Cache(directory=)", lambda: C(directory=tempfile.mkdtemp()).__class__.__name__))
    out.append(run("transact(retry=True)", lambda: c.transact(retry=True).__enter__() and True))
    out.append(run("Fanout.transact() no key", lambda: f.transact().__class__.__name__))
    out.append(run("Fanout.cache('x')", lambda: f.cache("x").__class__.__name__))
    out.append(run("Fanout size_limit kw", lambda: F(tempfile.mkdtemp(), size_limit=2**20).__class__.__name__))
    out.append(run("Fanout shards kw", lambda: F(tempfile.mkdtemp(), shards=2).__class__.__name__))
    out.append(run("Index.items()", lambda: list(ix.items())))
    out.append(run("Index.pop(missing)", lambda: ix.pop("missing")))
    out.append(run("Index[missing]", lambda: ix["missing"]))
    out.append(run("Index.setdefault", lambda: ix.setdefault("z", 5)))
    out.append(run("Index.update", lambda: ix.update({"q": 1})))
    out.append(run("Index int key", lambda: (ix.__setitem__(7, "x"), ix[7])))
    out.append(run("Index.__init__ kwargs", lambda: I(tempfile.mkdtemp(), a=1)["a"]))
    m = c.memoize()(lambda x: x * 2)
    out.append(run("memoize __cache_key__", lambda: m.__cache_key__(3)))
    out.append(run("memoize('name')", lambda: c.memoize("name")(lambda x: x)(1)))
    out.append(run("memoize ignore=", lambda: c.memoize(ignore=(0,))(lambda x: x)(1)))
    out.append(run("Timeout exc", lambda: mod.Timeout.__name__))
    out.append(run("RLock", lambda: mod.RLock.__name__))
    out.append(run("BoundedSemaphore", lambda: mod.BoundedSemaphore.__name__))
    out.append(run("throttle", lambda: mod.throttle.__name__))
    out.append(run("barrier", lambda: mod.barrier.__name__))
    out.append(run("memoize_stampede", lambda: mod.memoize_stampede.__name__))
    out.append(run("Deque", lambda: mod.Deque.__name__))
    out.append(run("Lock(cache,key) ctor", lambda: mod.Lock(c, "lk").__class__.__name__))
    out.append(run("Lock.locked()", lambda: mod.Lock(c, "lk2").locked()))
    out.append(run("close then get", lambda: (c.close(), c.get("k"))))
    return out

a = probe(diskcache, "diskcache"); b = probe(pysciqlop_cache, "ours")
for x, y in zip(a, b):
    print("DC  ", x); print("OURS", y); print()
