import inspect, sys, importlib
sys.path.insert(0, "/home/jeandet/Documents/prog/speasy_proxy/venv/lib/python3.11/site-packages")
sys.path.insert(0, "/home/jeandet/Documents/prog/Sciqlop-cache/build")
import diskcache, pysciqlop_cache
print("diskcache", diskcache.__version__)
def api(cls):
    out = {}
    for name, m in inspect.getmembers(cls):
        if name.startswith("_") and name not in ("__len__","__getitem__","__setitem__","__delitem__","__contains__","__iter__","__reversed__","__enter__","__exit__","__getstate__","__setstate__"):
            continue
        try:
            sig = str(inspect.signature(m))
        except Exception:
            sig = "<no sig>"
        kind = "prop" if isinstance(inspect.getattr_static(cls, name, None), property) else "meth"
        out[name] = (kind, sig)
    return out
pairs = [("Cache", diskcache.Cache, pysciqlop_cache.Cache),
         ("FanoutCache", diskcache.FanoutCache, pysciqlop_cache.FanoutCache),
         ("Index", diskcache.Index, pysciqlop_cache.Index),
         ("FanoutIndex(vs diskcache.Index)", diskcache.Index, pysciqlop_cache.FanoutIndex)]
for label, dc, ours in pairs:
    a, b = api(dc), api(ours)
    print(f"\n===== {label} =====")
    print("-- MISSING in pysciqlop_cache:")
    for n in sorted(set(a) - set(b)):
        print(f"   {n}{a[n][1]}")
    print("-- EXTRA in pysciqlop_cache (not in diskcache):")
    for n in sorted(set(b) - set(a)):
        print(f"   {n}{b[n][1]}")
    print("-- SIGNATURE DIFFERS:")
    for n in sorted(set(a) & set(b)):
        if a[n][1] != b[n][1]:
            print(f"   {n}\n      dc : {a[n][1]}\n      ours: {b[n][1]}")
print("\n===== module-level =====")
print("diskcache exports:", sorted(x for x in dir(diskcache) if not x.startswith("_")))
print("ours exports    :", sorted(x for x in dir(pysciqlop_cache) if not x.startswith("_")))
