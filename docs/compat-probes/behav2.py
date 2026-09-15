import sys, tempfile, time
sys.path.insert(0, "/home/jeandet/Documents/prog/SciQLop/.venv/lib/python3.14/site-packages")
sys.path.insert(0, "/home/jeandet/Documents/prog/Sciqlop-cache/build")
import diskcache, pysciqlop_cache
def run(label, fn):
    try: return f"{label}: OK -> {fn()!r}"[:150]
    except Exception as e: return f"{label}: {type(e).__name__}: {str(e).splitlines()[0][:100]}"
def probe(mod):
    c = mod.Cache(tempfile.mkdtemp()); out=[]
    for k in ["b","a","c"]: c.set(k, 1)
    out.append(run("iter order", lambda: list(c)))
    out.append(run("iter order after re-set a", lambda: (c.set("a", 2), list(c))[1]))
    c.set("exp", 1, expire=0); time.sleep(1.05)
    out.append(run("len incl expired", lambda: len(c)))
    out.append(run("'exp' in cache", lambda: "exp" in c))
    out.append(run("incr missing default=None", lambda: c.incr("nokey", default=None)))
    out.append(run("incr non-int value", lambda: (c.set("s","str"), c.incr("s"))))
    out.append(run("set expire=timedelta", lambda: c.set("td", 1, expire=__import__("datetime").timedelta(seconds=5))))
    out.append(run("add existing returns", lambda: c.add("a", 9)))
    out.append(run("get default kw", lambda: c.get("zz", default=7)))
    out.append(run("pop default kw", lambda: c.pop("zz", default=7)))
    out.append(run("with reuse", lambda: (c.__enter__(), c.__exit__(None,None,None), c.__enter__(), c.__exit__(None,None,None), c.get("a"))[-1]))
    out.append(run("evict_tag/evict(tag) count", lambda: (c.set("t1",1,tag="x"), c.set("t2",1,tag="x"), (c.evict_tag("x") if hasattr(c,"evict_tag") else c.evict("x")))[-1]))
    out.append(run("delete returns", lambda: c.delete("b")))
    out.append(run("Lock expire int", lambda: mod.Lock(c, "L", expire=5).__enter__() or True))
    out.append(run("memoize .__wrapped__", lambda: hasattr(c.memoize()(lambda: 1), "__wrapped__")))
    out.append(run("None value roundtrip distinguishes missing", lambda: (c.set("n", None), c.get("n", default="DEF"))[1]))
    out.append(run("key with '/' and unicode", lambda: (c.set("a/b/é", 1), c.get("a/b/é"))[1]))
    out.append(run("empty str key", lambda: (c.set("", 1), c.get(""))[1]))
    return out
for x,y in zip(probe(diskcache), probe(pysciqlop_cache)): print("DC  ",x); print("OURS",y); print()
