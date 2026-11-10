"""Tiny in-memory stand-in for happybase (sorted keys, prefix/range scans) used ONLY for offline tests."""
import bisect

class FakeBatch:
    def __init__(self, t): self.t = t
    def __enter__(self): return self
    def __exit__(self, *a): pass
    def put(self, k, d): self.t.put(k, d)

class FakeTable:
    def __init__(self): self.rows = {}; self.keys = []
    def put(self, k, d):
        if k not in self.rows: bisect.insort(self.keys, k); self.rows[k] = {}
        self.rows[k].update(d)
    def batch(self, batch_size=None): return FakeBatch(self)
    def scan(self, row_start=None, row_stop=None, row_prefix=None, columns=None, limit=None):
        if row_prefix is not None: row_start, row_stop = row_prefix, row_prefix[:-1] + bytes([row_prefix[-1] + 1])
        i = bisect.bisect_left(self.keys, row_start) if row_start else 0
        n = 0
        while i < len(self.keys):
            k = self.keys[i]
            if row_stop and k >= row_stop: break
            row = self.rows[k]
            if columns: row = {c: v for c, v in row.items() if any(c == x or c.startswith(x + b":") for x in columns)}
            yield k, row
            n += 1; i += 1
            if limit and n >= limit: break

class FakeConnection:
    _inst = None
    @classmethod
    def instance(cls):
        if cls._inst is None: cls._inst = cls(); cls._inst.tables = {}
        return cls._inst
    def table(self, name): return self.tables.setdefault(name, FakeTable())
