import os, sys, importlib
os.environ["HBASE_MOCK"] = "1"
sys.path.insert(0, os.path.dirname(__file__))
load = importlib.import_module("02_load_hbase"); q = importlib.import_module("03_queries")
from hb_common import get_connection
load.main("data", 1)
conn = get_connection()
uid = next(iter(conn.table("user_sessions").keys)).decode().split("#")[0]
res = q.latest_sessions(conn, uid, 3)
print(uid, "latest 3:"); [print(" ", k, v["meta:start_time"], v["meta:conversion_status"]) for k, v in res]
starts = [v["meta:start_time"] for _, v in q.latest_sessions(conn, uid, 100)]
assert starts == sorted(starts, reverse=True), "not newest-first!"
print("newest-first ordering OK over", len(starts), "sessions")
r = q.sessions_in_range(conn, uid, starts[-1][:10], starts[0][:10]); assert len(r) == len(starts)
print("range scan OK:", len(r))
k = next(iter(conn.table("product_metrics").keys)).decode(); pid = k.split("#")[0]
print(q.product_daily(conn, pid, "2000-01-01", "2100-01-01")[:2])
