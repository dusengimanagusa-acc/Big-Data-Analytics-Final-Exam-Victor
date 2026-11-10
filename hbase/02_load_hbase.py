"""
PART 1.2 - Load a subset of sessions into HBase (via Thrift / happybase).

  user_sessions   : one row per session  (key = user_id#reverse_ts)
  product_metrics : one row per product per day, aggregated while streaming the sessions

Usage: python hbase/02_load_hbase.py --data data --max-files 1
       (--max-files limits how many sessions_N.json files are loaded = "representative subset")
"""
import argparse, glob, json, os, sys
from collections import defaultdict
sys.path.insert(0, os.path.dirname(__file__))
from hb_common import get_connection, session_rowkey, product_rowkey


def session_to_cells(s):
    cells = {
        b"meta:session_id": s["session_id"], b"meta:start_time": s["start_time"],
        b"meta:end_time": s["end_time"], b"meta:duration_seconds": str(s["duration_seconds"]),
        b"meta:referrer": s["referrer"], b"meta:conversion_status": s["conversion_status"],
        b"meta:device_type": s["device_profile"]["type"], b"meta:os": s["device_profile"]["os"],
        b"meta:browser": s["device_profile"]["browser"],
        b"meta:city": s["geo_data"]["city"], b"meta:state": s["geo_data"]["state"],
        b"meta:country": s["geo_data"]["country"],
    }
    # sparse, variable-width part of the row: one cell per page view / cart line
    for i, pv in enumerate(s["page_views"]):
        cells[f"pv:{i:03d}".encode()] = "|".join(
            [pv["timestamp"], pv["page_type"], pv["product_id"] or "", pv["category_id"] or "",
             str(pv["view_duration"])])
    for pid, c in s["cart_contents"].items():
        cells[f"cart:{pid}".encode()] = f"{c['quantity']}|{c['price']}"
    return {k: (v if isinstance(v, bytes) else v.encode()) for k, v in cells.items()}


def main(data_dir, max_files):
    conn = get_connection()
    sess_tbl, prod_tbl = conn.table("user_sessions"), conn.table("product_metrics")
    metrics = defaultdict(lambda: defaultdict(float))
    n = 0
    files = sorted(glob.glob(f"{data_dir}/sessions_*.json"))[:max_files]
    for fp in files:
        with open(fp) as f:
            sessions = json.load(f)
        with sess_tbl.batch(batch_size=2000) as b:   # client-side buffering: far fewer RPCs
            for s in sessions:
                b.put(session_rowkey(s["user_id"], s["start_time"]), session_to_cells(s))
                n += 1
                day = s["start_time"][:10]
                for pv in s["page_views"]:
                    if pv["page_type"] == "product_detail" and pv["product_id"]:
                        m = metrics[(pv["product_id"], day)]
                        m["views"] += 1
                        m["view_seconds"] += pv["view_duration"]
                for pid, c in s["cart_contents"].items():
                    m = metrics[(pid, day)]
                    m["cart_adds"] += 1
                    m["cart_units"] += c["quantity"]
                    if s["conversion_status"] == "converted":
                        m["purchase_sessions"] += 1
                        m["purchased_units"] += c["quantity"]
        print(f"loaded {fp}: total sessions {n:,}")
    with prod_tbl.batch(batch_size=5000) as b:
        for (pid, day), m in metrics.items():
            b.put(product_rowkey(pid, day),
                  {f"m:{k}".encode(): str(int(v)).encode() for k, v in m.items()})
    print(f"product_metrics rows written: {len(metrics):,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--max-files", type=int, default=1)
    a = ap.parse_args()
    main(a.data, a.max_files)
