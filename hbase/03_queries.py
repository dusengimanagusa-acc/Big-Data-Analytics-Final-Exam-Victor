"""
PART 1.2 - Query examples for HBase.
  python hbase/03_queries.py user user_000042 --limit 5
  python hbase/03_queries.py user-range user_000042 2026-08-01 2026-08-31
  python hbase/03_queries.py product prod_00123 2026-08-01 2026-08-31
"""
import argparse, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from hb_common import get_connection, reverse_ts_from_ms, to_ms


def decode(row):
    return {k.decode(): v.decode() for k, v in row.items()}


def latest_sessions(conn, user_id, limit=5, full=False):
    """Newest-first prefix scan: row keys are user_id#reverse_ts."""
    t = conn.table("user_sessions")
    cols = None if full else [b"meta"]            # skip the heavy 'pv' family unless asked
    out = []
    for key, row in t.scan(row_prefix=f"{user_id}#".encode(), columns=cols, limit=limit):
        out.append((key.decode(), decode(row)))
    return out


def sessions_in_range(conn, user_id, start_date, end_date):
    """Time-range scan: because of the reversed timestamp, START is the *end* of the range."""
    t = conn.table("user_sessions")
    end_ms = to_ms(end_date + "T23:59:59")
    start_ms = to_ms(start_date + "T00:00:00")
    lo = f"{user_id}#{reverse_ts_from_ms(end_ms)}".encode()
    hi = f"{user_id}#{reverse_ts_from_ms(start_ms)}~".encode()
    return [(k.decode(), decode(r)) for k, r in t.scan(row_start=lo, row_stop=hi, columns=[b"meta"])]


def product_daily(conn, product_id, start_date, end_date):
    t = conn.table("product_metrics")
    lo = f"{product_id}#{start_date.replace('-', '')}".encode()
    hi = f"{product_id}#{end_date.replace('-', '')}~".encode()
    return [(k.decode(), decode(r)) for k, r in t.scan(row_start=lo, row_stop=hi)]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["user", "user-range", "product"])
    ap.add_argument("id")
    ap.add_argument("a", nargs="?"); ap.add_argument("b", nargs="?")
    ap.add_argument("--limit", type=int, default=5)
    x = ap.parse_args()
    conn = get_connection()
    if x.cmd == "user":
        res = latest_sessions(conn, x.id, x.limit)
    elif x.cmd == "user-range":
        res = sessions_in_range(conn, x.id, x.a, x.b)
    else:
        res = product_daily(conn, x.id, x.a, x.b)
    for k, v in res:
        print(k, v)
    print(f"-- {len(res)} rows")
