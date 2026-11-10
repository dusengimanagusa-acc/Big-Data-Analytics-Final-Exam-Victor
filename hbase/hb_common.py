"""Row-key helpers and connection factory shared by loader and queries."""
import os
from datetime import datetime, timezone

MAX_TS = 9_999_999_999_999   # 13 digits; reverse_ts = MAX_TS - epoch_ms


def to_ms(iso: str) -> int:
    # generator timestamps are naive local-time ISO strings; treat them as UTC consistently
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)


def session_rowkey(user_id: str, start_iso: str) -> bytes:
    return f"{user_id}#{MAX_TS - to_ms(start_iso):013d}".encode()


def reverse_ts_from_ms(ms: int) -> str:
    return f"{MAX_TS - ms:013d}"


def product_rowkey(product_id: str, iso: str) -> bytes:
    return f"{product_id}#{iso[:10].replace('-', '')}".encode()


def get_connection():
    if os.getenv("HBASE_MOCK") == "1":
        from fake_hbase import FakeConnection
        return FakeConnection.instance()
    import happybase
    host, port = os.getenv("HBASE_HOST", "localhost"), int(os.getenv("HBASE_PORT", "9090"))
    try:
        conn = happybase.Connection(host, port=port, timeout=60000)
        conn.tables()                         # forces a real round-trip so problems show up here
    except Exception as e:
        raise SystemExit(f"\nERROR: cannot connect to the HBase Thrift server at {host}:{port}  ({type(e).__name__}).\n"
                         "  -> HBase needs 60-90 s after 'docker compose up -d'. Wait, then retry.\n"
                         "  -> Check everything with:  python tools/check_environment.py --live --wait 120")
    return conn
