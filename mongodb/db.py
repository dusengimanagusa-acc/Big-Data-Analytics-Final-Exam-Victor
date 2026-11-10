"""Shared MongoDB connection helper. Set MONGO_MOCK=1 to use an in-memory mock (testing only)."""
import os

DB_NAME = "ecommerce"
_MOCK = None


def get_db(uri=None):
    global _MOCK
    if os.getenv("MONGO_MOCK") == "1":
        import mongomock
        if _MOCK is None:  # singleton so loader and query scripts share data in one process
            _MOCK = mongomock.MongoClient()
        return _MOCK[DB_NAME]
    from pymongo import MongoClient
    uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    try:
        client.admin.command("ping")          # fail fast with a clear message instead of a 30 s stack trace
    except Exception as e:
        raise SystemExit(f"\nERROR: cannot connect to MongoDB at {uri}  ({type(e).__name__}).\n"
                         "  -> Start it with:  docker compose up -d     then check:  docker ps\n"
                         "  -> Check everything with:  python tools/check_environment.py --live --wait 120")
    return client[DB_NAME]
