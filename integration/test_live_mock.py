"""Exercises the --source live code path using in-memory MongoDB/HBase stand-ins (offline test only)."""
import os, sys, importlib
os.environ["MONGO_MOCK"] = "1"; os.environ["HBASE_MOCK"] = "1"
root = os.path.join(os.path.dirname(__file__), "..")
for d in ["mongodb", "hbase"]: sys.path.insert(0, os.path.join(root, d))
importlib.import_module("01_load_mongo").main("data", None)
importlib.import_module("02_load_hbase").main("data", int(os.getenv("HBASE_MAX_FILES", "2")))
integ = importlib.import_module("09_integrated_clv_funnel")
integ.main("live", None, os.getenv("OUT_DIR", "outputs"))
