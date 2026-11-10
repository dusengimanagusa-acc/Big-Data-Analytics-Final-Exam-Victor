"""Runs loader + aggregations against mongomock so the logic can be tested without a server."""
import os, sys, importlib
os.environ["MONGO_MOCK"] = "1"
sys.path.insert(0, os.path.dirname(__file__))
load = importlib.import_module("01_load_mongo"); agg = importlib.import_module("02_aggregations")
from db import get_db

def strip_round(x):  # mongomock lacks $round (real MongoDB >= 4.2 has it); remove for the mock only
    if isinstance(x, dict):
        if set(x) == {"$round"}: return strip_round(x["$round"][0])
        return {k: strip_round(v) for k, v in x.items()}
    if isinstance(x, list): return [strip_round(i) for i in x]
    return x
load.main("data", None)
db = get_db()
for coll, pipe, name in [("transactions", agg.PIPE_A, "revenue_by_category_month"), ("transactions", agg.PIPE_B, "top_products"),
                         ("users", agg.build_pipe_c(db), "user_segments"), ("transactions", agg.PIPE_D, "revenue_state_payment")]:
    agg.run(db, coll, strip_round(pipe), "mongo_"+name, "outputs")
