"""
PART 1.1 - Load data into MongoDB with a document model designed for analytics.

Collections
  categories   : category document with EMBEDDED subcategories (small, always read together)
  products     : product + EMBEDDED price_history + DENORMALISED category/subcategory info
  transactions : transaction + EMBEDDED line items (each item carries category_id so revenue-by-
                 category needs no join); `ts` is a real BSON Date so time grouping/indexing works
  users        : profile + EMBEDDED, pre-computed purchase/engagement SUMMARY (computed pattern);
                 purchase_summary counts only completed/shipped/delivered orders (same as revenue)

Sessions (2M large, write-heavy time-series docs) are intentionally NOT put in MongoDB; they go to
HBase (see ../hbase) and Spark.

Usage: python mongodb/01_load_mongo.py --data data --uri mongodb://localhost:27017
"""
import argparse, glob, json, os, sys
from collections import defaultdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from db import get_db


# Only these order statuses count as realised revenue ('processing' orders are not paid/shipped yet).
# Must match the VALID filter used by the aggregation pipelines (02_aggregations.py) and the Spark jobs.
VALID_STATUSES = {"completed", "shipped", "delivered"}


def dt(s):
    return datetime.fromisoformat(s) if s else None


def load_json(path):
    with open(path) as f:
        return json.load(f)


def batches(seq, n=5000):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def main(data_dir, uri):
    db = get_db(uri)
    for c in ["categories", "products", "transactions", "users"]:
        db[c].drop()

    # ---------- categories ----------
    cats = load_json(f"{data_dir}/categories.json")
    for c in cats:
        c["_id"] = c["category_id"]
    db.categories.insert_many(cats)
    cat_by_id = {c["category_id"]: c for c in cats}

    # ---------- products ----------
    # NOTE: the generator does not emit subcategory_id; we assign one deterministically
    # (product number mod #subcategories) so the hierarchy / profit-margin analysis is possible.
    prods = load_json(f"{data_dir}/products.json")
    prod_info = {}
    for p in prods:
        cat = cat_by_id[p["category_id"]]
        subs = cat["subcategories"]
        sub = subs[int(p["product_id"].split("_")[1]) % len(subs)]
        p["_id"] = p["product_id"]
        p["category"] = {"category_id": cat["category_id"], "name": cat["name"]}
        p["subcategory"] = {"subcategory_id": sub["subcategory_id"], "name": sub["name"],
                            "profit_margin": sub["profit_margin"]}
        p["creation_date"] = dt(p["creation_date"])
        for ph in p["price_history"]:
            ph["date"] = dt(ph["date"])
        prod_info[p["product_id"]] = (cat["category_id"], sub["profit_margin"])
    for b in batches(prods):
        db.products.insert_many(b)

    # ---------- transactions + per-user rollups ----------
    txns = load_json(f"{data_dir}/transactions.json")
    roll = defaultdict(lambda: {"orders": 0, "spent": 0.0, "first": None, "last": None,
                                "cats": defaultdict(float)})
    pending = defaultdict(int)          # orders not yet realised (status 'processing')
    for t in txns:
        t["_id"] = t["transaction_id"]
        t["ts"] = dt(t.pop("timestamp"))
        for it in t["items"]:
            cid, margin = prod_info.get(it["product_id"], (None, 0))
            it["category_id"] = cid
            it["profit_margin"] = margin
        if t["status"] not in VALID_STATUSES:
            pending[t["user_id"]] += 1
            continue                      # keep the summary consistent with the revenue definition
        r = roll[t["user_id"]]
        r["orders"] += 1
        r["spent"] += t["total"]
        r["first"] = t["ts"] if not r["first"] or t["ts"] < r["first"] else r["first"]
        r["last"] = t["ts"] if not r["last"] or t["ts"] > r["last"] else r["last"]
        for it in t["items"]:
            r["cats"][it["category_id"]] += it["subtotal"]
    for b in batches(txns):
        db.transactions.insert_many(b)

    # ---------- session rollups (engagement), streamed file by file ----------
    sess_roll = defaultdict(lambda: {"n": 0, "dur": 0, "converted": 0})
    for fp in sorted(glob.glob(f"{data_dir}/sessions_*.json")):
        for s in load_json(fp):
            r = sess_roll[s["user_id"]]
            r["n"] += 1
            r["dur"] += s["duration_seconds"]
            r["converted"] += s["conversion_status"] == "converted"

    # ---------- users with embedded summaries ----------
    users = load_json(f"{data_dir}/users.json")
    for u in users:
        uid = u["user_id"]
        u["_id"] = uid
        u["registration_date"] = dt(u["registration_date"])
        u["last_active"] = dt(u["last_active"])
        r, s = roll.get(uid), sess_roll.get(uid)
        u["purchase_summary"] = {
            "total_orders": r["orders"] if r else 0,
            "total_spent": round(r["spent"], 2) if r else 0.0,
            "avg_order_value": round(r["spent"] / r["orders"], 2) if r else 0.0,
            "first_purchase": r["first"] if r else None,
            "last_purchase": r["last"] if r else None,
            "top_category": max(r["cats"], key=r["cats"].get) if r else None,
            "pending_orders": pending.get(uid, 0),
        }
        u["engagement_summary"] = {
            "session_count": s["n"] if s else 0,
            "avg_session_seconds": round(s["dur"] / s["n"], 1) if s else 0.0,
            "converted_sessions": s["converted"] if s else 0,
        }
    for b in batches(users):
        db.users.insert_many(b)

    # ---------- indexes matching the query patterns ----------
    db.transactions.create_index([("ts", 1)])
    db.transactions.create_index([("user_id", 1), ("ts", -1)])
    db.transactions.create_index([("items.product_id", 1)])
    db.products.create_index([("category.category_id", 1)])
    db.users.create_index([("purchase_summary.total_orders", 1)])
    db.users.create_index([("geo_data.state", 1)])

    for c in ["categories", "products", "transactions", "users"]:
        print(f"{c:13s} {db[c].count_documents({}):>9,} docs")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data")
    ap.add_argument("--uri", default=None)
    a = ap.parse_args()
    main(a.data, a.uri)
