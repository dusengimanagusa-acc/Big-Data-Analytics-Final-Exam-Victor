"""
PART 1.1 - Four non-trivial MongoDB aggregation pipelines.
Results are printed and exported to outputs/*.csv for the visualisation step.

  A. Revenue & estimated profit by category and month   ($unwind, $group, $lookup, $dateToString)
  B. Top products by revenue/units with category names  ($unwind, $group, $lookup, $sort, $limit)
  C. User segmentation by purchase frequency            ($bucket, boundaries = data quintiles)
  D. Revenue by US state & payment method               ($lookup users, $group)
"""
import argparse, os, sys
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from db import get_db

# Only completed/shipped/delivered orders count as revenue
VALID = {"status": {"$in": ["completed", "shipped", "delivered"]}}

PIPE_A = [
    {"$match": VALID},
    {"$unwind": "$items"},
    {"$group": {
        "_id": {"category_id": "$items.category_id",
                "month": {"$dateToString": {"format": "%Y-%m", "date": "$ts"}}},
        "revenue": {"$sum": "$items.subtotal"},
        "est_profit": {"$sum": {"$multiply": ["$items.subtotal", "$items.profit_margin"]}},
        "units": {"$sum": "$items.quantity"}}},
    {"$lookup": {"from": "categories", "localField": "_id.category_id",
                 "foreignField": "_id", "as": "cat"}},
    {"$project": {"_id": 0, "category_id": "$_id.category_id", "month": "$_id.month",
                  "category": {"$arrayElemAt": ["$cat.name", 0]},
                  "revenue": {"$round": ["$revenue", 2]},
                  "est_profit": {"$round": ["$est_profit", 2]}, "units": 1}},
    {"$sort": {"month": 1, "revenue": -1}},
]

PIPE_B = [
    {"$match": VALID},
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id",
                "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"},
                "orders": {"$sum": 1}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 20},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "p"}},
    {"$project": {"_id": 0, "product_id": "$_id", "units_sold": 1, "orders": 1,
                  "revenue": {"$round": ["$revenue", 2]},
                  "name": {"$arrayElemAt": ["$p.name", 0]},
                  "category": {"$arrayElemAt": ["$p.category.name", 0]}}},
]

def build_pipe_c(db):
    """Segment users by purchase frequency. Bucket boundaries are QUINTILES of the real order-count
    distribution, so the segmentation stays meaningful at any dataset scale (hard-coded boundaries
    would put everybody in one bucket on the full 500k-transaction dataset)."""
    counts = sorted(u["purchase_summary"]["total_orders"] for u in db.users.find({}, {"purchase_summary.total_orders": 1}))
    n = len(counts)
    cuts = sorted({0} | {counts[min(n - 1, int(n * q))] for q in (0.2, 0.4, 0.6, 0.8)})
    boundaries = cuts + [counts[-1] + 1]
    return [
        {"$bucket": {
            "groupBy": "$purchase_summary.total_orders",
            "boundaries": boundaries,
            "default": "other",
            "output": {"users": {"$sum": 1},
                       "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                       "total_spent": {"$sum": "$purchase_summary.total_spent"},
                       "avg_sessions": {"$avg": "$engagement_summary.session_count"}}}},
        {"$project": {"segment_min_orders": "$_id", "_id": 0, "users": 1,
                      "avg_spent": {"$round": ["$avg_spent", 2]},
                      "total_spent": {"$round": ["$total_spent", 2]},
                      "avg_sessions": {"$round": ["$avg_sessions", 1]}}},
    ]

PIPE_D = [
    {"$match": VALID},
    {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "_id", "as": "u"}},
    {"$unwind": "$u"},
    {"$group": {"_id": {"state": "$u.geo_data.state", "payment": "$payment_method"},
                "revenue": {"$sum": "$total"}, "orders": {"$sum": 1}}},
    {"$sort": {"revenue": -1}},
    {"$limit": 25},
    {"$project": {"_id": 0, "state": "$_id.state", "payment_method": "$_id.payment",
                  "revenue": {"$round": ["$revenue", 2]}, "orders": 1}},
]


def run(db, coll, pipe, name, out):
    df = pd.DataFrame(list(db[coll].aggregate(pipe, allowDiskUse=True)))
    os.makedirs(out, exist_ok=True)
    df.to_csv(f"{out}/{name}.csv", index=False)
    print(f"\n=== {name} ({len(df)} rows) ===")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=None)
    ap.add_argument("--out", default="outputs")
    a = ap.parse_args()
    db = get_db(a.uri)
    run(db, "transactions", PIPE_A, "mongo_revenue_by_category_month", a.out)
    run(db, "transactions", PIPE_B, "mongo_top_products", a.out)
    run(db, "users", build_pipe_c(db), "mongo_user_segments", a.out)
    run(db, "transactions", PIPE_D, "mongo_revenue_state_payment", a.out)
