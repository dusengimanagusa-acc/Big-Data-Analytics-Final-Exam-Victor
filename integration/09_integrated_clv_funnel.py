"""
PART 3 - Integrated analytics across MongoDB + HBase + Spark.

QUERY 1  Customer Lifetime Value (CLV) & engagement
  Business question : Which customers are worth the most over the next 12 months, and does browsing
                      engagement (sessions, time on site) predict value?
  Sources           : MongoDB  -> user profiles + per-user revenue/profit rollup (aggregation pushed
                                  down INTO MongoDB so only one small row per user leaves the DB)
                      HBase    -> per-session engagement events (sessions / duration / conversion)
                      Spark    -> joins the two, computes CLV, tiers (NTILE), correlation
  CLV formula       : monthly_profit = (gross item profit - discounts) / months_observed
                      CLV_12m = monthly_profit * sum_{k=0..11} r^k      (r = assumed monthly retention)

QUERY 2  Funnel conversion analysis
  Business question : Where do shoppers drop out between first product view and a confirmed purchase,
                      and does the funnel differ by device / referrer?
  Sources           : HBase sessions (view -> cart -> checkout, device, referrer)
                      MongoDB transactions (the system of record for 'purchase confirmed')
                      Spark joins on session_id; the gap between HBase 'converted' flag and a real
                      Mongo transaction is reported (mostly an artefact of the generator's transaction cap, see DQ report).

--source live  : read from running MongoDB + HBase (pymongo / happybase)   [real integration]
--source files : read the cleaned Parquet from Spark job 06 (same logic, no DB servers needed)

At production scale replace the happybase scan with the Spark-HBase connector and the pymongo calls
with the MongoDB Spark Connector so the reads are parallel across Spark executors.
"""
import argparse, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mongodb"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "hbase"))
from pyspark.sql import functions as F
from common import get_spark

VALID = ["completed", "shipped", "delivered"]


# ------------------------------------------------------------------ LIVE SOURCES
def load_live(spark, mongo_uri):
    from db import get_db
    from hb_common import get_connection
    db = get_db(mongo_uri)

    # --- MongoDB: users (profile) -------------------------------------------------
    users = [{"user_id": u["_id"], "state": u["geo_data"]["state"],
              "registration_ts": u["registration_date"]} for u in db.users.find({}, {"geo_data.state": 1, "registration_date": 1})]
    users_df = spark.createDataFrame(users, "user_id string, state string, registration_ts timestamp")

    # --- MongoDB: per-user revenue/profit rollup computed INSIDE MongoDB ----------------
    m = {"$match": {"status": {"$in": VALID}}}
    a1 = list(db.transactions.aggregate([m, {"$group": {"_id": "$user_id", "orders": {"$sum": 1},
            "revenue": {"$sum": "$total"}, "discount": {"$sum": "$discount"}}}], allowDiskUse=True))
    a2 = list(db.transactions.aggregate([m, {"$unwind": "$items"}, {"$group": {"_id": "$user_id",
            "gross_profit": {"$sum": {"$multiply": ["$items.subtotal", "$items.profit_margin"]}}}}], allowDiskUse=True))
    gp = {r["_id"]: r["gross_profit"] for r in a2}
    rows = [{"user_id": r["_id"], "orders": int(r["orders"]), "revenue": float(r["revenue"]),
             "discount": float(r["discount"]), "gross_profit": float(gp.get(r["_id"], 0.0))} for r in a1]
    txn_user = spark.createDataFrame(rows, "user_id string, orders int, revenue double, discount double, gross_profit double")

    # --- MongoDB: session ids that really have a transaction (for funnel reconciliation) -------
    # $group + cursor instead of distinct(): distinct() returns ONE document (16 MB limit) - not safe at full scale
    sids = [{"session_id": r["_id"]} for r in db.transactions.aggregate(
        [{"$match": {"session_id": {"$ne": None}}}, {"$group": {"_id": "$session_id"}}], allowDiskUse=True)]
    txn_sessions = spark.createDataFrame(sids, "session_id string")

    # --- HBase: scan sessions, derive per-session stage flags ------------------------------
    conn = get_connection()
    tbl = conn.table("user_sessions")
    srows = []
    for key, row in tbl.scan(columns=[b"meta", b"pv", b"cart"]):
        g = lambda c: row.get(c, b"").decode()
        pages = [v.decode().split("|")[1] for c, v in row.items() if c.startswith(b"pv:")]
        srows.append({"user_id": key.decode().split("#")[0], "session_id": g(b"meta:session_id"),
                      "device_type": g(b"meta:device_type"), "referrer": g(b"meta:referrer"),
                      "conversion_status": g(b"meta:conversion_status"),
                      "duration_seconds": int(g(b"meta:duration_seconds") or 0),
                      "viewed_product": "product_detail" in pages,
                      "added_to_cart": any(c.startswith(b"cart:") for c in row),
                      "reached_checkout": "checkout" in pages})
    sess = spark.createDataFrame(srows, "user_id string, session_id string, device_type string, referrer string, "
                                        "conversion_status string, duration_seconds int, viewed_product boolean, "
                                        "added_to_cart boolean, reached_checkout boolean")
    return users_df, txn_user, txn_sessions, sess


# ------------------------------------------------------------------ FILE SOURCES (same outputs)
def load_files(spark, clean):
    users_df = spark.read.parquet(f"{clean}/users").select("user_id", "state", F.col("registration_ts"))
    products = spark.read.parquet(f"{clean}/products").select("product_id", "profit_margin")
    txn = spark.read.parquet(f"{clean}/transactions").filter(F.col("status").isin(VALID))
    items = (txn.select("transaction_id", "user_id", F.explode("items").alias("i"))
             .join(F.broadcast(products), F.col("i.product_id") == products.product_id)
             .groupBy("user_id").agg(F.sum(F.col("i.subtotal") * F.col("profit_margin")).alias("gross_profit")))
    txn_user = (txn.groupBy("user_id").agg(F.count("*").cast("int").alias("orders"), F.sum("total").alias("revenue"),
                                           F.sum("discount").alias("discount"))
                .join(items, "user_id", "left").fillna(0.0, ["gross_profit"]))
    txn_sessions = spark.read.parquet(f"{clean}/transactions").filter("session_id IS NOT NULL").select("session_id").distinct()
    s = spark.read.parquet(f"{clean}/sessions")
    sess = s.select("user_id", "session_id", "device_type", "referrer", "conversion_status", "duration_seconds",
                    F.exists("page_views", lambda p: p.page_type == "product_detail").alias("viewed_product"),
                    (F.col("n_cart_products") > 0).alias("added_to_cart"),
                    F.exists("page_views", lambda p: p.page_type == "checkout").alias("reached_checkout"))
    return users_df, txn_user, txn_sessions, sess


# ------------------------------------------------------------------ ANALYSES
def clv_analysis(users_df, txn_user, sess, months_observed=3.0, retention=0.90):
    annuity = sum(retention ** k for k in range(12))             # sum of r^k, k=0..11
    engagement = (sess.groupBy("user_id").agg(
        F.count("*").alias("sessions"), F.round(F.avg("duration_seconds"), 1).alias("avg_session_sec"),
        F.round(F.avg((F.col("conversion_status") == "converted").cast("int")), 3).alias("session_conv_rate")))
    df = (users_df.join(txn_user, "user_id", "left").join(engagement, "user_id", "left")
          .fillna(0, ["orders", "revenue", "discount", "gross_profit", "sessions", "avg_session_sec", "session_conv_rate"])
          .withColumn("net_profit", F.col("gross_profit") - F.col("discount"))
          .withColumn("monthly_profit", F.col("net_profit") / months_observed)
          .withColumn("clv_12m", F.round(F.col("monthly_profit") * annuity, 2))
          .withColumn("tier", F.expr("CASE ntile(4) OVER (ORDER BY clv_12m DESC) WHEN 1 THEN 'Platinum' "
                                     "WHEN 2 THEN 'Gold' WHEN 3 THEN 'Silver' ELSE 'Bronze' END")))
    df.cache()
    tiers = (df.groupBy("tier").agg(F.count("*").alias("users"), F.round(F.avg("clv_12m"), 2).alias("avg_clv_12m"),
                                    F.round(F.sum("clv_12m"), 0).alias("total_clv_12m"), F.round(F.avg("orders"), 1).alias("avg_orders"),
                                    F.round(F.avg("sessions"), 1).alias("avg_sessions"), F.round(F.avg("avg_session_sec"), 0).alias("avg_session_sec"))
             .orderBy(F.desc("avg_clv_12m")))
    corr = {"clv~sessions": df.stat.corr("clv_12m", "sessions"), "clv~avg_session_sec": df.stat.corr("clv_12m", "avg_session_sec"),
            "clv~session_conv_rate": df.stat.corr("clv_12m", "session_conv_rate")}
    return df, tiers, corr


def funnel_analysis(sess, txn_sessions):
    """STRICTLY NESTED funnel: a session counts at a stage only if it also passed every earlier stage.
    (The generator's page logs are noisy - e.g. some sessions with an empty cart still contain a 'checkout'
    page - so counting each stage independently would overstate the later stages.)"""
    j = sess.join(txn_sessions.withColumn("has_txn", F.lit(True)), "session_id", "left").fillna(False, ["has_txn"])
    j = (j.withColumn("st_view", F.col("viewed_product"))
          .withColumn("st_cart", F.col("st_view") & F.col("added_to_cart"))
          .withColumn("st_checkout", F.col("st_cart") & F.col("reached_checkout"))
          .withColumn("st_converted", F.col("st_checkout") & (F.col("conversion_status") == "converted"))
          .withColumn("st_order", F.col("st_converted") & F.col("has_txn")))
    agg = [F.count("*").alias("sessions"),
           F.sum(F.col("st_view").cast("int")).alias("viewed_product"),
           F.sum(F.col("st_cart").cast("int")).alias("added_to_cart"),
           F.sum(F.col("st_checkout").cast("int")).alias("reached_checkout"),
           F.sum(F.col("st_converted").cast("int")).alias("converted_flag_hbase"),
           F.sum(F.col("st_order").cast("int")).alias("purchase_in_mongo")]
    overall = j.agg(*agg)
    by_dev = j.groupBy("device_type").agg(*agg).withColumn("conv_pct", F.round(100 * F.col("purchase_in_mongo") / F.col("sessions"), 2))
    by_ref = j.groupBy("referrer").agg(*agg).withColumn("conv_pct", F.round(100 * F.col("purchase_in_mongo") / F.col("sessions"), 2))
    return overall, by_dev, by_ref


def main(source, clean, out, mongo_uri=None, spark=None):
    spark = spark or get_spark("integration")
    users_df, txn_user, txn_sessions, sess = (load_live(spark, mongo_uri) if source == "live" else load_files(spark, clean))
    os.makedirs(out, exist_ok=True)

    df, tiers, corr = clv_analysis(users_df, txn_user, sess)
    print("\n===== QUERY 1: CLV tiers ====="); tiers.show(truncate=False)
    print("Correlations with 12m CLV:", {k: round(v, 3) for k, v in corr.items()})
    print("Top 10 customers:"); df.orderBy(F.desc("clv_12m")).select("user_id", "state", "orders", "revenue", "sessions", "clv_12m", "tier").show(10)
    df.select("user_id", "state", "orders", "revenue", "sessions", "avg_session_sec", "session_conv_rate", "clv_12m", "tier").toPandas().to_csv(f"{out}/integrated_clv_users.csv", index=False)
    tiers.toPandas().to_csv(f"{out}/integrated_clv_tiers.csv", index=False)
    import json
    json.dump(corr, open(f"{out}/integrated_clv_corr.json", "w"))

    overall, by_dev, by_ref = funnel_analysis(sess, txn_sessions)
    print("===== QUERY 2: Funnel ====="); overall.show(); by_dev.show(); by_ref.show()
    overall.toPandas().to_csv(f"{out}/integrated_funnel.csv", index=False)
    by_dev.toPandas().to_csv(f"{out}/integrated_funnel_by_device.csv", index=False)
    by_ref.toPandas().to_csv(f"{out}/integrated_funnel_by_referrer.csv", index=False)
    if spark is not None and not os.getenv("KEEP_SPARK"):
        spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["live", "files"], default="files")
    ap.add_argument("--clean", default="data/clean"); ap.add_argument("--out", default="outputs")
    ap.add_argument("--mongo-uri", default=None)
    a = ap.parse_args(); main(a.source, a.clean, a.out, a.mongo_uri)
