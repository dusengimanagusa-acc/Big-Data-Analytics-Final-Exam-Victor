"""
PART 2.1a - Clean & normalise raw JSON with Spark; write analysis-ready Parquet to data/clean/.

Checks / fixes applied (counts are printed as a data-quality report):
  * parse ISO strings -> TimestampType; derive date parts
  * dedupe on primary keys
  * standardise text (state upper-case, trim, lower-case categorical fields)
  * null / invalid handling: negative durations, non-positive prices/quantities, missing user ids
  * recompute transaction totals and flag mismatches (subtotal-discount != total, items sum != subtotal)
  * referential integrity: txn.user_id in users, txn.session_id in sessions, item.product_id in products
  * temporal sanity: transactions dated before the user's registration
  * reconciliation: sessions marked 'converted' that have NO matching transaction
Usage: spark-submit spark/06_clean_normalize.py --data data --out data/clean
"""
import argparse, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pyspark.sql import functions as F
from common import (CATEGORIES, PRODUCTS, SESSIONS, TRANSACTIONS, USERS, get_spark, read_json)


def main(data, out):
    spark = get_spark("clean_normalize")
    ts = lambda c: F.to_timestamp(F.col(c))
    report = {}

    # ---------------- users ----------------
    users_raw = read_json(spark, f"{data}/users.json", USERS)
    users = (users_raw.dropDuplicates(["user_id"]).filter(F.col("user_id").isNotNull())
             .select("user_id", F.upper(F.trim("geo_data.state")).alias("state"),
                     F.trim("geo_data.city").alias("city"), F.upper("geo_data.country").alias("country"),
                     ts("registration_date").alias("registration_ts"), ts("last_active").alias("last_active_ts"))
             .withColumn("registration_month", F.date_format("registration_ts", "yyyy-MM")))
    report["users raw/clean"] = (users_raw.count(), users.count())

    # ---------------- categories (explode hierarchy -> flat dimension) ----------------
    cats = read_json(spark, f"{data}/categories.json", CATEGORIES)
    subs = (cats.select(F.col("category_id"), F.col("name").alias("category_name"),
                        F.explode("subcategories").alias("s"))
            .select("category_id", "category_name", F.col("s.subcategory_id"),
                    F.col("s.name").alias("subcategory_name"), F.col("s.profit_margin")))

    # ---------------- products ----------------
    prod_raw = read_json(spark, f"{data}/products.json", PRODUCTS)
    # generator has no subcategory_id -> assign deterministically (same rule as MongoDB loader)
    n_sub = subs.groupBy("category_id").agg(F.count("*").alias("n_sub"))
    products = (prod_raw.dropDuplicates(["product_id"])
                .filter((F.col("base_price") > 0) & F.col("category_id").isNotNull())
                .join(n_sub, "category_id", "left")
                .withColumn("sub_idx", F.split("product_id", "_")[1].cast("int") % F.col("n_sub"))
                .withColumn("subcategory_id",
                            F.format_string("sub_%s_%02d", F.split("category_id", "_")[1], F.col("sub_idx")))
                .join(subs.select("subcategory_id", "profit_margin"), "subcategory_id", "left")
                .select("product_id", "name", "category_id", "subcategory_id", "profit_margin",
                        F.col("base_price").alias("price"), "current_stock", "is_active",
                        ts("creation_date").alias("creation_ts"),
                        F.size("price_history").alias("n_price_changes")))
    report["products raw/clean"] = (prod_raw.count(), products.count())

    # ---------------- sessions ----------------
    sess_raw = read_json(spark, f"{data}/sessions_*.json", SESSIONS)
    sessions = (sess_raw.dropDuplicates(["session_id"])
                .filter(F.col("user_id").isNotNull() & (F.col("duration_seconds") > 0))
                .select("session_id", "user_id", ts("start_time").alias("start_ts"),
                        ts("end_time").alias("end_ts"), "duration_seconds",
                        F.col("geo_data.state").alias("state"),
                        F.lower("device_profile.type").alias("device_type"),
                        F.col("device_profile.os").alias("os"), F.col("device_profile.browser").alias("browser"),
                        F.lower("referrer").alias("referrer"), F.lower("conversion_status").alias("conversion_status"),
                        "viewed_products", "page_views", "cart_contents")
                .withColumn("start_date", F.to_date("start_ts"))
                .withColumn("n_page_views", F.size("page_views"))
                .withColumn("n_cart_products", F.size(F.map_keys("cart_contents"))))
    report["sessions raw/clean"] = (sess_raw.count(), sessions.count())

    # ---------------- transactions ----------------
    txn_raw = read_json(spark, f"{data}/transactions.json", TRANSACTIONS)
    txn = (txn_raw.dropDuplicates(["transaction_id"]).filter(F.col("user_id").isNotNull())
           .withColumn("txn_ts", ts("timestamp")).drop("timestamp")
           .withColumn("items_sum", F.round(F.aggregate("items", F.lit(0.0), lambda a, i: a + i.subtotal), 2))
           .withColumn("flag_items_mismatch", F.abs(F.col("items_sum") - F.col("subtotal")) > 0.02)
           .withColumn("flag_total_mismatch", F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.02)
           .withColumn("is_orphan", F.col("session_id").isNull())          # not linked to a session
           .withColumn("txn_date", F.to_date("txn_ts"))
           .withColumn("txn_month", F.date_format("txn_ts", "yyyy-MM")))
    report["transactions raw/clean"] = (txn_raw.count(), txn.count())
    report["txn items mismatch"] = txn.filter("flag_items_mismatch").count()
    report["txn total mismatch"] = txn.filter("flag_total_mismatch").count()
    report["txn not linked to a session"] = txn.filter("is_orphan").count()
    report["txn with unknown user"] = txn.join(users, "user_id", "left_anti").count()
    report["txn session_id missing in sessions"] = (txn.filter("NOT is_orphan")
        .join(sessions.select("session_id"), "session_id", "left_anti").count())
    report["txn items with unknown product"] = (txn.select(F.explode("items").alias("i"))
        .join(products, F.col("i.product_id") == products.product_id, "left_anti").count())
    report["txn dated BEFORE the user's registration"] = (txn.join(users.select("user_id", "registration_ts"), "user_id")
        .filter(F.col("txn_ts") < F.col("registration_ts")).count())
    conv = sessions.filter("conversion_status = 'converted'")
    report["sessions 'converted'"] = conv.count()
    report["...converted but NO transaction (generator txn cap)"] = (conv.join(
        txn.select("session_id"), "session_id", "left_anti").count())

    # ---------------- write Parquet (columnar, partitioned) ----------------
    users.write.mode("overwrite").parquet(f"{out}/users")
    subs.write.mode("overwrite").parquet(f"{out}/subcategories")
    products.write.mode("overwrite").parquet(f"{out}/products")
    # partition by date: later date-filtered queries prune files
    sessions.write.mode("overwrite").partitionBy("start_date").parquet(f"{out}/sessions")
    txn.write.mode("overwrite").partitionBy("txn_month").parquet(f"{out}/transactions")

    print("\n===== DATA QUALITY REPORT =====")
    for k, v in report.items():
        print(f"{k:55s} {v}")
    os.makedirs("outputs", exist_ok=True)
    with open("outputs/data_quality_report.txt", "w") as f:
        for k, v in report.items():
            f.write(f"{k}: {v}\n")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data"); ap.add_argument("--out", default="data/clean")
    a = ap.parse_args()
    main(a.data, a.out)
