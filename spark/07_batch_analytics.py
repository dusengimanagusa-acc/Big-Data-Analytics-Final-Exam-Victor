"""
PART 2.1b - Two Spark batch analyses on the cleaned Parquet:
  (1) Product recommendation indicators
        a. "bought together" from transactions   (support, confidence, lift)
        b. "viewed together" from sessions        (co-view counts)
  (2) Cohort analysis: users grouped by REGISTRATION month, spend per calendar month of purchase

Optimisations used: Parquet + partition pruning, column pruning, broadcast join for the small
product dimension, dedupe of pairs with a < b condition (halves the self-join), caching of reused DFs.
Usage: spark-submit spark/07_batch_analytics.py --clean data/clean --out outputs
"""
import argparse, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from pyspark.sql import functions as F
from common import get_spark


def bought_together(txn, products, min_pair=3):
    # one row per (transaction, distinct product)
    basket = txn.select("transaction_id", F.explode("items.product_id").alias("product_id")).distinct().cache()
    n_baskets = basket.select("transaction_id").distinct().count()
    item_cnt = basket.groupBy("product_id").agg(F.count("*").alias("item_baskets"))
    a, b = basket.alias("a"), basket.alias("b")
    pairs = (a.join(b, (F.col("a.transaction_id") == F.col("b.transaction_id")) &
                       (F.col("a.product_id") < F.col("b.product_id")))      # each unordered pair once
             .groupBy(F.col("a.product_id").alias("p1"), F.col("b.product_id").alias("p2"))
             .agg(F.count("*").alias("pair_baskets")).filter(F.col("pair_baskets") >= min_pair))
    ic1, ic2 = item_cnt.alias("i1"), item_cnt.alias("i2")
    nm = products.select("product_id", "name")
    res = (pairs.join(ic1, F.col("p1") == F.col("i1.product_id")).join(ic2, F.col("p2") == F.col("i2.product_id"))
           .withColumn("support", F.col("pair_baskets") / n_baskets)
           .withColumn("conf_p1_to_p2", F.col("pair_baskets") / F.col("i1.item_baskets"))
           .withColumn("conf_p2_to_p1", F.col("pair_baskets") / F.col("i2.item_baskets"))
           .withColumn("lift", F.col("support") / ((F.col("i1.item_baskets") / n_baskets) *
                                                  (F.col("i2.item_baskets") / n_baskets)))
           .join(F.broadcast(nm.withColumnRenamed("product_id", "p1").withColumnRenamed("name", "name1")), "p1")
           .join(F.broadcast(nm.withColumnRenamed("product_id", "p2").withColumnRenamed("name", "name2")), "p2")
           .select("p1", "name1", "p2", "name2", "pair_baskets", "support", "conf_p1_to_p2", "conf_p2_to_p1", "lift")
           .orderBy(F.desc("pair_baskets"), F.desc("lift")))
    return res


def viewed_together(sessions, min_pair=5):
    v = sessions.filter(F.size("viewed_products") >= 2).select("session_id", "viewed_products")
    exploded = v.select("session_id", F.explode("viewed_products").alias("product_id")).distinct()
    a, b = exploded.alias("a"), exploded.alias("b")
    return (a.join(b, (F.col("a.session_id") == F.col("b.session_id")) & (F.col("a.product_id") < F.col("b.product_id")))
            .groupBy(F.col("a.product_id").alias("p1"), F.col("b.product_id").alias("p2"))
            .agg(F.count("*").alias("co_views")).filter(F.col("co_views") >= min_pair).orderBy(F.desc("co_views")))


def cohorts(txn, users):
    valid = txn.filter(F.col("status").isin("completed", "shipped", "delivered"))
    cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_users"))
    spend = (valid.join(users.select("user_id", "registration_month", "registration_ts"), "user_id")
             .withColumn("months_since_reg",
                         F.floor(F.months_between("txn_ts", "registration_ts")).cast("int"))
             .groupBy("registration_month", "txn_month")
             .agg(F.sum("total").alias("revenue"), F.countDistinct("user_id").alias("active_buyers"),
                  F.count("*").alias("orders"), F.avg("months_since_reg").alias("avg_months_since_reg")))
    return (spend.join(cohort_size, "registration_month")
            .withColumn("rev_per_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
            .withColumn("buyer_rate", F.round(F.col("active_buyers") / F.col("cohort_users"), 3))
            .orderBy("registration_month", "txn_month"))


def main(clean, out):
    spark = get_spark("batch_analytics")
    users = spark.read.parquet(f"{clean}/users")
    products = spark.read.parquet(f"{clean}/products")
    txn = spark.read.parquet(f"{clean}/transactions")
    sessions = spark.read.parquet(f"{clean}/sessions").select("session_id", "viewed_products")

    bt = bought_together(txn, products)
    bt.limit(50).toPandas().to_csv(f"{out}/spark_bought_together.csv", index=False)
    print("\n== Frequently bought together (top 10) =="); bt.show(10, truncate=40)

    vt = viewed_together(sessions)
    vt.limit(50).toPandas().to_csv(f"{out}/spark_viewed_together.csv", index=False)
    print("== Frequently viewed together (top 10) =="); vt.show(10)

    co = cohorts(txn, users)
    co.toPandas().to_csv(f"{out}/spark_cohorts.csv", index=False)
    print("== Cohort table =="); co.show(20)
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", default="data/clean"); ap.add_argument("--out", default="outputs")
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True); main(a.clean, a.out)
