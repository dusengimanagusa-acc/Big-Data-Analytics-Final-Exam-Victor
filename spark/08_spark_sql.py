"""
PART 2.2 - Spark SQL analytics. Each query is stored as a SQL string so it can be copied into the report.

Part A  : complex queries on the DataFrames derived from the JSON files (CTEs, window functions,
          conditional aggregation, array explode, ROLLUP).
Part B  : 'notional' MongoDB / HBase queries. We register temp views named mongo_* and hbase_* that
          stand in for data loaded through the connectors:
            spark.read.format("mongodb").option("connection.uri", "mongodb://host/ecommerce.users").load()
            (spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0)
            HBase: Spark-HBase connector (org.apache.hbase.connectors.spark:hbase-spark) or happybase scan
          The SQL logic is identical whichever way the data arrives.
Usage: spark-submit spark/08_spark_sql.py --clean data/clean --out outputs
"""
import argparse, os, sys
sys.path.insert(0, os.path.dirname(__file__))
from common import get_spark

QUERIES = {}

# 1. Top 3 products per category by revenue (window function + CTE + explode)
QUERIES["q1_top3_products_per_category"] = """
WITH lines AS (
  SELECT t.transaction_id, i.product_id, i.quantity, i.subtotal
  FROM transactions t LATERAL VIEW explode(t.items) x AS i
  WHERE t.status IN ('completed','shipped','delivered')
), prod_rev AS (
  SELECT p.category_id, p.product_id, p.name, SUM(l.subtotal) AS revenue, SUM(l.quantity) AS units
  FROM lines l JOIN products p ON p.product_id = l.product_id
  GROUP BY p.category_id, p.product_id, p.name
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rk
  FROM prod_rev
)
SELECT category_id, rk, product_id, name, ROUND(revenue,2) AS revenue, units
FROM ranked WHERE rk <= 3 ORDER BY category_id, rk
"""

# 2. Daily revenue with 7-day moving average and day-over-day growth (window frames)
QUERIES["q2_daily_revenue_moving_avg"] = """
WITH daily AS (
  SELECT txn_date, SUM(total) AS revenue, COUNT(*) AS orders
  FROM transactions WHERE status IN ('completed','shipped','delivered')
  GROUP BY txn_date
)
SELECT txn_date, ROUND(revenue,2) AS revenue, orders,
       ROUND(AVG(revenue) OVER (ORDER BY txn_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),2) AS ma7,
       ROUND(100*(revenue - LAG(revenue) OVER (ORDER BY txn_date)) / LAG(revenue) OVER (ORDER BY txn_date),1) AS dod_pct
FROM daily ORDER BY txn_date
"""

# 3. Session funnel by device (conditional aggregation over the nested page_views array)
QUERIES["q3_funnel_by_device"] = """
WITH f AS (   -- one row per session with its stage flags
  SELECT device_type, conversion_status,
         exists(page_views, p -> p.page_type = 'product_detail') AS v,
         n_cart_products > 0                                      AS c,
         exists(page_views, p -> p.page_type = 'checkout')        AS k
  FROM sessions
)
-- STRICTLY NESTED funnel: a stage counts only sessions that also passed all earlier stages
SELECT device_type,
       COUNT(*)                                                                      AS sessions,
       SUM(CASE WHEN v THEN 1 ELSE 0 END)                                            AS viewed_product,
       SUM(CASE WHEN v AND c THEN 1 ELSE 0 END)                                      AS added_to_cart,
       SUM(CASE WHEN v AND c AND k THEN 1 ELSE 0 END)                                AS reached_checkout,
       SUM(CASE WHEN v AND c AND k AND conversion_status = 'converted' THEN 1 ELSE 0 END) AS converted,
       ROUND(100*SUM(CASE WHEN v AND c AND k AND conversion_status = 'converted' THEN 1 ELSE 0 END)/COUNT(*),2) AS conv_pct
FROM f GROUP BY device_type ORDER BY sessions DESC
"""

# 4. Revenue by category -> subcategory with subtotals (ROLLUP)
QUERIES["q4_rollup_category_subcategory"] = """
WITH lines AS (
  SELECT i.product_id, i.subtotal
  FROM transactions t LATERAL VIEW explode(t.items) x AS i
  WHERE t.status IN ('completed','shipped','delivered')
)
SELECT p.category_id, p.subcategory_id, ROUND(SUM(l.subtotal),2) AS revenue,
       ROUND(SUM(l.subtotal*p.profit_margin),2) AS est_profit
FROM lines l JOIN products p ON p.product_id = l.product_id
GROUP BY ROLLUP(p.category_id, p.subcategory_id)
ORDER BY p.category_id NULLS FIRST, p.subcategory_id NULLS FIRST
"""

# ---- Part B: notional MongoDB/HBase joins --------------------------------------------------
# mongo_users = users collection (with embedded purchase_summary), hbase_sessions = user_sessions table
QUERIES["q5_mongo_users_x_hbase_engagement"] = """
SELECT u.state,
       COUNT(DISTINCT u.user_id)                      AS users,
       ROUND(AVG(m.total_spent),2)                    AS avg_spent_mongo,
       ROUND(AVG(h.sessions),1)                       AS avg_sessions_hbase,
       ROUND(AVG(h.avg_dur),1)                        AS avg_session_sec_hbase
FROM mongo_users u
JOIN mongo_purchase_summary m ON m.user_id = u.user_id
JOIN (SELECT user_id, COUNT(*) AS sessions, AVG(duration_seconds) AS avg_dur
      FROM hbase_sessions GROUP BY user_id) h ON h.user_id = u.user_id
GROUP BY u.state HAVING COUNT(DISTINCT u.user_id) >= 5
ORDER BY avg_spent_mongo DESC LIMIT 15
"""

QUERIES["q6_referrer_quality"] = """
SELECT s.referrer, COUNT(*) AS sessions,
       ROUND(100*AVG(CASE WHEN s.conversion_status='converted' THEN 1 ELSE 0 END),2) AS conv_pct,
       ROUND(AVG(s.duration_seconds),1) AS avg_duration
FROM hbase_sessions s GROUP BY s.referrer ORDER BY conv_pct DESC
"""


def main(clean, out):
    spark = get_spark("spark_sql")
    for name in ["users", "products", "transactions", "sessions"]:
        spark.read.parquet(f"{clean}/{name}").createOrReplaceTempView(name)
    # stand-ins for connector-loaded data
    spark.table("users").createOrReplaceTempView("mongo_users")
    spark.sql("""SELECT user_id, ROUND(SUM(total),2) AS total_spent, COUNT(*) AS total_orders
                 FROM transactions WHERE status IN ('completed','shipped','delivered')
                 GROUP BY user_id""").createOrReplaceTempView("mongo_purchase_summary")
    spark.table("sessions").createOrReplaceTempView("hbase_sessions")

    os.makedirs(out, exist_ok=True)
    for name, q in QUERIES.items():
        df = spark.sql(q)
        print(f"\n=== {name} ==="); df.show(8, truncate=30)
        df.toPandas().to_csv(f"{out}/sql_{name}.csv", index=False)
    with open(f"{out}/spark_sql_queries.sql", "w") as f:
        for name, q in QUERIES.items():
            f.write(f"-- {name}\n{q.strip()};\n\n")
    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", default="data/clean"); ap.add_argument("--out", default="outputs")
    a = ap.parse_args(); main(a.clean, a.out)
