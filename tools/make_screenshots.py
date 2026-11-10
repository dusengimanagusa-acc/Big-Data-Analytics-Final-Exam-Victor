"""
Creates terminal-style screenshots for the report from the logs in outputs/logs/ (written by run_all.sh).
  python tools/make_screenshots.py
Output: outputs/screenshots/*.png  (the report builder embeds them automatically)
"""
import json, os, sys
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from render_terminal import render

L, OUT = "outputs/logs", "outputs/screenshots"
os.makedirs(OUT, exist_ok=True)
info = json.load(open("outputs/run_info.json")) if os.path.exists("outputs/run_info.json") else {"mode": "live"}
offline = info.get("mode") == "offline"
NOTE_DB = "offline test harness: in-memory stand-in" if offline else ""
CMD_MONGO = "python mongodb/test_offline.py" if offline else "python mongodb/01_load_mongo.py && python mongodb/02_aggregations.py"
CMD_HBASE = "python hbase/test_offline.py" if offline else "python hbase/02_load_hbase.py && python hbase/03_queries.py user user_000042"
CMD_INTEG = "python integration/test_live_mock.py" if offline else "python integration/09_integrated_clv_funnel.py --source live"


def read(name):
    p = f"{L}/{name}"
    return open(p, errors="replace").read().splitlines() if os.path.exists(p) else []


def find_log(prefix):
    for f in sorted(os.listdir(L)):
        if f.startswith(prefix):
            return read(f)
    return []


def section(lines, marker, n, skip=0):
    for i, l in enumerate(lines):
        if marker in l:
            return lines[i + skip:i + n]
    return []


def shot(fname, lines, cmd, title, note="", width=118):
    if not lines:
        print("  (skipped, no log lines for", fname, ")"); return
    render(lines, f"{OUT}/{fname}.png", cmd, title, note, width); print("wrote", fname)


# 0 data generation
g = read("00_generate_data.log")
shot("s00_generate", g[-9:], "python dataset_generator.py --scale %s" % info.get("scale", ""), "Step 0 - dataset generation")

# 1 MongoDB load + aggregations
m = find_log("01_mongo")
shot("s01_mongo_load_agg", m[:15], CMD_MONGO,
     "Step 1 - MongoDB: load + revenue by category/month", NOTE_DB)
shot("s02_mongo_segments", section(m, "mongo_user_segments", 8) + [""] + section(m, "mongo_top_products", 7),
     CMD_MONGO + "   # pipelines B and C", "Step 1 - MongoDB: segmentation ($bucket) and top products", NOTE_DB)

# 2 HBase
h = find_log("04_hbase")
shot("s03_hbase", [x for x in h if len(x) < 400][:9], CMD_HBASE,
     "Step 2 - HBase: load subset, newest-first user scan, range scan", NOTE_DB, width=92)

# 3 Spark cleaning / DQ
shot("s04_spark_dq", read("06_spark_clean.log")[-14:], "python spark/06_clean_normalize.py", "Step 3 - Spark: cleaning + data-quality report")

# 4 Spark batch: bought together (selected columns from CSV)
bt = pd.read_csv("outputs/spark_bought_together.csv").head(8)
bt["name1"] = bt.name1.str.slice(0, 26); bt["name2"] = bt.name2.str.slice(0, 26)
txt = bt[["p1", "name1", "p2", "name2", "pair_baskets", "lift"]].round(2).to_string(index=False).splitlines()
shot("s05_spark_bought_together", txt, "python spark/07_batch_analytics.py   # frequently bought together", "Step 4 - Spark: recommendation indicators (support/lift)")
co = pd.read_csv("outputs/spark_cohorts.csv").head(8)[["registration_month", "txn_month", "revenue", "active_buyers", "cohort_users", "rev_per_user", "buyer_rate"]].round(2)
shot("s06_spark_cohort", co.to_string(index=False).splitlines(), "python spark/07_batch_analytics.py   # cohort analysis", "Step 4 - Spark: cohort table (first rows)")

# 5 Spark SQL
q = read("08_spark_sql.log")
shot("s07_sparksql_q1_q2", section(q, "q1_top3", 12) + [""] + section(q, "q2_daily", 12), "python spark/08_spark_sql.py   # queries 1 and 2", "Step 5 - Spark SQL: window functions", width=100)
shot("s08_sparksql_q3_q5", section(q, "q3_funnel", 9) + [""] + section(q, "q5_mongo", 14), "python spark/08_spark_sql.py   # queries 3 and 5", "Step 5 - Spark SQL: funnel + notional Mongo x HBase join", width=125)

# 6 Integration
i = read("09_integration.log")
shot("s09_clv", section(i, "QUERY 1", 14), CMD_INTEG, "Step 6 - Integrated query 1: CLV tiers (Mongo + HBase + Spark)", NOTE_DB, width=125)
shot("s10_funnel", section(i, "QUERY 2", 6), CMD_INTEG, "Step 6 - Integrated query 2: funnel (HBase sessions x Mongo orders)", NOTE_DB, width=150)

# 7 Independent verification
v = read("11_verify_results.log")
shot("s11_verify", v[:], "python tools/verify_results.py", "Step 7 - Independent verification against the raw JSON", width=110)
