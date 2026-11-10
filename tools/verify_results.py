"""
Independent verification of the pipeline results.

Re-computes the key numbers from the RAW JSON files in plain Python (no MongoDB, HBase or Spark involved) and compares
them with what the MongoDB pipelines, Spark jobs and the integration job wrote to outputs/.

  python tools/verify_results.py [--data data] [--out outputs]

Prints PASS / FAIL / SKIP per check. Exit code 0 = no FAIL, 1 = at least one FAIL.
(A SKIP means a result file or input was not found, e.g. you did not run that step.)
"""
import argparse, collections, glob, json, os, random, sys
import pandas as pd

VALID = {"completed", "shipped", "delivered"}      # statuses counted as revenue (same as every pipeline)
RETENTION, MONTHS_OBSERVED = 0.90, 3.0             # CLV assumptions (same as integration/09_integrated_clv_funnel.py)
results = []


def record(status, name, detail=""):
    results.append(status)
    print(f"[{status}] {name}" + (f"   {detail}" if detail else ""))


def close(name, truth, got, tol):
    ok = abs(float(truth) - float(got)) <= tol
    record("PASS" if ok else "FAIL", name, f"truth={float(truth):,.2f}  system={float(got):,.2f}")


def load(path):
    with open(path) as f:
        return json.load(f)


def read_csv(out, name):
    p = os.path.join(out, name)
    return pd.read_csv(p) if os.path.exists(p) else None


def main(data, out):
    tx = load(f"{data}/transactions.json")
    valid = [t for t in tx if t["status"] in VALID]
    items_rev = sum(i["subtotal"] for t in valid for i in t["items"])
    total_rev = sum(t["total"] for t in valid)
    users = load(f"{data}/users.json")
    prods = {p["product_id"]: p for p in load(f"{data}/products.json")}
    cats = {c["category_id"]: c for c in load(f"{data}/categories.json")}

    def margin(pid):   # same deterministic subcategory rule as the loaders
        c = cats[prods[pid]["category_id"]]
        return c["subcategories"][int(pid.split("_")[1]) % len(c["subcategories"])]["profit_margin"]

    # 1. MongoDB pipeline A / Spark SQL q4 / q2  (revenue)
    a = read_csv(out, "mongo_revenue_by_category_month.csv")
    if a is not None:   # pipeline rounds each row to cents -> allow a little rounding drift
        close("MongoDB pipeline A: total item revenue", items_rev, a.revenue.sum(), tol=0.01 * len(a) + 0.5)
    else:
        record("SKIP", "MongoDB pipeline A (outputs/mongo_revenue_by_category_month.csv not found)")
    q4 = read_csv(out, "sql_q4_rollup_category_subcategory.csv")
    if q4 is not None:
        close("Spark SQL q4 ROLLUP grand total (item revenue)", items_rev, q4[q4.category_id.isna()].revenue.iloc[0], tol=0.02)
    else:
        record("SKIP", "Spark SQL q4")
    q2 = read_csv(out, "sql_q2_daily_revenue_moving_avg.csv")
    if q2 is not None:
        close("Spark SQL q2 daily revenue summed (order totals)", total_rev, q2.revenue.sum(), tol=0.01 * len(q2) + 0.5)
    else:
        record("SKIP", "Spark SQL q2")

    # 2. MongoDB pipeline B (top product)
    b = read_csv(out, "mongo_top_products.csv")
    if b is not None:
        rev = collections.Counter()
        for t in valid:
            for i in t["items"]:
                rev[i["product_id"]] += i["subtotal"]
        top_id, top_rev = rev.most_common(1)[0]
        record("PASS" if b.iloc[0].product_id == top_id else "FAIL", "MongoDB pipeline B: top product id",
               f"truth={top_id}  system={b.iloc[0].product_id}")
        close("MongoDB pipeline B: top product revenue", top_rev, b.iloc[0].revenue, tol=0.01)
    else:
        record("SKIP", "MongoDB pipeline B")

    # 3. MongoDB pipeline C (segments cover every user)
    c = read_csv(out, "mongo_user_segments.csv")
    if c is not None:
        close("MongoDB pipeline C: users across all segments", len(users), c.users.sum(), tol=0)
    else:
        record("SKIP", "MongoDB pipeline C")

    # 4. Strict funnel (needs the same sessions that were loaded into HBase = the first N session files)
    f = read_csv(out, "integrated_funnel.csv")
    if f is not None:
        f = f.iloc[0]
        linked = {t["session_id"] for t in tx if t["session_id"]}
        fs, n = collections.Counter(), 0
        for fp in sorted(glob.glob(f"{data}/sessions_*.json"), key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0])):
            if n >= int(f.sessions):
                break
            for s in load(fp):
                pages = {p["page_type"] for p in s["page_views"]}
                v = "product_detail" in pages
                cart = v and bool(s["cart_contents"])
                chk = cart and "checkout" in pages
                cv = chk and s["conversion_status"] == "converted"
                fs["sessions"] += 1; fs["viewed_product"] += v; fs["added_to_cart"] += cart
                fs["reached_checkout"] += chk; fs["converted_flag_hbase"] += cv
                fs["purchase_in_mongo"] += cv and s["session_id"] in linked
            n = fs["sessions"]
        if n != int(f.sessions):
            record("SKIP", "Integrated funnel", f"loaded sessions ({int(f.sessions):,}) do not align with whole session files")
        else:
            for k in ["sessions", "viewed_product", "added_to_cart", "reached_checkout", "converted_flag_hbase", "purchase_in_mongo"]:
                close(f"Integrated funnel stage '{k}'", fs[k], f[k], tol=0)
    else:
        record("SKIP", "Integrated funnel")

    # 5. CLV for 5 random customers
    clv = read_csv(out, "integrated_clv_users.csv")
    if clv is not None:
        gp, disc = collections.defaultdict(float), collections.defaultdict(float)
        for t in valid:
            gp[t["user_id"]] += sum(i["subtotal"] * margin(i["product_id"]) for i in t["items"])
            disc[t["user_id"]] += t["discount"]
        annuity = sum(RETENTION ** k for k in range(12))
        clv = clv.set_index("user_id")
        random.seed(1)
        for u in random.sample(list(clv.index), min(5, len(clv))):
            close(f"CLV (12 months) of {u}", (gp[u] - disc[u]) / MONTHS_OBSERVED * annuity, clv.loc[u, "clv_12m"], tol=0.02)
    else:
        record("SKIP", "CLV")

    # 6. Cohort table
    co = read_csv(out, "spark_cohorts.csv")
    if co is not None:
        reg = {u["user_id"]: u["registration_date"][:7] for u in users}
        truth = collections.defaultdict(float)
        for t in valid:
            truth[(reg[t["user_id"]], t["timestamp"][:7])] += t["total"]
        worst = max(abs(truth[(r.registration_month, r.txn_month)] - r.revenue) for r in co.itertuples())
        record("PASS" if worst < 0.01 else "FAIL", f"Spark cohort table ({len(co)} cells)", f"max |truth - system| = {worst:.4f}")
    else:
        record("SKIP", "Spark cohort table")

    # 7. Bought together (top pair)
    bt = read_csv(out, "spark_bought_together.csv")
    if bt is not None and len(bt):
        r = bt.iloc[0]
        cnt = sum(1 for t in tx if {r.p1, r.p2} <= {i["product_id"] for i in t["items"]})
        close(f"Spark bought-together: baskets containing {r.p1} + {r.p2}", cnt, r.pair_baskets, tol=0)
    else:
        record("SKIP", "Spark bought-together")

    n_pass, n_fail, n_skip = (results.count(x) for x in ("PASS", "FAIL", "SKIP"))
    print(f"\nSUMMARY: {n_pass} passed, {n_fail} failed, {n_skip} skipped")
    return 1 if n_fail else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data"); ap.add_argument("--out", default="outputs")
    a = ap.parse_args()
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    os.chdir(root)
    sys.exit(main(a.data, a.out))
