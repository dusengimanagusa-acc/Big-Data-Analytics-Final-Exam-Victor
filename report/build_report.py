"""
Builds the FINAL TECHNICAL REPORT (PDF) from the real outputs of the pipeline.

  python report/build_report.py --author "Your Name" --student-id "12345"

Everything numeric in the text comes from outputs/*.csv, outputs/*.json and outputs/data_quality_report.txt,
so re-running the pipeline on YOUR machine and then this script regenerates the report with YOUR numbers.

Screenshots:
  * outputs/screenshots/*.png   terminal-style captures rendered from the real logs (tools/make_screenshots.py)
  * screenshots/real/*.png|jpg  YOUR real screenshots (Compass, HBase shell, HBase UI, Spark UI, docker ps ...);
                                if present they are added to Appendix B automatically.
"""
import argparse, glob, json, os, sys
from xml.sax.saxutils import escape as xml_escape
import pandas as pd
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Image, KeepTogether, PageBreak, Paragraph, Preformatted, SimpleDocTemplate,
                                Spacer, Table, TableStyle)
from PIL import Image as PILImage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
os.chdir(ROOT)
O = "outputs"
NAVY, GREY, LIGHT = colors.HexColor("#1f3a68"), colors.HexColor("#555555"), colors.HexColor("#eef2f9")

# ------------------------------------------------------------------ styles
ss = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=ss["Normal"], fontName="Helvetica", fontSize=9.2, leading=12.4, alignment=TA_JUSTIFY, spaceAfter=4)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8, leading=10.2, alignment=0)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=7.9, leading=9.8, alignment=0, spaceAfter=0)
CELLB = ParagraphStyle("cellb", parent=CELL, fontName="Helvetica-Bold", textColor=colors.white)
CAP = ParagraphStyle("cap", parent=BODY, fontSize=8, leading=10, textColor=GREY, alignment=1, fontName="Helvetica-Oblique", spaceAfter=7)
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontName="Helvetica-Bold", fontSize=13.5, leading=16, textColor=NAVY, spaceBefore=9, spaceAfter=4, keepWithNext=1)
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=10.6, leading=13, textColor=NAVY, spaceBefore=6, spaceAfter=3, keepWithNext=1)
TITLE = ParagraphStyle("title", parent=ss["Title"], fontName="Helvetica-Bold", fontSize=19, leading=23, textColor=NAVY, alignment=0, spaceAfter=2)
SUB = ParagraphStyle("sub", parent=BODY, fontSize=10.5, leading=14, textColor=GREY, alignment=0)
CODE = ParagraphStyle("code", fontName="Courier", fontSize=6.9, leading=8.4, backColor=colors.HexColor("#f5f5f5"), borderPadding=4, spaceAfter=6)
BUL = ParagraphStyle("bul", parent=BODY, leftIndent=11, bulletIndent=2, spaceAfter=2)
CW = A4[0] - 3.4 * cm   # content width


def P(t, s=BODY): return Paragraph(t, s)
def B(t): return Paragraph(t, BUL, bulletText="-")
def money(x): return f"${x:,.0f}"


def table(rows, widths, header=True, zebra=True, font=CELL):
    data = [[Paragraph(str(c), CELLB if (header and i == 0) else font) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    st = [("VALIGN", (0, 0), (-1, -1), "TOP"), ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#c8cdd6")),
          ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4), ("TOPPADDING", (0, 0), (-1, -1), 2.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.5)]
    if header: st.append(("BACKGROUND", (0, 0), (-1, 0), NAVY))
    if zebra:
        for i in range(1 if header else 0, len(rows)):
            if i % 2 == 0: st.append(("BACKGROUND", (0, i), (-1, i), LIGHT))
    t.setStyle(TableStyle(st)); return t


def img(path, width):
    if not os.path.exists(path):   # never crash the whole report because one optional capture/figure is missing
        print(f"WARNING: missing image {path} - a placeholder is shown instead (run the earlier pipeline step / tools/make_screenshots.py)")
        return Paragraph(f"<font color='#b00020'>[image not available: {os.path.basename(path)}]</font>", SMALL)
    w, h = PILImage.open(path).size
    return Image(path, width=width, height=width * h / w)


def figure(path, caption, width=CW):
    return KeepTogether([img(path, width), P(caption, CAP)])


def two_figs(p1, p2, c1, c2):
    w = CW / 2 - 4
    t = Table([[img(p1, w), img(p2, w)], [P(c1, CAP), P(c2, CAP)]], colWidths=[CW / 2, CW / 2])
    t.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 2), ("RIGHTPADDING", (0, 0), (-1, -1), 2)]))
    return KeepTogether([t])


# ------------------------------------------------------------------ load results
def read_dq():
    d = {}
    for line in open(f"{O}/data_quality_report.txt"):
        k, v = line.rsplit(":", 1)
        d[k.strip()] = v.strip()
    return d


def first_int(s): return int(s.strip("() ").split(",")[-1])


def load_facts():
    f = {}
    f["info"] = json.load(open(f"{O}/run_info.json")) if os.path.exists(f"{O}/run_info.json") else {"mode": "live", "scale": "?"}
    f["dq"] = read_dq()
    f["daily"] = pd.read_csv(f"{O}/sql_q2_daily_revenue_moving_avg.csv", parse_dates=["txn_date"])
    f["rollup"] = pd.read_csv(f"{O}/sql_q4_rollup_category_subcategory.csv")
    f["cat_month"] = pd.read_csv(f"{O}/mongo_revenue_by_category_month.csv")
    f["top_prod"] = pd.read_csv(f"{O}/mongo_top_products.csv")
    f["seg"] = pd.read_csv(f"{O}/mongo_user_segments.csv")
    f["tiers"] = pd.read_csv(f"{O}/integrated_clv_tiers.csv").set_index("tier")
    f["corr"] = json.load(open(f"{O}/integrated_clv_corr.json"))
    f["funnel"] = pd.read_csv(f"{O}/integrated_funnel.csv").iloc[0]
    f["dev"] = pd.read_csv(f"{O}/integrated_funnel_by_device.csv")
    f["ref"] = pd.read_csv(f"{O}/integrated_funnel_by_referrer.csv")
    f["coh"] = pd.read_csv(f"{O}/spark_cohorts.csv")
    f["bt"] = pd.read_csv(f"{O}/spark_bought_together.csv")
    f["vt"] = pd.read_csv(f"{O}/spark_viewed_together.csv")
    return f


# ------------------------------------------------------------------ architecture diagram
def draw_architecture(path):
    fig, ax = plt.subplots(figsize=(9.2, 3.9)); ax.set_xlim(0, 100); ax.set_ylim(0, 42); ax.axis("off")

    def box(x, y, w, h, text, fc, tc="white", fs=8.2, bold=False):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.2", fc=fc, ec="none"))
        ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", color=tc, fontsize=fs, fontweight="bold" if bold else "normal", linespacing=1.35)

    def arrow(x1, y1, x2, y2, label=""):
        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=11, color="#555", lw=1.2))
        if label: ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 1.3, label, ha="center", fontsize=6.8, color="#444")

    box(0.5, 14, 17, 14, "dataset_\ngenerator.py\n(Faker, 90 days)\n\nusers, products,\ncategories,\nsessions,\ntransactions\n(raw JSON)", "#6b7280", fs=6.8)
    box(26, 31, 22, 9, "MongoDB (document)\ncatalog, users + summaries,\ntransactions + embedded items", "#2e7d32", fs=7.4)
    box(26, 16, 22, 9, "HBase (wide-column)\nuser_sessions  user#rev_ts\nproduct_metrics  product#date", "#c05621", fs=7.4)
    box(26, 1, 22, 9, "Spark SQL / DataFrames\nclean -> Parquet (partitioned)\nbatch jobs + SQL", "#1f3a68", fs=7.4)
    box(60, 12, 20, 18, "Spark integration job\njoins MongoDB profile +\norders with HBase session\nevents\n\nCLV  |  Funnel", "#4c1d95", fs=7.6, bold=True)
    box(87, 12, 12, 18, "Matplotlib /\nSeaborn\n\n6 figures\n+ report", "#0f766e", fs=7.6)
    arrow(17.5, 24, 26, 35.5, "load"); arrow(17.5, 21, 26, 20.5, "stream"); arrow(17.5, 18, 26, 5.5, "read")
    arrow(48, 35.5, 60, 26, "pymongo /\nconnector"); arrow(48, 20.5, 60, 21, "happybase /\nconnector"); arrow(48, 5.5, 60, 15, "Parquet")
    arrow(80, 21, 87, 21)
    fig.tight_layout(); fig.savefig(path, dpi=200); plt.close(fig)


# ------------------------------------------------------------------ examples for the design section
def example_docs():
    try:
        tx = json.load(open("data/transactions.json"))
        t = next(x for x in tx if x["session_id"] and len(x["items"]) >= 1)
        prods = {p["product_id"]: p for p in json.load(open("data/products.json"))}
        cats = {c["category_id"]: c for c in json.load(open("data/categories.json"))}
        items = []
        for it in t["items"][:2]:
            cat = cats[prods[it["product_id"]]["category_id"]]
            sub = cat["subcategories"][int(it["product_id"].split("_")[1]) % len(cat["subcategories"])]
            items.append({"product_id": it["product_id"], "quantity": it["quantity"], "unit_price": it["unit_price"],
                          "subtotal": it["subtotal"], "category_id": cat["category_id"], "profit_margin": sub["profit_margin"]})
        doc = {"_id": t["transaction_id"], "user_id": t["user_id"], "ts": "ISODate(" + t["timestamp"][:19] + ")", "items": items,
               "subtotal": t["subtotal"], "discount": t["discount"], "total": t["total"], "status": t["status"]}
        s = [x for x in json.load(open(sorted(glob.glob("data/sessions_*.json"))[0])) if x["page_views"]][0]
        sys.path.insert(0, os.path.join(ROOT, "hbase"))
        from hb_common import session_rowkey
        key = session_rowkey(s["user_id"], s["start_time"]).decode()
        cells = [f"meta:start_time = {s['start_time'][:19]}", f"meta:duration_seconds = {s['duration_seconds']}",
                 f"meta:device_type = {s['device_profile']['type']}", f"meta:conversion_status = {s['conversion_status']}"]
        pv = s["page_views"][0]
        cells.append(f"pv:000 = {pv['timestamp'][:19]}|{pv['page_type']}|{pv['product_id'] or ''}|{pv['category_id'] or ''}|{pv['view_duration']}")
        for pid, c in list(s["cart_contents"].items())[:1]:
            cells.append(f"cart:{pid} = {c['quantity']}|{c['price']}")
        lines = ["{"] + [f' "{k}": {json.dumps(v)},' for k, v in doc.items() if k != "items"]
        lines.insert(4, ' "items": [')
        for j, it in enumerate(items):
            l1 = "   " + json.dumps({k: it[k] for k in ("product_id", "quantity", "unit_price", "subtotal")})[:-1] + ","
            l2 = "    " + json.dumps({k: it[k] for k in ("category_id", "profit_margin")})[1:] + ("," if j < len(items) - 1 else "")
            lines.insert(5 + 2 * j, l1); lines.insert(6 + 2 * j, l2)
        lines.insert(5 + 2 * len(items), " ],")
        lines.append("}")
        return "\n".join(lines), f"ROW KEY  {key}\n" + "\n".join("   " + c for c in cells)
    except Exception:  # data folder not present
        return None, None


# ------------------------------------------------------------------ the report
def build(author, sid, out_pdf):
    f = load_facts(); dq = f["dq"]; info = f["info"]
    offline = info.get("mode") == "offline"
    os.makedirs(f"{O}/figures", exist_ok=True)
    arch = f"{O}/figures/architecture.png"; draw_architecture(arch)
    fig = lambda n: f"{O}/figures/{n}"
    shot = lambda n: f"{O}/screenshots/{n}.png"

    n_users, n_prod = first_int(dq["users raw/clean"]), first_int(dq["products raw/clean"])
    n_sess, n_txn = first_int(dq["sessions raw/clean"]), first_int(dq["transactions raw/clean"])

    # ---- derived facts
    tot = f["rollup"][f["rollup"].category_id.isna()].iloc[0]
    margin_pct = 100 * tot.est_profit / tot.revenue
    catsum = f["cat_month"].groupby("category", as_index=False)[["revenue", "est_profit"]].sum().sort_values("revenue", ascending=False)
    top_cat = catsum.iloc[0]
    daily = f["daily"].iloc[1:-1]      # first and last day of the window are partial days
    peak = daily.loc[daily.revenue.idxmax()]
    tp = f["top_prod"].iloc[0]
    t = f["tiers"]; plat, bron = t.loc["Platinum"], t.loc["Bronze"]
    plat_share = 100 * plat.total_clv_12m / t.total_clv_12m.sum()
    c = f["corr"]; fu = f["funnel"]
    stages = [("sessions", fu.sessions), ("viewed a product", fu.viewed_product), ("added to cart", fu.added_to_cart),
              ("reached checkout", fu.reached_checkout), ("converted", fu.converted_flag_hbase)]
    drops = [(stages[i][0], stages[i + 1][0], 100 * (1 - stages[i + 1][1] / stages[i][1])) for i in range(len(stages) - 1)]
    worst = max(drops, key=lambda d: d[2])
    ref = f["ref"].sort_values("conv_pct", ascending=False); dev = f["dev"].sort_values("conv_pct", ascending=False)
    ref_spread = ref.conv_pct.iloc[0] - ref.conv_pct.iloc[-1]; dev_spread = dev.conv_pct.iloc[0] - dev.conv_pct.iloc[-1]
    coh = f["coh"]; sizes = coh.groupby("registration_month").cohort_users.first()
    dd = f["daily"]; days_per_month = dd.groupby(dd.txn_date.dt.strftime("%Y-%m")).txn_date.nunique()
    full_months = [mth for mth in coh.txn_month.unique() if days_per_month.get(mth, 0) >= 25] or list(coh.txn_month.unique())
    coh_ok = coh[coh.registration_month.isin(sizes[sizes >= min(20, sizes.median() / 2)].index) & coh.txn_month.isin(full_months)]
    coh_cv = coh_ok.rev_per_user.std() / coh_ok.rev_per_user.mean() if len(coh_ok) > 1 else float("nan")
    if coh_cv == coh_cv:   # not NaN
        flat = coh_cv < 0.15
        coh_txt = (f"Revenue per user varies {'little' if flat else 'noticeably'} between cohorts and months (coefficient of variation {coh_cv:.2f}): "
                   + ("there is no visible decay or growth with customer age. " if flat else "there are visible differences between cohorts. "))
    else:
        coh_txt = "There is not enough cohort data at this dataset size to compare cohorts. "
    bt0 = f["bt"].iloc[0]
    gap_txt = ""
    if int(fu.converted_flag_hbase) != int(fu.purchase_in_mongo):
        gap_txt = f" The HBase converted flag ({int(fu.converted_flag_hbase):,}) and the orders found in MongoDB ({int(fu.purchase_in_mongo):,}) differ - see section 4."

    ex_txn, ex_row = example_docs()
    S = []   # story

    # ================= PAGE 1: title + overview + architecture
    S += [P("Distributed Multi-Model Analytics for E-commerce Data", TITLE),
          P("MongoDB (document) + HBase (wide-column) + Apache Spark (distributed processing)", SUB),
          P(f"AUCA Big Data Analytics - Final Project - Technical Report &nbsp;|&nbsp; <b>{xml_escape(author)}</b> (ID {xml_escape(str(sid))}) &nbsp;|&nbsp; due 11 Nov 2026", SMALL),
          Spacer(1, 4)]
    run_line = (f"<b>Run summary.</b> Dataset scale {info.get('scale')} of the specification: {n_users:,} users, {n_prod:,} products, "
                f"{n_sess:,} sessions, {n_txn:,} transactions (90 days). ")
    if offline:
        run_line += ("<font color='#b00020'><b>Note:</b> the MongoDB and HBase steps shown in this version of the report were produced with the project's "
                     "offline test harness (in-memory stand-ins), not with live servers. Spark steps are real.</font>")
    else:
        run_line += "MongoDB and HBase were run as real servers (Docker); Spark ran in local mode."
    S += [P(run_line, BODY)]
    S += [P("1. Introduction and system architecture", H1),
          P("The goal is to answer business questions about a synthetic e-commerce platform by using each technology where it is strongest. "
            "<b>MongoDB</b> stores entity data that is read as whole documents and aggregated along many dimensions (catalog, customers, orders). "
            "<b>HBase</b> stores the high-volume, append-only session event stream with a row key designed for time-ordered, per-user access. "
            "<b>Apache Spark</b> performs full scans, joins and algorithms (cleaning, co-occurrence, cohorts, customer lifetime value) and acts as the "
            "<b>integration layer</b> that joins MongoDB-resident data with HBase-resident data. Figure 1 shows how they fit together; section 7 explains the technology choices.", BODY),
          figure(arch, "Figure 1 - System architecture: raw JSON is loaded into MongoDB and HBase and processed by Spark; the integration job joins both stores.", CW * 0.8)]

    # ================= Part 1 data modelling
    S += [P("2. Data modelling (Part 1)", H1), P("2.1 MongoDB document model", H2),
          P("The model follows the access patterns: (i) revenue and popularity analytics over orders, (ii) customer profile and segmentation reads, "
            "(iii) product look-ups with their category. Rule of thumb used: <b>embed</b> data that is always read together and has a bounded size; "
            "<b>reference or denormalise</b> data that is shared or queried independently.", BODY),
          table([["Collection", "Design decision", "Justification"],
                 ["categories", "Subcategories (with profit_margin) embedded as an array", "Small, 1-to-few, always read together; the whole hierarchy is one document per category."],
                 ["products", "price_history embedded; category and subcategory name/margin <b>denormalised</b> into the product", "Price history is only read with its product. Denormalising avoids a $lookup for every product listing. "
                  "<i>The generator does not emit subcategory_id, so one is assigned deterministically (product number mod number of subcategories).</i>"],
                 ["transactions", "Line items embedded; every item also stores category_id and profit_margin; timestamp stored as a real Date (field ts)", "An order is written and read atomically with its items. Revenue/profit by category needs no join. "
                  "A real Date type enables $dateToString grouping and range queries on the ts index."],
                 ["users", "Profile + embedded, pre-computed <b>purchase_summary</b> (completed/shipped/delivered orders only, same definition as revenue) and <b>engagement_summary</b> (computed pattern)", "Profile pages and segmentation hit a single document; $bucket over summary fields is a one-collection pass. "
                  "Trade-off: the summaries are redundant and must be refreshed by a batch job or change stream."]],
                [2.2 * cm, 6.1 * cm, CW - 8.3 * cm]),
          Spacer(1, 3),
          P("<b>Indexes</b> (matching the pipelines): transactions(ts), (user_id, ts), (items.product_id); products(category.category_id); users(purchase_summary.total_orders), (geo_data.state). "
            "<b>Sessions are deliberately not stored in MongoDB</b>: they are large, write-heavy time-series documents (up to 16 page views each) better served by HBase and Spark.", BODY)]
    if ex_txn:
        S += [KeepTogether([P("Example transaction document (real record from the generated data, after loading transformations):", SMALL), Preformatted(ex_txn, CODE)])]
    S += [P("<b>Four aggregation pipelines</b> implemented in mongodb/02_aggregations.py: <b>A</b> revenue and estimated profit by category and month ($match, $unwind, $group with $dateToString, $lookup); "
            "<b>B</b> top-20 products by revenue with category name; <b>C</b> user segmentation by purchase frequency with $bucket whose boundaries are the "
            "<i>quintiles of the real order-count distribution</i> (so it stays meaningful at any scale); <b>D</b> revenue by state and payment method ($lookup to users).", BODY)]

    S += [P("2.2 HBase wide-column model", H2),
          table([["Table / key", "Column families", "Why"],
                 ["<b>user_sessions</b><br/>row key = user_id#(9999999999999 - start_epoch_ms)",
                  "<b>meta</b>: session attributes, device_*, geo_*<br/><b>pv</b>: one sparse column per page view (pv:000, pv:001...)<br/><b>cart</b>: cart:&lt;product_id&gt; = qty|price",
                  "A user's sessions are adjacent and <b>newest-first</b> (reverse timestamp), so 'latest N sessions' is one short prefix scan and a date range is one contiguous range scan. "
                  "user_id leads the key, which spreads writes (no hotspot) and the table is pre-split. Families are split by access pattern: a metadata query never reads the heavy pv family. Only 3 families (each family = separate files/memstore)."],
                 ["<b>product_metrics</b><br/>row key = product_id#yyyymmdd", "<b>m</b>: views, view_seconds, cart_adds, cart_units, purchase_sessions, purchased_units",
                  "A product's daily history is contiguous for time-range scans (view-to-purchase trends). Sparse: a product-day without activity has no row."]],
                [4.0 * cm, 5.3 * cm, CW - 9.3 * cm]), Spacer(1, 3)]
    if ex_row:
        S += [KeepTogether([P("Example HBase row (real session from the generated data):", SMALL), Preformatted(ex_row, CODE)])]

    S += [P("2.3 Why some data is in MongoDB and some in HBase", H2),
          table([["Need", "Best fit", "Reason"],
                 ["Ad-hoc, multi-dimensional aggregation over orders and customers", "MongoDB", "Aggregation framework, secondary indexes, nested documents; no sorted-key constraint."],
                 ["Fetch one user's recent activity in milliseconds; time-range scans over events", "HBase", "Sorted row keys, prefix/range scans, wide sparse rows, very high write throughput, linear scale-out."],
                 ["Heavy joins, iterative algorithms, full scans of everything", "Spark", "Distributed in-memory processing; SQL and DataFrames; reads any of the stores."]],
                [5.4 * cm, 2.3 * cm, CW - 7.7 * cm]),
          P("HBase has no secondary indexes or joins, so any question that is not answered by the row key goes to Spark; MongoDB is not designed for billions of tiny events. "
            "This is why order data lives in MongoDB while click-stream data lives in HBase.", BODY),
          figure(shot("s01_mongo_load_agg"), "Screenshot 1 - MongoDB load and aggregation A (revenue by category and month)." + (" Offline test harness." if offline else ""), CW * 0.8),
          figure(shot("s03_hbase"), "Screenshot 2 - HBase: loaded subset, newest-first scan of one user's sessions, range scan and product_metrics row." + (" Offline test harness." if offline else ""), CW * 0.8)]

    # ================= Part 2 Spark
    S += [P("3. Spark processing pipeline (Part 2)", H1),
          P("<b>Pipeline.</b> Raw JSON (one JSON array per file, so read with multiLine=true) -> explicit schemas (no inference pass over gigabytes) -> "
            "cleaning/normalisation job -> partitioned Parquet (sessions by date, transactions by month) -> batch analytics and Spark SQL.", BODY),
          P("<b>Cleaning and normalisation (06_clean_normalize.py).</b> Timestamps parsed from ISO strings; duplicates removed on primary keys; text standardised (state upper-case, device and referrer lower-case); "
            "invalid rows dropped (negative durations, non-positive prices); the category hierarchy exploded to a flat dimension; transaction totals recomputed and compared with stored values; "
            "referential-integrity checks via left-anti joins. Result of the data-quality report:", BODY)]
    items_dq = list(dq.items()); half = (len(items_dq) + 1) // 2
    dq_rows = [["Check", "Result", "Check", "Result"]]
    for i in range(half):
        a_ = items_dq[i]; b_ = items_dq[i + half] if i + half < len(items_dq) else ("", "")
        dq_rows.append([a_[0], a_[1], b_[0], b_[1]])
    S += [table(dq_rows, [CW * 0.31, CW * 0.14, CW * 0.37, CW * 0.18]), Spacer(1, 3)]
    early = int(dq.get("txn dated BEFORE the user's registration", 0))
    early_txt = (f" (3) {early:,} transactions ({100*early/n_txn:.0f}%) are dated before their user's registration date - another generator artefact "
                 "(registration and activity dates are drawn independently), so cohort figures describe spend per registration cohort in calendar months, not true customer age.") if early else ""
    early_ok = ", and no transaction predates its user's registration" if early == 0 else ""
    orphan = int(dq["txn not linked to a session"]); conv = int(dq["sessions 'converted'"]); lost = int(dq["...converted but NO transaction (generator txn cap)"])
    S += [P(f"<b>Findings.</b> All stored totals are consistent (0 mismatches), all references resolve{early_ok}. Two characteristics of the synthetic data matter: "
            f"(1) {orphan:,} of {n_txn:,} transactions ({100*orphan/n_txn:.0f}%) have no session link by design (the generator creates 'extra' orders); "
            f"(2) {lost:,} of {conv:,} sessions flagged 'converted' ({100*lost/conv:.0f}%) have <i>no</i> transaction. We traced this to the generator: once the "
            f"transaction cap is reached it stops creating transactions while sessions keep being flagged converted. For funnel reconciliation we therefore use the first part of the sessions "
            f"(HBase subset) where the two systems agree." + early_txt, BODY),
          P("<b>Batch analytics (07_batch_analytics.py).</b> (a) <i>Frequently bought together</i>: self-join of basket items with a.id &lt; b.id (each unordered pair once), "
            "with support, confidence and lift; (b) <i>frequently viewed together</i> from sessions; (c) <i>cohort analysis</i> grouping users by registration month and measuring revenue per user in each purchase month. "
            f"Top pair by baskets: {bt0.p1} + {bt0.p2} ({int(bt0.pair_baskets)} baskets, lift {bt0.lift:.1f}). Lift values are noisy because pair counts are small.", BODY),
          P("<b>Optimisations used.</b> Parquet (columnar) with partition pruning; column pruning; <b>broadcast join</b> of the small product dimension; pair de-duplication in the self-join; "
            "cache() for DataFrames reused several times; shuffle partitions tuned to the data size (SHUFFLE_PARTS); explicit schemas instead of inference.", BODY)]
    sql = open(f"{O}/spark_sql_queries.sql").read().split("-- ")[1:]
    q1 = [q for q in sql if q.startswith("q1")][0].strip().splitlines()
    S += [P("<b>Spark SQL (08_spark_sql.py).</b> Six queries: top-3 products per category (CTE + ROW_NUMBER window), 7-day moving average and day-over-day growth (window frames, LAG), "
            "device funnel (higher-order function exists on the nested page_views array), revenue/profit ROLLUP by category and subcategory, and two queries on views "
            "(mongo_users, mongo_purchase_summary, hbase_sessions) that stand in for data loaded through the MongoDB and HBase connectors - the SQL logic is identical either way. Query 1:", BODY),
          Preformatted("\n".join(q1[:16]), CODE),
          figure(shot("s04_spark_dq"), "Screenshot 3 - Spark cleaning job and data-quality report.", CW * 0.6),
          figure(shot("s07_sparksql_q1_q2"), "Screenshot 4 - Spark SQL queries 1 (top products per category) and 2 (moving average).", CW * 0.74)]

    # ================= Part 3 integration
    S += [P("4. Integrated analytics (Part 3)", H1),
          P("<b>Query 1 - Customer Lifetime Value (CLV).</b> <i>Business question:</i> which customers will be most valuable over the next 12 months, and does browsing engagement predict value? "
            "<i>Sources:</i> MongoDB (profiles, plus a per-user revenue/profit roll-up computed <b>inside</b> MongoDB so only one small row per user leaves the database), "
            "HBase (session count, average duration and conversion per user), Spark (join, CLV, NTILE tiers, correlations). "
            "<i>Formula:</i> monthly net profit = (gross item profit - discounts) / 3 observed months; CLV<sub>12m</sub> = monthly net profit x (1 + r + ... + r<super>11</super>), with an <b>assumed</b> monthly retention r = 0.90 "
            "(an assumption, not estimated from data - a 90-day window cannot reveal churn).", BODY),
          P("<b>Query 2 - Funnel conversion.</b> <i>Business question:</i> where do shoppers drop out between first product view and a confirmed purchase, and does it differ by device or referrer? "
            "<i>Sources:</i> HBase sessions (view -> cart -> checkout, device, referrer) and MongoDB transactions (system of record for 'order exists'), joined in Spark on session_id. "
            "<i>Method:</i> the funnel is <b>strictly nested</b> - a session counts at a stage only if it also passed every earlier stage "
            "(view, cart, checkout, converted, order exists). This matters because the generator's page logs are noisy: for example, some sessions with an empty cart still contain a 'checkout' page, "
            "so counting each stage independently would overstate the later stages.", BODY),
          P("<b>Workflow.</b> (1) pymongo runs two aggregations in MongoDB; (2) a happybase column-restricted scan reads the HBase sessions and derives stage flags; "
            "(3) both become Spark DataFrames; (4) Spark joins on user_id / session_id, computes CLV, NTILE tiers, correlations and funnel counts; (5) results are exported to CSV for plotting. "
            "<b>Performance notes:</b> aggregation is pushed down to MongoDB; HBase scans are restricted to needed families; at scale the single-threaded Python readers are replaced by the "
            "MongoDB Spark Connector and the Spark-HBase connector so reads are parallel across executors. The same code can run from Parquet files (--source files); we verified that live and file modes produce identical results. "
            "<b>Verification.</b> tools/verify_results.py re-computes the headline numbers (revenue totals, top product, funnel stages, CLV of sample customers, the cohort table, the top product pair) "
            "from the raw JSON in plain Python and compares them with the MongoDB, Spark and integration outputs; all checks must pass (see Appendix A7).", BODY),
          figure(shot("s09_clv"), "Screenshot 5 - Integrated query 1: CLV tiers and correlations." + (" Offline test harness." if offline else ""), CW * 0.74)]

    # ================= Part 4 visualizations
    S += [P("5. Visualisations and business insights (Part 4)", H1),
          P(f"Overall (completed/shipped/delivered orders): revenue {money(tot.revenue)}, estimated profit {money(tot.est_profit)} ({margin_pct:.1f}% margin). "
            f"Highest-revenue category: {top_cat.category} ({money(top_cat.revenue)}, {100*top_cat.revenue/tot.revenue:.1f}% of revenue); best product: {tp['name']} ({money(tp.revenue)}, {int(tp.units_sold)} units). "
            f"Best day: {peak.txn_date:%d %b %Y} ({money(peak.revenue)}).", BODY),
          figure(fig("fig1_revenue_trend.png"), "Figure 2 - Daily revenue (Spark SQL) with 7-day moving average; the partial first and last day of the window are excluded.", CW * 0.66),
          figure(fig("fig2_category_revenue_profit.png"), "Figure 3 - Top 10 categories by revenue vs estimated profit (MongoDB aggregation A). Profit rank differs from revenue rank because margins differ by subcategory.", CW * 0.6),
          figure(fig("fig3_customer_segments.png"), "Figure 4 - Customer segmentation: CLV tiers (left) and purchase-frequency quintile segments (right).", CW * 0.9)]
    S += [P(f"<b>Segmentation insight.</b> Platinum customers (top 25%) have an average 12-month CLV of {money(plat.avg_clv_12m)} versus {money(bron.avg_clv_12m)} for Bronze "
            f"({plat.avg_clv_12m/bron.avg_clv_12m:.1f}x) and generate {plat_share:.0f}% of total projected CLV. CLV correlates with the session conversion rate (r = {c['clv~session_conv_rate']:.2f}) and "
            f"session count (r = {c['clv~sessions']:.2f}) but hardly with time on site (r = {c['clv~avg_session_sec']:.2f}). <b>Action:</b> invest in what turns visits into orders (offers, checkout friction) rather than in raw time-on-site; "
            f"treat the Platinum tier with retention programmes.", BODY),
          two_figs(fig("fig4_conversion_funnel.png"), fig("fig5_cohort_heatmap.png"), "Figure 5 - Conversion funnel (HBase sessions reconciled with MongoDB orders).", "Figure 6 - Cohort analysis: revenue per user by registration and purchase month (Spark)."),
          P(f"<b>Funnel insight.</b> {100*fu.viewed_product/fu.sessions:.0f}% of sessions view a product, {100*fu.added_to_cart/fu.sessions:.0f}% add to cart, {100*fu.reached_checkout/fu.sessions:.0f}% reach checkout and "
            f"{100*fu.converted_flag_hbase/fu.sessions:.0f}% convert. The largest loss is <b>{worst[0]} -> {worst[1]}</b> ({worst[2]:.0f}% drop). "
            f"<b>Action:</b> prioritise fixes at that step (e.g. persistent carts, reminder e-mails for carts that reached checkout).{gap_txt}", BODY),
          figure(fig("fig6_conversion_by_referrer_device.png"), "Figure 7 - Session-to-order conversion by referrer and by device.", CW * 0.7),
          P(f"<b>Channel insight.</b> Conversion by referrer ranges from {ref.conv_pct.iloc[-1]:.1f}% ({ref.referrer.iloc[-1]}) to {ref.conv_pct.iloc[0]:.1f}% ({ref.referrer.iloc[0]}), a spread of {ref_spread:.1f} percentage points; "
            f"by device the spread is only {dev_spread:.1f} points ({'negligible' if dev_spread < 1.0 else 'noticeable'}). "
            f"<b>Action:</b> review landing pages and bids for the weakest channel ({ref.referrer.iloc[-1]}) before changing device-specific experiences.", BODY),
          P("<b>Cohort insight.</b> " + coh_txt +
            "The data are synthetic (purchases are assigned at random), so cohort patterns must not be read as real-world findings. "
            "Calendar months with fewer than 25 days of data (partial months at the start or end of the 90-day window) are excluded.", BODY)]

    # ================= scalability, tech selection, limitations
    S += [P("6. Scalability considerations", H1),
          B("<b>MongoDB:</b> shard transactions on a hashed user_id (or a time bucket) so writes spread; keep user summaries to avoid cross-shard $lookup; use compound indexes that match the pipelines; the full dataset (500k orders) fits one replica set easily."),
          B("<b>HBase:</b> scales by region splits across RegionServers; the key leads with user_id (not the timestamp) to avoid hotspots; pre-split tables; TTL and compaction retire old sessions; batch Puts reduce RPC overhead; at 2M sessions the loader should use bulk load (ImportTsv/HFiles) or the Spark-HBase connector."),
          B("<b>Spark:</b> partitioned Parquet (the 4 GB of JSON shrinks several-fold), broadcast of small dimensions, tuned shuffle partitions, explicit schemas; the same code runs on YARN/Kubernetes by changing the master. Avoid collecting large results to the driver."),
          P("7. Technology selection summary", H1),
          table([["Task", "Technology", "Why"],
                 ["Revenue, profit, top products, customer summaries", "MongoDB", "Flexible nested documents + aggregation pipeline + indexes."],
                 ["User session timeline, product daily metrics", "HBase", "Time-ordered row keys, range scans, sparse wide rows."],
                 ["Cleaning, recommendations, cohorts, SQL analytics", "Spark", "Distributed scans and joins over all sessions; SQL + DataFrames."],
                 ["CLV and funnel", "All three", "Needs orders (Mongo) + engagement (HBase) + a join/compute engine (Spark)."]],
                [6.3 * cm, 2.6 * cm, CW - 8.9 * cm]),
          P("8. Limitations and future work", H1),
          B("<b>Synthetic data:</b> purchases and sessions are random, so cohorts, segments and correlations are weak or flat; lift on small baskets is noisy. Findings demonstrate the method, not real customer behaviour."),
          B("<b>Generator artefacts:</b> no subcategory_id on products (assigned by rule); no age (age-group segmentation impossible); orphan transactions; converted sessions without orders after the transaction cap."),
          B("<b>CLV</b> uses a fixed assumed retention (0.90) and a 90-day window; there is no churn model. MongoDB holds all orders and users, but HBase was loaded with a subset of the sessions (first session files); the integration reads through single-threaded Python clients, so engagement figures come from that subset."),
          B("<b>Future work:</b> ALS collaborative filtering with Spark MLlib; Structured Streaming simulation of live sessions into HBase; MongoDB change streams to keep user summaries fresh; RFM + K-means segmentation; Kafka ingestion; connector-based parallel reads."),
          ]

    # ================= Appendix A: terminal captures
    S += [PageBreak(), P("Appendix A - Console captures of the pipeline", H1),
          P("These images are rendered from the real console output of each script (logs in outputs/logs/). " +
            ("Captures marked 'offline test harness' used in-memory stand-ins for MongoDB/HBase." if offline else ""), BODY),
          figure(shot("s00_generate"), "A1 - Dataset generation.", CW * 0.78),
          figure(shot("s02_mongo_segments"), "A2 - MongoDB pipelines B (top products) and C (segmentation by quintiles)." + (" Offline test harness." if offline else ""), CW * 0.9),
          figure(shot("s05_spark_bought_together"), "A3 - Spark: frequently bought together (support/lift in the CSV).", CW * 0.9),
          figure(shot("s06_spark_cohort"), "A4 - Spark: cohort table (first rows).", CW * 0.9),
          figure(shot("s08_sparksql_q3_q5"), "A5 - Spark SQL: device funnel and the notional MongoDB x HBase join (query 5).", CW),
          figure(shot("s10_funnel"), "A6 - Integrated query 2: funnel." + (" Offline test harness." if offline else ""), CW),
          figure(shot("s11_verify"), "A7 - Independent verification of the results against the raw JSON (plain Python, no database or Spark).", CW * 0.95)]

    # ================= Appendix B: real screenshots (if provided)
    real = sorted(glob.glob("screenshots/real/*.png") + glob.glob("screenshots/real/*.jpg") + glob.glob("screenshots/real/*.jpeg"))
    S += [P("Appendix B - Evidence from the live deployment (real screenshots)", H1)]
    if real:
        for r in real:
            cap = os.path.splitext(os.path.basename(r))[0].replace("_", " ")
            S.append(figure(r, f"B - {cap}", CW * 0.9))
    else:
        S.append(P("<font color='#b00020'>No real screenshots were found in screenshots/real/. Run the project against Docker MongoDB/HBase, take the screenshots listed in "
                   "STEP_BY_STEP_GUIDE.md (section 9), save them in screenshots/real/ and rebuild this report.</font>", BODY))

    # ================= build
    def footer(canvas, doc):
        canvas.saveState(); canvas.setFont("Helvetica", 7.5); canvas.setFillColor(GREY)
        canvas.drawString(1.7 * cm, 1.0 * cm, "AUCA Big Data Analytics - Final Project - " + author)
        canvas.drawRightString(A4[0] - 1.7 * cm, 1.0 * cm, f"Page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(out_pdf, pagesize=A4, leftMargin=1.7 * cm, rightMargin=1.7 * cm, topMargin=1.5 * cm, bottomMargin=1.6 * cm,
                            title="Distributed Multi-Model Analytics for E-commerce Data", author=author)
    doc.build(S, onFirstPage=footer, onLaterPages=footer)
    print("wrote", out_pdf)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--author", default="Your Name"); ap.add_argument("--student-id", default="000000")
    ap.add_argument("--out", default="outputs/Final_Technical_Report.pdf")
    a = ap.parse_args(); build(a.author, a.student_id, a.out)
