"""
PART 4 - Static visualisations built from the CSV outputs of the Mongo / Spark / integration steps.
Run last:  python viz/10_visualizations.py --out outputs --fig outputs/figures
"""
import argparse, os
import pandas as pd, numpy as np
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid", context="talk", font_scale=0.75)
PAL = sns.color_palette("viridis", 5)


def fig1_revenue_trend(o, f):
    d = pd.read_csv(f"{o}/sql_q2_daily_revenue_moving_avg.csv", parse_dates=["txn_date"])
    d = d.iloc[1:-1].copy()   # the first and last day of the 90-day window are PARTIAL days -> exclude them
    d["ma7"] = d.revenue.rolling(7).mean()   # moving average recomputed on complete days only
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.bar(d.txn_date, d.revenue, color="#c7d3e8", label="Daily revenue", width=0.9)
    ax.plot(d.txn_date, d.ma7, color="#1f4e9c", lw=2.5, label="7-day moving average")
    ax.set(title="Daily revenue and 7-day moving average (complete days only)", ylabel="Revenue ($)", xlabel="")
    ax.set_ylim(0, d.revenue.max() * 1.2); ax.legend(frameon=False, loc="upper left", ncol=2); fig.autofmt_xdate(); fig.tight_layout(); fig.savefig(f"{f}/fig1_revenue_trend.png", dpi=170); plt.close(fig)


def fig2_category(o, f):
    d = pd.read_csv(f"{o}/mongo_revenue_by_category_month.csv")
    c = d.groupby("category", as_index=False)[["revenue", "est_profit"]].sum().nlargest(10, "revenue").iloc[::-1]
    fig, ax = plt.subplots(figsize=(10, 5.5))
    y = np.arange(len(c))
    ax.barh(y + 0.2, c.revenue, 0.4, label="Revenue", color="#1f4e9c")
    ax.barh(y - 0.2, c.est_profit, 0.4, label="Estimated profit", color="#f2a541")
    ax.set_yticks(y); ax.set_yticklabels(c.category.str.slice(0, 28))
    ax.set(title="Top 10 categories: revenue vs. estimated profit (MongoDB aggregation)", xlabel="$")
    ax.legend(frameon=False, loc="lower right"); fig.tight_layout(); fig.savefig(f"{f}/fig2_category_revenue_profit.png", dpi=170); plt.close(fig)


def fig3_segments(o, f):
    t = pd.read_csv(f"{o}/integrated_clv_tiers.csv")
    order = ["Platinum", "Gold", "Silver", "Bronze"]; t = t.set_index("tier").loc[order].reset_index()
    seg = pd.read_csv(f"{o}/mongo_user_segments.csv")
    seg = seg[seg.segment_min_orders != "other"].copy()
    seg["segment_min_orders"] = seg.segment_min_orders.astype(int)
    seg = seg.sort_values("segment_min_orders").reset_index(drop=True)
    lows = seg.segment_min_orders.tolist()          # labels such as "6-9", last one "12+"
    seg["label"] = [f"{lo}-{hi - 1}" if hi - 1 > lo else f"{lo}" for lo, hi in zip(lows[:-1], lows[1:])] + [f"{lows[-1]}+"]
    fig, ax = plt.subplots(1, 2, figsize=(13, 4.8))
    sns.barplot(data=t, x="tier", y="avg_clv_12m", hue="tier", palette="YlOrBr_r", ax=ax[0], legend=False)
    for i, r in t.iterrows():
        ax[0].text(i, r.avg_clv_12m, f"${r.avg_clv_12m:,.0f}\n{int(r.avg_orders)} orders", ha="center", va="bottom", fontsize=10)
    ax[0].set(title="CLV tiers: avg 12-month CLV\n(MongoDB + HBase + Spark)", xlabel="", ylabel="$ per customer"); ax[0].margins(y=0.2)
    sns.barplot(data=seg, x="label", y="avg_spent", color="#1f4e9c", ax=ax[1])
    for i, r in seg.iterrows():
        ax[1].text(i, r.avg_spent, f"n={int(r.users)}", ha="center", va="bottom", fontsize=10)
    ax[1].set(title="Average spend by purchase-frequency segment\n(MongoDB $bucket)", xlabel="Orders per customer (quintile-based segments)", ylabel="Avg total spent ($)"); ax[1].margins(y=0.15)
    fig.tight_layout(); fig.savefig(f"{f}/fig3_customer_segments.png", dpi=170); plt.close(fig)


def fig4_funnel(o, f):
    d = pd.read_csv(f"{o}/integrated_funnel.csv").iloc[0]
    stages = ["Sessions", "Viewed a product", "Added to cart", "Reached checkout", "Converted (HBase flag)", "Order in MongoDB"]
    vals = [d.sessions, d.viewed_product, d.added_to_cart, d.reached_checkout, d.converted_flag_hbase, d.purchase_in_mongo]
    fig, ax = plt.subplots(figsize=(10, 5))
    cols = sns.color_palette("Blues_r", len(vals))
    ax.barh(stages[::-1], vals[::-1], color=cols[::-1])
    for i, (s, v) in enumerate(zip(stages[::-1], vals[::-1])):
        pct = 100 * v / vals[0]
        ax.text(v, i, f"  {int(v):,} ({pct:.0f}%)", va="center", fontsize=11)
    ax.set(title="Conversion funnel: HBase sessions reconciled with MongoDB orders", xlabel="Sessions"); ax.margins(x=0.22)
    fig.tight_layout(); fig.savefig(f"{f}/fig4_conversion_funnel.png", dpi=170); plt.close(fig)


def fig5_cohort(o, f):
    c = pd.read_csv(f"{o}/spark_cohorts.csv")
    m = c.pivot(index="registration_month", columns="txn_month", values="rev_per_user")
    # drop partial calendar months at either end of the 90-day window (fewer than 25 days of data)
    days = pd.read_csv(f"{o}/sql_q2_daily_revenue_moving_avg.csv", parse_dates=["txn_date"])
    days = days.groupby(days.txn_date.dt.strftime("%Y-%m")).txn_date.nunique()
    full = [col for col in m.columns if days.get(col, 0) >= 25]
    if full:
        m = m[full]
    sizes = c.groupby("registration_month").cohort_users.first()
    keep = sizes[sizes >= min(20, sizes.median() / 2)].index              # ignore tiny cohorts (adaptive to scale)
    m = m.loc[[i for i in m.index if i in keep]]
    if m.empty:
        print("  fig5: not enough cohort data, skipped"); return
    fig, ax = plt.subplots(figsize=(8, 4.8))
    sns.heatmap(m, annot=True, fmt=".0f", cmap="YlGnBu", cbar_kws={"label": "Revenue per cohort user ($)"}, ax=ax)
    ax.set(title="Cohort analysis: revenue per user by registration month (Spark)", xlabel="Purchase month", ylabel="Registration cohort")
    fig.tight_layout(); fig.savefig(f"{f}/fig5_cohort_heatmap.png", dpi=170); plt.close(fig)


def fig6_referrer_device(o, f):
    r = pd.read_csv(f"{o}/integrated_funnel_by_referrer.csv").sort_values("conv_pct", ascending=False)
    d = pd.read_csv(f"{o}/integrated_funnel_by_device.csv").sort_values("conv_pct", ascending=False)
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.3), sharey=True)
    sns.barplot(data=r, x="referrer", y="conv_pct", color="#1f4e9c", ax=ax[0]); ax[0].set(title="Session-to-order conversion by referrer", xlabel="", ylabel="Conversion (%)")
    sns.barplot(data=d, x="device_type", y="conv_pct", color="#f2a541", ax=ax[1]); ax[1].set(title="...by device", xlabel="", ylabel="")
    for a in ax:
        for p in a.patches: a.annotate(f"{p.get_height():.1f}%", (p.get_x() + p.get_width() / 2, p.get_height()), ha="center", va="bottom", fontsize=10)
        a.set_ylim(0, max(r.conv_pct.max(), d.conv_pct.max()) * 1.2)
    fig.tight_layout(); fig.savefig(f"{f}/fig6_conversion_by_referrer_device.png", dpi=170); plt.close(fig)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--out", default="outputs"); ap.add_argument("--fig", default="outputs/figures")
    a = ap.parse_args(); os.makedirs(a.fig, exist_ok=True)
    for fn in [fig1_revenue_trend, fig2_category, fig3_segments, fig4_funnel, fig5_cohort, fig6_referrer_device]:
        fn(a.out, a.fig); print("saved", fn.__name__)
