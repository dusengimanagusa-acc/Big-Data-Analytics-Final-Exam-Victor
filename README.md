# Distributed Multi-Model Analytics for E-commerce (MongoDB + HBase + Apache Spark)
AUCA Big Data Analytics - Final Project (individual).

**Start here: [`STEP_BY_STEP_GUIDE.md`](STEP_BY_STEP_GUIDE.md)** - beginner-friendly instructions for installing, running and
reproducing everything, plus a troubleshooting table and a screenshot checklist.
The final report is `Final_Technical_Report.pdf` (rebuilt with your own numbers by `report/build_report.py`).

## Architecture
```
 dataset_generator.py ──► raw JSON (users, products, categories, transactions, sessions_N)
        ├─► MongoDB : catalog, users(+summaries), transactions(+embedded items)  -> aggregation pipelines
        ├─► HBase   : user_sessions (user#reverse_ts), product_metrics            -> per-user time-ordered scans
        └─► Spark   : clean -> Parquet -> batch + Spark SQL                       -> recommendations, cohorts
                         └─► integration job joins MongoDB + HBase data in Spark   -> CLV, conversion funnel
                                  └─► matplotlib/seaborn figures -> PDF report
```

## Quick start
```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt   # needs Java 17 or 21
docker compose up -d                       # MongoDB + HBase (wait ~90 s)
python tools/check_environment.py --live --wait 120   # everything installed / databases up?
./run_all.sh 0.05 live                     # whole pipeline, logs in outputs/logs/
python tools/make_screenshots.py && python report/build_report.py --author "Your Name" --student-id "ID"
```

## Project parts -> files
| Part | Files |
|---|---|
| 1.1 MongoDB model + 4 aggregation pipelines | `mongodb/01_load_mongo.py`, `mongodb/02_aggregations.py` |
| 1.2 HBase schema, load, user-session queries | `hbase/01_create_schema.hbase`, `hbase/02_load_hbase.py`, `hbase/03_queries.py` |
| 2.1 Spark cleaning, recommendations, cohorts | `spark/06_clean_normalize.py`, `spark/07_batch_analytics.py` |
| 2.2 Spark SQL (6 queries) | `spark/08_spark_sql.py` -> `outputs/spark_sql_queries.sql` |
| 3 Integrated queries (CLV + funnel) | `integration/09_integrated_clv_funnel.py` |
| 4 Visualisations (6 figures) | `viz/10_visualizations.py` |
| Independent verification (re-computes results from raw JSON) | `tools/verify_results.py` |
| Report | `report/build_report.py`, `Final_Technical_Report.pdf` |

## Design notes and assumptions
* The generator emits **no `subcategory_id` on products** (unlike the brief's sample). It is assigned deterministically
  (`product number % number of subcategories`) so hierarchy and profit-margin analysis work.
* Users have **no age**, so segmentation uses purchase frequency (quintiles of the real order counts), CLV tier, cohort and geography.
* The generator stops creating transactions after its `NUM_TRANSACTIONS` cap while sessions keep being flagged "converted";
  the data-quality report counts these, and the funnel uses the first (consistent) session files loaded into HBase.
* All timestamps are relative to the day the generator runs (last 90 days).
* CLV uses an **assumed** monthly retention of 0.90 (a 90-day window cannot estimate churn).

## Offline tests (no databases needed)
`mongodb/test_offline.py`, `hbase/test_offline.py`, `integration/test_live_mock.py` use in-memory stand-ins (mongomock / a tiny fake HBase) to
check the logic (`./run_all.sh 0.02 offline`). They are **not** evidence of a real deployment.

## How this project was verified (and what was NOT)
Verified in a sandbox:
* Clean-room run on a **fresh Python 3.11 environment** installed from `requirements.txt` (PySpark 4.3, pandas 3.1), plus the original Python 3.12 environment (PySpark 4.2).
* Every script compiles on Python 3.11 and passes `pyflakes`; no Python-3.12-only syntax.
* Results cross-checked between independent engines: MongoDB pipeline revenue = Spark SQL `ROLLUP` total = plain-Python total (to the cent);
  the live path (MongoDB + HBase -> Spark) and the file path (Parquet -> Spark) give **identical** CLV and funnel tables.
* Failure messages tested: MongoDB down, HBase down, missing packages, partial-month and tiny-dataset edge cases.

**Not verified here:** a real MongoDB server and a real HBase server (no Docker in the build sandbox). Their logic was tested with in-memory stand-ins
(`mongomock` and a small fake of the happybase API). Run `./run_all.sh 0.05 live` yourself; `tools/check_environment.py --live` tells you what to fix if a database is not ready.
