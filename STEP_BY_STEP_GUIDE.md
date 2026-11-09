# STEP-BY-STEP GUIDE - How to run the whole project

This guide assumes **no prior experience** with Docker, MongoDB, HBase or Spark. Follow the sections in order.
Every command is written so you can copy-paste it. Lines starting with `#` are comments, not commands.

**Time needed:** about 1 hour the first time (most of it is downloads).

---

## 0. What this project does (in 5 lines)
1. A script **generates fake e-commerce data** (users, products, orders, browsing sessions).
2. **MongoDB** stores products, users and orders, and answers questions like *"revenue per category"*.
3. **HBase** stores browsing sessions so we can quickly get *"the latest sessions of one user"*.
4. **Spark** cleans all the data, finds *"products bought together"*, builds cohorts and runs SQL.
5. A final job **combines MongoDB + HBase data in Spark** (customer lifetime value, conversion funnel), then we draw charts and build the **PDF report**.

### What is in the folder
```
ecommerce-analytics/
├── STEP_BY_STEP_GUIDE.md        <- this file
├── Final_Technical_Report.pdf   <- the report (rebuilt with YOUR numbers in step 7)
├── run_all.sh                   <- runs everything with one command
├── requirements.txt / requirements-tested.txt
├── docker-compose.yml           <- starts MongoDB + HBase
├── dataset_generator.py         <- creates the data (use --scale to choose the size)
├── mongodb/    01_load_mongo.py, 02_aggregations.py          (Part 1)
├── hbase/      01_create_schema.hbase, 02_load_hbase.py, 03_queries.py   (Part 1)
├── spark/      06_clean_normalize.py, 07_batch_analytics.py, 08_spark_sql.py   (Part 2)
├── integration/09_integrated_clv_funnel.py                   (Part 3)
├── viz/        10_visualizations.py                          (Part 4)
├── tools/      make_screenshots.py, render_terminal.py
├── report/     build_report.py                               (builds the PDF)
├── screenshots/real/            <- put YOUR real screenshots here (section 9)
└── outputs/    results (CSV), figures/, logs/, screenshots/  (created by the scripts)
```

---

## 1. What you need installed

| Tool | Version | Check with | Install |
|---|---|---|---|
| Python | 3.11 or newer (tested on 3.12) | `python3 --version` | https://www.python.org/downloads/ |
| Java | **17 or 21** (Java 11 is too old for Spark 4) | `java -version` | https://adoptium.net (needed by Spark) |
| Docker Desktop | any recent | `docker --version` | https://www.docker.com/products/docker-desktop |
| Git (optional) | any | `git --version` | https://git-scm.com |
| MongoDB Compass (optional, for screenshots) | any | - | https://www.mongodb.com/products/tools/compass |

**Computer:** 8 GB RAM minimum (16 GB is comfortable), 10 GB free disk.

### Windows users - read this
Spark and the `run_all.sh` script work best on Linux/macOS. On Windows, **use WSL2** (Windows Subsystem for Linux):
1. Open PowerShell as Administrator and run `wsl --install` (restart when asked). This installs Ubuntu.
2. Install Docker Desktop and tick **"Use the WSL 2 based engine"** (Settings -> General).
3. Open the **Ubuntu** app and do everything below inside it. Install Python and Java there:
   `sudo apt update && sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre-headless unzip`  (needs Ubuntu 22.04 or newer for Python 3.11+)

Mac and Linux users can use their normal terminal.

---

## 2. Get the code and install Python packages
```bash
unzip ecommerce-analytics.zip
cd ecommerce-analytics

python3 -m venv .venv              # creates an isolated Python environment
source .venv/bin/activate          # activate it (Windows PowerShell without WSL: .venv\Scripts\Activate.ps1)
pip install -r requirements.txt    # takes a few minutes
```
**Check everything at once:**
```bash
python tools/check_environment.py
```
Every line should say `[ OK ]` and the last line `all required checks passed`. If something says `[FAIL]` it tells you exactly what to install.
If `pip install -r requirements.txt` gives version conflicts, use the exact tested versions instead: `pip install -r requirements-tested.txt`.
You must run `source .venv/bin/activate` again every time you open a new terminal.

---

## 3. Start MongoDB and HBase
```bash
docker compose up -d
```
The first time Docker downloads the images (a few minutes). Then check:
```bash
docker ps
```
You should see two containers: **mongo** and **hbase**, both with status `Up`.
> **Take screenshot #1 now** (`docker_ps.png`, see section 9).

**Wait about 60-90 seconds** for HBase to finish starting, then open http://localhost:16010 in your browser - you should see the HBase Master page. 
(**Take screenshot #2:** `hbase_web_ui.png`.)

Check that both databases are reachable (it retries for up to 2 minutes while HBase starts):
```bash
python tools/check_environment.py --live --wait 120
```
You should see `MongoDB reachable` and `HBase Thrift server reachable` (the message about missing HBase tables is normal at this point - you create them in step 3 of section 5, or `run_all.sh` does it for you).

> **If HBase does not start or the Python test hangs:** see the Troubleshooting table (section 11).

---

## 4. Fast way: run everything with ONE command
```bash
chmod +x run_all.sh
./run_all.sh 0.05 live
```
* `0.05` = dataset size (5% of the full spec = 100,000 sessions, 25,000 orders). See section 8 to pick another size.
* `live` = use the real MongoDB and HBase from step 3.

The script first runs a **pre-flight check** (Python, Java, packages, and - in live mode - waits up to 2.5 minutes for MongoDB and HBase to be ready), then runs every step.
It prints each step and saves its console output to `outputs/logs/`. When it finishes you will see **ALL DONE**.
If a step fails, fix the problem and resume without redoing earlier steps: `FROM=06 ./run_all.sh 0.05 live` (starts at step 06).

You can skip to section 7 now - or read section 5 to understand every step.

---

## 5. Slow way: run each step yourself (and understand it)

Make sure your virtual environment is active and you are inside the `ecommerce-analytics` folder.

### Step 0 - Generate the data  *(provided generator)*
```bash
python dataset_generator.py --scale 0.05 --out data
```
**Creates** `data/users.json, products.json, categories.json, transactions.json, sessions_0.json ...`. 
**You should see** "Dataset generation complete" with 100,000 sessions and 25,000 transactions.

### Step 1 - MongoDB: load data *(Part 1.1)*
```bash
python mongodb/01_load_mongo.py --data data
```
Loads 4 collections with our document design (embedded order items, user summaries...). 
**You should see** counts like `categories 25`, `products 1,250`, `transactions 25,000`, `users 2,500`.

### Step 2 - MongoDB: aggregation pipelines *(Part 1.1)*
```bash
python mongodb/02_aggregations.py
```
Runs 4 analytics pipelines (revenue by category/month, top products, user segments, revenue by state/payment) and saves `outputs/mongo_*.csv`.
> **Screenshot #3-#5:** in MongoDB Compass connect to `mongodb://localhost:27017`, open database `ecommerce`, and capture: the list of collections with counts; one `transactions` document (showing the embedded `items`); one `users` document (showing `purchase_summary`).

### Step 3 - HBase: create the tables *(Part 1.2)*
```bash
docker exec -i hbase hbase shell < hbase/01_create_schema.hbase
```
(Windows PowerShell without WSL: `Get-Content hbase\01_create_schema.hbase | docker exec -i hbase hbase shell`)

**You should see** `Created table user_sessions` and `Created table product_metrics`. Verify:
```bash
docker exec -it hbase hbase shell
hbase> list
hbase> describe 'user_sessions'
hbase> exit
```
> **Screenshot #6:** the output of `list` and `describe 'user_sessions'` (`hbase_shell_describe.png`).

### Step 4 - HBase: load sessions *(Part 1.2)*
```bash
python hbase/02_load_hbase.py --data data --max-files 2
```
Loads the first 2 session files (a "representative subset", about 20,000 sessions) and builds the per-product daily metrics. 

### Step 5 - HBase: query a user's sessions *(Part 1.2)*
```bash
python hbase/03_queries.py user user_000042 --limit 5          # latest 5 sessions of one user
python hbase/03_queries.py user-range user_000042 2026-09-01 2026-10-31   # sessions in a date range (use dates inside YOUR data window)
python hbase/03_queries.py product prod_00123 2026-08-01 2026-12-31       # daily metrics of one product
```
The sessions come back **newest first** - that is the point of the reverse-timestamp row key.
The same query in the HBase shell:
```
hbase> scan 'user_sessions', {STARTROW => 'user_000042#', STOPROW => 'user_000042$', LIMIT => 3, COLUMNS => ['meta']}
```
> **Screenshot #7:** that shell scan (`hbase_shell_scan.png`).

### Step 6 - Spark: clean and normalise *(Part 2.1)*
```bash
python spark/06_clean_normalize.py
```
Reads all JSON, fixes types, removes duplicates, checks data quality and writes fast **Parquet** files to `data/clean/`. 
**You should see** a "DATA QUALITY REPORT". (If Spark runs out of memory: `SPARK_DRIVER_MEM=6g python spark/06_clean_normalize.py`.)

### Step 7 - Spark: recommendations and cohorts *(Part 2.1)*
```bash
python spark/07_batch_analytics.py
```
Outputs "frequently bought together" (with support/confidence/lift), "frequently viewed together", and a cohort table.

### Step 8 - Spark SQL *(Part 2.2)*
```bash
python spark/08_spark_sql.py
```
Runs 6 SQL queries (window functions, ROLLUP, funnel...). All queries are also saved in `outputs/spark_sql_queries.sql`.

### Step 9 - Integration: CLV and funnel *(Part 3)*
```bash
python integration/09_integrated_clv_funnel.py --source live
```
Reads **MongoDB** (customers + orders) and **HBase** (sessions), joins them in **Spark**, and computes Customer Lifetime Value tiers and a **strictly nested** conversion funnel (a session counts at a stage only if it also passed all earlier stages) and the conversion funnel.
(`--source files` does the same from the Parquet files - useful if a database is down.)

### Step 10 - Charts *(Part 4)*
```bash
python viz/10_visualizations.py
```
Creates 6 PNG charts in `outputs/figures/`.

### Step 11 - Verify the results *(recommended: proves your numbers are correct)*
```bash
python tools/verify_results.py
```
This re-computes the key numbers (revenue totals, top product, funnel stages, CLV of 5 sample customers, the cohort table, the top "bought together" pair)
**directly from the raw JSON files in plain Python** - no MongoDB, HBase or Spark - and compares them with what your pipelines produced.
**You should see** `[PASS]` on every line and `SUMMARY: <N> passed, 0 failed` (N is about 19). A `[FAIL]` line means one of the systems produced a wrong number: read the line, it shows both values.
(`[SKIP]` simply means you did not run that step.) `run_all.sh` runs this check automatically as its last step.

---

## 6. Where are my results?
| What | Where |
|---|---|
| All numbers (CSV) | `outputs/*.csv` |
| Six charts | `outputs/figures/*.png` |
| Console output of each step | `outputs/logs/*.log` |
| Data-quality report | `outputs/data_quality_report.txt` |
| Spark SQL text | `outputs/spark_sql_queries.sql` |
| Verification report | `outputs/logs/11_verify_results.log` |

Quick MongoDB checks (optional):
```bash
docker exec -it mongo mongosh
> use ecommerce
> show collections
> db.transactions.countDocuments()
> db.transactions.findOne()
> db.users.findOne({"purchase_summary.total_orders": {$gt: 0}})
```

---

## 7. Build your final report (PDF)
The report builder reads your result files, so **the numbers in the PDF are yours**.
```bash
python tools/make_screenshots.py                         # terminal-style captures from your logs
python report/build_report.py --author "Your Full Name" --student-id "YOUR_ID"
```
Output: `outputs/Final_Technical_Report.pdf` (about 7 pages of report + appendices).
Put your **real screenshots** (section 9) in `screenshots/real/` **before** running the second command - they are added automatically as Appendix B.
Then **read the PDF** and make sure you understand every sentence: you may be asked to explain your design.

---

## 8. Choosing the dataset size
| `--scale` | Sessions | Orders | Approx. JSON size | Suggested for |
|---|---|---|---|---|
| 0.02 | 40,000 | 10,000 | ~80 MB | quick test / low-memory laptop |
| **0.05** | 100,000 | 25,000 | ~200 MB | **recommended** (8 GB RAM) |
| 0.1 | 200,000 | 50,000 | ~400 MB | 16 GB RAM |
| 1.0 | 2,000,000 | 500,000 | ~4 GB | full specification, 16 GB+ RAM, expect long runs |

For larger sizes give Spark more memory: `SPARK_DRIVER_MEM=8g SHUFFLE_PARTS=64 ./run_all.sh 0.1 live`. 
HBase loading uses only the first `--max-files` session files by design ("representative subset"); raise it to load more.

---

## 9. Screenshot checklist for the report (real evidence)
Save these in `screenshots/real/` (PNG or JPG). The file name becomes the caption.

| # | File name | What to capture |
|---|---|---|
| 1 | `docker_ps.png` | terminal showing `docker ps` with mongo and hbase Up |
| 2 | `hbase_web_ui.png` | http://localhost:16010 - the tables list showing `user_sessions` and `product_metrics` |
| 3 | `mongo_compass_collections.png` | Compass: database `ecommerce`, 4 collections with document counts |
| 4 | `mongo_transaction_document.png` | one `transactions` document expanded (embedded `items`) |
| 5 | `mongo_user_document.png` | one `users` document (`purchase_summary`, `engagement_summary`) |
| 6 | `hbase_shell_describe.png` | `list` + `describe 'user_sessions'` |
| 7 | `hbase_shell_scan.png` | the `scan` of one user (newest first) |
| 8 | `mongo_aggregation_output.png` | terminal after `python mongodb/02_aggregations.py` |
| 9 | `spark_ui_job.png` (optional) | while a Spark step runs, open http://localhost:4040 (Jobs page) |
| 10 | `run_all_done.png` | terminal showing **ALL DONE** |

> Why this matters: the images the report generates automatically (Appendix A) come from the console logs. If you ran in `offline` mode they are labelled as such. **Real screenshots of the live databases are your proof that you really ran MongoDB and HBase.**

---

## 10. Submit on GitHub
```bash
git init
git add .
git commit -m "AUCA Big Data final project"
# create an empty repository on github.com, then:
git remote add origin https://github.com/<your-username>/<repo-name>.git
git branch -M main
git push -u origin main
```
`data/` is excluded by `.gitignore` (it is large and can be regenerated). The repository contains the code, README, this guide and the sample `outputs/`. 
Submit: the GitHub link + `Final_Technical_Report.pdf` (due 11 Nov 2026, 11:59).

---

## 11. Troubleshooting
| Problem | Likely cause | Fix |
|---|---|---|
| `java: command not found` / Spark error "JAVA_HOME" / `UnsupportedClassVersionError` | Java missing or too old (11) | Install Java 17 or 21, reopen terminal; check with `python tools/check_environment.py` |
| `docker: command not found` or daemon error | Docker Desktop not running | Start Docker Desktop, wait until it says "running" |
| Port already in use (27017 / 9090 / 16010) | another MongoDB/HBase running | Stop it, or edit the left side of the port in `docker-compose.yml` |
| `happybase` errors: `TTransportException`, timeouts | HBase still starting | Wait 1-2 min, then `docker logs hbase --tail 30`; retry |
| Same, even after waiting | image issue | Run `docker compose down -v`, then `docker compose up -d` again. Alternative image: replace `harisekhon/hbase:latest` with `dajobe/hbase` in `docker-compose.yml` (it also serves Thrift on 9090) |
| `Unknown host hbase` / HBase shell hangs when run on host | container hostname | Add the line `127.0.0.1 hbase` to `/etc/hosts` (Windows: `C:\Windows\System32\drivers\etc\hosts`) |
| `Created table` fails with "Compression ... not supported" | native codec missing | The schema already uses GZ; if you changed it back to SNAPPY, change it to GZ |
| Spark: `OutOfMemoryError` / process "Killed" | not enough RAM | Smaller `--scale` or `SPARK_DRIVER_MEM=6g`; close other programs |
| `FileNotFoundError: data/clean/...` | step 06 not run | Run step 06 first (steps depend on earlier ones) |
| `ModuleNotFoundError` | venv not active / packages missing | `source .venv/bin/activate` and `pip install -r requirements.txt` |
| A date-range query returns 0 rows | dates outside your data | Dates are relative to the day you generated data (last 90 days). Use dates from `user` query output |
| `run_all.sh: Permission denied` | not executable | `chmod +x run_all.sh` |
| Report says "offline test harness" | you used `offline` mode | Re-run with `live` mode (section 4) and rebuild the report |

### Tables already exist / I want to start HBase from scratch
`Table already exists` messages are harmless when re-running. To reset the tables: `docker exec -it hbase hbase shell`, then `disable 'user_sessions'`, `drop 'user_sessions'`, `disable 'product_metrics'`, `drop 'product_metrics'`, `exit`, and create them again (step 3). To reset MongoDB just re-run step 1 (it drops and reloads the collections).

### No Docker? (offline mode)
`./run_all.sh 0.02 offline` runs everything with in-memory stand-ins for MongoDB and HBase. It checks that the **logic** works, but it is **not** a real deployment - the report will say so, and you should not present it as live database evidence.

---

## 12. Mini-glossary
* **Document store (MongoDB):** data saved as JSON-like documents; an order can contain its list of items inside it.
* **Aggregation pipeline:** a chain of stages (`$match` -> `$group` -> `$sort`...) that MongoDB runs to compute statistics.
* **Wide-column store (HBase):** a huge sorted table; rows are found by **row key**; columns are grouped in **column families** and can differ per row.
* **Row key:** the "address" of an HBase row. Ours is `user_id#reverse_timestamp`, so one user's newest sessions are stored together.
* **Spark / DataFrame:** an engine that processes big tables in parallel; DataFrames behave like SQL tables.
* **Parquet:** a compressed column-based file format that Spark reads very fast.
* **Cohort:** a group of users that started at the same time (e.g. registered in the same month).
* **CLV (customer lifetime value):** the profit we expect from a customer in the future.
* **Funnel:** the steps shoppers go through (view -> cart -> checkout -> purchase) and how many are lost at each step.
* **Lift (recommendations):** how much more often two products are bought together than if they were independent (above 1 = positive association).
