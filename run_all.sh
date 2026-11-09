#!/usr/bin/env bash
# ============================================================================
# Runs the WHOLE project in order and saves each step's console output to outputs/logs/
#
#   ./run_all.sh [SCALE] [MODE]
#     SCALE : dataset size, 1.0 = full spec (2M sessions). Default 0.05 (100k sessions)
#     MODE  : live    -> uses real MongoDB + HBase (start them first: docker compose up -d)
#             offline -> uses in-memory test stand-ins (no Docker needed; checks the logic only)
#   Examples:  ./run_all.sh 0.05 live        ./run_all.sh 0.02 offline
#
#   Resume after a failure : FROM=06 ./run_all.sh 0.05 live    (skips steps numbered below 06)
#   Stop early             : TO=04   ./run_all.sh 0.05 live    (skips steps numbered above 04)
#   More Spark memory      : SPARK_DRIVER_MEM=8g ./run_all.sh 0.1 live
# ============================================================================
set -uo pipefail
SCALE="${1:-0.05}"; MODE="${2:-live}"
cd "$(dirname "$0")"
if [ "$MODE" != "live" ] && [ "$MODE" != "offline" ]; then echo "MODE must be 'live' or 'offline' (got '$MODE')"; exit 2; fi

# pick the python interpreter (the virtual environment's 'python' if active, otherwise python3)
PY="${PYTHON:-}"
if [ -z "$PY" ]; then if command -v python >/dev/null 2>&1; then PY=python; else PY=python3; fi; fi

mkdir -p outputs/logs data
NOISE='WARN|WARNING|Warning|log4j|log level|setLogLevel|require_minimum|Using Spark|NativeCodeLoader|SPARK_LOCAL_IP|loopback'
FROM="${FROM:-00}"; TO="${TO:-99}"

step () {  # step <NN_log-name> <command...>
  local name="$1"; shift
  if [[ "${name:0:2}" < "$FROM" ]] || [[ "${name:0:2}" > "$TO" ]]; then echo "(skipping $name)"; return 0; fi
  echo; echo "================ $name ================"
  "$@" 2>&1 | grep -v -E "$NOISE" | tee "outputs/logs/$name.log"
  if [ "${PIPESTATUS[0]}" -ne 0 ]; then echo "!!! step '$name' FAILED - see outputs/logs/$name.log"; exit 1; fi
}

echo "{\"mode\": \"$MODE\", \"scale\": $SCALE}" > outputs/run_info.json

echo "---- pre-flight check ----"
if [ "$MODE" = "live" ]; then
  "$PY" tools/check_environment.py --live --wait 150 || { echo "Fix the problems above, then run again."; exit 1; }
else
  "$PY" tools/check_environment.py || { echo "Fix the problems above, then run again."; exit 1; }
fi

step 00_generate_data        "$PY" dataset_generator.py --scale "$SCALE" --out data

if [ "$MODE" = "live" ]; then
  step 01_mongo_load          "$PY" mongodb/01_load_mongo.py --data data
  step 02_mongo_aggregations  "$PY" mongodb/02_aggregations.py
  if [[ "03" > "$FROM" || "03" == "$FROM" ]] && [[ "03" < "$TO" || "03" == "$TO" ]]; then
    echo; echo "================ 03_hbase_schema ================"
    # 'already exists' messages are harmless when you re-run
    docker exec -i hbase hbase shell < hbase/01_create_schema.hbase 2>&1 | tee outputs/logs/03_hbase_schema.log
    "$PY" tools/check_environment.py --live --need-tables > /dev/null || { echo "!!! HBase tables were not created - see outputs/logs/03_hbase_schema.log"; exit 1; }
  fi
  step 04_hbase_load          "$PY" hbase/02_load_hbase.py --data data --max-files 2
  step 05_hbase_queries       bash -c "$PY hbase/03_queries.py user user_000042 --limit 3; $PY hbase/03_queries.py product prod_00123 2000-01-01 2100-01-01 | tail -5"
else
  step 01_mongo_02_aggregations_offline  "$PY" mongodb/test_offline.py
  step 04_hbase_offline                  "$PY" hbase/test_offline.py
fi

step 06_spark_clean          "$PY" spark/06_clean_normalize.py
step 07_spark_batch          "$PY" spark/07_batch_analytics.py
step 08_spark_sql            "$PY" spark/08_spark_sql.py
if [ "$MODE" = "live" ]; then
  step 09_integration        "$PY" integration/09_integrated_clv_funnel.py --source live
else
  step 09_integration        "$PY" integration/test_live_mock.py
fi
step 10_visualizations       "$PY" viz/10_visualizations.py
step 11_verify_results      "$PY" tools/verify_results.py      # independent re-computation from the raw JSON
if [ "$TO" != "99" ] || [ "$FROM" != "00" ]; then echo; echo "Finished the requested steps ($FROM..$TO)."; fi
echo; echo "ALL DONE (for the steps run). Results: outputs/*.csv   figures: outputs/figures/   logs: outputs/logs/"
echo "Next: $PY tools/make_screenshots.py && $PY report/build_report.py --author \"Your Name\" --student-id \"ID\""
