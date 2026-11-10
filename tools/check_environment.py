"""
Pre-flight check: tells you in plain English what is missing BEFORE you run the pipeline.

  python tools/check_environment.py                 # Python, Java, packages
  python tools/check_environment.py --live          # ... plus MongoDB and HBase connectivity
  python tools/check_environment.py --live --wait 120 --need-tables
        --wait N       keep retrying the database checks for up to N seconds (HBase needs ~60-90 s to start)
        --need-tables  fail if the HBase tables user_sessions / product_metrics do not exist yet
Exit code 0 = everything required is OK, 1 = something required is missing.
"""
import argparse, importlib, os, re, subprocess, sys, time

OK, WARN, FAIL = "[ OK ]", "[WARN]", "[FAIL]"
failures = 0


def report(status, msg, hint=""):
    global failures
    print(f"{status} {msg}")
    if hint and status != OK:
        print(f"       -> {hint}")
    if status == FAIL:
        failures += 1


def check_python():
    v = sys.version_info
    if v < (3, 10):
        report(FAIL, f"Python {v.major}.{v.minor} is too old", "install Python 3.11 or newer")
    elif v < (3, 11):
        report(WARN, f"Python {v.major}.{v.minor} - the project was tested on 3.12; 3.11+ recommended")
    else:
        report(OK, f"Python {v.major}.{v.minor}.{v.micro}")


def java_major():
    try:
        out = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=20)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    m = re.search(r'version "(\d+)(?:\.(\d+))?', out.stderr + out.stdout)
    if not m:
        return None
    major = int(m.group(1))
    return int(m.group(2)) if major == 1 and m.group(2) else major    # "1.8.0" -> 8


def check_packages():
    spark_major = None
    for mod, pip_name in [("faker", "faker"), ("pandas", "pandas"), ("pymongo", "pymongo"), ("happybase", "happybase"),
                          ("pyspark", "pyspark"), ("matplotlib", "matplotlib"), ("seaborn", "seaborn"),
                          ("pyarrow", "pyarrow"), ("reportlab", "reportlab"), ("PIL", "pillow")]:
        try:
            m = importlib.import_module(mod)
            report(OK, f"package {pip_name} {getattr(m, '__version__', '')}")
            if mod == "pyspark":
                spark_major = int(m.__version__.split(".")[0])
        except Exception as e:  # ImportError or a broken install
            report(FAIL, f"package {pip_name} cannot be imported ({type(e).__name__})", "run: pip install -r requirements.txt  (is your virtual environment active?)")
    j = java_major()
    if j is None:
        report(FAIL, "Java not found", "install Java 17 or 21 (https://adoptium.net) and reopen the terminal")
    elif spark_major and spark_major >= 4 and j < 17:
        report(FAIL, f"Java {j} is too old for PySpark {spark_major}.x", "install Java 17 or 21")
    elif j < 11:
        report(FAIL, f"Java {j} is too old", "install Java 17 or 21")
    else:
        report(OK, f"Java {j}")


def check_live(wait, need_tables):
    deadline = time.time() + wait
    mongo_ok = hbase_ok = False
    while True:
        if not mongo_ok:
            try:
                from pymongo import MongoClient
                MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), serverSelectionTimeoutMS=3000).admin.command("ping")
                mongo_ok = True
            except Exception:
                pass
        if not hbase_ok:
            try:
                import happybase
                conn = happybase.Connection(os.getenv("HBASE_HOST", "localhost"), port=int(os.getenv("HBASE_PORT", "9090")), timeout=5000)
                tables = [t.decode() for t in conn.tables()]
                hbase_ok = True
            except Exception:
                pass
        if (mongo_ok and hbase_ok) or time.time() >= deadline:
            break
        print("       ... waiting for the databases to start (retrying every 5 s)")
        time.sleep(5)
    if mongo_ok:
        report(OK, "MongoDB reachable on localhost:27017")
    else:
        report(FAIL, "MongoDB not reachable on localhost:27017", "run: docker compose up -d   then check: docker ps")
    if hbase_ok:
        report(OK, "HBase Thrift server reachable on localhost:9090")
        missing = [t for t in ("user_sessions", "product_metrics") if t not in tables]
        if missing:
            report(FAIL if need_tables else WARN, f"HBase tables not created yet: {', '.join(missing)}",
                   "run: docker exec -i hbase hbase shell < hbase/01_create_schema.hbase")
        else:
            report(OK, "HBase tables user_sessions and product_metrics exist")
    else:
        report(FAIL, "HBase Thrift server not reachable on localhost:9090",
               "HBase needs 60-90 s to start; wait and retry, or see docker logs hbase --tail 30 (guide, section 11)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true"); ap.add_argument("--wait", type=int, default=0)
    ap.add_argument("--need-tables", action="store_true")
    a = ap.parse_args()
    check_python(); check_packages()
    if a.live:
        check_live(a.wait, a.need_tables)
    print("\nRESULT:", "all required checks passed" if failures == 0 else f"{failures} problem(s) found - fix them and run this check again")
    sys.exit(1 if failures else 0)
