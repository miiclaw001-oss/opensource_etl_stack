"""
etl_node_pipeline — Airflow DAG
================================
Runs the Node.js ETL pipeline (scripts/etl_pipeline.js) end-to-end.

Stages (each = one Airflow Task):
  1. check_minio        — health-check MinIO, fail fast if down
  2. generate_or_copy   — generate synthetic CSV *or* validate provided CSV
  3. run_etl_pipeline   — node scripts/etl_pipeline.js  (CSV→Transform→Parquet→MinIO)
  4. verify_uploads     — confirm objects landed in warehouse-raw / silver / gold
  5. notify             — log summary

Trigger:
  - Manually:   airflow dags trigger etl_node_pipeline
  - Scheduled:  every day at 06:00 UTC  (change schedule_interval below)
  - With params:
      airflow dags trigger etl_node_pipeline \\
        --conf '{"csv_path":"/data/my_file.csv","generate":false}'

Params (all optional):
  csv_path   str   Path to input CSV   (default: /tmp/etl_test_data.csv)
  generate   bool  Generate synthetic data if True or csv_path missing  (default: true)
  row_count  int   Rows when generating  (default: 2000)

Environment expected inside Airflow worker:
  NODE_PATH        path to node binary  (default: /usr/local/bin/node)
  ETL_SCRIPT_DIR   directory of etl_pipeline.js  (default: auto-detected)
  MINIO_ENDPOINT   (default: http://minio:9000)
  MINIO_ACCESS_KEY (default: minioadmin)
  MINIO_SECRET_KEY (default: minioadmin123)
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS    = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
AWS_REGION      = os.getenv("AWS_REGION",         "us-east-1")
POLARIS_URI     = os.getenv("POLARIS_URI",        "http://polaris:8181/api/catalog")

# Locate etl_pipeline.js relative to this DAG file or via env override
_DAG_DIR    = Path(__file__).parent                          # .../airflow/dags/
_REPO_ROOT  = _DAG_DIR.parent.parent                        # .../opensource_etl_stack/
ETL_SCRIPT  = os.getenv(
    "ETL_SCRIPT_DIR",
    str(_REPO_ROOT / "scripts" / "etl_pipeline.js"),
)
NODE_BIN    = os.getenv("NODE_PATH", "node")

RAW_BUCKET    = "warehouse-raw"
SILVER_BUCKET = "warehouse-silver"
GOLD_BUCKET   = "warehouse-gold"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":             "etl-team",
    "depends_on_past":   False,
    "email_on_failure":  False,
    "email_on_retry":    False,
    "retries":           1,
    "retry_delay":       timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

# ─────────────────────────────────────────────────────────────────────────────
# Task callables
# ─────────────────────────────────────────────────────────────────────────────

def _minio_health(**context) -> bool:
    """Returns True if MinIO responds on /minio/health/live."""
    url = f"{MINIO_ENDPOINT}/minio/health/live"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            ok = r.status == 200
            log.info(f"MinIO health {r.status}: {'OK' if ok else 'FAIL'}")
            return ok
    except Exception as exc:
        log.error(f"MinIO unreachable ({url}): {exc}")
        raise RuntimeError(
            f"MinIO not available at {MINIO_ENDPOINT}. "
            "Start with:  docker compose up -d minio && docker compose up minio-init"
        ) from exc


def _prepare_csv(**context) -> str:
    """
    Determine the CSV path to use.
    - If dag_run.conf['generate'] is True (or CSV missing) → generate synthetic data
    - Otherwise validate the provided path
    Returns the resolved CSV path and pushes it to XCom.
    """
    import csv, random
    from datetime import datetime as dt, timedelta as td

    conf       = context.get("dag_run").conf or {}
    csv_path   = conf.get("csv_path",  "/tmp/etl_airflow_input.csv")
    generate   = conf.get("generate",   True)
    row_count  = int(conf.get("row_count", 2000))

    if generate or not Path(csv_path).exists():
        log.info(f"Generating {row_count} synthetic NYC taxi rows → {csv_path}")

        headers = [
            "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime",
            "passenger_count","trip_distance","RatecodeID","store_and_fwd_flag",
            "PULocationID","DOLocationID","payment_type","fare_amount","extra",
            "mta_tax","tip_amount","tolls_amount","improvement_surcharge",
            "total_amount","congestion_surcharge",
        ]
        base = dt(2024, 1, 1)

        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=headers)
            w.writeheader()
            for _ in range(row_count):
                pickup  = base + td(days=random.randint(0,27), hours=random.randint(0,23),
                                    minutes=random.randint(0,59))
                dur     = random.randint(3, 90)
                dropoff = pickup + td(minutes=dur)
                dist    = round(random.uniform(0.5, 18.0), 2)   # cap at 18 mi → safe speed
                fare    = round(2.5 + dist * 2.5 + random.uniform(0, 5), 2)
                tip     = round(fare * random.uniform(0, 0.3), 2)
                w.writerow({
                    "VendorID":              random.randint(1, 2),
                    "tpep_pickup_datetime":  pickup.strftime("%Y-%m-%d %H:%M:%S"),
                    "tpep_dropoff_datetime": dropoff.strftime("%Y-%m-%d %H:%M:%S"),
                    "passenger_count":       random.randint(1, 6),
                    "trip_distance":         dist,
                    "RatecodeID":            1,
                    "store_and_fwd_flag":    "N",
                    "PULocationID":          random.randint(1, 265),
                    "DOLocationID":          random.randint(1, 265),
                    "payment_type":          random.randint(1, 4),
                    "fare_amount":           fare,
                    "extra":                 0.5,
                    "mta_tax":               0.5,
                    "tip_amount":            tip,
                    "tolls_amount":          0,
                    "improvement_surcharge": 0.3,
                    "total_amount":          round(fare + tip + 1.3, 2),
                    "congestion_surcharge":  2.5,
                })

        size = Path(csv_path).stat().st_size
        log.info(f"Generated CSV: {csv_path}  ({size / 1024:.1f} KB)")
    else:
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV not found: {csv_path}")
        size = Path(csv_path).stat().st_size
        log.info(f"Using existing CSV: {csv_path}  ({size / 1024:.1f} KB)")

    context["ti"].xcom_push(key="csv_path", value=csv_path)
    return csv_path


def _verify_uploads(**context) -> dict:
    """
    Check that files were actually written to MinIO by listing objects.
    Uses the MinIO S3 API (ListObjectsV2) with AWS Sig V4.
    """
    import hashlib, hmac as _hmac, time

    def sign(key, msg):
        return _hmac.new(key, msg.encode(), hashlib.sha256).digest()

    def list_bucket(bucket: str, prefix: str = "") -> list[str]:
        import urllib.parse
        now   = datetime.utcnow()
        date  = now.strftime("%Y%m%d")
        ts    = now.strftime("%Y%m%dT%H%M%SZ")
        host  = MINIO_ENDPOINT.split("://")[1]
        path  = f"/{bucket}"
        query = urllib.parse.urlencode({"list-type": "2", "prefix": prefix, "max-keys": "20"})

        payload_hash = hashlib.sha256(b"").hexdigest()
        headers = {
            "host":                 host,
            "x-amz-date":          ts,
            "x-amz-content-sha256": payload_hash,
        }
        signed_hdrs = ";".join(sorted(headers))
        canon_hdrs  = "".join(f"{k}:{v}\n" for k, v in sorted(headers.items()))
        canon_req   = f"GET\n{path}\n{query}\n{canon_hdrs}\n{signed_hdrs}\n{payload_hash}"

        scope   = f"{date}/{AWS_REGION}/s3/aws4_request"
        str2sign = f"AWS4-HMAC-SHA256\n{ts}\n{scope}\n{hashlib.sha256(canon_req.encode()).hexdigest()}"

        k = sign(sign(sign(sign(f"AWS4{MINIO_SECRET}".encode(), date), AWS_REGION), "s3"), "aws4_request")
        sig = _hmac.new(k, str2sign.encode(), hashlib.sha256).hexdigest()

        auth = f"AWS4-HMAC-SHA256 Credential={MINIO_ACCESS}/{scope}, SignedHeaders={signed_hdrs}, Signature={sig}"
        url  = f"{MINIO_ENDPOINT}{path}?{query}"

        req = urllib.request.Request(url, headers={**headers, "Authorization": auth})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                xml = r.read().decode()
                # Parse keys from XML (simple regex, no deps)
                import re
                keys = re.findall(r"<Key>(.*?)</Key>", xml)
                return keys
        except Exception as exc:
            log.warning(f"List {bucket}/{prefix} failed: {exc}")
            return []

    results = {}
    checks  = [
        (RAW_BUCKET,    "nyc_taxi/"),
        (SILVER_BUCKET, "silver/trips/"),
        (GOLD_BUCKET,   "gold/"),
    ]

    all_ok = True
    for bucket, prefix in checks:
        keys = list_bucket(bucket, prefix)
        if keys:
            log.info(f"✓  s3://{bucket}/{prefix}  →  {len(keys)} object(s)")
            for k in keys:
                log.info(f"    {k}")
            results[bucket] = keys
        else:
            log.warning(f"⚠  s3://{bucket}/{prefix}  →  no objects found")
            results[bucket] = []
            all_ok = False

    context["ti"].xcom_push(key="verify_results", value=results)

    if not all_ok:
        raise RuntimeError("Some buckets appear empty — check ETL logs above")

    return results


def _notify(**context):
    """Log final summary."""
    ti       = context["ti"]
    csv_path = ti.xcom_pull(task_ids="prepare_csv", key="csv_path") or "unknown"
    results  = ti.xcom_pull(task_ids="verify_uploads", key="verify_results") or {}

    raw_count    = len(results.get(RAW_BUCKET,    []))
    silver_count = len(results.get(SILVER_BUCKET, []))
    gold_count   = len(results.get(GOLD_BUCKET,   []))

    log.info(
        "\n"
        "╔═══════════════════════════════════════════════╗\n"
        "║   ETL Pipeline — Complete                     ║\n"
        "╠═══════════════════════════════════════════════╣\n"
        f"║  Source CSV:    {csv_path:<30} ║\n"
        f"║  Raw objects:   {raw_count:<30} ║\n"
        f"║  Silver objs:   {silver_count:<30} ║\n"
        f"║  Gold objects:  {gold_count:<30} ║\n"
        f"║  Run ID:        {context['run_id'][:30]:<30} ║\n"
        "╚═══════════════════════════════════════════════╝"
    )

# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="etl_node_pipeline",
    description="Node.js ETL: CSV → MinIO raw → Transform → Parquet → MinIO silver/gold",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # daily 06:00 UTC  — set None for manual-only
    catchup=False,
    max_active_runs=1,
    tags=["etl", "node", "iceberg", "minio", "nyc-taxi"],
    params={
        "csv_path":  Param("/tmp/etl_airflow_input.csv", type="string",
                           description="Source CSV path (inside Airflow worker)"),
        "generate":  Param(True,  type="boolean",
                           description="Generate synthetic data if True"),
        "row_count": Param(2000,  type="integer",
                           description="Rows to generate (when generate=true)"),
    },
    doc_md=__doc__,
) as dag:

    # ── 0. Start ───────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── 1. MinIO health check ──────────────────────────────────────────────────
    check_minio = PythonOperator(
        task_id="check_minio",
        python_callable=_minio_health,
        doc_md="Verify MinIO is reachable before starting the pipeline.",
    )

    # ── 2. Prepare / generate CSV ─────────────────────────────────────────────
    prepare_csv = PythonOperator(
        task_id="prepare_csv",
        python_callable=_prepare_csv,
        doc_md="Generate synthetic NYC taxi CSV or validate provided CSV path.",
    )

    # ── 3. Run Node.js ETL pipeline ───────────────────────────────────────────
    #
    #  BashOperator that calls etl_pipeline.js with:
    #    --csv  <path from XCom>
    #    --wait (poll MinIO until healthy, extra safety net)
    #
    #  The script is found via ETL_SCRIPT env or auto-detected from DAG dir.
    run_etl = BashOperator(
        task_id="run_etl_pipeline",
        bash_command="""
set -euo pipefail

# Resolve script path
ETL_JS="{{ var.value.get('ETL_SCRIPT_DIR', '') }}"
if [ -z "$ETL_JS" ]; then
    # Auto-detect: DAG is in .../airflow/dags/, script is in .../scripts/
    DAG_DIR="$(dirname "$(realpath "$0" 2>/dev/null || echo "{{ ti.dag_id }}")")"
    ETL_JS="{{ params.etl_script | default('/opt/airflow/dags/../../scripts/etl_pipeline.js') }}"
fi

# Prefer env override
ETL_JS="${ETL_SCRIPT_DIR:-{{ ti.xcom_pull(task_ids='prepare_csv', key='csv_path') | replace('/tmp/', '') | replace('.csv','') }}}"
ETL_JS_PATH="${ETL_SCRIPT_DIR:-/opt/airflow/scripts/etl_pipeline.js}"

# Fallback search order
for candidate in \
    "$ETL_JS_PATH" \
    "$(dirname "$(dirname "$AIRFLOW__CORE__DAGS_FOLDER")")/scripts/etl_pipeline.js" \
    "/opt/etl/scripts/etl_pipeline.js" \
    "/app/scripts/etl_pipeline.js"
do
    [ -f "$candidate" ] && { ETL_JS_PATH="$candidate"; break; }
done

CSV_PATH="{{ ti.xcom_pull(task_ids='prepare_csv', key='csv_path') }}"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ETL Script : $ETL_JS_PATH"
echo "  CSV        : $CSV_PATH"
echo "  MinIO      : $MINIO_ENDPOINT"
echo "  Node       : $(node --version 2>/dev/null || echo 'not found')"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ ! -f "$ETL_JS_PATH" ]; then
    echo "ERROR: etl_pipeline.js not found at $ETL_JS_PATH"
    echo "Mount the script or set ETL_SCRIPT_DIR env var."
    exit 1
fi

node "$ETL_JS_PATH" --csv "$CSV_PATH" --wait
""",
        env={
            "MINIO_ENDPOINT":   MINIO_ENDPOINT,
            "MINIO_ACCESS_KEY": MINIO_ACCESS,
            "MINIO_SECRET_KEY": MINIO_SECRET,
            "AWS_REGION":       AWS_REGION,
            # Allow override via Airflow Variable
            "ETL_SCRIPT_DIR":   os.getenv(
                "ETL_SCRIPT_DIR",
                str(_REPO_ROOT / "scripts" / "etl_pipeline.js"),
            ),
        },
        append_env=True,
        doc_md=(
            "Executes `scripts/etl_pipeline.js`.\n\n"
            "Full pipeline: CSV → MinIO raw → Transform → 10 DQ rules → "
            "Parquet → MinIO silver → Gold aggregation → MinIO gold → Iceberg manifest."
        ),
    )

    # ── 4. Verify uploads ──────────────────────────────────────────────────────
    verify = PythonOperator(
        task_id="verify_uploads",
        python_callable=_verify_uploads,
        doc_md="Lists objects in each MinIO bucket to confirm ETL wrote all layers.",
    )

    # ── 5. Notify ──────────────────────────────────────────────────────────────
    notify = PythonOperator(
        task_id="notify",
        python_callable=_notify,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Logs final pipeline summary to Airflow task log.",
    )

    # ── End ────────────────────────────────────────────────────────────────────
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependencies ───────────────────────────────────────────────────────────
    start >> check_minio >> prepare_csv >> run_etl >> verify >> notify >> end
