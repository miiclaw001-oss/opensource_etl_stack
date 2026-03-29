"""
meltano_ingest — Airflow DAG
============================
Triggert Meltano-Sync-Jobs via BashOperator.

Meltano läuft als CLI inside dem Airflow-Worker-Container — kein separater
Server nötig. Das Meltano-Projekt liegt in ./meltano/ und ist in den
Worker gemountet (/opt/meltano).

Pipelines:
  csv_to_s3        tap-csv      → target-s3 (MinIO raw)
  postgres_to_s3   tap-postgres → target-s3 (MinIO raw)
  rest_api_to_s3   tap-rest-api → target-s3 (MinIO raw)

Nach dem Ingest: ETL-Pipeline (etl_node_pipeline oder etl_iceberg_pipeline)
liest die Daten aus MinIO und schreibt via Polaris nach Iceberg.

On-Prem → Cloud Migration:
  Nur AWS_ENDPOINT_URL in meltano.yml entfernen → schreibt direkt nach AWS S3.
  DAG bleibt unverändert.

Trigger:
  Manuell:   airflow dags trigger meltano_ingest
  Scheduled: täglich 05:00 UTC (vor dem ETL-Pipeline-Run um 06:00)
  Mit Job:   airflow dags trigger meltano_ingest --conf '{"job":"postgres_to_s3"}'
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Config ─────────────────────────────────────────────────────────────────────
MELTANO_ROOT = os.getenv("MELTANO_PROJECT_ROOT", "/opt/meltano")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

default_args = {
    "owner":            "etl-team",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# ── Meltano base command ────────────────────────────────────────────────────────
MELTANO_CMD = f"meltano --project-root {MELTANO_ROOT}"

# ── Env für alle Meltano-Tasks ──────────────────────────────────────────────────
MELTANO_ENV = {
    "AWS_ACCESS_KEY_ID":     os.getenv("AWS_ACCESS_KEY_ID",     "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_DEFAULT_REGION":    os.getenv("AWS_DEFAULT_REGION",    "us-east-1"),
    # On-Prem: MinIO endpoint — für Cloud einfach weglassen
    "AWS_ENDPOINT_URL":      os.getenv("MINIO_ENDPOINT",        "http://minio:9000"),
    "MELTANO_ENVIRONMENT":   "dev",
}


def _pick_job(**context) -> str:
    """Branch: welcher Meltano-Job soll laufen?"""
    job = (context.get("dag_run").conf or {}).get("job", "csv_to_s3")
    valid = {"csv_to_s3", "postgres_to_s3", "rest_api_to_s3", "all"}
    if job not in valid:
        raise ValueError(f"Unknown job: {job}. Valid: {valid}")
    return f"run_{job}" if job != "all" else "run_csv_to_s3"


with DAG(
    dag_id="meltano_ingest",
    description="Meltano ELT: Quellen → MinIO raw (Singer-basiert, MIT)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 5 * * *",   # 05:00 UTC täglich — vor ETL-Pipeline
    catchup=False,
    max_active_runs=1,
    tags=["meltano", "ingest", "singer", "elt"],
    params={
        "job": Param(
            "csv_to_s3",
            type="string",
            enum=["csv_to_s3", "postgres_to_s3", "rest_api_to_s3", "all"],
            description="Meltano-Job der ausgeführt werden soll",
        ),
    },
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Branch: welcher Job? ────────────────────────────────────────────────────
    pick_job = BranchPythonOperator(
        task_id="pick_job",
        python_callable=_pick_job,
    )

    # ── Meltano install (stellt sicher dass alle pip-Deps da sind) ─────────────
    meltano_install = BashOperator(
        task_id="meltano_install",
        bash_command=f"""
set -euo pipefail
echo "Installing Meltano plugins..."
{MELTANO_CMD} install
echo "Install complete."
""",
        env=MELTANO_ENV,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Installiert alle Meltano-Plugins (idempotent, cached in .meltano/).",
    )

    # ── Job 1: CSV → S3 ────────────────────────────────────────────────────────
    run_csv_to_s3 = BashOperator(
        task_id="run_csv_to_s3",
        bash_command=f"""
set -euo pipefail
echo "Running: tap-csv → target-s3"
echo "  Source: CSV files in /tmp/etl-data/"
echo "  Target: s3://warehouse-raw/meltano/ (via MinIO)"
{MELTANO_CMD} run tap-csv target-s3
echo "CSV sync complete."
""",
        env=MELTANO_ENV,
        append_env=True,
        doc_md="tap-csv → target-s3 (MinIO warehouse-raw)",
    )

    # ── Job 2: Postgres → S3 ───────────────────────────────────────────────────
    run_postgres_to_s3 = BashOperator(
        task_id="run_postgres_to_s3",
        bash_command=f"""
set -euo pipefail
echo "Running: tap-postgres → target-s3"
echo "  Source: postgres (host: ${{POSTGRES_HOST:-postgres}})"
echo "  Target: s3://warehouse-raw/meltano/ (via MinIO)"
{MELTANO_CMD} run tap-postgres target-s3
echo "Postgres sync complete."
""",
        env=MELTANO_ENV,
        append_env=True,
        doc_md="tap-postgres → target-s3 (MinIO warehouse-raw)",
    )

    # ── Job 3: REST API → S3 ───────────────────────────────────────────────────
    run_rest_api_to_s3 = BashOperator(
        task_id="run_rest_api_to_s3",
        bash_command=f"""
set -euo pipefail
echo "Running: tap-rest-api-msdk → target-s3"
echo "  Target: s3://warehouse-raw/meltano/ (via MinIO)"
{MELTANO_CMD} run tap-rest-api-msdk target-s3
echo "REST API sync complete."
""",
        env=MELTANO_ENV,
        append_env=True,
        doc_md="tap-rest-api-msdk → target-s3 (MinIO warehouse-raw)",
    )

    # ── Notify ──────────────────────────────────────────────────────────────────
    notify = BashOperator(
        task_id="notify",
        bash_command="""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Meltano Ingest complete"
echo "  Data landed in: s3://warehouse-raw/meltano/"
echo "  Next: etl_node_pipeline or etl_iceberg_pipeline"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
""",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Dependencies ────────────────────────────────────────────────────────────
    start >> pick_job >> [run_csv_to_s3, run_postgres_to_s3, run_rest_api_to_s3]

    # meltano install läuft vor jedem Job
    pick_job >> meltano_install
    meltano_install >> [run_csv_to_s3, run_postgres_to_s3, run_rest_api_to_s3]

    [run_csv_to_s3, run_postgres_to_s3, run_rest_api_to_s3] >> notify >> end
