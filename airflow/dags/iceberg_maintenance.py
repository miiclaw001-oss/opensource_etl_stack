"""
Iceberg Maintenance DAG
Runs periodic Iceberg table maintenance: expire snapshots, remove orphan files, rewrite data files.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "etl-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

SPARK_MAINTENANCE_SCRIPT = """
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("IcebergMaintenance").getOrCreate()

tables = [
    "nessie.silver.trips",
    "nessie.gold.daily_summary",
]

cutoff = datetime.utcnow() - timedelta(days=7)
cutoff_ms = int(cutoff.timestamp() * 1000)

for table in tables:
    try:
        print(f"Maintaining table: {table}")

        # Expire old snapshots
        spark.sql(f\"\"\"
            CALL nessie.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}',
                retain_last => 3
            )
        \"\"\")
        print(f"  Expired snapshots for {table}")

        # Remove orphan files
        spark.sql(f\"\"\"
            CALL nessie.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
            )
        \"\"\")
        print(f"  Removed orphan files for {table}")

        # Rewrite small data files
        spark.sql(f\"\"\"
            CALL nessie.system.rewrite_data_files(
                table => '{table}',
                strategy => 'sort',
                sort_order => 'zorder(pickup_date)'
            )
        \"\"\")
        print(f"  Rewrote data files for {table}")

    except Exception as e:
        print(f"  Warning: maintenance failed for {table}: {e}")

print("Iceberg maintenance complete!")
spark.stop()
"""

with DAG(
    dag_id="iceberg_maintenance",
    description="Weekly Iceberg table maintenance (expire snapshots, compact files)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * 0",  # Sundays at 2am
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "iceberg", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    write_script = BashOperator(
        task_id="write_maintenance_script",
        bash_command=f"""
cat > /tmp/iceberg_maintenance.py << 'PYEOF'
{SPARK_MAINTENANCE_SCRIPT}
PYEOF
echo "Maintenance script written"
""",
    )

    run_maintenance = BashOperator(
        task_id="run_iceberg_maintenance",
        bash_command="""
spark-submit \
  --master spark://spark-master.spark.svc.cluster.local:7077 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie.nessie.svc.cluster.local:19120/api/v1 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3://warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc.cluster.local:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /tmp/iceberg_maintenance.py
""",
    )

    end = EmptyOperator(task_id="end")

    start >> write_script >> run_maintenance >> end
