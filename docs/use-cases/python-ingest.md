# Use Case 4: Python Ingest Only (No Airbyte)

**Best for:** quick testing, when you have your own CSV/Parquet data.

---

## Step 1 — Generate Sample Data

```bash
python3 sample_data/generate_sample_data.py --rows 10000 --output /tmp/nyc_taxi.csv

# Or for a specific month:
python3 sample_data/generate_sample_data.py \
    --rows 50000 \
    --year 2024 \
    --month 3 \
    --output /tmp/nyc_taxi_2024_03.csv
```

## Step 2 — Upload to MinIO Raw Bucket

Make sure MinIO is running first:

```bash
docker compose up -d minio && docker compose up minio-init
```

Then upload:

```bash
# Using AWS CLI
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin123 \
aws s3 cp /tmp/nyc_taxi.csv \
    s3://warehouse-raw/nyc_taxi/year=2024/month=01/nyc_taxi.csv \
    --endpoint-url http://localhost:9000 \
    --region us-east-1

# Or using mc
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc cp /tmp/nyc_taxi.csv local/warehouse-raw/nyc_taxi/year=2024/month=01/
```

## Step 3 — Run Spark Transformation (Raw → Silver)

Make sure Spark + Nessie + MinIO are running:

```bash
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.warehouse=s3://warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /path/to/raw_to_silver.py
```

## Step 4 — Verify the Silver Table

```bash
curl http://localhost:19120/api/v1/trees/tree/main/entries \
    | python3 -m json.tool | grep -A2 "silver"
```

!!! tip
    Install `pyarrow` for true Parquet output from the sample data generator:
    ```bash
    pip install pyarrow
    ```
    Without it, the generator silently falls back to CSV.
