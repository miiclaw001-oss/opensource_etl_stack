# Use Case 4: Node.js / Python Ingest (kein Meltano nötig)

**Best for:** schnelles Testen, eigene CSV/Parquet-Daten, kein Docker nötig.

---

## Option A — Node.js ETL Pipeline (empfohlen, 0 Dependencies)

Läuft ohne Docker, ohne Spark, ohne Meltano:

```bash
# Optional: echten Parquet-Output statt JSONL
npm install parquetjs-lite

# Synthetic data generieren + komplette Pipeline
node scripts/etl_pipeline.js --generate

# Eigene CSV
node scripts/etl_pipeline.js --csv /tmp/meine_daten.csv

# Mit MinIO (wenn Docker läuft) — wartet automatisch bis MinIO ready
node scripts/etl_pipeline.js --generate --wait
```

**Was passiert:**
1. CSV einlesen / generieren
2. Raw CSV → MinIO `warehouse-raw` hochladen
3. Transform: Typing, DQ (10 Rules), Feature Engineering
4. Parquet schreiben → MinIO `warehouse-silver`
5. Gold-Aggregation → MinIO `warehouse-gold`
6. Iceberg-Manifest schreiben

---

## Option B — Meltano (Singer-basiert, MIT)

```bash
# Meltano-Container starten (einmalig)
docker compose -f docker-compose.lean.yml run --rm meltano \
  meltano install

# CSV → MinIO
docker compose -f docker-compose.lean.yml run --rm meltano \
  meltano run tap-csv target-s3

# Postgres → MinIO
docker compose -f docker-compose.lean.yml run --rm meltano \
  meltano run tap-postgres target-s3

# REST API → MinIO
docker compose -f docker-compose.lean.yml run --rm meltano \
  meltano run tap-rest-api-msdk target-s3
```

---

## Option C — Manuell via AWS CLI / mc

MinIO zuerst starten:

```bash
docker compose -f docker-compose.lean.yml up -d minio minio-init
```

Upload:

```bash
# AWS CLI
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin123 \
aws s3 cp /tmp/nyc_taxi.csv \
    s3://warehouse-raw/nyc_taxi/year=2024/month=01/nyc_taxi.csv \
    --endpoint-url http://localhost:9000 \
    --region us-east-1

# MinIO mc
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc cp /tmp/nyc_taxi.csv local/warehouse-raw/nyc_taxi/year=2024/month=01/
```

---

## Spark Transformation: Raw → Silver (Polaris REST Catalog)

Spark + Polaris + MinIO müssen laufen:

```bash
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.polaris=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.polaris.type=rest \
    --conf spark.sql.catalog.polaris.uri=http://polaris:8181/api/catalog \
    --conf spark.sql.catalog.polaris.warehouse=warehouse \
    --conf spark.sql.catalog.polaris.credential=root:s3cr3t \
    --conf spark.sql.catalog.polaris.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /path/to/raw_to_silver.py
```

---

## Verify: Polaris Catalog API

```bash
# Token holen
TOKEN=$(curl -sf -X POST 'http://localhost:8181/api/catalog/v1/oauth/tokens' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

# Namespaces anzeigen
curl -s "http://localhost:8181/api/catalog/v1/warehouse/namespaces" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# Tables in silver
curl -s "http://localhost:8181/api/catalog/v1/warehouse/namespaces/silver/tables" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```
