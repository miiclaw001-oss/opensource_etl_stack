# Use Case 1: Full Stack (Docker Compose)

**Best for:** local development, demos, first-time setup.

**RAM needed:** ~8 GB

---

## Step 1 — Prepare Environment

```bash
cd opensource_etl_stack
cp .env.example .env
# Edit .env if you want custom MinIO credentials or passwords
```

## Step 2 — Start Everything

```bash
./scripts/setup.sh --mode docker
```

This script:

1. Pulls all Docker images
2. Starts MinIO + Nessie + Postgres
3. Initializes MinIO buckets (`warehouse`, `warehouse-raw`, `warehouse-silver`, `warehouse-gold`)
4. Starts Spark master + worker
5. Initializes Airflow DB and creates admin user
6. Starts Airflow webserver + scheduler
7. Starts Airbyte server + webapp
8. Waits for all health checks to pass

**Expected time:** 3–10 minutes on first run (image downloads), 60–90 seconds on subsequent runs.

## Step 3 — Open the UIs

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Airbyte | http://localhost:8000 | airbyte / password |
| Spark UI | http://localhost:8090 | — |
| Nessie API | http://localhost:19120/api/v1 | — |
| DataHub | http://localhost:9002 | datahub / datahub |

## Step 4 — Trigger the Pipeline

**Via Airflow UI:**

1. Go to http://localhost:8080
2. Find `etl_iceberg_pipeline` DAG
3. Toggle it ON (paused by default)
4. Click ▶ **Trigger DAG**
5. Watch the graph view — tasks turn green as they complete

**Via CLI:**

```bash
docker compose exec airflow-webserver airflow dags trigger etl_iceberg_pipeline
```

## Step 5 — Verify Results

```bash
# Check Nessie tables
curl http://localhost:19120/api/v1/trees/tree/main/entries

# Check raw data in MinIO
docker compose exec minio-init mc ls local/warehouse-raw/nyc_taxi/

# Query silver table via Spark
docker compose exec spark-master pyspark --packages \
  org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,...
# Then: spark.table("nessie.silver.trips").count()
```

---

## Tear Down

```bash
docker compose down          # stop, keep volumes
docker compose down -v       # stop and delete ALL data
```
