# Quick Start

Get the full stack running in three steps.

---

## 1. Clone and Configure

```bash
git clone <repo-url>
cd opensource_etl_stack
cp .env.example .env
```

Edit `.env` if you want custom MinIO credentials or passwords. The defaults work fine for local development.

## 2. Start the Stack

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

## 3. Trigger the Pipeline

Open the Airflow UI and run the DAG:

```
http://localhost:8080   →  admin / admin123
```

1. Find `etl_iceberg_pipeline`
2. Toggle it ON (paused by default)
3. Click ▶ **Trigger DAG**
4. Watch tasks turn green as they complete (~10–15 min)

Or trigger from the CLI:

```bash
docker compose exec airflow-webserver airflow dags trigger etl_iceberg_pipeline
```

---

## Verify Results

```bash
# Check Nessie tables
curl http://localhost:8181/api/catalog/trees/tree/main/entries

# Check raw data in MinIO
docker compose exec minio-init mc ls local/warehouse-raw/nyc_taxi/

# Run the full test suite
./scripts/test_pipeline.sh --mode docker
```

---

## Service URLs

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Airbyte | http://localhost:8000 | airbyte / password |
| Spark UI | http://localhost:8090 | — |
| Nessie API | http://localhost:8181/api/catalog | — |
| DataHub | http://localhost:9002 | datahub / datahub |
| DataHub GMS | http://localhost:8082 | — |

---

## Tear Down

```bash
docker compose down          # stop, keep volumes
docker compose down -v       # stop and delete ALL data
```
