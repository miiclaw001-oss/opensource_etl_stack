# Troubleshooting

---

## MinIO Won't Start

```bash
docker compose logs minio
# Check disk space:
df -h
# Check port conflicts:
lsof -i :9000
lsof -i :9001
```

---

## Nessie Returns 404 on `/api/v1`

```bash
# Correct health endpoint:
curl http://localhost:19120/q/health
# Correct API base:
curl http://localhost:19120/api/v1/config
# If 404 on all: check if Nessie started fully
docker compose logs nessie | tail -30
```

---

## Spark Jobs Fail with S3 Errors

```bash
# Verify S3A credentials
docker compose exec spark-master env | grep AWS

# Check MinIO bucket exists and is accessible
docker compose exec spark-master curl -s http://minio:9000/minio/health/live

# Common fix: ensure path-style access is enabled
# spark.hadoop.fs.s3a.path.style.access=true  ← must be set
```

---

## Airflow DAGs Not Showing Up

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-import-errors
# Check the DAGs volume is mounted correctly
docker compose exec airflow-webserver ls /opt/airflow/dags/
```

---

## Spark Thrift Server Never Becomes Healthy

The Thrift server downloads ~500 MB of JARs on first start. This is normal.

```bash
docker compose logs spark-thrift -f
# Wait until you see: "ThriftBinaryCLIService listening on 0.0.0.0/0.0.0.0:10001"
```

---

## Out of Memory / Container Killed

```bash
# Check which containers are using most memory
docker stats --no-stream

# Scale down Spark worker
docker compose up -d --scale spark-worker=0

# Or reduce limits in docker-compose.yml
# See: docs/reference/resources.md
```

---

## Airflow Tasks Stuck in "Queued" State

```bash
# Restart the scheduler
docker compose restart airflow-scheduler

# Check for zombie tasks
docker compose exec airflow-webserver airflow tasks clear etl_iceberg_pipeline -y
```

---

## dbt Fails to Connect to Spark Thrift

```bash
# Verify Thrift is listening
nc -zv localhost 10001

# Check dbt profiles.yml has correct host:
# docker mode: host=spark-thrift, port=10001
# local mode:  host=localhost, port=10001

# Test dbt connection
docker compose run --rm dbt dbt debug --profiles-dir /opt/dbt
```

---

## Airbyte Webapp Shows Blank Page

Airbyte's webapp takes 2–3 minutes to fully initialize after the server is healthy. Wait and refresh.

```bash
docker compose logs airbyte-server | grep -i error
```

---

## DataHub GMS Never Becomes Healthy

DataHub has a long startup chain (Zookeeper → Kafka → Schema Registry → Elasticsearch → MySQL → GMS). Full startup can take 3–5 minutes.

```bash
# Check GMS logs
docker compose logs datahub-gms -f

# Check dependency setup containers completed
docker compose ps datahub-kafka-setup datahub-elasticsearch-setup datahub-mysql-setup

# Use the health check script
./scripts/datahub_check.sh
```

---

## DataHub Lineage Emitter DAG Fails

```bash
# Check if DataHub GMS is reachable from Airflow
docker compose exec airflow-webserver curl http://datahub-gms:8080/health

# Check the DAG logs
docker compose exec airflow-webserver \
    airflow tasks logs datahub_lineage_emitter emit_dataset_metadata <run_id>
```

!!! note
    The emitter fails gracefully — if DataHub is unreachable, tasks log a warning and skip. The DAG should still succeed (green).
