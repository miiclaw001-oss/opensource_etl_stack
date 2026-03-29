# Resource Requirements

---

## By Mode

| Mode | Min RAM | Recommended RAM | Disk |
|------|---------|-----------------|------|
| Full Docker Compose (with DataHub) | 12 GB | 16 GB | 15 GB |
| Full Docker Compose (without DataHub) | 8 GB | 12 GB | 10 GB |
| Without Airbyte | 6 GB | 8 GB | 8 GB |
| Without Airbyte + Thrift | 4 GB | 6 GB | 6 GB |
| Kubernetes (kind) | 12 GB | 16 GB | 15 GB |
| dbt only (local) | 2 GB | 4 GB | 2 GB |

---

## Per-Service Limits (Docker Compose)

### Core ETL Services

| Service | Memory Limit | CPU Limit |
|---------|-------------|-----------|
| MinIO | 1 GB | 1.0 |
| Nessie | 1 GB | 1.0 |
| Spark Master | 1 GB | 1.0 |
| Spark Worker | 2.5 GB | 2.0 |
| Spark Thrift | 2 GB | 2.0 |
| Airflow Webserver | 1 GB | 1.0 |
| Airflow Scheduler | 1 GB | 1.0 |
| PostgreSQL | 512 MB | 0.5 |
| Airbyte Server | 1 GB | 1.0 |
| Airbyte DB | 256 MB | 0.25 |
| Airbyte Webapp | 256 MB | 0.5 |
| **Core Total** | **~11.5 GB** | **~11.25** |

### DataHub Services

| Service | Memory Limit |
|---------|-------------|
| datahub-elasticsearch | 1 GB |
| datahub-gms | 1.5 GB |
| datahub-kafka | 768 MB |
| datahub-mysql | 512 MB |
| datahub-frontend | 512 MB |
| datahub-zookeeper | 256 MB |
| datahub-schema-registry | 256 MB |
| **DataHub Total** | **~4.8 GB** |

---

## Reduce RAM Usage (8 GB Laptop)

### Option A: Skip DataHub

Don't start DataHub services:

```bash
docker compose up -d minio nessie spark-master spark-worker postgres airflow-webserver airflow-scheduler
```

### Option B: Skip Airbyte (use Python ingest instead)

```bash
docker compose up -d minio nessie spark-master spark-worker postgres airflow-webserver airflow-scheduler
```

### Option C: Reduce Spark Worker Memory

Edit `docker-compose.yml`:

```yaml
spark-worker:
  environment:
    SPARK_WORKER_MEMORY: "1g"   # was "2g"
  deploy:
    resources:
      limits:
        memory: 1.5g            # was 2.5g
```
