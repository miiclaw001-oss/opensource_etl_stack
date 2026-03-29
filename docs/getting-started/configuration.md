# Configuration

---

## Environment Variables (`.env`)

Copy `.env.example` to `.env` before starting:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` | MinIO secret key |

!!! warning "Security"
    All defaults are for local development only. Change credentials in `.env` before exposing the stack on a network.

---

## Airflow Configuration

Airflow is configured entirely via environment variables in `docker-compose.yml` (the `x-airflow-common` block). Key settings:

| Variable | Value | Notes |
|----------|-------|-------|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Single-node executor, fine for local dev |
| `AIRFLOW__CORE__FERNET_KEY` | hardcoded | Move to `.env` for production use |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | `etl-stack-secret-key-2024` | Move to `.env` for production use |
| `MINIO_ENDPOINT` | `http://minio:9000` | Internal Docker DNS |
| `NESSIE_URI` | `http://nessie:19120/api/v1` | Internal Docker DNS |

To generate a secure Fernet key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Reducing RAM Usage (8 GB Laptop)

Edit `docker-compose.yml` to reduce Spark worker memory:

```yaml
# Option A: Skip Airbyte entirely (use Python ingest)
docker compose up -d minio nessie spark-master spark-worker postgres airflow-webserver airflow-scheduler

# Option B: Reduce Spark worker limits
spark-worker:
  environment:
    SPARK_WORKER_MEMORY: "1g"   # was "2g"
  deploy:
    resources:
      limits:
        memory: 1.5g            # was 2.5g
```

See [Resource Requirements](../reference/resources.md) for full per-service limits.

---

## Nessie Persistent Storage

By default, Nessie uses `IN_MEMORY` storage (catalog is wiped on container restart). To enable JDBC persistence via PostgreSQL:

```yaml
nessie:
  environment:
    NESSIE_VERSION_STORE_TYPE: "JDBC"
    QUARKUS_DATASOURCE_JDBC_URL: "jdbc:postgresql://postgres:5432/nessie"
    QUARKUS_DATASOURCE_USERNAME: "airflow"
    QUARKUS_DATASOURCE_PASSWORD: "airflow123"
```

Also add to `scripts/init_db.sql`:

```sql
CREATE DATABASE nessie;
GRANT ALL PRIVILEGES ON DATABASE nessie TO airflow;
```

---

## dbt Targets

The `dbt/profiles.yml` defines two targets:

- **`docker`** — connects to `spark-thrift:10001` (for use inside Docker Compose)
- **`k8s`** — connects via port-forwarded `localhost:10001` (for Kubernetes mode)

To run dbt locally (outside Docker):

```bash
pip install dbt-spark==1.7.2 PyHive thrift
cd dbt/
dbt debug --profiles-dir . --target docker
```
