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
| `POLARIS_URI` | `http://polaris:8181/api/catalog` | Iceberg REST Catalog (Polaris) |
| `POLARIS_CREDENTIAL` | `root:s3cr3t` | Dev only — change in production |
| `MELTANO_PROJECT_ROOT` | `/opt/meltano` | Meltano project mount path |

To generate a secure Fernet key:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## Stack Variants

### Lean Stack (empfohlen)

```bash
# Lean: Polaris + Meltano, kein DataHub (~4-5 GB RAM, 100% MIT/Apache 2.0)
docker compose -f docker-compose.lean.yml up -d

# Nur Core (kein Meltano/dbt):
docker compose -f docker-compose.lean.yml up -d \
  minio minio-init polaris polaris-init \
  spark-master spark-worker spark-thrift \
  postgres airflow-init airflow-webserver airflow-scheduler
```

### Full Stack

```bash
# Inkl. DataHub (~8-10 GB RAM)
docker compose up -d
```

---

## Reducing RAM Usage

Spark Worker Memory reduzieren in `docker-compose.yml` / `docker-compose.lean.yml`:

```yaml
spark-worker:
  environment:
    SPARK_WORKER_MEMORY: "1g"   # war "2g"
  deploy:
    resources:
      limits:
        memory: 1.5g            # war 2.5g
```

Siehe [Resource Requirements](../reference/resources.md) für alle Service-Limits.

---

## Polaris: Persistente Catalog-Daten

Polaris verwendet standardmäßig In-Memory-Storage (Catalog wird bei Neustart zurückgesetzt).
Der `polaris-init`-Container legt den `warehouse`-Catalog bei jedem Start neu an — für lokale Entwicklung ausreichend.

Für persistenten Storage: Polaris unterstützt Eclipse Link / relational persistence (siehe [Polaris Docs](https://polaris.apache.org/docs/)).

---

## Meltano: On-Prem → Cloud Migration

In `meltano/meltano.yml` nur eine Zeile ändern:

```yaml
# On-Prem (MinIO):
s3_endpoint_url: http://minio:9000

# Cloud (AWS S3) — auskommentieren:
# s3_endpoint_url: http://minio:9000
```

Alle DAGs, Spark Jobs und dbt-Modelle bleiben unverändert.

---

## dbt Targets

Die `dbt/profiles.yml` definiert zwei Targets:

- **`docker`** — verbindet zu `spark-thrift:10001` (innerhalb Docker Compose)
- **`k8s`** — via port-forwarded `localhost:10001` (Kubernetes-Modus)

Lokal ausführen (außerhalb Docker):

```bash
pip install dbt-spark==1.7.2 PyHive thrift
cd dbt/
dbt debug --profiles-dir . --target docker
```
