# Local Apache Iceberg ETL Stack

A production-grade local ETL stack built on Apache Iceberg — runs entirely on your laptop or Mac Mini.

**Stack:** MinIO · Apache Polaris · Apache Spark 3.5 · Apache Airflow 2.8 · Meltano · dbt 1.7 · DataHub (optional)

> 💡 **Lean Stack** (`docker-compose.lean.yml`) — kein DataHub, Meltano statt Airbyte, ~4–5 GB RAM, 100% MIT/Apache 2.0

---

## Quick Start

### Option A — Lean Stack (empfohlen, ~4–5 GB RAM, 100% OSS)
```bash
git clone <repo-url>
cd opensource_etl_stack
docker compose -f docker-compose.lean.yml up -d
```
Airflow: http://localhost:8080 (admin / admin123) → DAG `etl_node_pipeline` triggern.

### Option B — Full Stack (mit DataHub, ~8–10 GB RAM)
```bash
cp .env.example .env
./scripts/setup.sh --mode docker
# oder:
docker compose up -d
```

### Option C — Lokal ohne Docker (sofort, 0 MB Download)
```bash
npm install parquetjs-lite   # optional, für echten Parquet-Output
node scripts/etl_pipeline.js --generate
# → 10/10 DQ Rules ✅, Parquet-Output in /tmp/
```

---

## Documentation

Full documentation is in the [`docs/`](docs/) directory and served with MkDocs Material.

```bash
pip install mkdocs-material mkdocs-mermaid2-plugin
mkdocs serve
# → http://localhost:8000
```

| Section | Description |
|---------|-------------|
| [Architecture](docs/architecture/overview.md) | Mermaid diagrams, medallion layers, Airflow DAG |
| [Getting Started](docs/getting-started/quickstart.md) | Prerequisites, quick start, configuration |
| [Use Cases](docs/use-cases/docker.md) | Docker, k8s, local demo, dbt-only, Spark shell |
| [DataHub](docs/datahub/overview.md) | Lineage, schema metadata, dbt integration |
| [Reference](docs/reference/service-urls.md) | Service URLs, resource requirements, Iceberg features |
| [Operations](docs/operations/testing.md) | Testing, troubleshooting, known issues |

---

## Architecture

The stack follows a **medallion architecture**: Raw → Silver → Gold, with Apache Polaris as the Iceberg REST Catalog.

```
CSV/Parquet → MinIO (raw) → Spark+Iceberg → Polaris (catalog) → MinIO (silver/gold) → dbt → DataHub
```

## Catalog: Apache Polaris

[Apache Polaris](https://polaris.apache.org) ist der Iceberg REST Catalog in diesem Stack — open-source (ASF), ursprünglich von Snowflake entwickelt und an die Apache Software Foundation gespendet.

### Warum Polaris statt Nessie?
- Implementiert den **Iceberg REST Catalog Spec** nativ (kein proprietäres Protokoll)
- **Vended Credentials** — Polaris reicht temporäre S3-Credentials direkt an Clients weiter
- Kompatibel mit jedem Iceberg-Client (Spark, Trino, Flink, PyIceberg, etc.)
- Multi-Catalog, RBAC, Principal Roles

### Endpoints
| Endpoint | Beschreibung |
|----------|-------------|
| `http://localhost:8181/api/catalog` | Iceberg REST API (Spark, Trino, …) |
| `http://localhost:8181/api/management/v1` | Management API (Catalogs, Principals, Roles) |
| `http://localhost:8181/healthcheck` | Health Check |

### Default Credentials (nur Dev!)
- Client ID: `root`
- Client Secret: `s3cr3t`

### Spark Usage
```python
# Tabelle lesen
df = spark.table("polaris.silver.trips")

# Namespace anlegen
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

# In Iceberg-Tabelle schreiben
df.writeTo("polaris.silver.trips").using("iceberg").createOrReplace()
```

### Bootstrap
Der `polaris-init` Container legt beim Start automatisch den `warehouse`-Catalog an.

---

## Service URLs

### Lean Stack (`docker-compose.lean.yml`)

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Polaris REST API | http://localhost:8181/api/catalog | root / s3cr3t |
| Polaris Management | http://localhost:8181/api/management/v1 | root / s3cr3t |
| Spark UI | http://localhost:8090 | — |

### Full Stack (`docker-compose.yml`)

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Polaris REST API | http://localhost:8181/api/catalog | root / s3cr3t |
| Spark UI | http://localhost:8090 | — |
| DataHub | http://localhost:9002 | datahub / datahub |

## Ingestion: Meltano

```bash
# Connector hinzufügen (einmalig)
docker compose -f docker-compose.lean.yml run --rm meltano \
  meltano add extractor tap-postgres

# Sync starten
docker compose -f docker-compose.lean.yml run --rm meltano \
  meltano run tap-postgres target-s3

# On-Prem → Cloud: in meltano/meltano.yml AWS_ENDPOINT_URL auskommentieren
```

Verfügbare Taps: `tap-csv`, `tap-postgres`, `tap-rest-api-msdk` (vorkonfiguriert)

---

## License

MIT
