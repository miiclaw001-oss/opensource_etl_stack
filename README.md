# Local Apache Iceberg ETL Stack

A production-grade local ETL stack built on Apache Iceberg — runs entirely on your laptop or Mac Mini.

**Stack:** MinIO · Apache Polaris · Apache Spark 3.5 · Apache Airflow 2.8 · dbt 1.7 · Airbyte 0.50 · DataHub

---

## Quick Start

```bash
git clone <repo-url>
cd opensource_etl_stack
cp .env.example .env
./scripts/setup.sh --mode docker
```

Open Airflow at http://localhost:8080 (admin / admin123), trigger `etl_iceberg_pipeline`, and watch tasks turn green.

**No Docker?** Try the instant local demo:

```bash
npm install parquetjs-lite   # optional, for Parquet output
node scripts/local_etl_demo.js --generate
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

## Service URLs (after stack start)

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Polaris REST API | http://localhost:8181/api/catalog | root / s3cr3t |
| Polaris Management | http://localhost:8181/api/management/v1 | root / s3cr3t |
| Spark UI | http://localhost:8090 | — |
| DataHub | http://localhost:9002 | datahub / datahub |

---

## License

MIT
