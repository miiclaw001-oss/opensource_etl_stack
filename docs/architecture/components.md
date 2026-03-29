# Stack Components

All services run as Docker containers on `etl-network` (bridge, `172.20.0.0/16`).

---

## Service Table

| Component | Version | Purpose | Port(s) |
|-----------|---------|---------|---------|
| **MinIO** | 2024-01-18 | S3-compatible Iceberg warehouse | 9000 (API) / 9001 (Console) |
| **Project Nessie** | 0.76.3 | Iceberg REST catalog + Git-like versioning | 19120 |
| **Apache Spark Master** | 3.5.0 | Distributed processing engine | 7077 / 8090 (UI) |
| **Apache Spark Worker** | 3.5.0 | Compute worker | 8091 (UI) |
| **Spark Thrift Server** | 3.5.0 | JDBC/ODBC endpoint for dbt | 10001 / 4040 (UI) |
| **Apache Airflow** | 2.8.0 | Pipeline orchestration + scheduling | 8080 |
| **Airbyte Server** | 0.50.45 | Data ingestion (sources → raw) | 8001 |
| **Airbyte Webapp** | 0.50.45 | Airbyte UI | 8000 |
| **dbt** | 1.7.2 | SQL transformations (raw → silver → gold) | — |
| **PostgreSQL** | 15 | Airflow metadata store | 5432 |
| **DataHub GMS** | head | Metadata graph service (REST API) | 8082 |
| **DataHub Frontend** | head | DataHub UI | 9002 |
| **DataHub Kafka** | 7.4.0 (CP) | Event streaming for DataHub | — |
| **DataHub Elasticsearch** | 7.10.1 | Search index for DataHub | — |
| **DataHub MySQL** | 5.7 | DataHub persistence | — |

---

## Component Roles

### MinIO
S3-compatible object store. Holds all Iceberg data files (Parquet) across four buckets:
- `warehouse` — general Iceberg warehouse root + Spark event logs
- `warehouse-raw` — raw ingest data, partitioned by year/month
- `warehouse-silver` — cleaned silver layer Parquet
- `warehouse-gold` — gold aggregation Parquet

### Project Nessie
Iceberg REST catalog with Git-like branching. Every table write goes through Nessie, which tracks snapshots, schema evolution, and branch history. The `main` branch is the production branch.

!!! warning "IN_MEMORY storage"
    By default, Nessie uses `IN_MEMORY` storage. Catalog metadata is lost on container restart. See [Known Issues](../operations/known-issues.md) for the JDBC persistence fix.

### Apache Spark
Runs raw→silver and silver→gold Spark jobs submitted by the Airflow DAG. The Spark Thrift Server also provides a HiveServer2-compatible endpoint that dbt connects to.

### Apache Airflow
Orchestrates the full pipeline. Two DAGs:
- `etl_iceberg_pipeline` — daily @ 6am, full ingest → transform → dbt run
- `iceberg_maintenance_dag` — weekly, expires old Iceberg snapshots
- `datahub_lineage_emitter` — daily @ 7am, pushes lineage metadata to DataHub

### Airbyte
Optional ingestion layer for non-CSV sources. Not required if using the Python ingest script.

### dbt
SQL transformation layer. Models live in `dbt/models/` and run against Spark Thrift. See [dbt Only](../use-cases/dbt-only.md) for standalone usage.

### DataHub
Metadata observability layer. Receives lineage and schema metadata via the Airflow `datahub_lineage_emitter` DAG. Provides a searchable UI at `http://localhost:9002`. See [DataHub Overview](../datahub/overview.md).
