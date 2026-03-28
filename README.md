# Local Apache Iceberg ETL Stack

A production-grade local ETL stack built on Apache Iceberg, running entirely on your laptop.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         LOCAL ETL STACK                                 │
│                                                                         │
│  Sources           Ingestion        Storage          Processing         │
│                                                                         │
│  ┌─────────┐      ┌─────────┐      ┌─────────┐      ┌──────────────┐  │
│  │  NYC    │─────▶│ Airbyte │─────▶│  MinIO  │◀────▶│    Spark     │  │
│  │  Taxi   │      │  0.50   │      │  (S3)   │      │   3.5 +      │  │
│  │  Data   │      └─────────┘      │         │      │  Iceberg     │  │
│  └─────────┘                       │warehouse│      └──────┬───────┘  │
│                                    │  -raw   │             │          │
│  ┌─────────┐      ┌─────────┐      │  -silver│      ┌──────▼───────┐  │
│  │ Custom  │─────▶│ Python  │─────▶│  -gold  │      │   Nessie     │  │
│  │  CSV    │      │  Script │      └────▲────┘      │   Catalog    │  │
│  └─────────┘      └─────────┘           │           │  (REST API)  │  │
│                                         │           └──────────────┘  │
│  ┌──────────────────────────────────────┼──────────────────────────┐  │
│  │              Orchestration           │                          │  │
│  │                                      │                          │  │
│  │  ┌─────────────────────────────────────────────────────────┐   │  │
│  │  │                    Apache Airflow 2.8                    │   │  │
│  │  │  ingest → upload_raw → spark_raw_to_silver               │   │  │
│  │  │         → spark_silver_to_gold → dbt_run → dbt_test     │   │  │
│  │  └─────────────────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    Transformations (dbt)                         │  │
│  │                                                                  │  │
│  │  nessie.raw.trips ──▶ nessie.silver.trips ──▶ nessie.gold.*    │  │
│  │  (raw_trips view)     (cleaned + enriched)   (daily_summary)   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Stack Components

| Component | Version | Purpose | Port |
|-----------|---------|---------|------|
| **MinIO** | RELEASE.2024-01-18 | S3-compatible Iceberg warehouse | 9000/9001 |
| **Project Nessie** | 0.76.3 | Iceberg REST catalog + versioning | 19120 |
| **Apache Spark** | 3.5.0 | Distributed processing engine | 7077/8090 |
| **Apache Airflow** | 2.8.0 | Pipeline orchestration | 8080 |
| **Airbyte** | 0.50.45 | Data ingestion (sources → raw) | 8000 |
| **dbt** | 1.7.2 | SQL transformations | — |
| **PostgreSQL** | 15 | Airflow metadata store | 5432 |
| **kind** | latest | Local Kubernetes cluster | — |

## Data Pipeline

```
Raw CSV/Parquet                Silver Iceberg Table           Gold Iceberg Tables
(MinIO warehouse-raw)    →     (nessie.silver.trips)    →    (nessie.gold.*)

- Raw NYC taxi fields          - Cleaned + typed               - daily_summary
- Partitioned by date          - Outliers removed              - location_performance
- Loaded by Airbyte or         - Feature-engineered            - Hourly aggregations
  Python ingest script         - Incremental merge             - Revenue KPIs
                               - Partitioned by day            - Peak hour flags
```

### dbt Lineage

```
sources.iceberg_raw.nyc_taxi_trips
    └── raw_trips (view)
            └── silver_trips (incremental iceberg)
                    ├── gold_daily_summary (table iceberg)
                    └── gold_location_performance (table iceberg)
```

## Quick Start

### Option A: Docker Compose (recommended for local dev)

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd opensource_etl_stack

# 2. Copy env file
cp .env.example .env

# 3. Start the full stack
./scripts/setup.sh --mode docker

# 4. Run the test pipeline
./scripts/test_pipeline.sh --mode docker
```

### Option B: Kubernetes (kind)

```bash
# Prerequisites: kind, kubectl, helm

# 1. Start the kind cluster + deploy all services
./scripts/setup.sh --mode k8s

# 2. Run end-to-end tests
./scripts/test_pipeline.sh --mode k8s
```

### Option C: Manual Docker Compose

```bash
cp .env.example .env

# Start services individually
docker compose up -d minio nessie postgres
docker compose up minio-init           # initialize buckets
docker compose up -d spark-master spark-worker
docker compose run --rm airflow-init  # init DB + admin user
docker compose up -d airflow-webserver airflow-scheduler
docker compose up -d airbyte-db airbyte-server airbyte-webapp
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| MinIO API | http://localhost:9000 | — |
| Nessie REST API | http://localhost:19120/api/v1 | — |
| Airflow UI | http://localhost:8080 | admin / admin123 |
| Airbyte UI | http://localhost:8000 | airbyte / password |
| Spark Master UI | http://localhost:8090 | — |
| Spark Thrift | localhost:10001 | — |

## Generating Sample Data

```bash
# Generate 10,000 synthetic NYC taxi trips
python3 sample_data/generate_sample_data.py --rows 10000 --output /tmp/nyc_taxi_sample.csv

# Generate 100k rows for a specific month
python3 sample_data/generate_sample_data.py \
    --rows 100000 \
    --year 2024 \
    --month 3 \
    --output /tmp/nyc_taxi_2024_03.csv
```

## Running the Pipeline

### Trigger via Airflow UI

1. Open http://localhost:8080
2. Enable the `etl_iceberg_pipeline` DAG
3. Click "Trigger DAG"
4. Monitor progress in Graph view

### Trigger via CLI

```bash
# Docker Compose
docker compose exec airflow-webserver airflow dags trigger etl_iceberg_pipeline

# Kubernetes
kubectl exec -n airflow deploy/airflow-webserver -- \
    airflow dags trigger etl_iceberg_pipeline
```

### Run Spark jobs manually

```bash
# Docker Compose: run raw → silver transformation
docker compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/work/raw_to_silver.py
```

### Run dbt transforms

```bash
# Docker Compose
docker compose run --rm dbt dbt run --profiles-dir /opt/dbt --target docker

# Test
docker compose run --rm dbt dbt test --profiles-dir /opt/dbt --target docker

# Generate docs
docker compose run --rm dbt dbt docs generate --profiles-dir /opt/dbt
```

## Querying Iceberg Tables

### Via Spark shell

```bash
docker compose exec spark-master pyspark \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,...

# In PySpark:
spark.table("nessie.silver.trips").show()
spark.table("nessie.gold.daily_summary").show()
spark.sql("SELECT COUNT(*) FROM nessie.gold.daily_summary").show()
```

### Via Nessie REST API

```bash
# List all tables on main branch
curl http://localhost:19120/api/v1/trees/tree/main/entries

# Get table metadata
curl http://localhost:19120/api/v1/trees/tree/main/entries?filter=entry.contentType=='ICEBERG_TABLE'
```

### Via MinIO (inspect raw files)

```bash
# Using mc CLI
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc ls local/warehouse/silver/trips/
mc ls local/warehouse-raw/nyc_taxi/
```

## Nessie Branching (Time-travel)

```python
# Create a new branch for experimentation
import requests

base = "http://localhost:19120/api/v1"
main_hash = requests.get(f"{base}/trees/tree/main").json()["hash"]

# Create feature branch
requests.post(f"{base}/trees/tree", json={
    "name": "feature-new-gold-model",
    "type": "BRANCH",
    "hash": main_hash,
    "sourceRefName": "main",
})

# Query silver trips on the new branch
# spark.conf.set("spark.sql.catalog.nessie.ref", "feature-new-gold-model")
```

## Resource Usage

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
| **Total** | **~11 GB** | **~11** |

> **Tip:** For laptops with 8GB RAM, disable Airbyte and Spark Thrift, and reduce worker memory to 1.5g.

## Project Structure

```
opensource_etl_stack/
├── kind-config.yaml              # kind cluster configuration
├── docker-compose.yml            # Docker Compose (alternative to k8s)
├── .env.example                  # Environment variables template
│
├── k8s/                          # Kubernetes manifests
│   ├── namespaces/
│   │   └── namespaces.yaml
│   ├── minio/
│   │   └── minio.yaml            # MinIO deployment + NodePort
│   ├── nessie/
│   │   └── nessie.yaml           # Nessie catalog
│   ├── spark/
│   │   └── spark.yaml            # Spark master + worker + history
│   ├── airflow/
│   │   ├── postgres.yaml         # Airflow metadata DB
│   │   └── airflow.yaml          # Webserver + scheduler
│   └── airbyte/
│       └── airbyte.yaml          # Airbyte server + webapp
│
├── spark/
│   └── conf/
│       └── spark-defaults.conf   # Spark + Iceberg + Nessie config
│
├── dbt/                          # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── raw/
│   │   │   ├── sources.yml
│   │   │   └── raw_trips.sql
│   │   ├── silver/
│   │   │   ├── silver_trips.sql  # Cleaned + enriched trips
│   │   │   └── silver_trips.yml
│   │   └── gold/
│   │       ├── gold_daily_summary.sql    # Hourly aggregations
│   │       ├── gold_daily_summary.yml
│   │       └── gold_location_performance.sql
│   ├── macros/
│   │   └── iceberg_helpers.sql
│   └── tests/
│       ├── assert_gold_no_negative_revenue.sql
│       └── assert_silver_trip_duration_positive.sql
│
├── airflow/
│   └── dags/
│       ├── etl_pipeline.py       # Main ETL pipeline DAG
│       └── iceberg_maintenance.py # Weekly Iceberg maintenance
│
├── sample_data/
│   └── generate_sample_data.py   # Synthetic NYC taxi data generator
│
└── scripts/
    ├── setup.sh                  # One-shot setup script
    ├── test_pipeline.sh          # End-to-end test script
    └── init_db.sql               # PostgreSQL initialization
```

## Troubleshooting

### MinIO not starting
```bash
docker compose logs minio
# Check disk space: df -h
# Check port conflicts: lsof -i :9000
```

### Nessie 404 on /api/v1
```bash
# Nessie uses /api/v1 prefix
curl http://localhost:19120/api/v1/config
# Health: curl http://localhost:19120/q/health
```

### Spark jobs failing on S3
```bash
# Verify S3A credentials in spark-defaults.conf
# Check MinIO bucket exists
docker compose exec minio mc ls local/warehouse/
```

### Airflow DAGs not showing
```bash
# Check for import errors
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow dags list-import-errors
```

### Out of memory
```bash
# Reduce Spark worker memory in docker-compose.yml:
# SPARK_WORKER_MEMORY: "1g"
# Or scale down replicas
docker compose scale spark-worker=0
```

## Iceberg Features Used

- **Partitioned tables**: `silver.trips` partitioned by day, `gold.daily_summary` by date
- **Merge-on-read deletes**: configured for efficient upserts
- **Time-travel**: all Iceberg tables support `FOR SYSTEM_TIME AS OF` queries
- **Schema evolution**: Nessie catalog supports schema changes with version history
- **Snapshot management**: `iceberg_maintenance` DAG expires old snapshots weekly
- **ZSTD compression**: all parquet files use ZSTD for 30-40% better compression

## Contributing

1. Fork the repository
2. Create a feature branch on Nessie: `feature/your-feature`
3. Make changes and test with `./scripts/test_pipeline.sh`
4. Submit a PR

## License

MIT
