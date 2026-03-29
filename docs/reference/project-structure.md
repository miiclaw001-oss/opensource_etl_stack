# Project Structure

```
opensource_etl_stack/
├── docker-compose.yml            # Full local stack (ETL + DataHub)
├── mkdocs.yml                    # MkDocs documentation config
├── kind-config.yaml              # kind cluster config (k8s mode)
├── .env.example                  # Environment variables template
│
├── docs/                         # MkDocs documentation source
│   ├── index.md
│   ├── architecture/
│   ├── getting-started/
│   ├── use-cases/
│   ├── datahub/
│   ├── reference/
│   └── operations/
│
├── k8s/                          # Kubernetes manifests
│   ├── namespaces/namespaces.yaml
│   ├── minio/minio.yaml
│   ├── nessie/nessie.yaml
│   ├── spark/spark.yaml
│   ├── airflow/
│   │   ├── postgres.yaml
│   │   └── airflow.yaml
│   └── airbyte/airbyte.yaml
│
├── spark/conf/
│   └── spark-defaults.conf       # Spark + Iceberg + Nessie config
│
├── dbt/                          # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml              # docker + k8s targets
│   ├── macros/iceberg_helpers.sql
│   ├── models/
│   │   ├── raw/
│   │   │   ├── sources.yml       # Iceberg source declaration
│   │   │   └── raw_trips.sql     # View over raw Iceberg table
│   │   ├── silver/
│   │   │   ├── silver_trips.sql  # Cleaned + enriched (incremental merge)
│   │   │   └── silver_trips.yml  # Schema tests
│   │   └── gold/
│   │       ├── gold_daily_summary.sql
│   │       ├── gold_daily_summary.yml
│   │       └── gold_location_performance.sql
│   └── tests/
│       ├── assert_gold_no_negative_revenue.sql
│       └── assert_silver_trip_duration_positive.sql
│
├── airflow/dags/
│   ├── etl_pipeline.py              # Main ETL DAG (daily @ 6am)
│   ├── iceberg_maintenance.py       # Snapshot expiry DAG (weekly)
│   └── datahub_lineage_emitter.py   # DataHub metadata push DAG (daily @ 7am)
│
├── sample_data/
│   └── generate_sample_data.py   # Synthetic NYC taxi data generator
│
└── scripts/
    ├── setup.sh                  # One-shot setup (docker or k8s)
    ├── test_pipeline.sh          # End-to-end test suite
    ├── init_db.sql               # PostgreSQL initialization
    └── datahub_check.sh          # DataHub health check + instructions
```
