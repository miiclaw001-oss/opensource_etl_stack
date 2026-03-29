# Architecture Overview

The stack implements a classic medallion architecture (Raw → Silver → Gold) on top of Apache Iceberg, with Nessie providing Git-like catalog versioning and DataHub providing lineage observability.

---

## Main Architecture

```mermaid
flowchart LR
    subgraph Sources
        A1[NYC Taxi CSV]
        A2[Custom CSV]
        A3[Airbyte Connector]
    end

    subgraph Ingestion
        B1[Python Ingest Script]
        B2[Airbyte 0.50]
    end

    subgraph Storage["☁️ MinIO (S3-compatible)"]
        C1[(warehouse-raw)]
        C2[(warehouse-silver)]
        C3[(warehouse-gold)]
    end

    subgraph Catalog
        D1[Project Nessie\nICEBERG REST API]
    end

    subgraph Processing
        E1[Apache Spark 3.5\n+ Iceberg Runtime]
        E2[dbt 1.7\n+ Spark Thrift]
    end

    subgraph Orchestration
        F1[Apache Airflow 2.8\nDAG: etl_iceberg_pipeline]
    end

    subgraph Observability
        G1[DataHub\nLineage & Catalog]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B2
    B1 --> C1
    B2 --> C1
    C1 --> E1
    E1 --> C2
    E1 --> D1
    C2 --> E2
    E2 --> C3
    E2 --> D1
    C3 --> D1
    F1 --> B1
    F1 --> E1
    F1 --> E2
    F1 --> G1
```

**Layers:**

- **Sources** — NYC Taxi CSV files or custom data via Airbyte connectors
- **Ingestion** — Python scripts or Airbyte write raw data to MinIO `warehouse-raw`
- **Storage** — MinIO stores all Parquet/Iceberg files across raw/silver/gold buckets
- **Catalog** — Nessie tracks all Iceberg table metadata with branching support
- **Processing** — Spark runs raw→silver transforms; dbt runs silver→gold SQL models
- **Orchestration** — Airflow's `etl_iceberg_pipeline` DAG ties it all together
- **Observability** — DataHub's `datahub_lineage_emitter` DAG pushes lineage metadata after each run

---

## Medallion Layers

```mermaid
flowchart LR
    subgraph Raw["🥉 Raw Layer\nwarehouse-raw"]
        R1[CSV / Parquet\nNYC Taxi Fields\nPartitioned by date]
    end

    subgraph Silver["🥈 Silver Layer\nnessie.silver.trips"]
        S1[Cleaned & Typed\nOutliers Removed\nFeature Engineered\nIncremental Merge]
    end

    subgraph Gold["🥇 Gold Layer\nnessie.gold.*"]
        G1[daily_summary\nHourly KPIs]
        G2[location_performance\nZone Rankings]
    end

    R1 -->|Spark raw_to_silver| S1
    S1 -->|dbt silver_trips| S1
    S1 -->|Spark silver_to_gold| G1
    S1 -->|dbt gold models| G2
```

- **Raw** — unmodified source files, partitioned by `year=YYYY/month=MM/`
- **Silver** — cleaned, typed, enriched Iceberg table with business rules applied
- **Gold** — aggregated KPI tables optimized for analytics queries

---

## Airflow DAG

```mermaid
flowchart TD
    start([▶ Start]) --> check{check_source_data}
    check -->|data available| ingest[ingest_nyc_taxi\nDownload or generate data]
    check -->|no data| skip[skip_pipeline]

    ingest --> upload[upload_to_minio_raw\nCSV → warehouse-raw]
    upload --> script1[write_raw_to_silver_script]
    script1 --> spark1[spark_raw_to_silver\nCSV → Iceberg silver]
    spark1 --> script2[write_silver_to_gold_script]
    script2 --> spark2[spark_silver_to_gold\nsilver → gold aggregates]

    spark2 --> dbt_run[dbt_run\nSQL transforms]
    spark2 --> dq[data_quality_checks\nNessie + row validation]

    dbt_run --> dbt_test[dbt_test]
    dbt_test --> notify[notify_completion]
    dq --> notify
    skip --> notify
    notify --> done([✅ End])

    style spark1 fill:#ff9f43,color:#000
    style spark2 fill:#ff9f43,color:#000
    style dbt_run fill:#48dbfb,color:#000
    style dbt_test fill:#48dbfb,color:#000
    style dq fill:#ff6b6b,color:#fff
    style notify fill:#1dd1a1,color:#000
```

The DAG runs daily at 6am. It is paused at creation — toggle it on in the Airflow UI or trigger manually.

---

## dbt Lineage

```mermaid
flowchart LR
    src[(iceberg_raw\n.nyc_taxi_trips)] --> raw[raw_trips\nview]
    raw --> silver[silver_trips\nincremental iceberg\nmerge strategy]
    silver --> g1[gold_daily_summary\npartitioned by date]
    silver --> g2[gold_location_performance\nzone KPIs]

    style src fill:#636e72,color:#fff
    style raw fill:#fdcb6e,color:#000
    style silver fill:#74b9ff,color:#000
    style g1 fill:#55efc4,color:#000
    style g2 fill:#55efc4,color:#000
```

dbt models are defined in `dbt/models/` and run against Spark Thrift Server on port 10001.
