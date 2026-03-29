# Local Apache Iceberg ETL Stack

A production-grade local ETL stack built on Apache Iceberg — runs entirely on your laptop or Mac Mini.

**Key links:** [Airflow](http://localhost:8080) · [MinIO](http://localhost:9001) · [Nessie API](http://localhost:19120/api/v1) · [Spark UI](http://localhost:8090) · [DataHub](http://localhost:9002)

---

## Architecture

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

---

## Get Started

<div class="grid cards" markdown>

- **Quick Start**

    New here? Start the full stack in one command.

    [→ Quick Start](getting-started/quickstart.md)

- **Local Demo (No Docker)**

    Try the pipeline instantly with Node.js — no Docker needed.

    [→ Local Demo](use-cases/local-demo.md)

- **Architecture Deep Dive**

    Understand the medallion layers, components, and data flow.

    [→ Architecture Overview](architecture/overview.md)

- **DataHub Integration**

    Explore data lineage and metadata observability.

    [→ DataHub Setup](datahub/setup.md)

</div>

---

## What's in the Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| MinIO | 2024-01-18 | S3-compatible Iceberg warehouse |
| Project Nessie | 0.76.3 | Iceberg REST catalog + Git-like versioning |
| Apache Spark | 3.5.0 | Distributed processing engine |
| Apache Airflow | 2.8.0 | Pipeline orchestration |
| Airbyte | 0.50.45 | Data ingestion |
| dbt | 1.7.2 | SQL transformations |
| DataHub | head | Data lineage & metadata catalog |

Full details → [Components](architecture/components.md)
