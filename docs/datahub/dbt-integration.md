# dbt Integration with DataHub

DataHub can ingest dbt model metadata (lineage, schema, test results) using the `dbt-datahub` plugin. This gives DataHub awareness of the full dbt DAG: sources → raw → silver → gold.

---

## Install the Plugin

Add to `dbt/packages.yml`:

```yaml
packages:
  - package: datahub-project/datahub_dbt
    version: [">=0.10.0", "<1.0.0"]
```

Then install:

```bash
docker compose run --rm dbt dbt deps --profiles-dir /opt/dbt
```

---

## Configure the Plugin

Add a DataHub target to `dbt/profiles.yml`:

```yaml
etl_stack:
  outputs:
    # ... existing docker/k8s targets ...

    datahub:
      type: spark
      host: spark-thrift
      port: 10001
      schema: silver
      threads: 1
```

Create `dbt/datahub_config.yml`:

```yaml
datahub:
  gms_server: "http://datahub-gms:8080"
  token: null   # no auth in local dev
  emit_lineage: true
  emit_schema: true
  emit_test_results: true
  environment: "PROD"
  platform_instance: "iceberg-etl"
```

---

## Trigger dbt → DataHub Push

After running `dbt docs generate`, push the manifest to DataHub:

```bash
# Generate dbt artifacts
docker compose run --rm dbt dbt docs generate --profiles-dir /opt/dbt

# Push to DataHub using the datahub CLI
docker compose exec airflow-webserver \
  datahub ingest -c /opt/dbt/datahub_config.yml
```

Or trigger from the Airflow UI by adding a `dbt_docs_datahub_push` task to the main pipeline DAG.

---

## What Gets Pushed

When the dbt-datahub plugin runs against a dbt project, DataHub receives:

| Artifact | Description |
|----------|-------------|
| **Models as datasets** | Each dbt model (`raw_trips`, `silver_trips`, etc.) registered as a DataHub dataset |
| **Model lineage** | Source → raw → silver → gold dependency graph from `dbt/manifest.json` |
| **Column schema** | Column names and types from dbt schema YAML files |
| **Test results** | Pass/fail status for each `dbt test` run |
| **Model descriptions** | Descriptions from `dbt/models/**/*.yml` |

This enriches the lineage graph already populated by the `datahub_lineage_emitter` DAG with dbt-level model granularity.

---

## Viewing in DataHub

After pushing dbt metadata, open the DataHub UI at http://localhost:9002 and:

1. Search for `silver_trips` — you'll see the dbt model registered as a dataset
2. Click **Lineage** — the upstream source and downstream gold models are visible
3. Click **Schema** — column names and descriptions from the dbt YAML are shown
4. Click **Assertions** — dbt test results are listed under dataset health
