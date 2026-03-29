# Iceberg Features Used

Apache Iceberg provides the table format for all Silver and Gold layer data. The following features are actively used in this stack.

---

## Feature Table

| Feature | Where Used | Notes |
|---------|-----------|-------|
| **Partitioned tables** | `silver.trips` (by day), `gold.daily_summary` (by date) | Efficient time-based queries and partition pruning |
| **Merge-on-read deletes** | `silver.trips` | Efficient upserts via dbt incremental merge strategy |
| **Time-travel** | All Iceberg tables | `FOR SYSTEM_TIME AS OF` or snapshot-id queries |
| **Schema evolution** | Nessie catalog | Branch-based schema changes without breaking readers |
| **Snapshot management** | `iceberg_maintenance_dag` | Weekly expiry of old snapshots to reclaim storage |
| **ZSTD compression** | All Parquet files | ~35% better compression ratio than Snappy |
| **Nessie branching** | Catalog-level | Git-like branches for safe experimentation |

---

## Time-Travel Examples

```python
# By timestamp (PySpark)
spark.sql("""
    SELECT * FROM nessie.silver.trips
    FOR SYSTEM_TIME AS OF '2024-01-15 12:00:00'
""").show()

# By snapshot ID
spark.read.format("iceberg") \
    .option("snapshot-id", 1234567890) \
    .load("nessie.silver.trips") \
    .count()

# List all snapshots
spark.sql("SELECT * FROM nessie.silver.trips.snapshots").show()
```

---

## Nessie Branching

Nessie adds Git-like semantics on top of Iceberg:

- Create branches for schema experiments without touching `main`
- Merge branches when the experiment is validated
- Tag snapshots for reproducible queries

```python
import requests
BASE = "http://localhost:19120/api/v1"

# Create a branch
main = requests.get(f"{BASE}/trees/tree/main").json()
requests.post(f"{BASE}/trees/tree", json={
    "name": "experiment/new-features",
    "type": "BRANCH",
    "hash": main["hash"],
    "sourceRefName": "main"
})
```

See [Nessie Time-Travel & Branching](../use-cases/nessie-branching.md) for full examples.

---

## Snapshot Expiry

The `iceberg_maintenance_dag` (weekly, Sundays) expires snapshots older than 7 days and removes orphan files. This prevents unbounded storage growth:

```bash
# Trigger manually
docker compose exec airflow-webserver \
    airflow dags trigger iceberg_maintenance_dag
```
