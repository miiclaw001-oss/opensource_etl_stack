# Use Case 7: Nessie Time-Travel & Branching

**Best for:** safe experimentation, A/B testing transforms, schema changes.

---

## List Branches and Tags

```bash
curl -s http://localhost:19120/api/v1/trees | python3 -m json.tool
```

---

## Create a Feature Branch

```python
import requests

BASE = "http://localhost:19120/api/v1"

# Get main branch hash
main = requests.get(f"{BASE}/trees/tree/main").json()
main_hash = main["hash"]

# Create feature branch
requests.post(f"{BASE}/trees/tree", json={
    "name": "feature/new-gold-model",
    "type": "BRANCH",
    "hash": main_hash,
    "sourceRefName": "main"
}).raise_for_status()

print("Branch created: feature/new-gold-model")
```

---

## Use a Branch in Spark

```python
# Point Spark at a specific branch
spark.conf.set("spark.sql.catalog.nessie.ref", "feature/new-gold-model")

# Now all writes go to the feature branch
new_gold.writeTo("nessie.gold.new_model").using("iceberg").createOrReplace()

# Merge: use the Nessie merge API when ready
# In production, use Nessie merge API to promote changes to main
```

---

## Time-Travel with Spark

```python
# By timestamp
spark.read.format("iceberg") \
    .option("as-of-timestamp", "2024-01-15T12:00:00.000+00:00") \
    .load("nessie.silver.trips") \
    .count()

# By snapshot ID (get from .snapshots table)
snapshots = spark.sql("SELECT * FROM nessie.silver.trips.snapshots").collect()
snapshot_id = snapshots[-2]["snapshot_id"]  # second-to-last snapshot

spark.read.format("iceberg") \
    .option("snapshot-id", snapshot_id) \
    .load("nessie.silver.trips") \
    .count()
```

---

## Expire Old Snapshots (Cleanup)

The `iceberg_maintenance_dag` runs weekly automatically, or trigger it manually:

```bash
docker compose exec airflow-webserver \
    airflow dags trigger iceberg_maintenance_dag
```
