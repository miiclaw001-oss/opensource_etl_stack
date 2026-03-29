# Use Case 5: dbt Transformations Only

**Best for:** when data is already in MinIO/Nessie and you only want to run SQL transforms.

**Requires:** Spark Thrift Server running.

---

## Step 1 — Start Spark Thrift

```bash
docker compose up -d minio nessie spark-master spark-worker
docker compose up -d spark-thrift
# Wait ~5 min for JARs to download on first run
```

## Step 2 — Verify Thrift Server Is Up

```bash
# Port 10001 should be listening
nc -zv localhost 10001
```

## Step 3 — Run dbt Models

```bash
# Full run
docker compose run --rm dbt dbt run \
    --profiles-dir /opt/dbt \
    --project-dir /opt/dbt \
    --target docker

# Single model
docker compose run --rm dbt dbt run \
    --profiles-dir /opt/dbt \
    --select silver_trips

# Run tests
docker compose run --rm dbt dbt test \
    --profiles-dir /opt/dbt

# Generate + serve docs
docker compose run --rm dbt dbt docs generate --profiles-dir /opt/dbt
docker compose run --rm dbt dbt docs serve --profiles-dir /opt/dbt --port 8585
```

## Step 4 — Run Locally (Without Docker)

```bash
pip install dbt-spark==1.7.2 PyHive thrift

cd dbt/
dbt debug --profiles-dir . --target docker    # verify connection
dbt run   --profiles-dir . --target docker    # run models
dbt test  --profiles-dir . --target docker    # run tests
```

---

## dbt Model Lineage

```
sources.iceberg_raw.nyc_taxi_trips
    └── raw_trips (view)
            └── silver_trips (incremental iceberg, merge strategy)
                    ├── gold_daily_summary (iceberg table, partitioned by date)
                    └── gold_location_performance (iceberg table)
```

!!! warning "Re-ingestion duplicates"
    If you re-run for the same date range, add `--full-refresh` to avoid duplicates in `silver_trips`:
    ```bash
    docker compose run --rm dbt dbt run --full-refresh --select silver_trips
    ```
