# Testing

---

## Test Summary

| Category | Tests | Passed | Failed | Skipped |
|----------|-------|--------|--------|---------|
| File structure | 18 | 18 | 0 | 0 |
| YAML syntax | 5 | 5 | 0 | 0 |
| Python syntax | 2 | 2 | 0 | 0 |
| DAG structure | 2 | 2 | 0 | 0 |
| Synthetic data generation | 1 | 1 | 0 | 0 |
| Transform logic (Python simulation) | 1 | 1 | 0 | 0 |
| Running stack (infrastructure) | 8 | — | — | 8 |
| **Total** | **37** | **29** | **0** | **8** |

All static tests pass. Infrastructure tests require a running Docker stack.

---

## Run the Test Suite

```bash
# Static tests (no running stack needed)
./scripts/test_pipeline.sh

# Full integration tests (stack must be running)
./scripts/setup.sh --mode docker
./scripts/test_pipeline.sh --mode docker

# Kubernetes tests
./scripts/setup.sh --mode k8s
./scripts/test_pipeline.sh --mode k8s

# dbt tests only
docker compose run --rm dbt dbt test --profiles-dir /opt/dbt
```

---

## Static Tests (No Docker Required)

### File Structure

All required files verified present:

| File | Status |
|------|--------|
| `docker-compose.yml` | ✅ |
| `kind-config.yaml` | ✅ |
| `.env.example` | ✅ |
| `airflow/dags/etl_pipeline.py` | ✅ |
| `airflow/dags/iceberg_maintenance.py` | ✅ |
| `dbt/dbt_project.yml` | ✅ |
| `dbt/profiles.yml` | ✅ docker + k8s targets |
| `dbt/models/raw/raw_trips.sql` | ✅ |
| `dbt/models/raw/sources.yml` | ✅ |
| `dbt/models/silver/silver_trips.sql` | ✅ incremental merge |
| `dbt/models/silver/silver_trips.yml` | ✅ |
| `dbt/models/gold/gold_daily_summary.sql` | ✅ |
| `dbt/models/gold/gold_daily_summary.yml` | ✅ |
| `dbt/models/gold/gold_location_performance.sql` | ✅ |
| `dbt/tests/assert_gold_no_negative_revenue.sql` | ✅ |
| `dbt/tests/assert_silver_trip_duration_positive.sql` | ✅ |
| `scripts/setup.sh` | ✅ |
| `scripts/test_pipeline.sh` | ✅ |

### YAML Syntax Validation

| File | Status |
|------|--------|
| `dbt/dbt_project.yml` | ✅ valid |
| `dbt/profiles.yml` | ✅ valid |
| `dbt/models/silver/silver_trips.yml` | ✅ valid |
| `dbt/models/gold/gold_daily_summary.yml` | ✅ valid |
| `dbt/models/raw/sources.yml` | ✅ valid |

### Python Syntax (DAGs)

Both DAG files compile cleanly with `python3 -m py_compile`:

| File | Syntax | DAG Structure |
|------|--------|---------------|
| `airflow/dags/etl_pipeline.py` | ✅ | ✅ DAG with-block found |
| `airflow/dags/iceberg_maintenance.py` | ✅ | ✅ DAG with-block found |

### Synthetic Data Generation

- 10,000 rows generated in < 1 second
- All required columns present
- Timestamps correctly formatted
- Numeric fields within expected ranges

### Transform Logic Simulation

Simulated the raw → silver → gold pipeline in pure Python:

- **Input:** 1,000 synthetic raw rows
- **Silver output:** all rows passed validation (0 filtered)
- **Business rules verified:** `trip_distance > 0`, `fare_amount > 0`, `trip_duration_minutes > 0`
- **Gold aggregation:** hourly buckets computed correctly

---

## Infrastructure Tests (Require Running Stack)

| Test | What It Checks |
|------|---------------|
| MinIO API liveness | `GET /minio/health/live → 200` |
| MinIO API readiness | `GET /minio/health/ready → 200` |
| Nessie API health | `GET /q/health/ready → 200` |
| Airflow health | `GET /health → 200` |
| Bucket `warehouse` exists | Created by `minio-init` |
| Bucket `warehouse-raw` exists | Created by `minio-init` |
| MinIO upload test | Upload CSV → verify size > 0 |
| Nessie `main` branch | `GET /api/v1/trees/tree/main → {name: "main"}` |

---

## Manual Verification Checklist

```
[ ] MinIO Console opens at :9001
[ ] MinIO has 4 buckets: warehouse, warehouse-raw, warehouse-silver, warehouse-gold
[ ] Nessie API returns 200 at /api/v1/trees/tree/main
[ ] Airflow UI opens at :8080, shows etl_iceberg_pipeline DAG
[ ] Spark UI shows 1 worker at :8090
[ ] Trigger DAG → all tasks turn green (10-15 min)
[ ] Nessie shows silver.trips and gold.daily_summary tables
[ ] Run: spark.table("nessie.silver.trips").count() > 0
[ ] Run: spark.table("nessie.gold.daily_summary").count() > 0
[ ] dbt run completes without errors
[ ] dbt test passes (no negative revenue, positive durations)
[ ] DataHub UI opens at :9002 (if DataHub started)
[ ] datahub_lineage_emitter DAG completes without errors
```

---

## Quick Test Commands

```bash
# Nessie API test
curl -s http://localhost:19120/api/v1/trees/tree/main | python3 -m json.tool

# Airflow DAG syntax check (no Airflow needed)
python3 -m py_compile airflow/dags/etl_pipeline.py && echo "OK"
python3 -m py_compile airflow/dags/iceberg_maintenance.py && echo "OK"
python3 -m py_compile airflow/dags/datahub_lineage_emitter.py && echo "OK"

# DataHub health
curl http://localhost:8082/health
```
