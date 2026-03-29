# Known Issues & Workarounds

---

## 1. Nessie Uses IN_MEMORY Storage (Data Lost on Restart)

**Severity:** High for any persistent use

**Issue:** The `nessie` service uses `NESSIE_VERSION_STORE_TYPE=IN_MEMORY`. Every container restart wipes the Nessie catalog. MinIO data files survive, but Iceberg metadata pointers are gone.

**Workaround:** Switch to JDBC backend in `docker-compose.yml`:

```yaml
nessie:
  environment:
    NESSIE_VERSION_STORE_TYPE: "JDBC"
    QUARKUS_DATASOURCE_JDBC_URL: "jdbc:postgresql://postgres:5432/nessie"
    QUARKUS_DATASOURCE_USERNAME: "airflow"
    QUARKUS_DATASOURCE_PASSWORD: "airflow123"
```

Also add to `scripts/init_db.sql`:

```sql
CREATE DATABASE nessie;
GRANT ALL PRIVILEGES ON DATABASE nessie TO airflow;
```

---

## 2. dbt Unique Key May Cause Duplicates on Re-Ingestion

**Severity:** Medium

**Issue:** The incremental merge key is `[vendor_id, tpep_pickup_datetime, pu_location_id]`. Re-running the pipeline for the same date range without `--full-refresh` can accumulate duplicate rows in `silver_trips`.

**Workaround:** Use `--full-refresh` when reprocessing historical data:

```bash
docker compose run --rm dbt dbt run --full-refresh --select silver_trips
```

---

## 3. Airflow Fernet Key and Secret Key Are Hardcoded

**Severity:** Medium (dev only)

**Location:** `docker-compose.yml`, `x-airflow-common` environment block

**Fix:** Move to `.env` and generate random values:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## 4. `generate_sample_data.py` Falls Back to CSV Silently

**Severity:** Medium

**Issue:** The script falls back to CSV without `pyarrow` installed, without a clear error message.

**Recommendation:** Add an explicit warning:

```python
try:
    import pyarrow
except ImportError:
    print("WARNING: pyarrow not installed, writing CSV instead of Parquet")
```

Install pyarrow for true Parquet output:

```bash
pip install pyarrow
```

---

## 5. `test_pipeline.sh` Test Count Is Slightly Off in k8s Mode

**Severity:** Minor

**Issue:** The `warn` shell function inside the k8s manifest validation loop doesn't increment the `PASS` counter consistently with `ok`. Test count in k8s mode may be slightly understated.

**Impact:** Cosmetic only — no tests are skipped incorrectly.

---

## 6. Spark Event Log Bucket `spark-logs` Prefix Documentation

**Issue:** `spark-defaults.conf` and the main DAG reference `s3a://warehouse/spark-logs` for event logging, but `minio-init` only creates top-level buckets. The `spark-logs` path is a prefix inside `warehouse`, not a separate bucket.

**Impact:** Low — Spark creates the prefix automatically on first write. Just be aware it lives inside `s3://warehouse/spark-logs/`, not a dedicated bucket.

---

## 7. DAG `start_date` Uses 2024-01-01 with `catchup=False`

**Location:** `airflow/dags/etl_pipeline.py`

**Note:** With `catchup=False`, Airflow won't backfill historical runs. This is intentional for a demo stack. If you need historical loads, set `catchup=True` and adjust `start_date` accordingly.

---

## 8. Airbyte 0.50.45 Is Significantly Outdated

**Current latest:** 0.63+ as of early 2025

**Impact:** UI may behave differently than documented. The connector API is stable but the webapp may show quirks.

**Note:** Connection may show "failed" in the UI even when working. Check `docker compose logs airbyte-server` instead.

---

## 9. DataHub Startup Takes 3–5 Minutes

**Issue:** DataHub has a strict dependency chain (Zookeeper → Kafka → Schema Registry → Elasticsearch → MySQL → GMS). All setup containers must complete before GMS starts.

**Workaround:** Use the provided health check script:

```bash
./scripts/datahub_check.sh
```

Do not attempt to emit lineage until GMS is healthy.
