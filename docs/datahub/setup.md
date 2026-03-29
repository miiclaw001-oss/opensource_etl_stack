# DataHub Setup

---

## Starting DataHub Services

DataHub services are defined in `docker-compose.yml` alongside the core ETL services. Start them with:

```bash
# Start only DataHub services
docker compose up -d \
  datahub-zookeeper \
  datahub-kafka \
  datahub-schema-registry \
  datahub-kafka-setup \
  datahub-elasticsearch \
  datahub-elasticsearch-setup \
  datahub-mysql \
  datahub-mysql-setup \
  datahub-gms \
  datahub-frontend

# Or start the full stack (includes DataHub)
./scripts/setup.sh --mode docker
```

!!! note "Startup order"
    DataHub has a strict dependency chain. The setup containers (`datahub-kafka-setup`, `datahub-elasticsearch-setup`, `datahub-mysql-setup`) run once and exit — this is expected.

---

## Health Check

Use the provided script to wait for DataHub GMS to become healthy:

```bash
./scripts/datahub_check.sh
```

This polls `http://localhost:8082/health` every 10 seconds, up to 20 retries (3.3 minutes).

Expected output:

```
Waiting for DataHub GMS to become healthy...
  Attempt 1/20 — retrying in 10s...
  Attempt 2/20 — retrying in 10s...
✓ DataHub GMS is healthy

DataHub Services:
  UI:      http://localhost:9002  (admin / datahub)
  GMS API: http://localhost:8082

To emit lineage metadata:
  docker compose exec airflow-webserver airflow dags trigger datahub_lineage_emitter
```

---

## Access the UI

Open http://localhost:9002 — default credentials: **datahub / datahub**

From the UI you can:
- Browse datasets in the search bar
- View the lineage graph for any dataset
- Inspect column-level schemas
- Add owners and tags

---

## Resource Requirements

DataHub adds approximately **4 GB RAM** to the stack:

| Service | Memory Limit |
|---------|-------------|
| datahub-elasticsearch | 1 GB |
| datahub-gms | 1.5 GB |
| datahub-kafka | 768 MB |
| datahub-mysql | 512 MB |
| datahub-frontend | 512 MB |
| datahub-zookeeper | 256 MB |
| datahub-schema-registry | 256 MB |
| **Total** | **~4.8 GB** |

If you are running on an 8 GB machine, consider not starting DataHub by default and only bringing it up when needed.

---

## Emit Lineage After ETL

Trigger the lineage emitter DAG manually after a pipeline run:

```bash
docker compose exec airflow-webserver airflow dags trigger datahub_lineage_emitter
```

Or let it run automatically — the DAG is scheduled daily at 7am (one hour after `etl_iceberg_pipeline` at 6am).
