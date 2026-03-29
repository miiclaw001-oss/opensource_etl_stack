# Service URLs & Credentials

All services run locally. URLs are for Docker Compose mode (use `kubectl port-forward` in k8s mode).

| Service | URL | User | Password |
|---------|-----|------|----------|
| MinIO Console | http://localhost:9001 | minioadmin | minioadmin123 |
| MinIO API | http://localhost:9000 | — | — |
| Nessie REST API | http://localhost:19120/api/v1 | — | — |
| Airflow UI | http://localhost:8080 | admin | admin123 |
| Airbyte UI | http://localhost:8000 | airbyte | password |
| Spark Master UI | http://localhost:8090 | — | — |
| Spark Worker UI | http://localhost:8091 | — | — |
| Spark Thrift | localhost:10001 | — | — |
| PostgreSQL | localhost:5432 | airflow | airflow123 |
| DataHub UI | http://localhost:9002 | datahub | datahub |
| DataHub GMS API | http://localhost:8082 | — | — |

!!! warning "Security"
    All passwords are defaults for local development only. Change them in `.env` before exposing on a network.

---

## Quick Health Checks

```bash
# MinIO
curl http://localhost:9000/minio/health/live

# Nessie
curl http://localhost:19120/q/health/ready
curl http://localhost:19120/api/v1/trees/tree/main

# Airflow
curl http://localhost:8080/health

# Spark
curl http://localhost:8090   # HTML page

# DataHub GMS
curl http://localhost:8082/health
```
