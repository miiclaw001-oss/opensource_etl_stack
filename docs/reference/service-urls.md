# Service URLs & Credentials

Alle Services laufen lokal. URLs für Docker Compose Modus (`kubectl port-forward` für Kubernetes).

---

## Lean Stack (`docker-compose.lean.yml`) — empfohlen

| Service | URL | Login |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| MinIO S3 API | http://localhost:9000 | — |
| Polaris REST Catalog | http://localhost:8181/api/catalog | root / s3cr3t |
| Polaris Management API | http://localhost:8181/api/management/v1 | root / s3cr3t |
| Polaris Healthcheck | http://localhost:8181/healthcheck | — |
| Spark Master UI | http://localhost:8090 | — |
| Spark Worker UI | http://localhost:8091 | — |
| Spark Thrift (JDBC) | localhost:10001 | — |
| PostgreSQL | localhost:5432 | airflow / airflow123 |

**Meltano** hat kein Web-UI — läuft als CLI:
```bash
docker compose -f docker-compose.lean.yml run --rm meltano meltano --help
```

---

## Full Stack (`docker-compose.yml`) — inkl. DataHub

| Service | URL | Login |
|---------|-----|-------|
| Airflow UI | http://localhost:8080 | admin / admin123 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Polaris REST Catalog | http://localhost:8181/api/catalog | root / s3cr3t |
| Polaris Management | http://localhost:8181/api/management/v1 | root / s3cr3t |
| Spark Master UI | http://localhost:8090 | — |
| DataHub UI | http://localhost:9002 | datahub / datahub |
| DataHub GMS API | http://localhost:8082 | — |

!!! warning "Sicherheit"
    Alle Passwörter sind Defaults für lokale Entwicklung. In `.env` ändern bevor der Stack im Netzwerk exposed wird.

---

## Health Checks

```bash
# MinIO
curl http://localhost:9000/minio/health/live

# Polaris
curl http://localhost:8181/healthcheck

# Polaris Catalog API (mit Token)
TOKEN=$(curl -sf -X POST 'http://localhost:8181/api/catalog/v1/oauth/tokens' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

curl -s "http://localhost:8181/api/catalog/v1/warehouse/namespaces" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# Airflow
curl http://localhost:8080/health

# Spark UI
curl -s http://localhost:8090 | grep -o "Spark Master"

# DataHub GMS (Full Stack only)
curl http://localhost:8082/health
```
