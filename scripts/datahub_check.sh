#!/usr/bin/env bash
set -euo pipefail

GMS_URL="http://localhost:8082/health"
MAX_RETRIES=20
RETRY_INTERVAL=10

echo "Waiting for DataHub GMS to become healthy..."
for i in $(seq 1 $MAX_RETRIES); do
  if curl -sf "$GMS_URL" > /dev/null 2>&1; then
    echo "✓ DataHub GMS is healthy"
    break
  fi
  echo "  Attempt $i/$MAX_RETRIES — retrying in ${RETRY_INTERVAL}s..."
  sleep $RETRY_INTERVAL
done

echo ""
echo "DataHub Services:"
echo "  UI:      http://localhost:9002  (admin / datahub)"
echo "  GMS API: http://localhost:8082"
echo ""
echo "To emit lineage metadata:"
echo "  docker compose exec airflow-webserver airflow dags trigger datahub_lineage_emitter"
