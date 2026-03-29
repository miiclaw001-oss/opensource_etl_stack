# Use Case 7: Polaris Catalog — Time Travel & Management

**Best for:** Iceberg Time Travel, Catalog-Management, Schema-Inspektion.

> ℹ️ Dieser Stack nutzt **Apache Polaris** (Iceberg REST Catalog Spec) statt Nessie.
> Polaris unterstützt Iceberg Time Travel via Spark nativ.

---

## Polaris API: Token holen

```bash
TOKEN=$(curl -sf -X POST 'http://localhost:8181/api/catalog/v1/oauth/tokens' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
echo "Token: ${TOKEN:0:30}..."
```

---

## Catalog-Inhalt inspizieren

```bash
# Alle Catalogs
curl -s http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# Namespaces im warehouse-Catalog
curl -s http://localhost:8181/api/catalog/v1/warehouse/namespaces \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# Tables in silver
curl -s http://localhost:8181/api/catalog/v1/warehouse/namespaces/silver/tables \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

---

## Time Travel via Spark

```python
# Spark Shell starten (siehe Use Case 6)

# Alle Snapshots anzeigen
spark.sql("SELECT * FROM polaris.silver.trips.snapshots").show(truncate=False)

# Zu einem bestimmten Zeitpunkt zurückgehen
spark.sql("""
    SELECT COUNT(*), SUM(total_amount)
    FROM polaris.silver.trips
    FOR SYSTEM_TIME AS OF '2024-01-15 12:00:00'
""").show()

# Zu einem bestimmten Snapshot-ID
spark.sql("""
    SELECT * FROM polaris.silver.trips
    FOR VERSION AS OF 12345678901234567
    LIMIT 10
""").show()

# Iceberg-Metadaten
spark.sql("SELECT * FROM polaris.silver.trips.files LIMIT 5").show()
spark.sql("SELECT * FROM polaris.silver.trips.manifests LIMIT 5").show()
spark.sql("SELECT * FROM polaris.silver.trips.history").show()
```

---

## Namespace anlegen

```bash
# Via API
curl -sf -X POST http://localhost:8181/api/catalog/v1/warehouse/namespaces \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"namespace": ["staging"], "properties": {}}'

# Via Spark
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.staging")
```

---

## Table löschen (Rollback via Snapshots)

```python
# Auf alten Snapshot zurücksetzen
spark.sql("""
    CALL polaris.system.rollback_to_snapshot(
      'silver.trips',
      12345678901234567
    )
""")

# Alte Snapshots aufräumen (Iceberg Expire)
spark.sql("""
    CALL polaris.system.expire_snapshots(
      'silver.trips',
      TIMESTAMP '2024-01-01 00:00:00'
    )
""")
```
