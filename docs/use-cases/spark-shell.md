# Use Case 6: Query with Spark Shell

**Best for:** ad-hoc analysis, debugging, exploring tables.

---

## Interactive PySpark Shell

```bash
docker compose exec spark-master pyspark \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.3,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.warehouse=s3://warehouse/ \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true
```

---

## Useful Queries

```python
# List all namespaces/tables
spark.sql("SHOW NAMESPACES IN nessie").show()
spark.sql("SHOW TABLES IN nessie.silver").show()
spark.sql("SHOW TABLES IN nessie.gold").show()

# Row counts
spark.table("nessie.silver.trips").count()
spark.table("nessie.gold.daily_summary").count()

# Sample data
spark.table("nessie.silver.trips").show(10, truncate=False)

# Revenue by day
spark.sql("""
    SELECT pickup_date, SUM(total_amount) as revenue, COUNT(*) as trips
    FROM nessie.silver.trips
    GROUP BY pickup_date
    ORDER BY pickup_date
""").show(30)

# Top pickup zones
spark.sql("""
    SELECT pu_location_id, COUNT(*) as pickups, AVG(total_amount) as avg_fare
    FROM nessie.silver.trips
    GROUP BY pu_location_id
    ORDER BY pickups DESC
    LIMIT 20
""").show()

# Time-travel: query a past snapshot
spark.sql("""
    SELECT * FROM nessie.silver.trips
    FOR SYSTEM_TIME AS OF '2024-01-15 12:00:00'
    LIMIT 5
""").show()

# Iceberg table metadata
spark.sql("SELECT * FROM nessie.silver.trips.snapshots").show()
spark.sql("SELECT * FROM nessie.silver.trips.files LIMIT 5").show()
```

---

## Spark SQL Shell (Non-Python)

```bash
docker compose exec spark-master spark-sql \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1
```
