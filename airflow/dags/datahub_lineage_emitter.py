"""
DataHub Lineage Emitter DAG
Emits pipeline lineage and schema metadata to DataHub after each ETL run.
Runs daily at 7am, or can be triggered manually after etl_iceberg_pipeline.
Falls back gracefully if DataHub is unreachable.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DATAHUB_GMS_URL = "http://datahub-gms:8080"

PLATFORM_S3 = "urn:li:dataPlatform:s3"
PLATFORM_ICEBERG = "urn:li:dataPlatform:iceberg"
ENV = "PROD"

RAW_DATASET = f"urn:li:dataset:({PLATFORM_S3},warehouse-raw.nyc_taxi,{ENV})"
SILVER_DATASET = f"urn:li:dataset:({PLATFORM_ICEBERG},polaris.silver.trips,{ENV})"
GOLD_DAILY_DATASET = f"urn:li:dataset:({PLATFORM_ICEBERG},polaris.gold.daily_summary,{ENV})"
GOLD_LOCATION_DATASET = f"urn:li:dataset:({PLATFORM_ICEBERG},polaris.gold.location_performance,{ENV})"

default_args = {
    "owner": "etl-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "execution_timeout": timedelta(minutes=10),
}


def get_emitter():
    """Create DataHub REST emitter, returns None if DataHub unreachable."""
    try:
        from datahub.emitter.rest_emitter import DatahubRestEmitter
        emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_URL)
        emitter.test_connection()
        logger.info("DataHub GMS reachable at %s", DATAHUB_GMS_URL)
        return emitter
    except Exception as e:
        logger.warning("DataHub unreachable (%s) — skipping metadata emission", e)
        return None


def emit_dataset_metadata(**context):
    """Emit dataset properties for all three layers."""
    emitter = get_emitter()
    if not emitter:
        return

    try:
        from datahub.emitter.mce_builder import make_dataset_urn
        from datahub.metadata.schema_classes import (
            MetadataChangeEventClass,
            DatasetSnapshotClass,
            DatasetPropertiesClass,
        )

        datasets = [
            (RAW_DATASET, "NYC Taxi Raw Layer", "Raw CSV/Parquet files from NYC Taxi dataset, partitioned by year/month"),
            (SILVER_DATASET, "NYC Taxi Silver Layer", "Cleaned, typed and feature-engineered Iceberg table. Incremental merge on vendor_id+pickup_datetime+location."),
            (GOLD_DAILY_DATASET, "Gold Daily Summary", "Daily + hourly KPIs: trip count, revenue, avg fare, avg distance. Partitioned by date."),
            (GOLD_LOCATION_DATASET, "Gold Location Performance", "Zone-level KPIs: trips per pickup zone, avg revenue, rank."),
        ]

        for urn, name, description in datasets:
            mce = MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=urn,
                    aspects=[
                        DatasetPropertiesClass(
                            name=name,
                            description=description,
                            customProperties={
                                "stack": "iceberg-etl",
                                "catalog": "polaris",
                                "storage": "minio",
                            },
                        )
                    ],
                )
            )
            emitter.emit_mce(mce)
            logger.info("Emitted dataset metadata: %s", name)

    except Exception as e:
        logger.error("Failed to emit dataset metadata: %s", e)


def emit_lineage(**context):
    """Emit upstream lineage: raw→silver, silver→gold_daily, silver→gold_location."""
    emitter = get_emitter()
    if not emitter:
        return

    try:
        from datahub.metadata.schema_classes import (
            MetadataChangeEventClass,
            DatasetSnapshotClass,
            UpstreamLineageClass,
            UpstreamClass,
            DatasetLineageTypeClass,
        )

        lineage_map = [
            (SILVER_DATASET, [RAW_DATASET]),
            (GOLD_DAILY_DATASET, [SILVER_DATASET]),
            (GOLD_LOCATION_DATASET, [SILVER_DATASET]),
        ]

        for downstream_urn, upstream_urns in lineage_map:
            upstreams = [
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
                for upstream_urn in upstream_urns
            ]
            mce = MetadataChangeEventClass(
                proposedSnapshot=DatasetSnapshotClass(
                    urn=downstream_urn,
                    aspects=[UpstreamLineageClass(upstreams=upstreams)],
                )
            )
            emitter.emit_mce(mce)
            logger.info("Emitted lineage → %s", downstream_urn)

    except Exception as e:
        logger.error("Failed to emit lineage: %s", e)


def emit_silver_schema(**context):
    """Emit column-level schema metadata for the silver trips table."""
    emitter = get_emitter()
    if not emitter:
        return

    try:
        from datahub.metadata.schema_classes import (
            MetadataChangeEventClass,
            DatasetSnapshotClass,
            SchemaMetadataClass,
            SchemaFieldClass,
            SchemaFieldDataTypeClass,
            StringTypeClass,
            NumberTypeClass,
            DateTypeClass,
            OtherSchemaClass,
        )

        fields = [
            ("vendor_id", StringTypeClass()),
            ("tpep_pickup_datetime", DateTypeClass()),
            ("tpep_dropoff_datetime", DateTypeClass()),
            ("trip_distance", NumberTypeClass()),
            ("fare_amount", NumberTypeClass()),
            ("tip_amount", NumberTypeClass()),
            ("total_amount", NumberTypeClass()),
            ("tip_pct", NumberTypeClass()),
            ("fare_per_mile", NumberTypeClass()),
            ("avg_speed_mph", NumberTypeClass()),
            ("trip_duration_minutes", NumberTypeClass()),
            ("time_of_day", StringTypeClass()),
            ("day_name", StringTypeClass()),
            ("is_weekend", StringTypeClass()),
            ("payment_type_label", StringTypeClass()),
            ("pu_location_id", NumberTypeClass()),
            ("do_location_id", NumberTypeClass()),
        ]

        schema_fields = [
            SchemaFieldClass(
                fieldPath=field_name,
                type=SchemaFieldDataTypeClass(type=field_type),
                nativeDataType=type(field_type).__name__.replace("TypeClass", "").lower(),
                description="",
            )
            for field_name, field_type in fields
        ]

        mce = MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(
                urn=SILVER_DATASET,
                aspects=[
                    SchemaMetadataClass(
                        schemaName="polaris.silver.trips",
                        platform="urn:li:dataPlatform:iceberg",
                        version=0,
                        hash="",
                        platformSchema=OtherSchemaClass(rawSchema="iceberg"),
                        fields=schema_fields,
                    )
                ],
            )
        )
        emitter.emit_mce(mce)
        logger.info("Emitted silver schema with %d fields", len(fields))

    except Exception as e:
        logger.error("Failed to emit silver schema: %s", e)


with DAG(
    dag_id="datahub_lineage_emitter",
    default_args=default_args,
    description="Emits ETL lineage and schema metadata to DataHub",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["datahub", "metadata", "lineage"],
) as dag:

    t1 = PythonOperator(
        task_id="emit_dataset_metadata",
        python_callable=emit_dataset_metadata,
    )

    t2 = PythonOperator(
        task_id="emit_lineage",
        python_callable=emit_lineage,
    )

    t3 = PythonOperator(
        task_id="emit_silver_schema",
        python_callable=emit_silver_schema,
    )

    t1 >> [t2, t3]
