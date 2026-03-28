{{
  config(
    materialized='view',
    schema='raw',
    tags=['raw', 'nyc_taxi']
  )
}}

/*
  Raw layer view: exposes the source Iceberg table (written by Spark raw→silver job)
  with minimal transformation — just column aliasing and basic type casting.

  Source: nessie.silver.trips (written by Spark ingest job)
  Note: In a real Airbyte setup, this would read directly from the Airbyte-ingested table.
*/

SELECT
    CAST(vendor_id AS INT)                          AS vendor_id,
    CAST(pickup_datetime AS TIMESTAMP)              AS tpep_pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP)             AS tpep_dropoff_datetime,
    CAST(passenger_count AS INT)                    AS passenger_count,
    CAST(trip_distance AS DOUBLE)                   AS trip_distance,
    CAST(pu_location_id AS INT)                     AS pu_location_id,
    CAST(do_location_id AS INT)                     AS do_location_id,
    CAST(payment_type AS INT)                       AS payment_type,
    CAST(fare_amount AS DOUBLE)                     AS fare_amount,
    CAST(tip_amount AS DOUBLE)                      AS tip_amount,
    CAST(total_amount AS DOUBLE)                    AS total_amount,
    CAST(trip_duration_minutes AS DOUBLE)           AS trip_duration_minutes,
    CAST(pickup_date AS DATE)                       AS pickup_date,
    CAST(pickup_hour AS INT)                        AS pickup_hour,
    CAST(day_of_week AS INT)                        AS day_of_week

FROM nessie.silver.trips
