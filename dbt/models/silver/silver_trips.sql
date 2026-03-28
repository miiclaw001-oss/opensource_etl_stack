{{
  config(
    materialized='incremental',
    schema='silver',
    file_format='iceberg',
    incremental_strategy='merge',
    unique_key=['vendor_id', 'tpep_pickup_datetime', 'pu_location_id'],
    partition_by={'field': 'pickup_date', 'granularity': 'day'},
    tags=['silver', 'cleaned', 'nyc_taxi']
  )
}}

/*
  Silver layer: cleaned and enriched NYC taxi trip data.
  Applies:
    - Data type validation
    - Outlier removal (distance, fare, duration filters)
    - Feature engineering (speed, time-of-day buckets, tip flag)
    - Payment type and day-of-week labels
    - Incremental loading by pickup_date
*/

WITH source AS (
    SELECT * FROM {{ ref('raw_trips') }}

    {% if is_incremental() %}
        -- Only process new partitions on incremental runs
        WHERE pickup_date >= (
            SELECT COALESCE(MAX(pickup_date), '2019-01-01') FROM {{ this }}
        )
    {% endif %}
),

cleaned AS (
    SELECT
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        trip_duration_minutes,
        pickup_date,
        pickup_hour,
        day_of_week

    FROM source

    -- Remove rows with nulls in critical columns
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND trip_distance IS NOT NULL
      AND fare_amount IS NOT NULL
      AND total_amount IS NOT NULL

    -- Enforce business rules
      AND trip_distance BETWEEN {{ var('min_trip_distance') }} AND {{ var('max_trip_distance') }}
      AND fare_amount   BETWEEN {{ var('min_fare_amount') }} AND {{ var('max_fare_amount') }}
      AND total_amount  > 0
      AND trip_duration_minutes BETWEEN {{ var('min_trip_duration_minutes') }} AND {{ var('max_trip_duration_minutes') }}
      AND tpep_dropoff_datetime > tpep_pickup_datetime
),

enriched AS (
    SELECT
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        trip_duration_minutes,
        pickup_date,
        pickup_hour,
        day_of_week,

        -- Speed calculation (mph)
        CASE
            WHEN trip_duration_minutes > 0
            THEN ROUND(trip_distance / (trip_duration_minutes / 60.0), 2)
            ELSE NULL
        END AS avg_speed_mph,

        -- Time of day bucket
        CASE
            WHEN pickup_hour BETWEEN 6  AND 9  THEN 'morning_rush'
            WHEN pickup_hour BETWEEN 10 AND 15 THEN 'midday'
            WHEN pickup_hour BETWEEN 16 AND 19 THEN 'evening_rush'
            WHEN pickup_hour BETWEEN 20 AND 22 THEN 'evening'
            ELSE 'overnight'
        END AS time_of_day,

        -- Day label
        CASE day_of_week
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END AS day_name,

        -- Is weekend?
        CASE WHEN day_of_week IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,

        -- Payment label
        CASE payment_type
            WHEN 1 THEN 'credit_card'
            WHEN 2 THEN 'cash'
            WHEN 3 THEN 'no_charge'
            WHEN 4 THEN 'dispute'
            ELSE 'unknown'
        END AS payment_type_label,

        -- Tipped?
        CASE WHEN tip_amount > 0 THEN TRUE ELSE FALSE END AS was_tipped,

        -- Tip percentage
        CASE
            WHEN fare_amount > 0
            THEN ROUND((tip_amount / fare_amount) * 100, 2)
            ELSE 0.0
        END AS tip_pct,

        -- Fare per mile
        CASE
            WHEN trip_distance > 0
            THEN ROUND(fare_amount / trip_distance, 2)
            ELSE NULL
        END AS fare_per_mile,

        -- Revenue per minute
        CASE
            WHEN trip_duration_minutes > 0
            THEN ROUND(total_amount / trip_duration_minutes, 2)
            ELSE NULL
        END AS revenue_per_minute

    FROM cleaned
)

SELECT * FROM enriched
