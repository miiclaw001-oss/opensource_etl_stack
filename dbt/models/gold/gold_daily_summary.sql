{{
  config(
    materialized='table',
    schema='gold',
    file_format='iceberg',
    partition_by={'field': 'pickup_date', 'granularity': 'day'},
    tags=['gold', 'analytics', 'aggregated']
  )
}}

/*
  Gold layer: daily trip summary analytics table.
  Aggregates silver_trips to hourly and daily granularity with KPIs:
    - Trip volume metrics
    - Revenue metrics
    - Distance/duration metrics
    - Tipping behavior
    - Peak hour analysis
    - Payment mix
*/

WITH daily_hourly AS (
    SELECT
        pickup_date,
        pickup_hour,
        day_name,
        is_weekend,
        time_of_day,

        -- Volume
        COUNT(*)                                                        AS trip_count,
        SUM(passenger_count)                                            AS total_passengers,

        -- Revenue
        ROUND(SUM(total_amount), 2)                                     AS total_revenue,
        ROUND(AVG(total_amount), 2)                                     AS avg_fare,
        ROUND(SUM(fare_amount), 2)                                      AS total_fare_amount,
        ROUND(SUM(tip_amount), 2)                                       AS total_tips,
        ROUND(AVG(tip_pct), 2)                                          AS avg_tip_pct,

        -- Distance
        ROUND(SUM(trip_distance), 2)                                    AS total_miles,
        ROUND(AVG(trip_distance), 2)                                    AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2)                            AS avg_duration_minutes,
        ROUND(AVG(avg_speed_mph), 2)                                    AS avg_speed_mph,

        -- Rates
        ROUND(AVG(fare_per_mile), 2)                                    AS avg_fare_per_mile,
        ROUND(AVG(revenue_per_minute), 2)                               AS avg_revenue_per_minute,

        -- Tipping
        SUM(CASE WHEN was_tipped THEN 1 ELSE 0 END)                    AS tipped_trips,
        ROUND(
            SUM(CASE WHEN was_tipped THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                                                               AS tip_rate_pct,

        -- Payment mix
        SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END)             AS credit_card_trips,
        SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END)             AS cash_trips,
        ROUND(
            SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                                                               AS credit_card_pct,

        -- Zones
        COUNT(DISTINCT pu_location_id)                                  AS unique_pickup_zones,
        COUNT(DISTINCT do_location_id)                                  AS unique_dropoff_zones

    FROM {{ ref('silver_trips') }}
    GROUP BY
        pickup_date, pickup_hour, day_name, is_weekend, time_of_day
),

with_rank AS (
    SELECT
        *,
        -- Rank hours within each day by trip volume
        ROW_NUMBER() OVER (
            PARTITION BY pickup_date
            ORDER BY trip_count DESC
        ) AS hour_rank_by_volume,

        -- Rank hours within each day by revenue
        ROW_NUMBER() OVER (
            PARTITION BY pickup_date
            ORDER BY total_revenue DESC
        ) AS hour_rank_by_revenue,

        -- Rolling 7-day avg trips (approximate, same hour)
        AVG(trip_count) OVER (
            PARTITION BY pickup_hour
            ORDER BY pickup_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg_trips

    FROM daily_hourly
)

SELECT
    pickup_date,
    pickup_hour,
    day_name,
    is_weekend,
    time_of_day,
    trip_count,
    total_passengers,
    total_revenue,
    avg_fare,
    total_fare_amount,
    total_tips,
    avg_tip_pct,
    total_miles,
    avg_distance,
    avg_duration_minutes,
    avg_speed_mph,
    avg_fare_per_mile,
    avg_revenue_per_minute,
    tipped_trips,
    tip_rate_pct,
    credit_card_trips,
    cash_trips,
    credit_card_pct,
    unique_pickup_zones,
    unique_dropoff_zones,
    hour_rank_by_volume,
    hour_rank_by_revenue,
    ROUND(rolling_7d_avg_trips, 1)                                      AS rolling_7d_avg_trips,

    -- Is this a peak hour? (top 3 hours by volume for the day)
    CASE WHEN hour_rank_by_volume <= 3 THEN TRUE ELSE FALSE END         AS is_peak_hour,

    -- Revenue per trip
    ROUND(total_revenue / NULLIF(trip_count, 0), 2)                    AS revenue_per_trip,

    -- Passengers per trip
    ROUND(total_passengers * 1.0 / NULLIF(trip_count, 0), 2)          AS avg_passengers_per_trip

FROM with_rank
ORDER BY pickup_date, pickup_hour
