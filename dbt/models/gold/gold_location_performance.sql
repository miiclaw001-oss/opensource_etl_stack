{{
  config(
    materialized='table',
    schema='gold',
    file_format='iceberg',
    tags=['gold', 'analytics', 'locations']
  )
}}

/*
  Gold layer: pickup zone performance analytics.
  Ranks NYC taxi zones by revenue, volume, and average fare.
*/

WITH zone_stats AS (
    SELECT
        pu_location_id                                                  AS location_id,
        COUNT(*)                                                        AS trip_count,
        ROUND(SUM(total_amount), 2)                                     AS total_revenue,
        ROUND(AVG(total_amount), 2)                                     AS avg_fare,
        ROUND(AVG(trip_distance), 2)                                    AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2)                            AS avg_duration_minutes,
        ROUND(AVG(tip_pct), 2)                                          AS avg_tip_pct,
        ROUND(
            SUM(CASE WHEN was_tipped THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
        )                                                               AS tip_rate_pct,
        COUNT(DISTINCT pickup_date)                                     AS active_days,
        ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date), 1)         AS avg_daily_trips

    FROM {{ ref('silver_trips') }}
    GROUP BY pu_location_id
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC)                 AS revenue_rank,
        ROW_NUMBER() OVER (ORDER BY trip_count DESC)                    AS volume_rank,
        ROW_NUMBER() OVER (ORDER BY avg_fare DESC)                      AS avg_fare_rank,
        PERCENT_RANK() OVER (ORDER BY total_revenue)                    AS revenue_percentile
    FROM zone_stats
)

SELECT
    location_id,
    trip_count,
    total_revenue,
    avg_fare,
    avg_distance,
    avg_duration_minutes,
    avg_tip_pct,
    tip_rate_pct,
    active_days,
    avg_daily_trips,
    revenue_rank,
    volume_rank,
    avg_fare_rank,
    ROUND(revenue_percentile * 100, 1)                                  AS revenue_percentile,
    CASE
        WHEN revenue_rank <= 10  THEN 'top_10'
        WHEN revenue_rank <= 50  THEN 'top_50'
        WHEN revenue_rank <= 100 THEN 'top_100'
        ELSE 'other'
    END                                                                 AS revenue_tier

FROM ranked
ORDER BY revenue_rank
