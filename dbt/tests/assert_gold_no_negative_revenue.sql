-- Test: Gold daily summary should never have negative revenue
-- Fails if any rows have total_revenue <= 0

SELECT
    pickup_date,
    pickup_hour,
    total_revenue
FROM {{ ref('gold_daily_summary') }}
WHERE total_revenue <= 0
