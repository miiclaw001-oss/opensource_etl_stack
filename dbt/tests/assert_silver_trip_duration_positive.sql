-- Test: All silver trips must have positive trip duration
-- Fails if any trips have duration <= 0 (dropoff before pickup)

SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_duration_minutes
FROM {{ ref('silver_trips') }}
WHERE trip_duration_minutes <= 0
   OR tpep_dropoff_datetime <= tpep_pickup_datetime
