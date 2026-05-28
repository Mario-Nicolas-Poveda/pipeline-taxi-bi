{{ config(materialized='table', schema='gold_taxi') }}
SELECT
  MONOTONICALLY_INCREASING_ID()          AS trip_id,
  DATE_FORMAT(t.pickup_at, 'yyyyMMddHH') AS date_id,
  DATE(t.pickup_at)                       AS pickup_date,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.payment_type,
  t.passenger_count,
  ROUND(t.trip_distance, 2)              AS trip_distance,
  ROUND(t.trip_duration_minutes, 2)      AS trip_duration_minutes,
  ROUND(t.fare_amount, 2)                AS fare_amount,
  ROUND(t.tip_amount, 2)                 AS tip_amount,
  ROUND(t.total_amount, 2)               AS total_amount,
  ROUND(t.tip_percentage * 100, 1)       AS tip_pct,
  t.pickup_at,
  t.dropoff_at
FROM {{ ref('silver_trips') }} t
WHERE t.pickup_at IS NOT NULL
  AND t.trip_duration_minutes BETWEEN 1 AND 180