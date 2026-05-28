{{ config(materialized='table', schema='silver_taxi') }}

WITH source AS (
    SELECT * FROM bronze_taxi.trips
    WHERE pickup_datetime IS NOT NULL
    AND total_amount IS NOT NULL
),
cleaned AS (
    SELECT
        CAST(pickup_datetime  AS TIMESTAMP) AS pickup_at,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_at,
        (UNIX_TIMESTAMP(CAST(dropoff_datetime AS TIMESTAMP)) -
         UNIX_TIMESTAMP(CAST(pickup_datetime  AS TIMESTAMP))) / 60
                                            AS trip_duration_minutes,
        CAST(passenger_count  AS INT)       AS passenger_count,
        CAST(trip_distance    AS DOUBLE)    AS trip_distance,
        CAST(pickup_location  AS INT)       AS pickup_location_id,
        CAST(dropoff_location AS INT)       AS dropoff_location_id,
        CAST(payment_type     AS INT)       AS payment_type,
        CAST(fare_amount      AS DOUBLE)    AS fare_amount,
        CAST(tip_amount       AS DOUBLE)    AS tip_amount,
        CAST(total_amount     AS DOUBLE)    AS total_amount,
        CASE
            WHEN CAST(fare_amount AS DOUBLE) > 0
            THEN CAST(tip_amount AS DOUBLE) / CAST(fare_amount AS DOUBLE)
            ELSE 0
        END                                 AS tip_percentage
    FROM source
    WHERE CAST(trip_distance  AS DOUBLE) BETWEEN 0.1 AND 100
      AND CAST(total_amount   AS DOUBLE) BETWEEN 1   AND 500
      AND CAST(passenger_count AS INT)   BETWEEN 1   AND 6
      AND CAST(fare_amount    AS DOUBLE) > 0
)
SELECT * FROM cleaned