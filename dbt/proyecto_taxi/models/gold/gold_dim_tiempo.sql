{{ config(materialized='table', schema='gold_taxi') }}
WITH horas AS (
  SELECT DISTINCT DATE_TRUNC('HOUR', pickup_at) AS hora
  FROM {{ ref('silver_trips') }}
  WHERE pickup_at IS NOT NULL
)
SELECT
  DATE_FORMAT(hora, 'yyyyMMddHH')   AS date_id,
  hora                               AS full_datetime,
  YEAR(hora)                         AS year,
  QUARTER(hora)                      AS quarter,
  MONTH(hora)                        AS month,
  DATE_FORMAT(hora, 'MMMM')          AS month_name,
  DAY(hora)                          AS day,
  HOUR(hora)                         AS hour,
  DAYOFWEEK(hora)                    AS day_of_week,
  DATE_FORMAT(hora, 'EEEE')          AS day_name,
  CASE WHEN DAYOFWEEK(hora) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
  CASE WHEN HOUR(hora) BETWEEN 7 AND 9
            OR HOUR(hora) BETWEEN 17 AND 19 THEN 1 ELSE 0 END AS is_rush_hour
FROM horas