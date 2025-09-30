{{ config(
    materialized='table',
    schema='staging'
) }}

WITH ranked_data AS (
    SELECT 
        time,
        temperature_2m,
        relativehumidity_2m,
        city,
        ROW_NUMBER() OVER (
            PARTITION BY time, city
            ORDER BY time DESC
        ) as rn
    FROM weather_staging
)

SELECT 
    time,
    temperature_2m,
    relativehumidity_2m,
    city
FROM ranked_data
WHERE rn = 1