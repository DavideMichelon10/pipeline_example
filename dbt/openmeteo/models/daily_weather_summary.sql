{{ config(
    materialized='table'
) }}

SELECT 
    DATE(time) as date,
    city,
    AVG(temperature_2m) as avg_temperature,
    MIN(temperature_2m) as min_temperature,
    MAX(temperature_2m) as max_temperature,
    AVG(relativehumidity_2m) as avg_humidity
FROM {{ ref('stg_deduplicated_data') }}
GROUP BY DATE(time), city