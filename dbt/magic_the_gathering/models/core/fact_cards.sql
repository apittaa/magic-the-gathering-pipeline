{{ config(materialized='table') }}

SELECT
    *
FROM {{ ref('stg_cards') }}
ORDER BY usd_price DESC