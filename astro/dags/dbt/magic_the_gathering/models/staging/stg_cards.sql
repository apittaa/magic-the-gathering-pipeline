{{ config(materialized="view") }}

--handling deduplication
WITH cards AS
(
    SELECT 
        *
        , row_number() OVER(PARTITION BY card_id, released_at) AS rn
    FROM {{ source("s3-magic-the-gathering", "cards") }}
)
SELECT 
    -- identifiers
    CAST(({{ dbt_utils.generate_surrogate_key(['card_id', 'released_at']) }}) AS string) AS case_id,
    CAST(card_id AS string) AS card_id,

    -- Cards info
    CAST(name AS string) AS name,
    CAST(released_at AS date) AS released_at,
    CAST(color_identity AS string) AS color_identity,
    CAST(set_name AS string) AS set_name,
    CAST(artist AS string) AS artist,
    CAST(usd_price AS float) AS usd_price,

FROM cards
WHERE rn = 1
