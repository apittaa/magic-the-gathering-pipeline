{% macro export_cards_data(schema, table) %}
{% set s3_path = env_var('TRANSFORM_S3_PATH_OUTPUT') %}
-- {% set schema = 'gold' %}
    COPY (
        SELECT 
            *
        FROM {{ schema }}.{{ table }}
    )
    TO '{{ s3_path }}{{ table }}.parquet'
     (FORMAT PARQUET);
{% endmacro %}