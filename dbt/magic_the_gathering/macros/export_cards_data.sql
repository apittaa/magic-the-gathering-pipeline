{% macro export_cards_data(table) %}
{% set s3_path = env_var('TRANSFORM_S3_PATH_OUTPUT', 'my-bucket-path') %}
    COPY (
        SELECT 
            *
        FROM {{ table }}
    ) 
    TO '{{ s3_path }}{{ table }}.parquet'
     (FORMAT PARQUET);
{% endmacro %}