{{
    config(
        materialized='table'
    )
}}

SELECT
    *
FROM
    {{ source('public', 'sales_data_test') }}
