{{
    config(
        materialized='table'
    )
}}

SELECT
    country
    , ROUND(SUM(sales)::NUMERIC, 2) AS sales_sum
    , NOW()::timestamp AS load_date
FROM
    {{ source('public', 'sales_data_test') }}
GROUP BY
    country
