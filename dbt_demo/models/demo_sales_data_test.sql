{{
    config(
        materialized='table'
    )
}}

SELECT
    order_number
    , quantity_ordered
    , price_each
    , order_linenumber
    , sales
    , order_date
    , `status`
    , qtr_id
    , month_id
    , year_id
    , product_line
    , msrp
    , product_code
    , customer_name
    , phone
    , address_line_1
    , address_line_2
    , city
    , `state`
    , postal_code
    , country
    , territory
    , contact_lastname
    , contact_firstname
    , deal_size
    , NOW() AS load_date
FROM
    {{ source('demo', 'sales_data_test') }}
