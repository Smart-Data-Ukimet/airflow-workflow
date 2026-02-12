{{
    config(
        materialized='table'
    )
}}

select
  toInt64(order_number)          as order_number,
  toInt64(quantity_ordered)      as quantity_ordered,
  toFloat64(price_each)          as price_each,
  toInt64(order_linenumber)      as order_linenumber,
  toFloat64(sales)               as sales,
  toDateTime(order_date)         as order_date,
  status                         as status,
  toInt64(qtr_id)                as qtr_id,
  toInt64(month_id)              as month_id,
  toInt64(year_id)               as year_id,
  product_line                   as product_line,
  toInt64(msrp)                  as msrp,
  product_code                   as product_code,
  customer_name                  as customer_name,
  phone                          as phone,
  address_line_1                 as address_line_1,
  address_line_2                 as address_line_2,
  city                           as city,
  state                          as state,
  postal_code                    as postal_code,
  country                        as country,
  territory                      as territory,
  contact_lastname               as contact_lastname,
  contact_firstname              as contact_firstname,
  deal_size                      as deal_size,
  toDateTime(load_date)          as load_date
FROM
    {{ source('demo', 'sales_data_test') }}
