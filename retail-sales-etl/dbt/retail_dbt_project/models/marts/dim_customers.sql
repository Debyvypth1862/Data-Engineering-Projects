SELECT DISTINCT
  customer_id,
  'Customer ' || customer_id AS customer_name
FROM {{ ref('stg_sales') }}