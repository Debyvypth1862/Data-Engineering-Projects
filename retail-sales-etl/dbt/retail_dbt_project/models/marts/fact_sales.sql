SELECT
  order_id,
  customer_id,
  product_id,
  quantity,
  order_date
FROM {{ ref('stg_sales') }}