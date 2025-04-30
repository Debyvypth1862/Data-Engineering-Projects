SELECT
  order_id,
  customer_id,
  product_id,
  quantity,
  CAST(order_date AS DATE) AS order_date
FROM raw.sales