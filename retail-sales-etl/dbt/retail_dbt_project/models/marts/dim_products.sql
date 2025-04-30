SELECT DISTINCT
  product_id,
  'Product ' || product_id AS product_name
FROM {{ ref('stg_sales') }}