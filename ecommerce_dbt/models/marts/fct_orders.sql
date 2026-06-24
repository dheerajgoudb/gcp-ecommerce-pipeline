SELECT
  order_id,
  user_id,
  status,
  order_date
FROM {{ ref('stg_orders') }}
