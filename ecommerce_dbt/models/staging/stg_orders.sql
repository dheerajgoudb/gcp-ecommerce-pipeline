SELECT
  order_id,
  user_id,
  status,
  created_at,
  DATE(created_at) AS order_date
FROM {{ source('raw', 'orders') }}
WHERE status IS NOT NULL
