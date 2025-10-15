SELECT
    user_id,
    created_at,
    COUNT(*) as total_orders
FROM orders
WHERE created_at > CURRENT_DATE()
GROUP BY user_id, created_at
