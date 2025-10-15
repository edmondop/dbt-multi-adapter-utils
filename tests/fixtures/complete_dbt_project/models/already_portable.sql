SELECT
    user_id,
    {{ portable_current_date() }} as report_date,
    COUNT(*) as total
FROM orders
GROUP BY user_id
