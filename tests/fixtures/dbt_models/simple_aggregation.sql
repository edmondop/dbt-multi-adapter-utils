-- Simple aggregation functions
SELECT
    user_id,
    COUNT(*) as total_count,
    SUM(amount) as total_amount,
    MIN(created_at) as first_seen,
    MAX(created_at) as last_seen
FROM users
GROUP BY user_id
