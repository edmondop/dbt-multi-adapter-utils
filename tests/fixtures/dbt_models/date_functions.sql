-- Date manipulation functions (Spark-specific syntax)
SELECT
    user_id,
    DATE_TRUNC('month', created_at) as month,
    DATE_ADD(created_at, 7) as week_later,
    CURRENT_DATE() as today
FROM events
WHERE created_at >= DATE_ADD(CURRENT_DATE(), -90)
