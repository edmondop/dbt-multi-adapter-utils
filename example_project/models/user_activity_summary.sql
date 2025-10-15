-- Model: user_activity_summary.sql
-- Uses Spark-specific date and array functions

SELECT
    user_id,
    DATE_TRUNC('month', activity_date) as activity_month,
    COUNT(DISTINCT session_id) as total_sessions,
    COLLECT_LIST(DISTINCT page_url) as visited_pages,
    COLLECT_LIST(action_type) as all_actions,
    MIN(activity_date) as first_activity,
    MAX(activity_date) as last_activity
FROM {{ ref('raw_user_activities') }}
WHERE activity_date >= DATE_ADD(CURRENT_DATE(), -90)
GROUP BY
    user_id,
    DATE_TRUNC('month', activity_date)
