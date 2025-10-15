-- Model with Jinja expressions (for testing Jinja handling)
SELECT
    DATE_TRUNC('day', event_timestamp) as event_date,
    COUNT(*) as event_count,
    COLLECT_LIST(event_name) as events
FROM {{ ref('user_events') }}
WHERE event_timestamp >= DATE_ADD(CURRENT_DATE(), -{{ var('lookback_days', 30) }})
GROUP BY DATE_TRUNC('day', event_timestamp)
HAVING COUNT(*) > {{ var('min_events', 10) }}
