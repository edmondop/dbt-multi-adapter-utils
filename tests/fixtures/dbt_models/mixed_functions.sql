-- Mixed functions without Jinja
SELECT
    DATE_TRUNC('day', event_timestamp) as event_date,
    COUNT(*) as event_count,
    COLLECT_LIST(event_name) as events,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM user_events
WHERE event_timestamp >= DATE_ADD(CURRENT_DATE(), -30)
GROUP BY DATE_TRUNC('day', event_timestamp)
HAVING COUNT(*) > 10
