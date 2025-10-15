SELECT
    DATE_TRUNC('month', created_at) as month,
    REGEXP_EXTRACT(email, '@(.+)') as domain,
    {{ var('partition_key') }} as partition,
    user_id
FROM {{ ref('raw_users') }}
WHERE created_at > DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
