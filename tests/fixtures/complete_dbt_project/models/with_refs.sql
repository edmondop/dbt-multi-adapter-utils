SELECT
    user_id,
    email,
    created_at
FROM {{ ref('raw_users') }}
WHERE created_at > CURRENT_DATE()
