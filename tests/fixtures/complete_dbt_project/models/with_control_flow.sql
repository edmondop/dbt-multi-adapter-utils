SELECT
    user_id,
    {% if var('include_email', false) %}
    email,
    {% endif %}
    DATE_TRUNC('day', created_at) as created_day
FROM users
