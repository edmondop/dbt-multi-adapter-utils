-- Array aggregation functions (Spark COLLECT_LIST vs others ARRAY_AGG)
SELECT
    user_id,
    COLLECT_LIST(product_id) as all_products,
    COLLECT_SET(category) as unique_categories,
    COUNT(DISTINCT session_id) as session_count
FROM purchases
GROUP BY user_id
