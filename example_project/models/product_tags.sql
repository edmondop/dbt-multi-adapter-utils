-- Model: product_tags.sql
-- Aggregates product tags using Spark functions

SELECT
    product_id,
    product_name,
    category,
    COLLECT_LIST(tag_name) as all_tags,
    COLLECT_SET(tag_category) as tag_categories,
    COUNT(*) as tag_count
FROM {{ ref('raw_product_tags') }}
GROUP BY
    product_id,
    product_name,
    category
HAVING COUNT(*) > 0
