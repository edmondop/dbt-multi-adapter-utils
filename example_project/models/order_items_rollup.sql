-- Model: order_items_rollup.sql
-- Rolls up order line items with various Spark functions

SELECT
    order_id,
    customer_id,
    order_date,
    COLLECT_LIST(product_id) as product_ids,
    COLLECT_LIST(product_name) as product_names,
    SUM(quantity) as total_items,
    SUM(line_total) as order_total,
    DATE_TRUNC('day', order_date) as order_day
FROM {{ ref('raw_order_items') }}
WHERE order_date >= DATE_ADD(CURRENT_DATE(), -30)
GROUP BY
    order_id,
    customer_id,
    order_date
