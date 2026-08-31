-- Gold aggregation: sales by product.
-- Inputs: {{SILVER_PRODUCTS_TABLE}}, {{SILVER_ORDERS_TABLE}}
-- Output columns: product_id, product_name, category, total_orders,
--   total_revenue, avg_order_value
--
-- Gold boundary filter (intentional): only orders with
-- quality_check_result = '{{QUALITY_RESULT_PASS}}' are included.
-- Silver retains every Bronze row (including FAIL) for auditability;
-- Gold excludes untrustworthy orders so revenue/order counts are not
-- corrupted by NULL FKs, orphan keys, or other quality failures.

WITH clean_orders AS (
    SELECT
        o.order_id,
        o.product_id,
        CAST(o.total_amount AS DECIMAL(18, 2)) AS total_amount
    FROM {{SILVER_ORDERS_TABLE}} AS o
    WHERE o.quality_check_result = '{{QUALITY_RESULT_PASS}}'
),
product_order_stats AS (
    SELECT
        product_id,
        COUNT(order_id) AS total_orders,
        SUM(total_amount) AS total_revenue
    FROM clean_orders
    GROUP BY
        product_id
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(s.total_revenue, CAST(0.00 AS DECIMAL(18, 2))) AS total_revenue,
    CASE
        WHEN COALESCE(s.total_orders, 0) = 0 THEN CAST(0.00 AS DECIMAL(18, 2))
        ELSE CAST(s.total_revenue / s.total_orders AS DECIMAL(18, 2))
    END AS avg_order_value
FROM {{SILVER_PRODUCTS_TABLE}} AS p
LEFT JOIN product_order_stats AS s
    ON p.product_id = s.product_id
ORDER BY
    total_revenue DESC,
    p.product_id
