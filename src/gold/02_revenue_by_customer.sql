-- Gold aggregation: revenue by customer.
-- Inputs: {{SILVER_CUSTOMERS_TABLE}}, {{SILVER_ORDERS_TABLE}}
-- Output columns: customer_id, customer_name, customer_segment,
--   total_orders, total_revenue, avg_order_value, lifetime_value_actual
--
-- Gold boundary filter (intentional): only orders with
-- quality_check_result = '{{QUALITY_RESULT_PASS}}' are included.
-- Silver keeps all rows for auditability; Gold uses PASS-only orders
-- so customer revenue metrics are not skewed by bad order rows.

WITH clean_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        CAST(o.total_amount AS DECIMAL(18, 2)) AS total_amount
    FROM {{SILVER_ORDERS_TABLE}} AS o
    WHERE o.quality_check_result = '{{QUALITY_RESULT_PASS}}'
),
customer_order_stats AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        SUM(total_amount) AS total_revenue
    FROM clean_orders
    GROUP BY
        customer_id
),
-- One dimension row per customer_id (Silver may contain duplicate keys flagged by
-- uniqueness checks; Gold reporting must not double-count revenue).
customer_dimension AS (
    SELECT
        customer_id,
        customer_name,
        customer_segment
    FROM (
        SELECT
            c.customer_id,
            c.customer_name,
            c.customer_segment,
            ROW_NUMBER() OVER (
                PARTITION BY c.customer_id
                ORDER BY c.customer_id
            ) AS row_num
        FROM {{SILVER_CUSTOMERS_TABLE}} AS c
    ) AS ranked_customers
    WHERE row_num = 1
)
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    COALESCE(s.total_orders, 0) AS total_orders,
    COALESCE(s.total_revenue, CAST(0.00 AS DECIMAL(18, 2))) AS total_revenue,
    CASE
        WHEN COALESCE(s.total_orders, 0) = 0 THEN CAST(0.00 AS DECIMAL(18, 2))
        ELSE CAST(s.total_revenue / s.total_orders AS DECIMAL(18, 2))
    END AS avg_order_value,
    COALESCE(s.total_revenue, CAST(0.00 AS DECIMAL(18, 2))) AS lifetime_value_actual
FROM customer_dimension AS c
LEFT JOIN customer_order_stats AS s
    ON c.customer_id = s.customer_id
ORDER BY
    total_revenue DESC,
    c.customer_id
