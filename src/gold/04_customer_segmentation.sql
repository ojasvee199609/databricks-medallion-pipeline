-- Gold aggregation: behavioral customer segmentation.
-- Inputs: {{SILVER_CUSTOMERS_TABLE}}, {{SILVER_ORDERS_TABLE}}
-- Output columns: segment_type, customer_count, avg_revenue, total_revenue
--
-- Segmentation logic (applied per customer after PASS-only order stats):
--   1. Inactive     — 0 qualifying (PASS) orders
--   2. High-Value   — among customers with >= 1 PASS order, revenue ranks
--                     in the top {{HIGH_VALUE_TOP_PERCENT}}% via NTILE({{HIGH_VALUE_N_TILES}})
--                     ordered by total_revenue DESC (tile 1 = top quintile)
--   3. Repeat       — >= {{REPEAT_MIN_ORDERS}} PASS orders and not High-Value
--   4. One-Time     — exactly 1 PASS order and not High-Value
--
-- Gold boundary filter (intentional): only PASS orders contribute to
-- order counts and revenue used for segmentation. Silver retains FAIL
-- rows for auditability; Gold segmentation reflects trustworthy behavior.

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
customer_dimension AS (
    SELECT
        customer_id
    FROM (
        SELECT
            c.customer_id,
            ROW_NUMBER() OVER (
                PARTITION BY c.customer_id
                ORDER BY c.customer_id
            ) AS row_num
        FROM {{SILVER_CUSTOMERS_TABLE}} AS c
    ) AS ranked_customers
    WHERE row_num = 1
),
all_customers AS (
    SELECT
        c.customer_id,
        COALESCE(s.total_orders, 0) AS total_orders,
        COALESCE(s.total_revenue, CAST(0.00 AS DECIMAL(18, 2))) AS total_revenue
    FROM customer_dimension AS c
    LEFT JOIN customer_order_stats AS s
        ON c.customer_id = s.customer_id
),
active_customer_segments AS (
    SELECT
        customer_id,
        total_orders,
        total_revenue,
        NTILE({{HIGH_VALUE_N_TILES}}) OVER (
            ORDER BY
                total_revenue DESC,
                customer_id ASC
        ) AS revenue_tile
    FROM all_customers
    WHERE total_orders > 0
),
classified_customers AS (
    SELECT
        ac.customer_id,
        ac.total_orders,
        ac.total_revenue,
        CASE
            WHEN ac.total_orders = 0 THEN 'Inactive'
            WHEN acs.revenue_tile = 1 THEN 'High-Value'
            WHEN ac.total_orders >= {{REPEAT_MIN_ORDERS}} THEN 'Repeat'
            ELSE 'One-Time'
        END AS segment_type
    FROM all_customers AS ac
    LEFT JOIN active_customer_segments AS acs
        ON ac.customer_id = acs.customer_id
)
SELECT
    segment_type,
    COUNT(customer_id) AS customer_count,
    CAST(AVG(total_revenue) AS DECIMAL(18, 2)) AS avg_revenue,
    CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue
FROM classified_customers
GROUP BY
    segment_type
ORDER BY
    CASE segment_type
        WHEN 'High-Value' THEN 1
        WHEN 'Repeat' THEN 2
        WHEN 'One-Time' THEN 3
        WHEN 'Inactive' THEN 4
        ELSE 5
    END
