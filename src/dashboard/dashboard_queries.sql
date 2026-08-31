-- =============================================================================
-- Medallion Pipeline — Databricks SQL Dashboard Queries
-- =============================================================================
-- Gold source tables (no catalog/schema prefix — same convention as Gold layer):
--   gold_sales_by_product
--   gold_revenue_by_customer
--   gold_customer_segmentation
--
-- Query → visualization mapping:
--   QUERY 1  →  Bar chart     — Top 10 products by revenue
--   QUERY 2  →  Histogram     — Customer revenue distribution (non-zero only)
--   QUERY 3  →  Pie chart     — Customer count by behavioral segment
-- =============================================================================

-- -----------------------------------------------------------------------------
-- QUERY 1: Bar chart — Top 10 products by revenue
-- Visualization: Bar chart (X = product_name, Y = total_revenue)
-- Shows which products drive the most revenue across PASS-quality orders.
-- -----------------------------------------------------------------------------
SELECT
    product_name,
    category,
    total_revenue,
    total_orders
FROM gold_sales_by_product
ORDER BY
    total_revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- QUERY 2: Histogram — Customer revenue distribution
-- Visualization: Histogram (bin column = total_revenue)
-- Shows how customer lifetime revenue is spread across active buyers.
--
-- Judgment call: exclude customers with total_revenue = 0 (no qualifying
-- PASS orders). Including them would create a large spike at zero and
-- obscure the shape of the paying-customer distribution.
-- -----------------------------------------------------------------------------
SELECT
    customer_id,
    total_revenue
FROM gold_revenue_by_customer
WHERE
    total_revenue > 0
ORDER BY
    total_revenue DESC;


-- -----------------------------------------------------------------------------
-- QUERY 3: Pie chart — Customer segmentation
-- Visualization: Pie chart
--   Label / dimension: segment_type
--   Measure / value:    customer_count  (aggregation = SUM, NOT COUNT)
--
-- IMPORTANT (Databricks UI): If every slice shows ~25%, the chart is counting
-- query ROWS (4 segments) instead of customer_count. Set Measure to
-- SUM(customer_count), not COUNT(*) or COUNT of rows.
--
-- Expected approximate shares: Repeat ~80%, High-Value ~20%, One-Time/Inactive ~0%.
-- -----------------------------------------------------------------------------
SELECT
    segment_type,
    customer_count
FROM gold_customer_segmentation
ORDER BY
    customer_count DESC;
