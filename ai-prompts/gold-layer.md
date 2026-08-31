# AI prompts used for gold layer aggregation development.

## Session: Gold layer implementation (2026-08-27)

**Prompt sent:** Create the Gold layer for the medallion pipeline in src/gold/. This
layer reads from the Silver Delta tables (silver_customers,
silver_orders, silver_products) and produces three
business-ready aggregation tables for BI/dashboard consumption.

IMPORTANT — row selection rule: Gold aggregations should be computed
using only rows where quality_check_result = "PASS" from Silver.
Flagged/failed rows stay visible in Silver (per the flag-don't-delete
rule) but should not silently corrupt business aggregations in Gold —
e.g., an order with a NULL product_id or an orphan customer_id should
not be counted toward revenue-by-product or revenue-by-customer
totals. State clearly in code comments that this filtering is
happening at the Gold boundary specifically, and why (Silver keeps all
rows for auditability, Gold is for trustworthy reporting).

## Files to create

**src/gold/01_sales_by_product.sql**
One row per product. Columns:
- product_id, product_name, category
- total_orders (count of orders referencing this product)
- total_revenue (sum of total_amount across those orders)
- avg_order_value (total_revenue / total_orders)
Join clean (PASS) orders to products. Products with zero qualifying
orders should still appear with 0/0.00 values, not be dropped.

**src/gold/02_revenue_by_customer.sql**
One row per customer. Columns:
- customer_id, customer_name, customer_segment
- total_orders (count of orders from this customer)
- total_revenue (sum of total_amount across those orders)
- avg_order_value (total_revenue / total_orders)
- lifetime_value_actual (computed total_revenue, to compare against
  the customers table's stated lifetime_value field)
Join clean (PASS) orders to customers. Customers with zero qualifying
orders should still appear with 0/0.00 values, not be dropped.

**src/gold/04_customer_segmentation.sql**
One row per segment_type. Segment_type is derived per customer (not
the same as customer_segment in the source data) based on their
computed order behavior:
- High-Value: total_revenue in the top 20% of all customers with orders
- Repeat: 2+ orders, not already High-Value
- One-Time: exactly 1 order
- Inactive: 0 qualifying orders
Columns: segment_type, customer_count, avg_revenue, total_revenue
State your exact thresholds/logic in a comment above the query since
"High-Value" isn't numerically defined in the source spec — pick a
reasonable definition and make it explicit and easy to adjust.

**src/gold/create_gold_tables.py**
- Orchestrates running all three SQL files against Silver
- Writes results to Gold Delta tables: gold_sales_by_product,
  gold_revenue_by_customer, gold_customer_segmentation
- Prints row counts for each output table
- Prints a basic sanity-check summary: sum of total_revenue across
  gold_sales_by_product should equal sum of total_revenue across
  gold_revenue_by_customer (both are derived from the same set of
  PASS orders, just grouped differently) — flag a warning if these
  don't match within rounding tolerance, since a mismatch would
  indicate a join/filter bug
- Do NOT build src/gold/03_daily_weekly_trends.sql — that's an
  optional stretch item, skip unless asked separately

## Code requirements
- SQL: uppercase keywords, one clause per line for anything beyond a
  trivial SELECT, comment non-obvious joins/filters
- Python orchestrator: docstrings, type hints, no hardcoded table
  names (use/extend the existing config pattern from bronze_config.py
  / silver_config.py into a gold_config.py)
- Include error handling: missing Silver table, empty result set after
  filtering to PASS rows only

After writing and running this, show me:
1. Row counts for each of the three Gold tables
2. The cross-check: total_revenue sum from sales_by_product vs.
   revenue_by_customer (should match)
3. A manually-computable sample: pick ONE customer and ONE product from
   the output, show their Gold row, and show the underlying Silver
   orders that should sum to those numbers, so I can verify the math
   by hand
4. The exact logic/thresholds used for customer segmentation, called
   out separately so I can review whether the definition makes sense

**Response summary:** Implemented `gold_config.py`, three SQL files, and
orchestrator with revenue cross-check and manual verification sample output.

**Accepted:**
- PASS-only filter at Gold boundary with explicit SQL comments explaining why
- Configurable table names and segmentation thresholds via `gold_config.py`
- Revenue reconciliation between product and customer aggregations
- Customer dimension deduplication (one row per `customer_id`) to avoid
  double-counting when Silver contains duplicate customer keys

**Fix applied during validation:** Initial revenue cross-check failed because
10 duplicate `customer_id` values in Silver (20 rows) inflated
`gold_revenue_by_customer` totals. Fixed with `customer_dimension` CTE using
`ROW_NUMBER()` in `02_revenue_by_customer.sql` and `04_customer_segmentation.sql`.

**Validation (local run):**
- Row counts: 500 / 10,000 / 4
- Revenue cross-check: 139,462,031.78 = 139,462,031.78 (MATCH)
- 99,600 PASS orders used from Silver (420 excluded)
