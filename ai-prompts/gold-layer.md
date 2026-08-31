# AI prompts used for gold layer aggregation development.

## Session: Gold layer implementation (2026-08-27)

**Prompt sent:** Create Gold layer in `src/gold/` with three SQL aggregations
(sales by product, revenue by customer, customer segmentation) plus
`create_gold_tables.py`. Gold must filter to `quality_check_result = PASS`
only (Silver keeps all rows for audit). Skip daily/weekly trends.

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
