# AI prompts used for silver layer quality validation development.

## Session: Silver layer implementation (2026-08-27)

**Prompt sent:** Create the Silver layer for the medallion pipeline in src/silver/. This
layer reads from the Bronze Delta tables (bronze_customers, bronze_orders,
bronze_products , applies data quality checks,
and writes Silver Delta tables.

CRITICAL RULE: Never delete or filter out bad rows. Every row from
Bronze must still exist in Silver. Instead, add quality flag columns
that mark which checks each row failed. This is a hard requirement,
not a preference — do not implement this as a filter/dropna anywhere.

## Files to create

**src/silver/01_quality_completeness.py**
- Check for NULLs in the required fields per table:
  - customers: email
  - orders: customer_id, product_id
- For each row, add a boolean column
  `completeness_check_passed` (True/False)
- Also add a column `completeness_check_details` (STRING) listing which
  specific field(s) failed, e.g. "email is NULL" or "customer_id,
  product_id are NULL" — empty string if passed
- Threshold for the metrics report: >99% pass rate expected

**src/silver/02_quality_uniqueness.py**
- Check for duplicate customer_id in customers, duplicate order_id in
  orders (based on primary key duplication, not full-row duplication)
- Add `uniqueness_check_passed` (True/False) — every row sharing a
  duplicated key should be flagged, not just the "extra" copies
- Add `uniqueness_check_details` (STRING) noting the duplicated key value
- Threshold: 100% pass rate expected

**src/silver/03_quality_type_validation.py**
- Validate that fields conform to expected types/ranges even though
  Bronze stored everything as StringType, e.g.:
  - customer_id, order_id, product_id, quantity should be castable to
    INT and not negative
  - unit_price, total_amount, lifetime_value, price, cost should be
    castable to DECIMAL and not negative
  - order_date, signup_date, payment_date (if not NULL) should be
    valid, parseable dates
  - order_status should be one of: Pending, Completed, Cancelled
  - customer_segment should be one of: Premium, Standard, Basic
- Add `type_validation_passed` (True/False) and
  `type_validation_details` (STRING) listing which field(s) failed and why

**src/silver/04_quality_referential_integrity.py**
- Check every non-NULL customer_id in orders exists in customers
- Check every non-NULL product_id in orders exists in products
- Add `referential_integrity_passed` (True/False) and
  `referential_integrity_details` (STRING)
- NULL FKs should NOT be double-flagged here as a referential integrity
  failure — that's already captured by the completeness check; this
  check is specifically for orphan (non-NULL but non-existent) FK values
- Threshold: >99.9% pass rate expected

**src/silver/create_silver_tables.py**
- Orchestrates: reads Bronze tables, applies all four checks per
  applicable table (customers gets completeness + uniqueness + type
  validation; orders gets all four; products gets type validation)
- Adds one final combined column `quality_check_result` (STRING:
  "PASS" if all applicable checks passed, otherwise "FAIL")
- Writes to Silver Delta tables: silver_customers, silver_orders,
  silver_products (adjust naming to match whatever schema convention
  Bronze uses)
- Row count going into Silver must exactly match row count coming out
  of Bronze — flagging is not filtering; if these counts don't match,
  raise an error
- Generates and prints a quality metrics report: for each table, for
  each check, show total rows checked, rows passed, rows failed, %
  passed — and flag in the report if any check falls below its stated
  threshold

## Code requirements
- Add a module-level docstring per file (purpose, inputs, outputs)
- Add a docstring to every function
- Use type hints
- Make check thresholds configurable via constants (not hardcoded
  inline) so they can be tuned/verified against the spec
- No hardcoded table names — use constants/config, ideally reusing or
  extending bronze_config.py's pattern into a silver_config.py
- Include basic error handling: missing Bronze table, empty table,
  unexpected schema

After writing the scripts, run create_silver_tables.py and show me:
1. Row counts: Bronze in vs. Silver out per table (must match exactly)
2. The full quality metrics report (per check, per table: passed/failed/% )
3. A sample of 5 rows from silver_orders where quality_check_result =
   "FAIL", showing the specific flag columns and details
4. Confirm the counts of flagged rows roughly line up with the known
   injected issues (e.g., ~100 completeness failures on customer_id,
   ~200 on product_id, ~50+30 referential integrity failures, ~20+10
   uniqueness failures across orders/customers) — note any
   discrepancy rather than silently accepting a mismatched number

**Response summary:** Implemented `silver_config.py` (shared config, thresholds,
read/write helpers, metrics report), four check modules (`01`–`04`), and
`create_silver_tables.py` orchestrator. Products get type validation only;
customers get completeness + uniqueness + type; orders get all four. N/A checks
get passing placeholder columns. Combined `quality_check_result` column added.

**Accepted:**
- Configurable thresholds via constants/env vars
- Row-count guard before/after Silver writes
- Databricks `__file__` bootstrap pattern in orchestrator
- Referential integrity excludes NULL FKs (completeness handles those)


**Validation (local run after Bronze ingest):**
- Row counts: customers 10,010 / products 500 / orders 100,020 — all match
- Injected issue counts confirmed: 50 NULL emails, 20 dup customer rows,
  100 NULL customer_id, 200 NULL product_id, 40 dup order rows,
  50 orphan customer_id, 30 orphan product_id
- Uniqueness checks correctly report below 100% threshold (expected with injected dupes)

**Automated test:** `python3 test_data_quality.py` — programmatic assertion of all
seven injection categories against Silver flag columns (see `debugging-notes.md`).
