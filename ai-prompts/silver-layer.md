# AI prompts used for silver layer quality validation development.

## Session: Silver layer implementation (2026-08-27)

**Prompt sent:** Create the Silver layer in `src/silver/` with four quality checks
(completeness, uniqueness, type validation, referential integrity) plus
`create_silver_tables.py` orchestrator. Hard rule: never filter/delete bad rows;
add flag columns only; Bronze row count must equal Silver row count.

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

**Rejected / not implemented:**
- `05_quality_business_logic.py` left as placeholder (not in spec)

**Validation (local run after Bronze ingest):**
- Row counts: customers 10,010 / products 500 / orders 100,020 — all match
- Injected issue counts confirmed: 50 NULL emails, 20 dup customer rows,
  100 NULL customer_id, 200 NULL product_id, 40 dup order rows,
  50 orphan customer_id, 30 orphan product_id
- Uniqueness checks correctly report below 100% threshold (expected with injected dupes)
