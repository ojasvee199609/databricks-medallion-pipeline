# Debugging Notes and Issue Resolution Log

Comprehensive reference for **debugging measures** and **flagging** implemented
across the databricks-medallion-pipeline project — from data generation through
the SQL dashboard.

Related: `design-notes.md` (Debugging Approach), `data-quality-strategy.md`,
`test_data_quality.py`, `src/dashboard/DASHBOARD_GUIDE.md`, `ai-prompts/debugging.md`.

---

## Debugging Philosophy

| Principle | Where applied |
|-----------|---------------|
| **Fail loud on data loss** | Silver row-count guard; Bronze read/write count match |
| **Flag, don't delete** | Silver quality columns; bad rows stay auditable |
| **Filter at Gold boundary only** | `quality_check_result = 'PASS'` in Gold SQL |
| **Reconcile across aggregations** | Gold product vs customer revenue cross-check |
| **Compare to known fixtures** | Injected issue counts in sample data; `test_data_quality.py` |
| **Print samples for inspection** | Bronze anomalies, Silver FAIL rows, Gold hand-check |
| **Warn vs fatal** | Uniqueness threshold breach = warning; revenue mismatch = exit code 1 |

---

## Flagging Overview (All Layers)

### Layer 1 — Data generation (intentional defects)

`generate_sample_data.py` injects known defects so downstream checks can be
verified. Injection counts are printed at generation time — use as the baseline
when validating Silver metrics.

| Source | Flag target (Silver check) | Injected count |
|--------|---------------------------|---------------:|
| customers | Completeness (`email` NULL) | 50 |
| customers | Uniqueness (duplicate `customer_id`) | 10 keys → 20 flagged rows |
| orders | Completeness (`customer_id` NULL) | 100 |
| orders | Completeness (`product_id` NULL) | 200 |
| orders | Referential integrity (orphan `customer_id` = 99999) | 50 |
| orders | Referential integrity (orphan `product_id` = 99999) | 30 |
| orders | Uniqueness (duplicate `order_id`) | 20 keys → 40 flagged rows |

**Debug output:** `print_summary()` — row counts per file + per-issue breakdown.

---

### Layer 2 — Silver (row-level quality flags)

Every Bronze row survives in Silver. Each applicable check adds:

| Column pattern | Type | Purpose |
|----------------|------|---------|
| `{check}_check_passed` | BOOLEAN | `true` if row passed this check |
| `{check}_check_details` | STRING | Human-readable failure reason(s) |
| `quality_check_result` | STRING | `PASS` or `FAIL` (combined) |

#### Check modules and flag logic

| Module | Table(s) | What gets flagged |
|--------|----------|-------------------|
| `01_quality_completeness.py` | customers, orders | NULL or blank `email`; NULL/blank `customer_id`, `product_id` |
| `02_quality_uniqueness.py` | customers, orders | **All rows** sharing a duplicated `customer_id` or `order_id` |
| `03_quality_type_validation.py` | all three | Invalid int/decimal/date/enum on present values |
| `04_quality_referential_integrity.py` | orders | Non-NULL orphan `customer_id` / `product_id` (NULL FKs excluded) |

N/A checks get placeholder columns (`passed = true`, `details = ""`).

#### Example detail strings

- `email is NULL`
- `customer_id is NULL, product_id is NULL`
- `duplicate order_id: 12345`
- `orphan customer_id: 99999`
- `order_status is invalid`

#### Thresholds (configurable in `silver_config.py`)

| Check | Threshold | Below threshold behaviour |
|-------|-----------|---------------------------|
| Completeness | >99% | WARNING in metrics report |
| Uniqueness | 100% | WARNING (expected with injected dupes) |
| Type validation | >99% | WARNING |
| Referential integrity | >99.9% | WARNING |

**Validated Silver metrics (local run, seed 42):**

| Table | Check | Failed rows | % Passed |
|-------|-------|------------:|---------:|
| silver_customers | completeness | 50 | 99.50% |
| silver_customers | uniqueness | 20 | 99.80% |
| silver_orders | completeness | 300 | 99.70% |
| silver_orders | uniqueness | 40 | 99.96% |
| silver_orders | referential_integrity | 80 | 99.92% |

~**420 orders** with `quality_check_result = FAIL`; ~**99,600 PASS** orders for Gold.

---

### Layer 3 — Gold (trust boundary filter)

Gold does **not** add per-row flag columns. It applies a **set filter** documented
in SQL comments:

```sql
WHERE quality_check_result = 'PASS'
```

Used in `01_sales_by_product.sql`, `02_revenue_by_customer.sql`,
`04_customer_segmentation.sql`.

**Additional Gold safeguards:**

- `validate_pass_orders_exist()` — raises if zero PASS orders after filter
- `customer_dimension` CTE — `ROW_NUMBER()` dedupes `customer_id` to prevent
  revenue double-count when Silver has duplicate customer keys
- Empty Gold table write blocked (`ValueError` if 0 rows)

---

### Layer 4 — Dashboard (visualization flags)

Dashboard issues are usually **configuration errors**, not data bugs:

| Symptom | Root cause | Fix |
|---------|------------|-----|
| Pie chart 25% per segment | Measure = COUNT of rows (4) instead of `customer_count` | Set measure to **SUM(customer_count)** |
| Histogram empty | Filter too aggressive | Use `total_revenue > 0` only |
| Bar chart wrong totals | Stale Gold data | Re-run Gold + dashboard refresh |

Verification SQL in `DASHBOARD_GUIDE.md` .

---

## Built-In Debugging Measures by Layer

### Data generation (`src/data_generation/`)

| Measure | Location | What it does |
|---------|----------|--------------|
| Fixed seed | `RANDOM_SEED = 42` | Reproducible data and defects |
| Isolated injection functions | `inject_*_data_quality_issues()` | Defects traceable to named functions |
| Generation summary | `print_summary()` | File paths, row counts, issue breakdown |
| Configurable constants | Top of `generate_sample_data.py` | Tune counts without code changes |

---

### Bronze (`src/bronze/`)

| Measure | Location | What it does |
|---------|----------|--------------|
| Explicit CSV schema | `bronze_config.py` `*_SOURCE_SCHEMA` | `validate_dataframe_columns()` — column name/order mismatch → `ValueError` |
| Missing file | `validate_source_path()`, `read_csv_with_schema()` | `FileNotFoundError` with path |
| Empty file | `read_csv_with_schema()` | `ValueError` if 0 data rows |
| Read/write row match | `write_bronze_table()` | `RuntimeError` if read ≠ written count |
| Ingestion metadata | `_ingested_at`, `_source_file` | Trace which file produced each row |
| Per-source logging | `01`–`03_ingest_*.py` | Rows read, rows written, archive path |
| `IngestResult` dataclass | All ingest scripts | Structured status, duration, error message |
| Continue on failure | `ingest_all.py` | One source failure does not block others; exit code 1 if any fail |
| Ingestion summary table | `print_summary_table()` | Source, rows read/written, status, duration |
| Delta verification | `verify_delta_tables_exist()` | Confirms successful tables readable + row count |
| Anomaly sample | `show_orders_anomaly_sample()` | Up to 5 `bronze_orders` rows with NULL/orphan FKs |
| Databricks path bootstrap | `inspect.currentframe()` + `BRONZE_SRC_DIR` | Fixes `NameError: __file__` in Jobs |
| Staging path logging | `ingest_all.main()` | Prints `to_process` / `processed` configuration |

---

### Silver (`src/silver/`)

| Measure | Location | What it does |
|---------|----------|--------------|
| Empty Bronze guard | `read_bronze_table()` | `ValueError` if Bronze table has 0 rows |
| Row-count guard (pre-write) | `write_silver_table()` | `RuntimeError` if input count ≠ expected Bronze count |
| Row-count guard (post-write) | `write_silver_table()` | `RuntimeError` if written count ≠ expected |
| Bronze vs Silver summary | `print_row_count_summary()` | Per-table Match YES/NO |
| Quality metrics report | `print_quality_metrics_report()` | Total, passed, failed, % passed, threshold, meets threshold |
| Below-threshold listing | `print_quality_metrics_report()` + `main()` | Lists checks under threshold |
| Failed orders sample | `show_failed_orders_sample()` | Up to 5 FAIL rows with all flag columns |
| Configurable thresholds | `silver_config.py` env vars | Tune without code change |
| `QualityCheckMetric` dataclass | `silver_config.py` | Structured metrics for programmatic use |
| Automated DQ test | `test_data_quality.py` (repo root) | Asserts Silver flag failure counts vs injection constants from `generate_sample_data.py` |
| Databricks bootstrap | `SILVER_SRC_DIR` + `inspect` | Notebook/Job compatibility |

**Exit behaviour:** `main()` returns `0` even when thresholds breach (warnings only).
Tables are always written when row-count guard passes.

---

### Gold (`src/gold/`)

| Measure | Location | What it does |
|---------|----------|--------------|
| PASS order count log | `create_gold_tables()` | Prints count of orders used at Gold boundary |
| Empty Silver guard | `read_silver_table()` | `ValueError` if Silver table empty |
| Zero PASS orders guard | `validate_pass_orders_exist()` | `ValueError` if no PASS orders |
| Empty Gold guard | `write_gold_table()` | `ValueError` if result has 0 rows |
| Write failure handling | `write_gold_table()` | `RuntimeError` on Delta write failure |
| Row count summary | `print_row_count_summary()` | Rows per Gold table (500 / 10,000 / 4 expected) |
| Revenue cross-check | `print_revenue_cross_check()` | Compares `SUM(total_revenue)` product vs customer |
| Cross-check tolerance | `REVENUE_CROSS_CHECK_TOLERANCE = 0.01` | Configurable via env var |
| Segmentation logic print | `print_segmentation_logic()` | Documents High-Value / Repeat / One-Time / Inactive rules |
| Manual verification sample | `show_manual_verification_sample()` | One product + one customer: Gold row vs underlying Silver PASS orders |
| SQL boundary comments | `01`, `02`, `04` `.sql` files | Documents why PASS filter exists |
| Customer dedupe CTE | `02`, `04` SQL | Prevents duplicate-key revenue inflation |
| Missing SQL file | `load_sql_query()` | `FileNotFoundError` |

**Exit behaviour:** `main()` returns `1` if revenue cross-check **MISMATCH**.

**Validated cross-check (local):** 139,462,031.78 = 139,462,031.78 (MATCH).

---

### Dashboard (`src/dashboard/`)

| Measure | Location | What it does |
|---------|----------|--------------|
| Documented verification SQL | `DASHBOARD_GUIDE.md` §3 | Checks A–D (pie totals, revenue plausibility, histogram count, cross-layer revenue) |
| Query comments | `dashboard_queries.sql` | Chart type and column purpose per query |
| Pie chart troubleshooting | `DASHBOARD_GUIDE.md` | `SUM(customer_count)` vs row count |

---

### Databricks Jobs (`databricks-job.json`, `databricks-job-data-generation.json`)

| Measure | Location | What it does |
|---------|----------|--------------|
| Job failure email alerts | `databricks-job.json` — Bronze task `email_notifications` | Databricks sends email on task **failure** (and **start**) to the recipient configured at job setup |
| Job-level notifications | `email_notifications` on both job JSON files | Base notification settings for the job definition |
| File-arrival trigger | `databricks-job.json` `trigger.file_arrival` | Job 2 starts when CSVs land in `to_process/` |
| Task dependency chain | Bronze → Silver → Gold → Dashboard_Refresh | Downstream tasks skip if an upstream task fails (`run_if: ALL_SUCCESS`) |
| Run history & logs | Databricks Jobs UI | Inspect failed task output, cluster logs, and notebook run details after email alert |

**On failure:** review the alert email, then open the failed run in the Jobs UI to read
task logs (same flow as local orchestrator output: ingestion summary, Silver metrics,
Gold cross-check). Silver threshold warnings alone do not fail the notebook; a hard
error (empty table, row-count mismatch, Gold revenue MISMATCH exit code) triggers
failure and email.

---

## Issue Resolution Log

Issues encountered during development and how they were resolved.

### 1. Databricks Job: `NameError: __file__`

| | |
|---|---|
| **Symptom** | Bronze/Silver/Gold scripts fail when run as Databricks Job tasks |
| **Cause** | Databricks executes notebooks via `exec()` — `__file__` is undefined |
| **Fix** | Bootstrap module path with `inspect.currentframe()`; set `BRONZE_SRC_DIR`, `SILVER_SRC_DIR`, `GOLD_SRC_DIR` in notebooks |
| **Files** | `01`–`03_ingest_*.py`, `create_silver_tables.py`, `create_gold_tables.py`, notebooks |

### 2. Silver notebook failure on threshold breach

| | |
|---|---|
| **Symptom** | `RuntimeError` / exit code 1 after successful Silver build |
| **Cause** | Injected duplicate keys breach 100% uniqueness threshold; orchestrator treated as fatal |
| **Fix** | Threshold misses → WARNING only; `main()` returns 0; tables still written |
| **Files** | `create_silver_tables.py`, `silver_notebook.ipynb` |

### 3. Gold revenue cross-check mismatch (~103K difference)

| | |
|---|---|
| **Symptom** | `SUM(total_revenue)` from product ≠ customer aggregations |
| **Cause** | 10 duplicate `customer_id` values in Silver (20 rows) double-counted in customer JOIN |
| **Fix** | `customer_dimension` CTE with `ROW_NUMBER()` in revenue-by-customer and segmentation SQL |
| **Files** | `02_revenue_by_customer.sql`, `04_customer_segmentation.sql` |

### 4. Pie chart equal 25% slices

| | |
|---|---|
| **Symptom** | Four segments each show 25% on pie chart |
| **Cause** | Visualization counted 4 query rows instead of `customer_count` measure |
| **Fix** | Configure pie measure as **SUM(customer_count)** |
| **Files** | `DASHBOARD_GUIDE.md` |

### 5. Bronze schema drift

| | |
|---|---|
| **Symptom** | Unexpected NULLs or missing columns after upstream CSV change |
| **Cause** | Extra columns dropped silently; missing columns → NULLs without error |
| **Mitigation** | `validate_dataframe_columns()` catches name/order mismatch; document contract tests for production |
| **Files** | `bronze_config.py` |

### 6. Local Spark: `UnresolvedAddressException`

| | |
|---|---|
| **Symptom** | PySpark fails to bind on Mac |
| **Fix** | `SPARK_LOCAL_IP=127.0.0.1` when starting Spark locally |

### 7. Databricks Job parameter format

| | |
|---|---|
| **Symptom** | Job rejects `--output-dir /path` as single parameter string |
| **Fix** | JSON array: `["--output-dir", "/Volumes/.../to_process"]` |
| **Files** | `databricks-job-data-generation.json` |



## Debugging Workflow (Recommended Order)

```text
1. Data generation
   └─ Confirm print_summary() issue counts match spec

2. Bronze ingest
   └─ Ingestion summary: rows read = rows written = success
   └─ Delta verification: tables exist with expected counts
   └─ Sample bronze_orders anomalies visible (NULL / 99999 FKs)

3. Silver build
   └─ Bronze in = Silver out (Match YES for all three tables)
   └─ Metrics report: failure counts align with injected issues
   └─ python3 test_data_quality.py (all 7 injection categories vs flag columns)
   └─ Sample FAIL orders: inspect flag columns + details
   └─ Expect uniqueness WARNING (injected dupes)

4. Gold build
   └─ PASS order count ~99,600
   └─ Row counts: 500 / 10,000 / 4
   └─ Revenue cross-check: MATCH
   └─ Manual verification sample: Gold totals = sum of Silver PASS orders

5. Dashboard
   └─ Run DASHBOARD_GUIDE.md checks
   └─ Confirm pie uses SUM(customer_count)

6. Databricks Jobs (when scheduled/triggered)
   └─ On failure: email alert from job notification settings
   └─ Jobs UI: open failed run → task logs and notebook output
```

---

## Verification Commands

### Local pipeline (full run)

```bash
export SPARK_LOCAL_IP=127.0.0.1   # if needed on Mac

python3 src/data_generation/generate_sample_data.py
cp data/*.csv dbfs/FileStore/medallion/ingestion/to_process/
python3 src/bronze/ingest_all.py
python3 src/silver/create_silver_tables.py
python3 test_data_quality.py
python3 src/gold/create_gold_tables.py
```

### Automated Silver DQ test (`test_data_quality.py`)

After Bronze + Silver, run from the repo root:

```bash
SPARK_LOCAL_IP=127.0.0.1 python3 test_data_quality.py
```

Asserts failed-row counts on Silver flag columns against the seven injection
categories (constants imported from `generate_sample_data.py`). Exit code `0` =
PASS; non-zero lists each mismatch.

### CSV spot-checks (injected defects)

```bash
awk -F',' 'NR>1 && $3=="" {c++} END{print "NULL email:", c+0}' data/customers.csv
awk -F',' 'NR>1 && $2=="" {c++} END{print "NULL customer_id:", c+0}' data/orders.csv
awk -F',' 'NR>1 && $4=="" {c++} END{print "NULL product_id:", c+0}' data/orders.csv
awk -F',' 'NR>1 && $2=="99999" {c++} END{print "Orphan customer_id:", c+0}' data/orders.csv
awk -F',' 'NR>1 && $4=="99999" {c++} END{print "Orphan product_id:", c+0}' data/orders.csv
```

### SQL sanity checks (Databricks or local Spark SQL)

```sql
-- Silver: row counts
SELECT COUNT(*) FROM silver_customers;   -- 10,010
SELECT COUNT(*) FROM silver_orders;      -- 100,020
SELECT COUNT(*) FROM silver_products;   -- 500

-- Silver: FAIL vs PASS orders
SELECT quality_check_result, COUNT(*)
FROM silver_orders
GROUP BY quality_check_result;

-- Gold: revenue reconciliation
SELECT SUM(total_revenue) FROM gold_sales_by_product;
SELECT SUM(total_revenue) FROM gold_revenue_by_customer;

-- Dashboard: segmentation totals
SELECT SUM(customer_count) FROM gold_customer_segmentation;
SELECT COUNT(*) FROM gold_revenue_by_customer;
```

### Expected values (seed 42, validated local run)

| Check | Expected |
|-------|----------|
| Bronze/Silver row counts | 10,010 / 500 / 100,020 |
| Silver FAIL orders | ~420 |
| Silver PASS orders | ~99,600 |
| Gold tables | 500 / 10,000 / 4 rows |
| Revenue cross-check | MATCH (139,462,031.78) |
| Pie total vs customer rows | 10,000 = 10,000 |

---

## Architecture Questions Resolved via Debugging

| Question | Answer |
|----------|--------|
| Is Bronze deleted when Silver runs? | No — only CSV archived at Bronze; Bronze Delta untouched |
| Does Silver “clean” data? | No — flags only; row count preserved |
| Where are bad orders excluded? | Gold SQL `PASS` filter only |
| Multi-run behaviour? | Overwrite at each layer; latest snapshot wins |
| SCD / history needed? | No for this scope; Gold is point-in-time aggregates |

---

## Gaps and Follow-Ups

| Item | Status |
|------|--------|
| Silver DQ automated test | Implemented — `test_data_quality.py` |
| Broader unit/integration test suite | Not implemented beyond Silver flag-count test |
| Quarantine table for FAIL rows | Not implemented — query `silver_* WHERE quality_check_result = 'FAIL'` |

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `test_data_quality.py` | Programmatic Silver flag-count assertions vs injected defects |
| `data-quality-strategy.md` | Check definitions, thresholds, injected issues |
| `design-notes.md` | Architecture and debugging approach summary |
| `requirements-analysis.md` | Edge cases and clarifications |
| `src/dashboard/DASHBOARD_GUIDE.md` | Dashboard verification SQL |
| `ai-prompts/silver-layer.md` | Silver validation session log |
| `ai-prompts/gold-layer.md` | Gold cross-check fix session log |

*Last updated: 2026-08-31*
