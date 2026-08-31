# Data Quality Strategy

This document describes how data quality is enforced across the medallion
pipeline. **Bronze** preserves all raw defects; **Silver** detects and flags
them without deleting rows; **Gold** uses only `quality_check_result = 'PASS'`
rows for business aggregations.

Implementation: `src/silver/` (`01`–`04` quality modules, `create_silver_tables.py`).

---

## Quality Checks Overview

Silver applies four row-level checks. Each check adds `{check}_check_passed`
(BOOLEAN) and `{check}_check_details` (STRING). A combined
`quality_check_result` column is set to `PASS` only when all applicable checks
pass for that row.

| Table | Completeness | Uniqueness | Type validation | Referential integrity |
|-------|:---:|:---:|:---:|:---:|
| silver_customers | email | customer_id | ✓ | N/A |
| silver_orders | customer_id, product_id | order_id | ✓ | customer_id, product_id |
| silver_products | — | — | ✓ | N/A |

Checks marked N/A receive placeholder columns (`passed = true`, `details = ""`).

**Core rule:** Silver row count must equal Bronze row count — no filtering or
dropping of bad rows at this layer.

---

### 1. Completeness Check

- **What:** No NULL or blank values in critical fields required for downstream joins and identity.
- **How:** Evaluate `isNull() OR trim(column) = ''` on required columns.
  - **customers:** `email`
  - **orders:** `customer_id`, `product_id`
- **Threshold:** >99% complete (`COMPLETENESS_THRESHOLD_PCT = 99.0`)
- **Result:** Flag rows with missing required values; `completeness_check_details` lists which field(s) failed (e.g. `email is NULL`, `customer_id is NULL`).

---

### 2. Uniqueness Check

- **What:** No duplicate primary-key values within a table.
- **How:** Window function `COUNT(*) OVER (PARTITION BY key) > 1` on:
  - **customers:** `customer_id`
  - **orders:** `order_id`
- **Threshold:** 100% unique (`UNIQUENESS_THRESHOLD_PCT = 100.0`)
- **Result:** Flag **all rows** sharing a duplicated key (not only the “extra” copy). `uniqueness_check_details` includes the duplicated key value.

---

### 3. Referential Integrity

- **What:** Foreign keys in orders must reference existing parent keys when present.
- **How:** Left join `orders.customer_id` to distinct `customers.customer_id` and
  `orders.product_id` to distinct `products.product_id`. Flag when a non-NULL,
  non-blank FK has no matching parent row.
- **Threshold:** >99.9% valid (`REFERENTIAL_INTEGRITY_THRESHOLD_PCT = 99.9`)
- **Result:** Flag orphan records; `referential_integrity_details` notes
  `orphan customer_id: …` and/or `orphan product_id: …`. NULL FKs are **not**
  evaluated here — completeness already flags them.

---

### 4. Type Validation Check

*(Additional check required by the project; applied in Silver alongside the three above.)*

- **What:** Bronze string columns must cast to expected logical types and allowed enums.
- **How:** Per-column rules — non-negative integer cast, decimal cast, date parse,
  enum membership (`order_status`, `customer_segment`). Present-but-invalid values
  fail; NULL/blank optional fields are skipped.
- **Threshold:** >99% valid (`TYPE_VALIDATION_THRESHOLD_PCT = 99.0`)
- **Result:** Flag rows with type or enum violations; `type_validation_details` lists failed fields.

---

## Quality Metrics Report

After Silver tables are built, `create_silver_tables.py` prints a **Silver
Quality Metrics Report** via `print_quality_metrics_report()` in
`silver_config.py`.

### Report columns

| Column | Description |
|--------|-------------|
| Table | Silver table name (`silver_customers`, `silver_orders`, `silver_products`) |
| Check | Check name (Completeness, Uniqueness, Type validation, Referential integrity) |
| Total | Total rows evaluated |
| Passed | Rows where `{check}_check_passed = true` |
| Failed | Rows where the check failed |
| % Passed | `(passed / total) × 100`, two decimal places |
| Threshold | Configured minimum pass rate (e.g. `99.0%`, `100.0%`) |
| Meets Threshold | `YES` or `NO` |

### Example output shape

```text
Silver Quality Metrics Report
-----------------------------+----------+--------+--------+--------+----------+-----------+-----------------
Table                        | Check    | Total  | Passed | Failed | % Passed | Threshold | Meets Threshold
-----------------------------+----------+--------+--------+--------+----------+-----------+-----------------
silver_customers             | ...      | 10,010 | ...    | ...    | ...      | 99.0%     | YES
...
```

### Threshold behaviour

- Metrics are computed **per table, per check** after all flag columns are applied.
- If any check falls below its threshold, a **WARNING** is printed listing
  `table.check: X.XX% < Y.Y%`.
- Threshold misses (e.g. uniqueness below 100% when duplicate keys are injected)
  are **warnings only** — Silver tables are still written so flagged rows remain
  available for audit.
- Row-count guard: if Bronze count ≠ Silver count before or after write, the
  orchestrator raises `RuntimeError` (prevents silent row loss).

### Combined row outcome

`quality_check_result`:

- `PASS` — all applicable `{check}_check_passed` columns are `true` for that row.
- `FAIL` — one or more applicable checks failed.

Gold layer SQL filters `WHERE quality_check_result = 'PASS'` when aggregating
orders (~99,600 of 100,020 orders in the current sample).

### Sample failed rows

`create_silver_tables.py` also prints up to five sample `silver_orders` rows
where `quality_check_result = FAIL`, showing individual check columns and
detail strings for manual inspection.

---

## Sample Data Quality Issues

Synthetic data is generated by `src/data_generation/generate_sample_data.py`
(`RANDOM_SEED = 42`). Clean base volumes: **10,000 customers**, **500 products**,
**100,000 orders**. Deliberate defects are injected in isolated functions so
the Silver layer has realistic failures to detect.

The project target is **~700 problematic rows (~0.7% defect rate)** across the
dataset (`project-context.md`). The table below lists the **460 deliberate
injection operations** plus **30 appended duplicate rows** (10 customer, 20
order copies). Silver may flag more rows than injections when duplicate keys
flag both copies, or when one order fails multiple checks.

### Injected issues by source file

| File | Issue | Injected count | Silver check(s) affected | Expected Silver impact |
|------|-------|---------------:|--------------------------|------------------------|
| customers.csv | NULL `email` | 50 | Completeness | 50 rows fail completeness |
| customers.csv | Duplicate `customer_id` | 10 keys (10 appended rows) | Uniqueness | 20 rows fail uniqueness (original + copy per key) |
| orders.csv | NULL `customer_id` | 100 | Completeness | 100 rows fail completeness |
| orders.csv | NULL `product_id` | 200 | Completeness | 200 rows fail completeness |
| orders.csv | `customer_id` not in customers (`99999`) | 50 | Referential integrity | 50 rows fail referential integrity |
| orders.csv | `product_id` not in products (`99999`) | 30 | Referential integrity | 30 rows fail referential integrity |
| orders.csv | Duplicate `order_id` | 20 keys (20 appended rows) | Uniqueness | 40 rows fail uniqueness (original + copy per key) |

**Injection total:** 460 field-level modifications + 30 extra rows = **490
direct touches**. Orphan FK value used: `99999` (not present in parent tables).

### Resulting dataset sizes

| File | Clean rows | After injection | Notes |
|------|------------|-----------------|-------|
| customers.csv | 10,000 | 10,010 | +10 duplicate-row appends |
| products.csv | 500 | 500 | No injected defects |
| orders.csv | 100,000 | 100,020 | +20 duplicate-row appends |

### Overlap and Gold impact

- Order injections use **disjoint row index sets** (NULL customer → NULL product
  → invalid customer → invalid product → duplicates), so each injection type
  maps to distinct rows where possible.
- A single order row can still fail **multiple** checks in edge cases; most
  failures are single-check.
- After all Silver checks, approximately **420 orders** have
  `quality_check_result = FAIL` and are excluded from Gold aggregations; the
  remainder (~99,600) are `PASS`.

### Verification

Regenerate and confirm injection counts:

```bash
python3 src/data_generation/generate_sample_data.py
```

Run Silver and review the metrics report:

```bash
python3 src/silver/create_silver_tables.py
```

Assert Silver flag counts programmatically (all seven injection categories):

```bash
SPARK_LOCAL_IP=127.0.0.1 python3 test_data_quality.py
```

See `test_data_quality.py` and `debugging-notes.md` for assertion details.

Quick CSV spot-checks (local `data/` folder):

```bash
awk -F',' 'NR>1 && $3=="" {c++} END{print "NULL email:", c+0}' data/customers.csv
awk -F',' 'NR>1 && $2=="" {c++} END{print "NULL customer_id:", c+0}' data/orders.csv
awk -F',' 'NR>1 && $4=="" {c++} END{print "NULL product_id:", c+0}' data/orders.csv
```

These issues are **required test fixtures** — do not remove them from the
generator. The goal is for Silver to detect and flag them, not to “fix” them
upstream.

---

## Related documents

| Document | Purpose |
|----------|---------|
| `design-notes.md` | Layer architecture and DQ boundary at Gold |
| `project-context.md` | Business requirements and injection spec |
| `src/silver/silver_config.py` | Threshold constants and metrics helpers |
| `test_data_quality.py` | Automated Silver flag-count test vs injected defects |
| `ai-prompts/silver-layer.md` | AI prompt history for Silver implementation |

*Last updated: 2026-08-31*
