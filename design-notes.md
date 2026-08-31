# Design decisions and architecture notes

Design document for the **databricks-medallion-pipeline** project. For run
instructions see `README.md`; for business background see `project-context.md`.

---

## Architecture Overview

The pipeline implements a **Databricks Medallion Architecture** for synthetic
e-commerce data: customers, orders, and products. Data flows through four stages
plus orchestration on Unity Catalog volumes.

```text
Job 1: Data_generation
        │
        ▼
CSV → /Volumes/.../medalion/to_process/
        │
        ▼  (file_arrival trigger)
Job 2: Bronze → Silver → Gold → Dashboard refresh
```

### Layer flow

```text
DATA GENERATION     Faker + injected DQ issues → timestamped CSVs
        │
        ▼
BRONZE              Raw CSV → Delta (bronze_customers, bronze_orders, bronze_products)
                    Archive CSV to processed/; no cleaning
        │
        ▼
SILVER              Quality flags on every row; row count == Bronze
                    silver_* tables + quality_check_result
        │
        ▼
GOLD                PASS-only aggregations → gold_* tables
        │
        ▼
DASHBOARD           SQL Dashboard: bar, histogram, pie (reads Gold)
```

### Orchestration (Databricks)

| Job | Definition file | Trigger | Tasks |
|-----|-----------------|---------|-------|
| Data_generation | `databricks-job-data-generation.json` | Manual / schedule | `Data_gen_notebook` → writes CSVs to `to_process/` |
| Data Ingestion and Processing | `databricks-job.json` | File arrival on `to_process/` | Bronze → Silver → Gold → Dashboard_Refresh |

### Core principles

| Principle | Layer | Rationale |
|-----------|-------|-----------|
| Raw ingest | Bronze | Preserve NULLs, dupes, orphan FKs for Silver to detect |
| Flag, don't delete | Silver | Auditability; every Bronze row survives in Silver |
| PASS-only filter | Gold | Untrustworthy orders excluded from business metrics only |
| Overwrite snapshots | Bronze, Silver, Gold | Batch evaluation scope; latest run replaces prior Delta state |
| Config per layer | `*_config.py` | Same code locally and on Databricks via env vars |

### Storage (Databricks)

| Path | Purpose |
|------|---------|
| `.../medalion/to_process/` | Incoming CSV staging |
| `.../medalion/processed/` | Archived CSVs after Bronze |
| `.../medalion/bronze/`, `silver/`, `gold/` | Delta tables per layer |

Locally: `dbfs/FileStore/medallion/ingestion/` and `delta/{bronze,silver,gold}/`.

---

## Data Model & Schema

### Source CSVs (data generation)

All fields are generated as strings in CSV; Bronze reads them as `StringType`.

#### customers (~10,010 rows after injection)

| Column | Logical type | Notes |
|--------|--------------|-------|
| customer_id | INT (PK) | Sequential; 10 duplicate keys injected |
| customer_name | STRING | Faker-generated |
| email | STRING | 50 NULLs injected |
| country | STRING | |
| signup_date | DATE | |
| customer_segment | STRING | Premium / Standard / Basic |
| lifetime_value | DECIMAL(2dp) | |

#### products (~500 rows)

| Column | Logical type | Notes |
|--------|--------------|-------|
| product_id | INT (PK) | Sequential |
| product_name | STRING | |
| category | STRING | |
| price | DECIMAL(2dp) | Always > cost |
| cost | DECIMAL(2dp) | |
| stock_quantity | INT | |
| reorder_level | INT | |

#### orders (~100,020 rows after injection)

| Column | Logical type | Notes |
|--------|--------------|-------|
| order_id | INT (PK) | 20 duplicate keys injected |
| customer_id | INT (FK) | 100 NULLs; 50 orphan (99999) |
| order_date | DATE | |
| product_id | INT (FK) | 200 NULLs; 30 orphan (99999) |
| quantity | INT | |
| unit_price | DECIMAL(2dp) | Matches product price at order time |
| total_amount | DECIMAL(2dp) | quantity × unit_price |
| order_status | STRING | Pending / Completed / Cancelled |
| payment_date | DATE | NULL when Pending or Cancelled |

### Bronze Delta tables

Source columns plus metadata:

| Column | Type | Purpose |
|--------|------|---------|
| `_ingested_at` | TIMESTAMP | Ingestion time |
| `_source_file` | STRING | Source CSV filename |

Tables: `bronze_customers`, `bronze_orders`, `bronze_products`.

### Silver Delta tables

All Bronze columns retained, plus per-check flag columns:

| Column pattern | Example |
|----------------|---------|
| `{check}_check_passed` | `completeness_check_passed` (BOOLEAN) |
| `{check}_check_details` | `completeness_check_details` (STRING) |
| `quality_check_result` | `PASS` or `FAIL` (combined) |

Tables: `silver_customers`, `silver_orders`, `silver_products`.

### Gold Delta tables

| Table | Grain | Key columns |
|-------|-------|-------------|
| `gold_sales_by_product` | One row per product | product_id, product_name, category, total_orders, total_revenue, avg_order_value |
| `gold_revenue_by_customer` | One row per unique customer_id | customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual |
| `gold_customer_segmentation` | One row per behavioral segment | segment_type, customer_count, avg_revenue, total_revenue |

### Relationships

```text
customers (1) ──< orders (N) >── (1) products
```

Bronze does not enforce FKs; Silver referential integrity check validates them.

---

## Bronze Layer Design

### Purpose

Raw landing zone: ingest CSVs exactly as delivered, including all intentional
quality defects. No NULL handling, deduplication, type casting, or FK validation.

### Components

| File | Role |
|------|------|
| `bronze_config.py` | Paths, schemas, Spark session, ingest/archive helpers |
| `01_ingest_customers.py` | customers CSV → `bronze_customers` |
| `02_ingest_orders.py` | orders CSV → `bronze_orders` |
| `03_ingest_products.py` | products CSV → `bronze_products` |
| `ingest_all.py` | Orchestrator: customers → products → orders |

### Ingestion workflow

1. Discover oldest matching file in `to_process/` (regex: `customers_*.csv`, etc.).
2. Read with explicit `StructType` (all `StringType`).
3. Append `_ingested_at`, `_source_file`.
4. Write Delta with `mode("overwrite")`.
5. Validate row count read == row count written.
6. Move source CSV to `processed/` (same filename).

### Design decisions

- **StringType everywhere** — anomalies stay representable; type rules deferred to Silver.
- **Overwrite write** — each batch replaces Bronze; not append-only history.
- **Continue on failure** — `ingest_all.py` attempts all sources; exits non-zero if any fail.
- **Databricks bootstrap** — `inspect.currentframe()` when `__file__` undefined in Jobs.

### Schema drift (known behaviour)

| Scenario | Outcome |
|----------|---------|
| Extra CSV column | Dropped silently |
| Missing expected column | NULLs inserted — silent risk |
| Column reorder | OK |
| Re-ingest same batch | Overwrites Delta; CSV already in processed |

---

## Silver Layer Design

### Purpose

Apply data quality checks and **flag** every row. Row count must equal Bronze.
Bad rows are never deleted or filtered out of Silver tables.

### Components

| File | Check |
|------|-------|
| `silver_config.py` | Thresholds, read/write, metrics report |
| `01_quality_completeness.py` | NULL/blank required fields |
| `02_quality_uniqueness.py` | Duplicate PK values |
| `03_quality_type_validation.py` | Cast, range, enum validation |
| `04_quality_referential_integrity.py` | Orphan FKs (non-NULL only) |
| `create_silver_tables.py` | Orchestrator + metrics report |

### Checks per table

| Table | Completeness | Uniqueness | Type | Referential |
|-------|:---:|:---:|:---:|:---:|
| silver_customers | email | customer_id | ✓ | N/A |
| silver_orders | customer_id, product_id | order_id | ✓ | ✓ |
| silver_products | — | — | ✓ | N/A |

N/A checks get placeholder columns (`passed=True`, `details=""`).

### Flag columns

Each check adds `{check}_check_passed` (BOOLEAN) and `{check}_check_details`
(STRING). Final `quality_check_result` = `PASS` only when all applicable
checks pass for that row.

### Row count guard

```text
Bronze count == Silver input count == Silver written count
```

Mismatch raises `RuntimeError` — prevents silent row loss from accidental filters.

### Orchestration

Reads Bronze Delta → applies checks in sequence → writes Silver Delta → prints
quality metrics report (total, passed, failed, % per check per table).

Threshold misses (e.g. uniqueness < 100% with injected dupes) are **warnings**;
tables are still written.

---

## Gold Layer Design

### Purpose

Business-ready aggregations for BI. Uses **only orders where
`quality_check_result = 'PASS'`** — documented in SQL as the Gold boundary filter.
Silver retains all rows for audit; Gold excludes untrustworthy orders from metrics.

### Components

| File | Role |
|------|------|
| `gold_config.py` | Paths, segmentation constants, SQL template rendering |
| `01_sales_by_product.sql` | Revenue and orders per product |
| `02_revenue_by_customer.sql` | Revenue and orders per customer |
| `04_customer_segmentation.sql` | Behavioral segments |
| `create_gold_tables.py` | Runs SQL, writes Delta, revenue cross-check |

Daily/weekly trends (`03_daily_weekly_trends.sql`) explicitly **not built** (stretch).

### Aggregation logic

**Sales by product** — LEFT JOIN products to PASS order stats; products with zero
qualifying orders show 0 / 0.00.

**Revenue by customer** — LEFT JOIN deduplicated customer dimension to PASS order
stats; `lifetime_value_actual` = computed `total_revenue` from orders.

**Customer segmentation** — behavioral `segment_type` (not source `customer_segment`):

| segment_type | Rule |
|--------------|------|
| Inactive | 0 PASS orders |
| High-Value | `NTILE(5) = 1` by revenue among customers with ≥1 PASS order (top 20%) |
| Repeat | ≥2 PASS orders, not High-Value |
| One-Time | Exactly 1 PASS order, not High-Value |

Constants: `HIGH_VALUE_TOP_PERCENT=20`, `HIGH_VALUE_N_TILES=5`, `REPEAT_MIN_ORDERS=2`.

### Customer deduplication

Silver may contain duplicate `customer_id` rows (injected issue). Gold SQL uses a
`customer_dimension` CTE with `ROW_NUMBER()` to count each customer once — prevents
revenue double-counting in customer and segmentation tables.

### Validation built into orchestrator

- Revenue cross-check: `SUM(total_revenue)` from sales-by-product must equal
  revenue-by-customer (tolerance ±0.01).
- Manual verification sample: one product + one customer with underlying Silver
  PASS orders shown for hand-checking.

### Dashboard consumption

Gold tables feed `src/dashboard/dashboard_queries.sql` (bar, histogram, pie).

---

## Data Quality Validation Strategy

### Philosophy

| Layer | DQ approach |
|-------|-------------|
| Bronze | None — preserve raw data |
| Silver | Detect, flag, measure — never delete |
| Gold | Filter to PASS at aggregation boundary only |
| Dashboard | Read trustworthy Gold metrics |

### Silver checks (detail)

#### 1. Completeness (>99% threshold)

| Table | Required fields |
|-------|-----------------|
| customers | email |
| orders | customer_id, product_id |

NULL or blank string → fail. Details list which field(s) failed.

#### 2. Uniqueness (100% threshold)

| Table | Key |
|-------|-----|
| customers | customer_id |
| orders | order_id |

**All rows** sharing a duplicated key are flagged, not just extra copies.

#### 3. Type validation (>99% threshold)

Validates string Bronze values can cast to expected types:

- INT fields: non-negative integer (`customer_id`, `order_id`, `product_id`, `quantity`, etc.)
- DECIMAL fields: non-negative (`price`, `cost`, `total_amount`, `lifetime_value`, etc.)
- DATE fields: parseable (`signup_date`, `order_date`, `payment_date`)
- ENUM fields: `order_status` ∈ {Pending, Completed, Cancelled}; `customer_segment` ∈ {Premium, Standard, Basic}

Present-but-invalid fails; NULL/blank skipped for optional fields.

#### 4. Referential integrity (>99.9% threshold)

| FK | Reference |
|----|-----------|
| orders.customer_id | customers.customer_id |
| orders.product_id | products.product_id |

NULL FKs **excluded** — completeness already flags them. Only orphan non-NULL values fail.

### Metrics report

Per table, per check: total rows, passed, failed, % passed, threshold, meets threshold.

### Expected results with injected data

| Issue | Expected Silver failures |
|-------|--------------------------|
| NULL email | 50 completeness (customers) |
| Duplicate customer_id | 20 uniqueness (customers) |
| NULL customer_id / product_id | 100 / 200 completeness (orders) |
| Orphan FKs | 50 + 30 referential (orders) |
| Duplicate order_id | 40 uniqueness (orders) |

Uniqueness thresholds will breach 100% by design — reported as warnings, not build failures.

### Gold DQ boundary

```sql
WHERE quality_check_result = 'PASS'
```

Applied in all three Gold SQL files. ~99,600 of 100,020 orders qualify after Silver
validation (420 excluded).

---

## Debugging Approach

### General methodology

1. **Verify row counts first** — Bronze in == Silver out == expected; Gold PASS count sensible.
2. **Compare to injected issue counts** — Silver failure counts should match spec (~50/100/200/etc.).
3. **Cross-layer reconciliation** — Gold product revenue sum == customer revenue sum.
4. **Sample hand-check** — pick one customer/product; sum underlying Silver PASS orders.
5. **Log prompts and fixes** in `ai-prompts/debugging.md` and `debugging-notes.md`.

### Issues encountered and resolution

#### Databricks Job: `NameError: __file__`

**Symptom:** Bronze/Silver/Gold scripts fail when run as Job tasks.

**Cause:** Databricks executes via `exec()` — `__file__` is undefined.

**Fix:** Bootstrap path with `inspect.currentframe()`; set `BRONZE_SRC_DIR` /
`SILVER_SRC_DIR` / `GOLD_SRC_DIR` in notebooks.

#### Silver notebook "failure" on threshold

**Symptom:** `RuntimeError` after successful Silver build.

**Cause:** Injected duplicate keys breach 100% uniqueness threshold; orchestrator
returned exit code 1.

**Fix:** Treat threshold misses as warnings; tables still written. Notebook calls
`main()` without raising on threshold breach.

#### Gold revenue cross-check mismatch

**Symptom:** `SUM(total_revenue)` product ≠ customer (~103K difference).

**Cause:** 10 duplicate `customer_id` keys in Silver (20 rows) double-counted in
customer aggregation JOIN.

**Fix:** `customer_dimension` CTE with `ROW_NUMBER()` in `02_revenue_by_customer.sql`
and `04_customer_segmentation.sql`.

#### Pie chart 25% per segment

**Symptom:** Dashboard shows four equal 25% slices.

**Cause:** Visualization used COUNT of query rows (4 segments) instead of
`customer_count` measure.

**Fix:** Set pie measure to **SUM(customer_count)**; documented in
`DASHBOARD_GUIDE.md`.

#### Schema drift at Bronze

**Symptom:** Unexpected NULLs or missing data after upstream CSV change.

**Approach:** Trace `read_csv_with_schema` + explicit StructType behaviour (see
Bronze Layer Design). Extra columns dropped; missing columns → NULLs without error.
Mitigation for production: header contract tests or schema evolution policy.

#### Local Spark: `UnresolvedAddressException`

**Symptom:** PySpark fails to start locally on some Mac setups.

**Fix:** Run with `SPARK_LOCAL_IP=127.0.0.1`.

#### Job parameters rejected

**Symptom:** Databricks Job rejects `--output-dir /path` as single string.

**Fix:** JSON array format: `["--output-dir", "/Volumes/.../to_process"]`.

### Verification commands

```bash
# Local pipeline
python3 src/bronze/ingest_all.py
python3 src/silver/create_silver_tables.py
python3 test_data_quality.py
python3 src/gold/create_gold_tables.py

# CSV issue counts (data/)
awk -F',' 'NR>1 && $3=="" {c++} END{print c}' data/customers.csv   # NULL email
awk -F',' 'NR>1 && $2=="" {c++} END{print c}' data/orders.csv     # NULL customer_id
awk -F',' 'NR>1 && $4=="" {c++} END{print c}' data/orders.csv     # NULL product_id
```

```sql
-- Dashboard sanity check
SELECT SUM(customer_count) FROM gold_customer_segmentation;
SELECT COUNT(*) FROM gold_revenue_by_customer;
-- Both should equal 10,000
```

### Architecture questions debugged via design review

| Question | Resolution |
|----------|------------|
| Is Bronze deleted when Silver runs? | No — only CSV archived; Bronze Delta untouched by Silver |
| Multi-run behaviour? | Overwrite at each layer; Job 1 → Job 2 chain per batch |
| Need SCD in Gold? | No — SCD belongs in Silver if history required; Gold is snapshot aggregates |

---

## Related documents

| Document | Purpose |
|----------|---------|
| `README.md` | Run instructions, Databricks job definitions |
| `project-context.md` | Business requirements and evaluation criteria |
| `data-quality-strategy.md` | Extended DQ documentation (to align with this doc) |
| `test_data_quality.py` | Automated Silver flag-count test vs injected defects |
| `debugging-notes.md` | Debugging measures and verification commands |
| `data-model.md` | Entity-level schema reference (to align with this doc) |
| `src/dashboard/DASHBOARD_GUIDE.md` | Dashboard build and verification |
| `ai-prompts/{layer}.md` | AI prompt history per pipeline activity |

*Last updated: 2026-08-31*
