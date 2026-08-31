# Requirement Analysis

Analysis of requirements for the **databricks-medallion-pipeline** project —
an AI-capability evaluation exercise building a Databricks Medallion Architecture
pipeline for synthetic e-commerce sales data.

Sources: `project-context.md`, project brief, and implemented pipeline behaviour.

---

## Problem Statement

An e-commerce company receives daily batch extracts from three operational
systems — a customer database, an order system, and a product catalog — as CSV
files. The business needs a reliable analytics pipeline on Databricks that:

1. **Lands data as-is** so nothing is lost before quality review.
2. **Proves data trustworthiness** through explicit, measurable quality checks.
3. **Produces business-ready metrics** (product sales, customer revenue, behavioral
   segments) for stakeholders.
4. **Visualizes key KPIs** in a SQL dashboard.


---

## Functional Requirements

### Data generation

- Generate synthetic **customers** (~10,000), **products** (~500), and **orders**
  (~100,000) CSV files using Faker with a fixed seed for reproducibility.
- Inject deliberate data quality defects (~0.7% problematic rows) so Silver has
  realistic failures to detect — do not “fix” these in the generator.
- Support local output (`data/`) and Databricks volume output (`to_process/`).
- Print row counts and a breakdown of injected issue types per file.

### Bronze layer (raw ingestion)

- Ingest three CSV sources: `customers`, `orders`, `products`.
- **No cleaning, filtering, or transformation** — preserve NULLs, duplicates, and
  orphan foreign keys.
- Read with explicit schemas; store all business columns as strings.
- Append ingestion metadata: `_ingested_at`, `_source_file`.
- Write Delta tables: `bronze_customers`, `bronze_orders`, `bronze_products`.
- Archive processed CSVs from `to_process/` to `processed/`.
- Log row counts; validate rows read equals rows written.
- Provide per-source ingest scripts plus an orchestrator (`ingest_all.py`).

### Silver layer (quality & validation)

- Implement four required checks:
  1. **Completeness** — NULL/blank in `email` (customers), `customer_id` and
     `product_id` (orders); threshold >99%.
  2. **Uniqueness** — duplicate `customer_id` (customers), duplicate `order_id`
     (orders); threshold 100%.
  3. **Referential integrity** — orphan `customer_id` / `product_id` in orders;
     threshold >99.9%.
  4. **Type validation** — cast, range, date parse, and enum checks; threshold >99%.
- **Flag bad rows, never delete or filter** — add `quality_check_result` (`PASS`/`FAIL`)
  and per-check detail columns; Silver row count must equal Bronze.
- Produce a **quality metrics report** showing total, passed, failed, % passed,
  threshold, and meets-threshold per check per table.
- Orchestrate all checks via `create_silver_tables.py`.

### Gold layer (aggregations)

- Build three required aggregation tables:
  1. **Sales by product** — `product_id`, `product_name`, `category`,
     `total_orders`, `total_revenue`, `avg_order_value`.
  2. **Revenue by customer** — `customer_id`, `customer_name`, `customer_segment`,
     `total_orders`, `total_revenue`, `avg_order_value`, `lifetime_value_actual`.
  3. **Customer segmentation** — behavioral `segment_type`
     (High-Value / Repeat / One-Time / Inactive), `customer_count`, `avg_revenue`,
     `total_revenue`.
- Use **only orders with `quality_check_result = 'PASS'`** at the Gold boundary.
- Deduplicate `customer_id` in customer-facing Gold outputs when Silver contains
  duplicate keys.
- Validate revenue consistency: sum of product revenue must equal sum of customer
  revenue (within tolerance).

### Dashboard

- Databricks SQL Dashboard with at least three visualizations fed by Gold tables:
  - **Bar chart** — top 10 products by revenue.
  - **Histogram** — customer revenue distribution.
  - **Pie chart** — customer segmentation by `customer_count` (not row count).
- Provide SQL queries (`dashboard_queries.sql`) and setup guide (`DASHBOARD_GUIDE.md`).

### Orchestration (Databricks)

- **Job 1 (Data_generation):** generate CSVs → write to `to_process/`.
- **Job 2 (Data Ingestion and Processing):** file-arrival trigger on `to_process/`
  → Bronze → Silver → Gold → dashboard refresh.

### Documentation & AI workflow

- Maintain root docs: README, design notes, data model, data quality strategy,
  requirements analysis, debugging notes, reflection, AI usage summary.
- Log prompts and outcomes per activity in `ai-prompts/{activity}.md`.
- README must support end-to-end setup from a clean clone (local and Databricks).

### Testing

- Data quality validation via `test_data_quality.py` — asserts Silver flag-column
  failure counts against all seven known injection categories from
  `generate_sample_data.py` (run after Bronze + Silver).
- Orchestrator-built guards and documented SQL spot-checks supplement the test
  (see `debugging-notes.md`, `data-quality-strategy.md`).

---

## Non-Functional Requirements

| Area | Requirement |
|------|-------------|
| **Platform** | Databricks Community Edition; Python, PySpark, SQL, Delta Lake |
| **Portability** | Same pipeline code runs locally and on Databricks via config/env vars |
| **Reproducibility** | Fixed random seed (42); deterministic sample data and issue injection |
| **Data privacy** | Synthetic data only — no real customer PII |
| **Auditability** | Silver retains all Bronze rows with explicit failure reasons |
| **Observability** | Row-count logging at ingest; quality metrics report; Gold cross-checks |
| **Error handling** | Bronze handles missing files, empty files, schema mismatch; Silver fails on row-count drift |

---

## Assumptions

- **Batch-only pipeline** — daily (or on-demand) full snapshots, not incremental CDC.
- **Overwrite semantics** — Bronze, Silver, and Gold Delta tables use `mode("overwrite")`;
  each run replaces the previous snapshot (no SCD/history in Delta).
- **Single-tenant evaluation** — one catalog/volume path (`workspace.default.medalion`);
  no multi-environment promotion in scope.
- **CSV as source of truth** — three flat files are the only ingestion format.
- **String-at-Bronze** — logical types are enforced in Silver validation and cast at Gold.
- **Injected defects are intentional** — uniqueness thresholds may breach 100% by design;
  that is a warning, not a pipeline failure.
- **Gold trusts Silver flags** — business metrics exclude `FAIL` orders; Silver does not
  “clean” data by removing rows.
- **Customer segmentation is behavioral** — derived from PASS order history, distinct
  from source `customer_segment` (Premium/Standard/Basic).
- **Databricks Jobs** — notebook paths and volume paths match the deployer’s workspace;
  Job 1 must run before Job 2 can be triggered via file arrival.
- **Evaluator access** — reviewer can run locally with PySpark/Delta or follow Databricks
  notebook/job instructions in README.

---

## Edge Cases

| Edge case | Expected behaviour |
|-----------|-------------------|
| **NULL FK in orders** | Completeness flags NULL; referential integrity skips NULL FKs |
| **Orphan FK (e.g. `99999`)** | Referential integrity flags; order may still fail completeness if other fields bad |
| **Duplicate primary keys** | Uniqueness flags **all** rows sharing the key (original + copy) |
| **One order fails multiple checks** | `quality_check_result = FAIL`; details list all failed checks |
| **Duplicate `customer_id` in Silver** | Gold dedupes via `ROW_NUMBER()` — revenue not double-counted |
| **Product with zero PASS orders** | Appears in `gold_sales_by_product` with 0 orders / 0 revenue |
| **Customer with zero PASS orders** | Classified as **Inactive** in segmentation |
| **Uniqueness below 100% threshold** | Warning in metrics report; Silver tables still written |
| **Bronze schema drift — extra CSV column** | Dropped silently (explicit StructType) |
| **Bronze schema drift — missing column** | NULLs inserted without error — risk documented |
| **Re-ingest same batch** | Delta overwritten; CSV already in `processed/` |
| **Empty or missing CSV** | Bronze ingest fails with error; orchestrator reports failure |

---

## Clarifications Needed

The following were ambiguous in the initial brief and were **resolved during
implementation** (documented for traceability):

| Topic | Resolution |
|-------|------------|
| Silver “cleaning” vs flagging | Silver **flags only** — no row deletion; Gold filters `PASS` orders |
| SCD / customer history | **Out of scope** — point-in-time snapshots; SCD would belong in Silver if added later |
| Daily/weekly trends aggregation | **Stretch goal** — not built unless explicitly requested |
| Uniqueness threshold breach | **Warning only** — tables still written so injected dupes remain auditable |
| Job failure alerting | **Email on failure** — configured in Databricks job setup (`databricks-job.json` `email_notifications.on_failure` on Bronze task); Jobs UI for logs |

**Optional clarifications** if extending beyond the evaluation scope:

- Should failed Silver rows be quarantined to a separate table for ops review?
- Is tracking history in silver and gold required so that the discovering of the time based trends be done?
- Should `order_status = Cancelled` orders be excluded from Gold revenue even when `PASS`?


---

## Related documents

| Document | Purpose |
|----------|---------|
| `project-context.md` | Full business and technical context |
| `design-notes.md` | Architecture and layer design |
| `data-model.md` | Entity schemas across layers |
| `data-quality-strategy.md` | Check definitions, thresholds, injected issues |
| `test_data_quality.py` | Automated Silver flag-count test |
| `README.md` | Setup and run instructions |

*Last updated: 2026-08-31*
