# Project Context — databricks-medallion-pipeline

> Purpose: This document is the persistent context I share with Cursor
> (pasted into chat at the start of a session, or referenced via
> @project-context.md) so it understands the full project without me
> re-explaining it in every prompt. It complements .cursorrules (which
> covers coding conventions) by covering the *business and technical
> problem* in depth.

## 1. What This Project Is

This is an AI-capability evaluation exercise for data engineers. I am
building a complete Databricks Medallion Architecture pipeline
(Bronze → Silver → Gold → Dashboard) for a e-commerce
company's sales data. The evaluation is not just about whether the
pipeline works — it's about how I use AI tools (Cursor) across the
full data engineering lifecycle: design, implementation, testing,
debugging, and reflection. Documentation and prompt history are graded
as heavily as the code itself.

## 2. Business Context

**Company:** An e-commerce company.

**Problem:** The company ingests daily sales data from three sources
(customer database, order system, product catalog) into Databricks.
They need:
- **Bronze:** raw ingestion of CSVs from S3/DBFS, no transformation
- **Silver:** data quality checks, cleaning, validation
- **Gold:** business-ready aggregations for analytics
- **Dashboard:** BI visualizations for business stakeholders

This mirrors a realistic daily-batch analytics pipeline: get the data
in reliably, prove it's trustworthy, summarize it for the business,
and visualize it.

## 3. Data Sources & Schema

**customers.csv** (~10,000 rows, ~500 KB)
| Field | Type | Notes |
|---|---|---|
| customer_id | INT | Primary Key |
| customer_name | STRING | |
| email | STRING | |
| country | STRING | |
| signup_date | DATE | |
| customer_segment | STRING | Premium / Standard / Basic |
| lifetime_value | DECIMAL | |

**orders.csv** (~100,000 rows, ~2-3 MB)
| Field | Type | Notes |
|---|---|---|
| order_id | INT | Primary Key |
| customer_id | INT | FK → customers |
| order_date | DATE | |
| product_id | INT | FK → products |
| quantity | INT | |
| unit_price | DECIMAL | |
| total_amount | DECIMAL | |
| order_status | STRING | Pending / Completed / Cancelled |
| payment_date | DATE | nullable |

**products.csv** (~500 rows, ~50 KB)
| Field | Type | Notes |
|---|---|---|
| product_id | INT | Primary Key |
| product_name | STRING | |
| category | STRING | |
| price | DECIMAL | |
| cost | DECIMAL | |
| stock_quantity | INT | |
| reorder_level | INT | |

## 4. Intentional Data Quality Issues

The sample data must include realistic, deliberately injected issues
so the Silver layer has something real to catch (~700 problematic rows
out of ~100,000, i.e. ~0.7%):

| File | Issue | Count |
|---|---|---|
| customers.csv | NULL email | 50 |
| customers.csv | duplicate customer_id | 10 |
| orders.csv | NULL customer_id | 100 |
| orders.csv | NULL product_id | 200 |
| orders.csv | customer_id not present in customers | 50 |
| orders.csv | product_id not present in products | 30 |
| orders.csv | duplicate order_id | 20 |

These are not bugs — they're required test fixtures. Do not "fix" them
in the generator; the point is for Silver to detect and flag them.

## 5. Architecture — Layer by Layer

**Bronze (raw ingestion):**
- Read the three CSVs from S#/DBFS as-is.
- No cleaning, no filtering, no transformation.
- Create Bronze Delta tables preserving source structure.
- Log ingestion metadata: row counts, timestamp, source file.

**Silver (quality & validation):**
Four required checks:
1. **Completeness** — no NULLs in email, customer_id, product_id (threshold >99%)
2. **Uniqueness** — no duplicate order_id / customer_id (threshold 100%)
3. **Referential integrity** — every customer_id/product_id in orders
   must exist in customers/products (threshold >99.9%)
4. **Type validation** — values conform to expected types/ranges

Rule: bad rows are **flagged**, not deleted — add a
`quality_check_result` column. Produce a quality metrics report
showing % passed per check.

**Gold (aggregations):**
Three required tables:
1. **Sales by Product** — product_id, product_name, category,
   total_orders, total_revenue, avg_order_value
2. **Revenue by Customer** — customer_id, customer_name,
   customer_segment, total_orders, total_revenue, avg_order_value,
   lifetime_value_actual
3. **Customer Segmentation** — segment_type
   (High-Value/Repeat/One-Time/Inactive), customer_count, avg_revenue,
   total_revenue

(Optional stretch only, not required: daily/weekly trends.)

**Dashboard:**
Databricks SQL Dashboard with 3+ tiles:
- Bar chart: Top 10 products by revenue
- Histogram: Customer revenue distribution
- Pie chart: Customer segmentation

## 6. Tech Stack

- Databricks Community Edition (free tier)
- Python, PySpark
- SQL (Databricks SQL for Gold/dashboard queries)
- Delta Lake for table storage

## 7. What "Done" Looks Like

- Sample data generated with the intentional issues above
- Bronze ingests all three sources successfully
- All four Silver quality checks implemented and passing correctly
  (i.e., correctly flagging the known issues, not silently passing them)
- Quality report showing % passed per check
- All three Gold aggregation tables produced with correct math
- Dashboard with 3+ working visualizations
- README with setup instructions that work end-to-end from a clean clone
- At least one test tier (data quality tests and/or pipeline tests)
- `test_data_quality.py` — Silver flag counts vs injected defect constants
- Full AI prompt history and lifecycle documentation (this is treated
  as equally important as the code — see .cursorrules)

## 8. Explicit Non-Goals (do not build unless asked)

- Streaming ingestion
- Orchestration frameworks (Airflow, Dagster, etc.)
- Production-grade CI/CD
- Additional aggregations/dashboard tiles beyond the required set
- Real customer PII of any kind — all data is synthetic

## 9. How I Want to Work With You (Cursor)

- I will usually give you a design note or spec before asking for code
  for a new component — build against that spec, don't invent scope.
- Prefer incremental, testable pieces (e.g., one quality check at a
  time) over large one-shot generations.
- After generating code, tell me how to validate it before I accept it.
- If something you suggest conflicts with the rules above (e.g.,
  deleting flagged rows, adding untracked scope), flag it rather than
  just doing it.
- I am logging this conversation's prompts and your responses in
  ai-prompts/{activity}.md — keep responses reasonably concise so
  they're easy to summarize honestly.
