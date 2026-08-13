# .cursorrules — databricks-medallion-pipeline

## Project Context
This repo implements a Databricks Medallion Architecture pipeline
(Bronze → Silver → Gold → Dashboard) for e-commerce sales data
(customers, orders, products). Stack: Python, PySpark, SQL, Delta Lake,
Databricks Community Edition. This is an AI-capability evaluation
project — code quality AND documentation/prompt-history are both
graded, so follow the structure and conventions below exactly.

## Repository Structure — Do Not Deviate
Always place new files according to this structure. Do not invent new
top-level folders or rename existing ones:

- src/data_generation/  — sample data generator + notes
- src/bronze/           — raw ingestion scripts (01_, 02_, 03_ prefix per source, plus ingest_all.py)
- src/silver/           — one file per quality check (completeness, uniqueness,
                           type_validation, referential_integrity, business_logic) + create_silver_tables.py
- src/gold/             — one .sql per aggregation + create_gold_tables.py
- src/dashboard/        — dashboard_queries.sql + DASHBOARD_GUIDE.md
- data/                 — the 3 required CSVs only
- database/             — schema.sql, seed-data-notes.md, setup-notes.md
- ai-prompts/           — one .md per activity (data-generation, bronze-layer,
                           silver-layer, gold-layer, dashboard, debugging, documentation)
- Root-level docs: README.md, candidate-info.md, tool-workflow.md,
  requirements-analysis.md, design-notes.md, data-model.md,
  data-quality-strategy.md, debugging-notes.md, reflection.md,
  final-ai-usage-summary.md

Never suggest restructuring this layout. If a new file is genuinely
needed, ask first and explain why it doesn't fit an existing location.

## Layer Rules

**Bronze layer:**
- No transformations, no cleaning, no filtering. Raw ingest only.
- Preserve source schema; log row counts and ingestion timestamp.
- One script per source + an orchestrator (ingest_all.py).

**Silver layer:**
- Implement exactly these checks: completeness (NULLs in email,
  customer_id, product_id), uniqueness (duplicate order_id,
  customer_id), referential integrity (orphan FKs against
  customers/products), type validation.
- Never delete bad rows — add a quality_check_result column and flag.
- Every check must produce a % passed metric usable in a quality report.

**Gold layer:**
- Exactly three required aggregations: Sales by Product, Revenue by
  Customer, Customer Segmentation (High-Value/Repeat/One-Time/Inactive).
  Daily/weekly trends is optional stretch only — do not build it unless
  explicitly asked.


**Dashboard:**
- Minimum 3 SQL queries feeding: bar chart (top 10 products by revenue),
  histogram (customer revenue distribution), pie chart (segmentation).

## Code Style
- Python: PEP8, type hints on function signatures, docstrings on every
  function explaining purpose/inputs/outputs.
- PySpark: prefer DataFrame API over raw RDDs; avoid collect() on full
  datasets; use explicit schemas (StructType) rather than relying on
  inferSchema for anything beyond Bronze exploration.
- SQL: uppercase keywords, one clause per line for anything beyond a
  trivial SELECT, comment non-obvious joins/filters.
- Every script needs a header comment: purpose, inputs, outputs.
- No hardcoded file paths — use variables/config at the top of the file.
- No secrets, tokens, or real customer PII in code or sample data ever.

## AI Workflow Requirements (this is graded — do not skip)
- Before generating any non-trivial code, first produce or reference a
  short design note for that piece (what it does, inputs/outputs,
  edge cases) — don't jump straight to code from a one-line prompt.
- After generating code, always state how it should be tested/validated
  before it is accepted.
- When a suggestion is accepted, rejected, or modified, log it in the
  matching ai-prompts/{activity}.md file (prompt sent, response
  summary, what was accepted/changed/rejected and why). Only log what
  actually happened in the session — never fabricate intent.
- Flag any suggestion that silently deletes/drops flagged rows instead
  of marking them, and any suggestion that expands scope beyond the
  Core requirements (e.g., extra aggregations, extra dashboard tiles,
  extra layers) without being asked.
- Prefer specific, testable increments over large one-shot generations
  (e.g., generate one quality check at a time, not all four at once).

## Things to Avoid
- Do not use real customer data or anything resembling real PII in
  generated sample data — synthetic/faker-generated only.
- Do not over-engineer: no orchestration frameworks (Airflow, etc.),
  streaming, or production-grade CI/CD unless explicitly requested —
  this is a scoped evaluation exercise, not a production build.
- Do not skip error handling on ingestion (missing file, schema
  mismatch, empty file) even though this is a sample project.
