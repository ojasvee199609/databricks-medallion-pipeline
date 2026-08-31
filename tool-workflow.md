# Tool Workflow

How AI tools were used across the **databricks-medallion-pipeline** project —
from requirements through debugging — and how the same workflow would carry
into production.

**Candidate:** Ojasvee | **Primary AI tool:** Cursor | **Stack:** Python, PySpark, SQL, Databricks CE

---

## Primary AI Tool Used

**Cursor** was the primary AI tool for the entire lifecycle:

| Activity | How Cursor was used |
|----------|---------------------|
| Scaffolding & docs | Repository structure, README, design notes, data model |
| Pipeline code | PySpark Bronze/Silver orchestrators, quality check modules, Gold SQL |
| Databricks ops | Job JSON definitions, notebook bootstrap patterns |
| Debugging | Root-cause analysis for `__file__` errors, revenue mismatch, dashboard config |
| Documentation | `requirements-analysis.md`, `data-quality-strategy.md`, `debugging-notes.md` |

Cursor was used in **Agent mode** for implementation and **chat** for design
questions. Underlying models varied by task (code generation, explanation); the
workflow stayed consistent: **context first → spec/design → incremental build →
verify → log prompts**.


---

## How I Provide Project Context to the Tool

Context was layered so each session did not start from zero:

### 1. Persistent rules — `cursorrules.md`

Copied into **Cursor User Rules** so conventions apply automatically every session:

- Medallion layer rules (Bronze raw only, Silver flag-don't-delete, Gold PASS filter)
- Repository structure (no invented folders)
- Code style (type hints, docstrings, explicit schemas)
- AI workflow requirements (design note before code, log prompts, incremental builds)

### 2. Business & technical context — `project-context.md`

Referenced at session start via **`@project-context.md`**:

- Schemas, injected DQ issues, layer requirements, non-goals, “done” criteria


### 3. Design artifacts before code

| Artifact | When used |
|----------|-----------|
| `design-notes.md` | Architecture, layer boundaries, debugging approach |
| `data-model.md` | Entity schemas across Bronze/Silver/Gold |
| `data-quality-strategy.md` | Check definitions, thresholds, expected failure counts |
| `requirements-analysis.md` | Functional/non-functional requirements, edge cases |

### 4. Layer-specific prompt logs — `ai-prompts/{activity}.md`

Each layer session started with a **detailed spec prompt** (see `ai-prompts/bronze-layer.md`,
`silver-layer.md`, `gold-layer.md`, etc.) so the model had file names, column names,
thresholds, and validation expectations in one place.

### 5. Codebase as context

`@` references to existing files (e.g. `bronze_config.py` when building Silver) so
new layers extended patterns instead of reinventing config/path handling.

```text
Session start checklist:
  User Rules (cursorrules)     → automatic
  @project-context.md          → business + schemas
  @design-notes.md (if new)    → architecture for this piece
  ai-prompts/{layer}.md        → log prompt + outcome after session
```

---

## How I Use AI for Requirement Analysis

1. **Started from the assessment brief** — captured business problem, schemas, injected
   issues, and layer deliverables in `project-context.md` .

2. **Asked AI to draft `requirements-analysis.md`** from the template:
   - Problem statement in own words
   - Functional requirements per layer
   - Non-functional requirements (reproducibility, auditability, scope limits)
   - Assumptions, edge cases, clarifications needed / resolved

3. **Used AI to challenge ambiguities** — e.g. “Silver cleaning” vs flagging, SCD,
   Gold PASS boundary, job failure alerting. Resolutions were recorded in
   `requirements-analysis.md` and `design-notes.md`.

4. **Rejected scope creep** — AI occasionally suggested extra aggregations, streaming,
   or Airflow; `.cursorrules` and `project-context.md` non-goals were cited to stay
   within the brief.

**What worked:** Treating requirements as a **living doc** updated when design decisions
were made (not a one-time AI dump).

---

## How I Use AI for Pipeline Design (Medallion Architecture)

Design was **layer-by-layer**, not one monolithic prompt:

| Layer | Design focus | Key AI-assisted decisions |
|-------|--------------|---------------------------|
| **Bronze** | Raw landing, StringType, metadata columns | Explicit StructType; archive CSV to `processed/`; no DQ |
| **Silver** | Flag-don't-delete; four checks | Per-check modules; `quality_check_result`; row-count guard |
| **Gold** | PASS-only boundary; three aggregations | SQL templates; customer dedupe CTE; revenue cross-check |
| **Dashboard** | Gold-only reads; three chart types | Query separation; pie measure = `SUM(customer_count)` |

**Process:**

1. Write or update **`design-notes.md`** section for the layer (inputs, outputs, edge cases).
2. Prompt Cursor with that design + `project-context.md` layer rules.
3. Review AI output for violations (e.g. filtering bad rows in Silver, extra Gold tables).
4. Log accepted/rejected choices in `ai-prompts/{layer}.md`.

**Medallion principle enforced in prompts:** Bronze preserves truth → Silver proves
quality → Gold reports trustworthy metrics → Dashboard visualizes Gold.

---

## How I Use AI for Code Generation (Python / PySpark / SQL)

### Incremental generation (not one-shot)

Per `.cursorrules`, preferred **testable increments**:

- Data generation → then Bronze (one ingest script pattern, then orchestrator)
- Silver → one quality check module at a time, then `create_silver_tables.py`
- Gold → one `.sql` file at a time, then `create_gold_tables.py`
- Dashboard → `dashboard_queries.sql` then `DASHBOARD_GUIDE.md`

### Patterns AI was instructed to follow

- Shared `*_config.py` per layer (paths, table names, env vars)
- Header comments: purpose, inputs, outputs
- Databricks bootstrap: `inspect.currentframe()` + `*_SRC_DIR` when `__file__` missing
- SQL: uppercase keywords, commented Gold boundary filter

### Prompt structure that worked

Each layer prompt included:

- **Files to create** (exact paths)
- **Hard rules** (e.g. Silver: never `filter`/`dropna` bad rows)
- **Validation output expected** (row counts, metrics report, sample FAIL rows)
- **What to avoid** (hardcoded paths, inferSchema on production ingest)

### What I reviewed before accepting

- Silver: no row-dropping transforms
- Gold: `WHERE quality_check_result = 'PASS'` in SQL comments and logic
- Bronze: read count == write count
- No secrets or real PII in generated data

---

## How I Validate AI-Generated Code and Logic

Validation was **manual + scripted**, not trust-on-first-run:

### Per layer

| Layer | Validation |
|-------|------------|
| Data generation | `print_summary()` — row counts + injected issue breakdown; `RANDOM_SEED=42` reproducibility |
| Bronze | Ingestion summary table; Delta verify; sample anomaly rows; read == written |
| Silver | Bronze in == Silver out; full metrics report; FAIL sample; counts vs injected spec (~50/100/200/80/60 dup rows) |
| Gold | Table row counts (500 / 10,000 / 4); revenue cross-check MATCH; manual verification sample (one product + one customer) |
| Dashboard | `DASHBOARD_GUIDE.md` checks A–D; pie total = 10,000 customers |

### Cross-layer reconciliation

- `SUM(total_revenue)` product == customer (Gold orchestrator, tolerance ±0.01)
- `SUM(customer_count)` segmentation == `COUNT(*)` revenue_by_customer
- ~99,600 PASS orders from ~100,020 Silver orders

### Commands used routinely

```bash
python3 src/data_generation/generate_sample_data.py
python3 src/bronze/ingest_all.py
python3 src/silver/create_silver_tables.py
python3 src/gold/create_gold_tables.py
```

Plus `awk` spot-checks on CSVs and SQL sanity queries documented in `debugging-notes.md`.

**Rule:** Do not accept AI output until **at least one** of row counts, metrics, or
cross-checks matches expected values from the known injected dataset.

---

## How I Use AI for Testing and Validation

`test_data_quality.py` at the repo root provides automated Silver validation:
it reads Silver Delta tables and asserts failed-row counts on flag columns against
the seven injection constants from `generate_sample_data.py`. Run after Bronze +
Silver: `python3 test_data_quality.py`.

Additional validation layers:

1. **Orchestrator-built checks** — AI helped embed assertions in pipeline code:
   - Bronze: `write_bronze_table()` row-count match
   - Silver: `write_silver_table()` pre/post count guard
   - Gold: `validate_pass_orders_exist()`, revenue cross-check exit code

2. **AI-generated verification SQL** — dashboard and debugging docs include
   reproducible queries for stakeholders and evaluators.

3. **Known fixture testing** — injected defect counts are the test oracle; Silver
   failure counts must align (`test_data_quality.py` + `data-quality-strategy.md`).

4. **Databricks Job runs** — end-to-end on CE after local validation; email on
   failure configured in `databricks-job.json`.

**For production:** I would add pytest/Great Expectations on top of this workflow;
AI would help draft tests from the same `requirements-analysis.md` and metrics report
shape already in Silver.

---

## How I Use AI for Debugging (Issues, Root Causes)

### Workflow

1. **Paste symptom** — error message, unexpected metric, or screenshot behaviour
   (e.g. pie chart 25% slices).
2. **Point AI at relevant files** — `@create_gold_tables.py`, `@02_revenue_by_customer.sql`.
3. **Ask for hypothesis + minimal fix** — not a broad refactor.
4. **Re-run validation** — same cross-checks that failed before.
5. **Log in `debugging-notes.md` and `ai-prompts/debugging.md`** — symptom, cause, fix, files.

### Issues debugged with AI assistance

| Issue | Root cause | Fix |
|-------|------------|-----|
| `NameError: __file__` on Databricks Jobs | Notebook `exec()` has no `__file__` | `inspect.currentframe()` + env `*_SRC_DIR` |
| Silver notebook exit 1 | Uniqueness threshold breach treated as fatal | Warnings only; still write tables |
| Gold revenue mismatch | Duplicate `customer_id` double-counted | `customer_dimension` dedupe CTE |
| Pie chart 25% × 4 | UI counted rows not `customer_count` | `SUM(customer_count)` measure |
| Local Spark bind error | Mac network binding | `SPARK_LOCAL_IP=127.0.0.1` |

**What worked:** Giving AI **numbers** (expected vs actual revenue, row counts) led to
faster root cause than vague “Gold is wrong.”

**What didn't:** Accepting AI’s first fix without re-running the revenue cross-check —
the duplicate-customer bug required a second iteration.

---

## How I Use AI for Data Quality Checks

1. **Design phase** — AI drafted check logic aligned to spec:
   - Completeness, uniqueness, referential integrity, type validation
   - Thresholds as constants in `silver_config.py`
   - Separate detail strings per failed field

2. **Implementation** — one module per check (`01`–`04`), orchestrator combines
   into `quality_check_result`.

3. **Validation** — compare metrics report to injected issue table:

   | Injected | Expected Silver impact |
   |----------|------------------------|
   | 50 NULL emails | 50 completeness failures (customers) |
   | 100/200 NULL FKs | 300 completeness failures (orders) |
   | 50+30 orphans | 80 referential failures |
   | 10+20 dup keys | 20+40 uniqueness failures |

4. **Gold boundary** — AI documented PASS-only filter in SQL; I verified ~420 orders
   excluded, metrics still sum correctly.

5. **Documentation** — `data-quality-strategy.md` and `debugging-notes.md` capture
   flag column names, thresholds, and verification commands for auditors.

**Hard rule enforced in every Silver prompt:** flag, never delete — AI suggestions
that used `filter`/`drop` on bad rows were rejected.

---

## What I Avoid Sharing Unnecessarily with AI Tools

| Do not share | Why |
|--------------|-----|
| **Real customer PII** | Project uses Faker-only synthetic data; no production extracts in prompts |
| **API keys, tokens, passwords** | Databricks PATs, GitHub tokens — use env/auth locally; never paste in chat |
| **Production connection strings** | CE workspace paths only; no prod catalog/credentials |
| **Unrelated proprietary data** | Keep prompts scoped to schemas and sample volumes in the repo |
| **Full credential-bearing job exports** | Job JSON in repo uses notification emails at setup time — rotate if exposed |

**Practice:** Reference **file paths and schemas** from the repo; run authenticated
commands locally rather than pasting secrets into Cursor.

If a secret was accidentally pasted (e.g. PAT in chat), **revoke and rotate**
immediately — do not commit secrets to the repository.

---

## How I Would Reuse This Workflow in a Real Production Pipeline

| Step | Evaluation project | Production adaptation |
|------|-------------------|------------------------|
| Context | `project-context.md` + User Rules | Team `AGENTS.md` / data contract docs in repo |
| Design | `design-notes.md` per feature | ADR or design doc PR before implementation |
| Build | Incremental layer/check prompts | Same — one PR per layer or check |
| Validate | Row counts + metrics + cross-checks | + dbt tests / Great Expectations / CI on PR |
| DQ | Silver flags + Gold PASS filter | + quarantine tables, alerting on threshold SLA breach |
| Debug | `debugging-notes.md` + prompt log | Runbook linked from Job failure emails |
| Orchestration | Two Databricks Jobs | DABs (Asset Bundles), env-specific configs, promotion pipeline |
| AI logging | `ai-prompts/*.md` | Optional — team norms for when AI-assisted code needs extra review |

**Production additions I would keep from this exercise:**

- Explicit **Gold boundary** documentation in SQL
- **Revenue reconciliation** (or equivalent) between related marts
- **Row-count guards** between layers
- **Design-before-code** for any non-trivial change

---

## Lessons Learned

### What worked

- **Layered context** (`cursorrules` + `project-context` + design docs) — fewer
  wrong assumptions per session.
- **Incremental prompts** — one quality check or one Gold SQL at a time; easier to
  review and test.
- **Validation built into orchestrators** — metrics reports and cross-checks catch
  bugs without separate test harness initially.
- **Known injected defects** — definitive oracle for Silver/Gold correctness.
- **Prompt logging** — `ai-prompts/{activity}.md` made evaluation and reflection honest.
- **Flag-don't-delete** — clearer audit story than silent drops; AI needed explicit
  reinforcement on this rule.

### What didn't work / what I would change

- **Large one-shot generation** — early attempts to generate all Silver checks at once
  produced harder-to-review diffs; splitting by check was better.
- **Trusting threshold exit codes** — treating uniqueness breach as fatal broke notebooks
  despite correct tables; warnings were the right behaviour for known bad data.
- **Assuming dashboard SQL alone defines UI** — pie chart needed UI measure config;
  document **both** query and visualization settings.
- **Skipping `__file__` testing on Databricks early** — local-only validation missed
  Job execution path until deploy; test on target runtime sooner.
- **Thin `documentation.md` prompt log** — should log every major doc session, not
  only folder scaffold.

### Overall takeaway

Cursor accelerated scaffolding and boilerplate (config modules, StructTypes, SQL
templates) but **did not replace** validation. The highest-value use was pairing AI
speed with **explicit specs, hard rules, and numeric verification** against a known
dataset. That pairing is what I would scale to production — with CI, secrets management,
and stricter test coverage added on top.

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `candidate-info.md` | Candidate and tool summary |
| `project-context.md` | Persistent business/technical context for AI |
| `cursorrules.md` | Coding and AI workflow rules |
| `ai-prompts/*.md` | Per-activity prompt and outcome log |
| `debugging-notes.md` | Issues, measures, verification commands |
| `test_data_quality.py` | Automated Silver DQ flag-count test |
| `final-ai-usage-summary.md` | *(to be completed)* executive summary of AI usage |

*Last updated: 2026-08-31*
