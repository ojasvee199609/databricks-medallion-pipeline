# Reflection

## What I Built

A complete Databricks Medallion Architecture pipeline for e-commerce sales data,
covering all four required layers plus orchestration on Databricks Community Edition:

- **Data generation:** a synthetic dataset generator (`faker`-based) producing
  ~10,000 customers, ~500 products, and ~100,000 orders, with seven categories
  of intentional data quality issues injected at precise, documented counts (NULL
  emails, duplicate keys, orphan foreign keys) so the Silver layer would have
  real, verifiable defects to catch.
- **Bronze:** raw, untransformed ingestion of all three sources into Delta tables,
  with row-count parity enforcement (read count must equal written count or
  ingestion fails loudly), ingestion metadata (`_ingested_at`, `_source_file`),
  and explicit `StructType` schemas. I ran a dedicated schema-drift analysis
  (documented in `ai-prompts/bronze-layer.md`) before changing anything: extra
  columns are dropped silently, missing columns can NULL-fill without error, and
  header renames can corrupt data — gaps I documented in `requirements-analysis.md`
  and `debugging-notes.md` rather than over-building for this evaluation scope.
- **Silver:** four quality checks — completeness, uniqueness, type validation,
  and referential integrity — implemented as flag columns, never as filters.
  Every row that entered Silver from Bronze is still present in Silver; nothing
  was deleted. All seven injection categories were verified against Silver
  metrics (e.g. 50 NULL emails, 100/200 NULL FKs, 80 orphan FKs); uniqueness
  correctly flags every row sharing a duplicated key (20 customer rows, 40 order
  rows), not just the appended copies.
- **Gold:** three required business aggregations (sales by product, revenue by
  customer, customer segmentation), computed only from rows that passed all
  Silver checks, with a cross-check that `gold_sales_by_product` and
  `gold_revenue_by_customer` revenue totals reconcile within tolerance — a real
  correctness guard, not a cosmetic one. The cross-check initially failed until
  duplicate `customer_id` rows in Silver were deduplicated via a
  `customer_dimension` CTE.
- **Dashboard:** three visualizations (top-10 products bar chart, customer
  revenue histogram, segmentation pie chart) built from Gold tables, with
  queries in `dashboard_queries.sql` and a manual build guide in
  `DASHBOARD_GUIDE.md` (assembly happens in the Databricks SQL UI, not in code).
- **Orchestration:** two Databricks Jobs — data generation to `to_process/`, then
  file-arrival triggered Bronze → Silver → Gold → dashboard refresh — with
  email notification on task failure configured at job setup.

Alongside the pipeline, I maintained the full set of lifecycle artifacts this
project asks for: requirements analysis, design notes, data model, data quality
strategy, per-activity AI prompt logs, debugging notes, tool workflow, and this
reflection.

---

## How I Used AI Across the Lifecycle

I used **Cursor** as the primary tool, and deliberately front-loaded context
before generating any code:

- Wrote `requirements-analysis.md` and `design-notes.md` **before** prompting
  for the data generator, so the AI was working against a spec I'd already
  thought through, not inventing scope itself.
- Set up `cursorrules.md` (as Cursor User Rules) and `project-context.md` early
  so the schema, the exact injected-issue counts, the fixed repo structure, and
  the non-goals (no SCD, no orchestration frameworks, no extra aggregations)
  persisted across every session without needing to be re-explained per prompt.
- Used AI heavily for code generation at every layer, but wrote highly specific
  prompts rather than vague ones — full schemas, exact row counts, exact
  thresholds, explicit constraints (e.g. "never delete rows, only flag" stated
  as a hard requirement, not a preference). The Silver layer prompt in
  particular specified upfront that uniqueness checks must flag *every* copy of
  a duplicated key, not just the "extra" occurrence — a design decision I made
  deliberately rather than accepting whatever default the AI would otherwise pick.
- Used AI to help me interrogate my own pipeline, not just generate it — for
  example, before deciding how to handle schema drift, I first asked the AI to
  trace through and explain how the *existing* Bronze code would behave under six
  different drift scenarios, without changing anything. That diagnostic step
  surfaced real gaps (silent NULL-fill on missing columns, silent drop of extra
  columns) before I chose to document them rather than implement full
  production-grade drift handling within evaluation scope.
- Used AI for two scoping decisions that mattered more than they looked: whether
  to introduce Slowly Changing Dimensions in Silver, and whether to create a new
  Unity Catalog for the project. In both cases, I asked the AI to check the
  actual source requirements rather than reason from general best-practice
  instinct — and in both cases the honest answer was "not required for this
  scope," which I documented explicitly rather than either silently skipping it
  or over-building a feature nobody asked for.

---

## What AI Got Wrong or Needed Correction

Most generated code matched the spec on the first pass — a direct result of
writing precise prompts with counts and constraints spelled out rather than
open-ended requests. But a few things needed genuine scrutiny rather than blind
acceptance:

- The Silver layer's uniqueness check could easily have defaulted to flagging only
  the "extra" copy of a duplicated key (a common `.duplicated()` shortcut)
  instead of every row sharing that key. I caught this by specifying the
  requirement explicitly upfront rather than discovering it as a bug afterward —
  but it's exactly the kind of default AI-generated code tends toward if not told
  otherwise.
- Bronze's initial design did not fully handle schema drift — new columns are
  dropped silently, missing columns can NULL-fill without error. This wasn't
  caught by the AI proactively; it surfaced because I asked a direct "what if
  the schema changes" question and documented the behaviour honestly rather than
  claiming drift was "solved."
- The Gold layer's revenue cross-check failed on first run (~103K mismatch)
  because duplicate `customer_id` rows in Silver double-counted customer revenue.
  AI-assisted debugging led to the `customer_dimension` dedupe CTE fix in
  `02_revenue_by_customer.sql` and `04_customer_segmentation.sql`.
- The Gold layer's "High-Value customer" segmentation threshold isn't numerically
  defined anywhere in the source spec. Left unchecked, an AI-generated definition
  here is just as much an invented assumption as anything else — I made sure
  this was called out explicitly in SQL comments and `print_segmentation_logic()`
  rather than buried, so it's a visible, reviewable decision.
- The dashboard pie chart showed four equal 25% slices until the visualization
  measure was set to `SUM(customer_count)` instead of counting query rows — a
  UI configuration issue, not a data bug, caught by following `DASHBOARD_GUIDE.md`
  verification checks.

---

## How I Validated Output

I didn't treat any printed "success" summary as sufficient on its own. Concretely:

- Ran **`test_data_quality.py`** — a small programmatic test that reads Silver
  Delta tables and asserts failed-row counts on each flag column against the
  seven known injection constants from `generate_sample_data.py` (50 NULL emails,
  20 duplicate-customer rows, 300 NULL FK rows, 80 orphan FK rows, 40
  duplicate-order rows). Run after Bronze + Silver: `python3 test_data_quality.py`.
- Ran independent verification beyond the test — e.g. confirming order injections
  used disjoint row index sets so no single row was double-counted across
  injection types.
- Used the Gold-layer cross-check (`SUM(total_revenue)` from sales-by-product
  must equal revenue-by-customer within ±0.01) as a structural correctness
  guard, since both tables are derived from the same PASS order set but grouped
  differently — a mismatch would mean a real join or filter bug.
- Used the manual verification sample in `create_gold_tables.py` — one product
  and one customer reconstructed from underlying Silver PASS orders — rather
  than trusting aggregation logic by inspection alone.
- Ran the pipeline locally (`python3 src/bronze|silver|gold/...`) and on
  Databricks via notebooks and Jobs, including fixing the `__file__` /
  `inspect.currentframe()` bootstrap required for Job execution.
- Validated dashboard totals with SQL checks in `DASHBOARD_GUIDE.md` (pie
  `SUM(customer_count)` = 10,000 unique customers).

Orchestrator-built guards (row-count parity, Gold revenue cross-check) and
documented SQL spot-checks complement the automated Silver DQ test above.

---

## What I'd Do Differently / Improve Next Time

- I'd write the schema-drift diagnostic prompt (asking "how does this currently
  behave" before asking "now fix it") as a standard step for every layer from
  the start, rather than only applying it after Bronze was already built. It
  surfaced a real gap cheaply and I should treat it as routine, not a one-off.
- I'd define numerically ambiguous business logic (like the "High-Value"
  segmentation threshold) in `requirements-analysis.md` *before* prompting for
  the Gold layer, rather than letting the AI propose a definition that then
  needs to be reviewed after the fact. It worked out fine here because I caught
  it, but deciding it upfront would have been cleaner.
- I'd test on the Databricks Job runtime earlier — local validation missed the
  `__file__` issue until notebooks were deployed as Job tasks.
- I'd log prompts slightly more consistently in real time rather than
  occasionally reconstructing evaluation notes shortly after a response came
  back — the project brief is explicit that retroactive prompt history reads as
  weaker evidence than live logging, and while I don't believe I fabricated
  anything, tightening this habit further would strengthen the artifact.

---

## How I'd Reuse This Workflow

The pattern that worked best across every layer was: write the design constraint
down first (in a doc or directly in the prompt), generate code against that
explicit spec, then independently verify the result against known ground truth
(the injected issue counts, a hand-computed sample, a cross-table
reconciliation) rather than trusting the AI's own summary of its output. I'd
carry that same sequence — spec first, generate second, verify third with real
numbers — into any future data engineering project involving AI-assisted
development, since it's what turned "the code ran without errors" into "the
code is actually correct."

For production, I would add pytest or Great Expectations on top of this workflow,
use Databricks Asset Bundles for promotion across environments, and keep the
same layered context (`cursorrules` / data contracts), incremental prompts, and
orchestrator reconciliation checks that made this evaluation pipeline auditable.

---

## Related Documents

| Document | Purpose |
|----------|---------|
| `tool-workflow.md` | Detailed AI tool workflow across the lifecycle |
| `debugging-notes.md` | Issues, measures, and verification commands |
| `test_data_quality.py` | Automated Silver DQ validation test |
| `ai-prompts/*.md` | Per-activity prompt and outcome log |
| `final-ai-usage-summary.md` | Executive summary of AI usage |

*Last updated: 2026-08-31*
