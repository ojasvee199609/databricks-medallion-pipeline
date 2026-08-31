# Final AI Usage Summary

Executive summary of how AI (primarily **Cursor**) was used across the full lifecycle of this project. Detailed per-activity prompts and evaluations live in `ai-prompts/*.md`; this document synthesizes the pattern across all of them. See also `tool-workflow.md` for tool-level workflow details and `reflection.md` for lessons learned.

## 1. Primary Tool & Overall Approach

**Primary tool:** Cursor, with persistent project context maintained via `cursorrules.md` (Cursor User Rules) and `project-context.md`, so the schema, fixed repo structure, injected data-quality-issue counts, and explicit non-goals (no SCD, no orchestration frameworks beyond what was scoped, no extra aggregations) persisted across sessions without being re-explained per prompt.

**Overall pattern used consistently across every layer:**

1. Write the design constraint or spec down first (in a doc, or directly in the prompt) — never prompt for code before requirements exist.
2. Generate code against that explicit, specific spec — full schemas, exact row counts, exact thresholds, hard constraints stated as requirements, not preferences.
3. Independently verify the result against known ground truth (the injected issue counts, a hand-computed sample, a cross-table reconciliation) rather than trusting the AI's own printed summary.

This **spec-first / generate-second / verify-third** sequence is the throughline across data generation, Bronze, Silver, Gold, and dashboard work, and is what turned "the code ran without errors" into "the code is demonstrably correct."

## 2. Validation

Validation was never "it ran without error" — every layer had a specific, falsifiable check:

| Layer | Validation approach |
|-------|---------------------|
| Data generation | Recomputed all 7 injected-issue counts against the generator's printed summary |
| Bronze | Read-count-equals-written-count as a hard, fail-loud guard |
| Silver | `test_data_quality.py` asserts flag-column failure counts vs injection constants; metrics report cross-check |
| Gold | Structural revenue cross-check (product total = customer total); caught duplicate `customer_id` double-count bug |
| Dashboard | Documented SQL checks (e.g. pie chart `SUM(customer_count)` = total customers) |
| Runtime | Validated locally and on Databricks Jobs (`__file__` / `inspect` bootstrap issue surfaced in Jobs only) |

## 3. Related Documents

| Document | Purpose |
|----------|---------|
| `tool-workflow.md` | Detailed AI workflow by lifecycle phase |
| `reflection.md` | What worked, what didn't, reuse in production |
| `ai-prompts/*.md` | Per-activity prompt and outcome logs |
