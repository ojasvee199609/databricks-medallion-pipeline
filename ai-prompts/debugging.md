# AI prompts used for debugging and troubleshooting.

## Session: Add `test_data_quality.py` (2026-08-31)

**PROMPT SENT:**
"write a test_data_quality.py that asserts your known injected counts against
Silver's flag columns programmatically — this can be small, even 20-30 lines
covering the 7 injection categories. Then update reflection's 'How I Validated
Output' section to describe that test file instead of saying tests 'were not
implemented.'"

**AI RESPONSE SUMMARY:**
Created root-level `test_data_quality.py` that reads `silver_customers` and
`silver_orders` Delta tables, imports injection constants from
`generate_sample_data.py`, and asserts failed-row counts on completeness,
uniqueness, and referential-integrity flag columns (covering all seven injection
categories). Updated `reflection.md` validation section accordingly.

**ACCEPTED:**
- Single `main()` with exit code 0/1 (no pytest dependency)
- Reuses `silver_config.py` read helpers and generator constants (DRY)
- Uniqueness expectations use `DUPLICATE_ID * 2` (all rows sharing a key flagged)

**VALIDATION:**
```bash
SPARK_LOCAL_IP=127.0.0.1 python3 test_data_quality.py
# test_data_quality.py PASSED — all 7 injection categories match Silver flags.
```

**REFERENCES:**
- Implementation: `test_data_quality.py`
- Runbook: `debugging-notes.md` (Verification Commands → Automated Silver DQ test)
- Strategy: `data-quality-strategy.md` (Verification section)

---

## Prior debugging sessions (see `debugging-notes.md`)

| Issue | Log location |
|-------|----------------|
| `NameError: __file__` on Databricks Jobs | `debugging-notes.md` § Issue Resolution Log #1 |
| Silver threshold exit code | #2 |
| Gold revenue cross-check mismatch | #3; `ai-prompts/gold-layer.md` |
| Pie chart 25% slices | #4; `DASHBOARD_GUIDE.md` |
| Bronze schema drift analysis | #5; `ai-prompts/bronze-layer.md` |
| Local Spark `UnresolvedAddressException` | #6 |
| Job CLI parameter format | #7 |
