# AI prompts used for bronze layer ingestion development.
prompt used : 
Create the Bronze ingestion layer for the medallion pipeline in
src/bronze/. This layer performs RAW ingestion only — no cleaning, no
filtering, no type casting beyond what's needed to read the CSV, no
deduplication, no NULL handling. Bronze must preserve the source data
exactly as-is, including all injected data quality issues (NULLs,
duplicates, orphan FKs) — those get handled in Silver, not here.

Environment: Databricks (PySpark), reading from data/*.csv (mounted or
uploaded to DBFS) and writing to Delta tables.

## Files to create

**src/bronze/01_ingest_customers.py**
- Read data/customers.csv into a Spark DataFrame
- Define an explicit StructType schema matching the customers schema
  (don't rely on inferSchema) — read as strings/loose types where the
  source might have anomalies (e.g., don't force email to a strict
  type that would fail on NULLs)
- Add ingestion metadata columns: `_ingested_at` (current timestamp)
  and `_source_file` (literal "customers.csv")
- Write to a Delta table `bronze_customers` (managed table or path,
  whichever is standard for this workspace — use a config variable for
  the path so it's not hardcoded inline)
- Print/log: row count read from CSV, row count written to Delta table
  (these must match exactly — if they don't, raise an error, don't
  silently continue)

**src/bronze/02_ingest_orders.py**
- Same pattern as above for data/orders.csv -> bronze_orders
- Explicit schema, loose types where the source has NULLs
- Same metadata columns and same row-count validation

**src/bronze/03_ingest_products.py**
- Same pattern as above for data/products.csv -> bronze_products

**src/bronze/ingest_all.py**
- Orchestrates all three scripts in sequence: customers, products,
  then orders (products/customers first, since orders references them
  — even though Bronze itself doesn't enforce FK integrity, this keeps
  ingestion order sensible and matches how a real pipeline would stage
  runs)
- Catches and reports failures per source without letting one failure
  silently skip the others — if customers ingestion fails, still
  attempt products and orders, but exit with a non-zero/error status
  summarizing which source(s) failed
- Prints a final summary table: source name, rows read, rows written,
  status (success/fail), duration

## Code requirements
- Add a module-level docstring per file (purpose, inputs, outputs)
- Add a docstring to every function
- Use type hints
- No hardcoded file paths or table names — use constants/config at the
  top of each file
- Include basic error handling: missing source file, empty file,
  schema mismatch — these should raise a clear, specific error message,
  not a generic Spark stack trace
- Do NOT add any data quality logic here (no NULL checks, no dedup
  logic, no referential integrity checks) — that's explicitly Silver's
  job, not Bronze's

After writing the scripts, run ingest_all.py and show me:
1. Row counts read vs. written for each of the three tables (they
   must match exactly)
2. Confirmation that bronze_customers, bronze_orders, bronze_products
   exist as Delta tables
3. A sample of 5 rows from bronze_orders showing that the injected
   NULLs/anomalies are still present untouched


Response : 
The Bronze ingestion layer is in place and was run successfully locally. Here's what was created and verified.

Files created
File	Purpose
src/bronze/bronze_config.py
Shared paths, schemas, metadata helpers, Delta write/validation
src/bronze/01_ingest_customers.py
customers.csv → bronze_customers
src/bronze/02_ingest_orders.py
orders.csv → bronze_orders
src/bronze/03_ingest_products.py
products.csv → bronze_products
src/bronze/ingest_all.py
Orchestrator (customers → products → orders)
1. Row counts read vs. written (exact match)
Source	Rows Read	Rows Written	Status
customers
10,010
10,010
success
products
500
500
success
orders
100,020
100,020
success
2. Delta table confirmation
All three Bronze Delta tables exist locally at:

delta/bronze/bronze_customers (10,010 rows)
delta/bronze/bronze_products (500 rows)
delta/bronze/bronze_orders (100,020 rows)
On Databricks, set BRONZE_USE_MANAGED_TABLES=true (auto-detected) to write managed tables instead of paths. Override paths via:

BRONZE_DATA_DIR — source CSV location (e.g. /Volumes/.../data)
BRONZE_BASE_PATH — Delta output base path
3. Sample bronze_orders rows (anomalies preserved)
+--------+-----------+----------+------------+------------+
|order_id|customer_id|product_id|order_status|payment_date|
+--------+-----------+----------+------------+------------+
|567     |7142       |NULL      |Completed   |2025-03-01  |
|1652    |7722       |NULL      |Pending     |NULL        |
|1672    |3765       |NULL      |Completed   |2026-03-19  |
|2093    |99999      |354       |Cancelled   |NULL        |
|2260    |NULL       |4         |Completed   |2025-12-24  |
+--------+-----------+----------+------------+------------+
NULL FKs, orphan customer_id = 99999, and nullable payment_date are all preserved untouched — no cleaning in Bronze.

Design highlights
All source columns read as StringType — preserves CSV values as-is, including empty/NULL fields
Metadata only: _ingested_at, _source_file
Row-count guard: raises RuntimeError if read ≠ written
Error handling: missing file, empty file, schema mismatch → clear errors
ingest_all.py: continues on per-source failure, exits non-zero with failure summary

Re-run locally:

python3 src/bronze/ingest_all.py

Prompt for checking the fault schema drift 
Before we change anything, walk through exactly how the current Bronze
ingestion code in src/bronze/ (01_ingest_customers.py,
02_ingest_orders.py, 03_ingest_products.py, bronze_config.py) behaves
under each of these schema drift scenarios. Do not modify any code yet
— just trace through the existing logic and tell me what actually
happens in each case, referencing the specific lines responsible.

For each scenario, tell me: does it fail loudly with a clear error,
fail silently (e.g., NULLs inserted, column dropped), or crash with an
unclear Spark stack trace?

1. **New column added** to a source CSV (e.g., orders.csv gains a
   `discount_code` column) that isn't in the explicit StructType schema.
2. **A column is removed** from a source CSV that the explicit schema
   expects (e.g., orders.csv no longer has `payment_date`).
3. **A column is renamed** in the source CSV (e.g., `customer_id`
   becomes `cust_id`) without the underlying data changing.
4. **A column's position changes** (e.g., columns reordered) but names
   and types stay the same.
5. **A column's data type changes** at the source in a way that's
   incompatible with the current StringType-everything approach (this
   may be a non-issue since everything is read as StringType — confirm).
6. **ingest_all.py is run twice in a row** without regenerating source
   data — does Bronze table state get duplicated, overwritten cleanly,
   or something else? (Not strictly schema drift, but relevant to
   understanding current write-mode behavior before we touch this code.)

For each scenario, also tell me: is this failure mode acceptable for
this project's scope (a batch pipeline reading from a known, mostly
stable CSV format), or is it a real gap worth closing? Give your
honest assessment, don't assume everything needs to be fixed.

Response summary :
Scenario	Outcome	Loud / silent / unclear	Worth fixing for this project?
1. New column   Extra column dropped    Silent loss     Low — unless upstream drift is expected
2. Removed column   NULLs for missing column    Silent NULLs    Medium — document or add header check
3. Renamed column   Wrong NULLs + dropped data      Silent corruption   Medium for eval; high for production
4. Reordered columns    Works correctly     OK      No
5. Type changes     All read as String      OK at Bronze    No
6. Double ingest_all    No files → fail; re-stage → overwrite   Loud (missing file)     No