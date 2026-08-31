# databricks-medallion-pipeline

AI-assisted Databricks medallion pipeline (Bronze → Silver → Gold → Dashboard) for
e-commerce sales data — built with Cursor, including full prompt history, data quality
validation, and lifecycle documentation.

## Architecture


Job 1: Data_generation
        │
        ▼
CSV written to Volume …/medalion/to_process/
        │
        ▼  (file_arrival trigger)
Job 2: Data Ingestion and Processing
        │
        ├─► Bronze  → silver_* → gold_* → Dashboard refresh
        │
        ▼
   processed/ archive
```


Bronze layer          Raw ingest → bronze_customers, bronze_orders, bronze_products
        │
        ▼
Silver layer          Quality flags (no row deletion) → silver_* tables
        │
        ▼
Gold layer            PASS-only aggregations → gold_* tables
        │
        ▼
Dashboard             SQL visualizations (bar, histogram, pie)
```

| Layer | Location | Purpose |
|-------|----------|---------|
| Data generation | `src/data_generation/` | Synthetic CSVs with injected quality issues |
| Bronze | `src/bronze/` | Raw CSV → Delta (archive CSV to `processed/`) |
| Silver | `src/silver/` | Completeness, uniqueness, type, referential integrity checks |
| Gold | `src/gold/` | Sales by product, revenue by customer, segmentation |
| Dashboard | `src/dashboard/` | Queries + UI guide for Databricks SQL Dashboard |

## Run locally

```bash
# 1. Generate sample CSVs
python3 src/data_generation/generate_sample_data.py

# 2. Stage CSVs for Bronze (local DBFS simulation)
cp data/*.csv dbfs/FileStore/medallion/ingestion/to_process/

# 3. Run pipeline layers
python3 src/bronze/ingest_all.py
python3 src/silver/create_silver_tables.py
python3 test_data_quality.py   # assert Silver flags vs injected defect counts
python3 src/gold/create_gold_tables.py
```

## Run on Databricks (notebooks)

| Step | Notebook |
|------|----------|
| 1 | `src/data_generation/Data_gen_notebook.ipynb` |
| 2 | `src/bronze/Ingestion_notbook.ipynb` |
| 3 | `src/silver/silver_notebook.ipynb` |
| 4 | `src/gold/gold_notebook.ipynb` |

Update the `REPO` path in each notebook to match your workspace path.

Dashboard setup: see `src/dashboard/DASHBOARD_GUIDE.md`.

---

## Databricks Jobs (two-job workflow)

The pipeline uses **two separate Databricks jobs** that chain via the Unity Catalog
volume folder `to_process/`:

| Order | Job file | Trigger | What it does |
|-------|----------|---------|--------------|
| 1 | [`databricks-job-data-generation.json`](databricks-job-data-generation.json) | Manual / schedule *(no file trigger)* | Generates CSVs → writes to `to_process/` |
| 2 | [`databricks-job.json`](databricks-job.json) | **File arrival** on `to_process/` | Bronze → Silver → Gold → Dashboard refresh |

```text
┌─────────────────────────┐
│  Job 1: Data_generation │  (run on schedule or manually)
│  Data_gen_notebook      │
└───────────┬─────────────┘
            │ writes customers_*.csv, products_*.csv, orders_*.csv
            ▼
   /Volumes/.../medalion/to_process/
            │ file_arrival trigger fires
            ▼
┌─────────────────────────┐
│  Job 2: Data Ingestion  │
│  and Processing         │
│  Bronze → Silver → Gold │
│  → Dashboard_Refresh    │
└─────────────────────────┘
```

Run **Job 1 first** (or on a schedule). When its CSVs land in `to_process/`, **Job 2**
starts automatically.

---

## Databricks Job 1: Data_generation

Definition: [`databricks-job-data-generation.json`](databricks-job-data-generation.json)

This job runs **before** ingestion and processing. It generates synthetic e-commerce
CSVs (with intentional quality issues) and writes them directly to the volume path
that Bronze reads from — which also triggers Job 2.

### Job overview

| Property | Value | Meaning |
|----------|-------|---------|
| `name` | Data_generation | Display name in the Jobs UI |
| `max_concurrent_runs` | `1` | One generation run at a time |
| `timeout_seconds` | `0` | No job-level timeout |
| `performance_target` | `PERFORMANCE_OPTIMIZED` | Favors faster startup |
| `queue.enabled` | `true` | Queues runs when job is busy |
| `environments` | `Default` / version `5` | Serverless environment spec for the task |

**No `trigger` block** — this job does not auto-start on file arrival. Run it on a
schedule (e.g. daily) or manually before you need a fresh data batch.

### Task — `Data_Generation`

| Field | Value |
|-------|-------|
| Type | `notebook_task` |
| Notebook | `.../src/data_generation/Data_gen_notebook` |
| Depends on | *(none — single task)* |

The notebook:

1. Installs `faker` (`%pip install faker`)
2. Restarts Python (`dbutils.library.restartPython()`)
3. Calls `generate_sample_data.main(output_dir="/Volumes/workspace/default/medalion/to_process")`

Output: timestamped `customers_*.csv`, `products_*.csv`, `orders_*.csv` in
`/Volumes/workspace/default/medalion/to_process/`.

When those files appear, the **file_arrival** trigger on Job 2
(`Data Ingestion and Processing`) starts the Bronze → Silver → Gold → Dashboard chain.

### Deploy Job 1

1. **Workflows** → **Jobs** → **Create job** → paste JSON from `databricks-job-data-generation.json`.
2. Ensure the notebook path matches your workspace (`Data_gen_notebook`).
3. Attach serverless compute / environment **Default** (environment version 5).
4. Optionally add a **schedule** (e.g. cron) if you want recurring batches.

### Path to verify

| Job path | Repo file |
|----------|-----------|
| `.../data_generation/Data_gen_notebook` | `src/data_generation/Data_gen_notebook.ipynb` |

---

## Databricks Job 2: Data Ingestion and Processing

The job definition is stored in [`databricks-job.json`](databricks-job.json). It automates
the full medallion pipeline and refreshes the SQL dashboard when new files arrive.

### How to deploy

1. Open **Workflows** → **Jobs** → **Create job** (or edit an existing job).
2. Switch to the **JSON** view (or use the Databricks CLI / REST API).
3. Paste the contents of `databricks-job.json`, adjusting paths and IDs for your workspace.
4. Attach a cluster or serverless compute policy to each notebook task as required.

### Job overview

| Property | Value | Meaning |
|----------|-------|---------|
| `name` | Data Ingestion and Processing | Display name in the Jobs UI |
| `max_concurrent_runs` | `1` | Only one run at a time — avoids overlapping overwrites |
| `timeout_seconds` | `0` | No job-level timeout (tasks also use `0` = unlimited) |
| `performance_target` | `PERFORMANCE_OPTIMIZED` | Favors faster startup over cost savings |
| `queue.enabled` | `true` | Extra runs wait in queue instead of failing when busy |

### Trigger: file arrival

```json
"trigger": {
  "pause_status": "UNPAUSED",
  "file_arrival": {
    "url": "/Volumes/workspace/default/medalion/to_process/"
  }
}
```

- **`file_arrival`** — The job starts automatically when a new file lands in the Unity
  Catalog volume folder `medalion/to_process/`.
- **`pause_status: UNPAUSED`** — Trigger is active.
- This matches the Bronze ingestion staging path used by `bronze_config.py` and
  `generate_sample_data.py` on Databricks.

**Upstream dependency:** CSVs in `to_process/` are produced by **Job 1**
(`Data_generation`) or manual upload. Job 2 does not generate data itself.

Typical flow: Job 1 writes timestamped CSVs to `to_process/` → file_arrival trigger
fires Job 2 → Bronze ingests and archives files to `processed/` → Silver → Gold →
Dashboard refresh.

### Task pipeline (dependency chain)

```text
Bronze_layer_ingestion
        │
        ▼
Silver_ingestion
        │
        ▼
Gold_ingestion
        │
        ▼
Dashboard_Refresh
```

Each task uses `"run_if": "ALL_SUCCESS"` — if a parent task fails, downstream tasks
are skipped.

#### Task 1 — `Bronze_layer_ingestion`

| Field | Value |
|-------|-------|
| Type | `notebook_task` |
| Notebook | `.../src/bronze/Ingestion_notbook` |
| Depends on | *(none — entry point after trigger)* |

Runs `ingest_all.main()`: reads oldest pending `customers*.csv`, `products*.csv`,
`orders*.csv` from `to_process/`, writes Bronze Delta tables, archives CSVs to
`processed/`.

**Notifications:** Email on start and failure to `ojasvee.bajra@tothenew.com`.
Skipped runs do not alert (`no_alert_for_skipped_runs: true` at task level).

#### Task 2 — `Silver_ingestion`

| Field | Value |
|-------|-------|
| Type | `notebook_task` |
| Notebook | `.../src/silver/Silver_notebook.ipynb` |
| Depends on | `Bronze_layer_ingestion` |

Runs `create_silver_tables.main()`: applies four quality checks, writes `silver_*`
tables with flag columns (no bad rows deleted), prints metrics report.

#### Task 3 — `Gold_ingestion`

| Field | Value |
|-------|-------|
| Type | `notebook_task` |
| Notebook | `.../src/gold/Gold_notebook.ipynb` |
| Depends on | `Silver_ingestion` |

Runs `create_gold_tables.main()`: builds three Gold aggregations using only
`quality_check_result = 'PASS'` orders, runs revenue cross-check.

#### Task 4 — `Dashboard_Refresh`

| Field | Value |
|-------|-------|
| Type | `dashboard_task` |
| `dashboard_id` | `01f1a516dfd71a78bfcd1149365da3a5` |
| `warehouse_id` | `daad63d1bf3c857e` |
| Depends on | `Gold_ingestion` |

Refreshes the Databricks SQL Dashboard after Gold data is updated so visualizations
(bar chart, histogram, pie chart) reflect the latest `gold_*` tables. Queries are
defined in `src/dashboard/dashboard_queries.sql`.

### Notifications summary

| Scope | Behavior |
|-------|----------|
| Job-level | Alerts if runs are skipped (`no_alert_for_skipped_runs: false`) |
| Bronze task | Email on start + failure |
| Silver / Gold / Dashboard | No email notifications configured |

### Paths to verify before running

Ensure notebook paths in `databricks-job.json` match files in your workspace:

| Job path | Repo file |
|----------|-----------|
| `.../bronze/Ingestion_notbook` | `src/bronze/Ingestion_notbook.ipynb` |
| `.../silver/Silver_notebook.ipynb` | `src/silver/silver_notebook.ipynb` *(case may differ)* |
| `.../gold/Gold_notebook.ipynb` | `src/gold/gold_notebook.ipynb` *(case may differ)* |

If the job fails with “notebook not found”, align casing and extensions with the
actual workspace paths.

### What this job does *not* include

- **No retry policy** — failed tasks stop the chain unless configured separately in the UI.

---

## Related documentation

- `databricks-job-data-generation.json` — Job 1: synthetic CSV generation
- `databricks-job.json` — Job 2: medallion ingest + dashboard refresh
- `project-context.md` — full project specification
- `src/dashboard/DASHBOARD_GUIDE.md` — dashboard build and verification
- `ai-prompts/` — AI prompt history per pipeline activity
- `candidate-info.md` — submission metadata
