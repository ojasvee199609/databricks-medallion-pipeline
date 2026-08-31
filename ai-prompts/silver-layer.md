# AI prompts used for silver layer quality validation development.

## Session: Silver layer implementation (2026-08-27)

**Prompt sent:** Create the Silver layer in `src/silver/` with four quality checks
(completeness, uniqueness, type validation, referential integrity) plus
`create_silver_tables.py` orchestrator. Hard rule: never filter/delete bad rows;
add flag columns only; Bronze row count must equal Silver row count.

**Response summary:** Implemented `silver_config.py` (shared config, thresholds,
read/write helpers, metrics report), four check modules (`01`–`04`), and
`create_silver_tables.py` orchestrator. Products get type validation only;
customers get completeness + uniqueness + type; orders get all four. N/A checks
get passing placeholder columns. Combined `quality_check_result` column added.

**Accepted:**
- Configurable thresholds via constants/env vars
- Row-count guard before/after Silver writes
- Databricks `__file__` bootstrap pattern in orchestrator
- Referential integrity excludes NULL FKs (completeness handles those)


**Validation (local run after Bronze ingest):**
- Row counts: customers 10,010 / products 500 / orders 100,020 — all match
- Injected issue counts confirmed: 50 NULL emails, 20 dup customer rows,
  100 NULL customer_id, 200 NULL product_id, 40 dup order rows,
  50 orphan customer_id, 30 orphan product_id
- Uniqueness checks correctly report below 100% threshold (expected with injected dupes)


Sample response post execution


Silver Row Count Summary
------------------------+
Table                     Bronze In   Silver Out    Match
------------------------+
silver_customers             10,010       10,010      YES
silver_products                 500          500      YES
silver_orders               100,020      100,020      YES
------------------------+

Silver Quality Metrics Report
-----------------+-----------------------+---------+---------+--------+----------+-----------+----------------
Table            | Check                 | Total   | Passed  | Failed | % Passed | Threshold | Meets Threshold
-----------------+-----------------------+---------+---------+--------+----------+-----------+----------------
silver_customers | completeness          | 10,010  | 9,960   | 50     | 99.50%   | 99.0%     | YES            
silver_customers | uniqueness            | 10,010  | 9,990   | 20     | 99.80%   | 100.0%    | NO             
silver_customers | type_validation       | 10,010  | 10,010  | 0      | 100.00%  | 99.0%     | YES            
silver_products  | type_validation       | 500     | 500     | 0      | 100.00%  | 99.0%     | YES            
silver_orders    | completeness          | 100,020 | 99,720  | 300    | 99.70%   | 99.0%     | YES            
silver_orders    | uniqueness            | 100,020 | 99,980  | 40     | 99.96%   | 100.0%    | NO             
silver_orders    | type_validation       | 100,020 | 100,020 | 0      | 100.00%  | 99.0%     | YES            
silver_orders    | referential_integrity | 100,020 | 99,940  | 80     | 99.92%   | 99.9%     | YES            
-----------------+-----------------------+---------+---------+--------+----------+-----------+----------------

Checks below threshold:
  - silver_customers.uniqueness: 99.80% < 100.0%
  - silver_orders.uniqueness: 99.96% < 100.0%

Sample silver_orders rows where quality_check_result = FAIL (up to 5):
+--------+-----------+----------+--------------------+-------------------------+--------------------------+-----------------------+------------------------+----------------------+-----------------------+----------------------------+-----------------------------+
|order_id|customer_id|product_id|quality_check_result|completeness_check_passed|completeness_check_details|uniqueness_check_passed|uniqueness_check_details|type_validation_passed|type_validation_details|referential_integrity_passed|referential_integrity_details|
+--------+-----------+----------+--------------------+-------------------------+--------------------------+-----------------------+------------------------+----------------------+-----------------------+----------------------------+-----------------------------+
|10280   |NULL       |408       |FAIL                |false                    |customer_id is NULL       |true                   |                        |true                  |                       |true                        |                             |
|10443   |NULL       |415       |FAIL                |false                    |customer_id is NULL       |true                   |                        |true                  |                       |true                        |                             |
|10603   |99999      |416       |FAIL                |true                     |                          |true                   |                        |true                  |                       |false                       |orphan customer_id: 99999    |
|10660   |7197       |NULL      |FAIL                |false                    |product_id is NULL        |true                   |                        |true                  |                       |true                        |                             |
|10887   |8247       |NULL      |FAIL                |false                    |product_id is NULL        |true                   |                        |true                  |                       |true                        |                             |
+--------+-----------+----------+--------------------+-------------------------+--------------------------+-----------------------+------------------------+----------------------+-----------------------+----------------------------+-----------------------------+
only showing top 5 rows

One or more quality checks fell below the configured threshold.
