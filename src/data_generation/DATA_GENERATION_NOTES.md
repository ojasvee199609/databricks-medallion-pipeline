# Notes on sample data generation approach and parameters.
# AI prompts used for sample data generation.

Create a Python script at src/data_generation/generate_sample_data.py that
generates synthetic sample data for the e-commerce medallion pipeline
project. Use the `faker` library for realistic names/emails/dates. Do
NOT use any real customer data. Output three CSV files into the data/
folder: customers.csv, orders.csv, products.csv.

Use a fixed random seed (e.g., 42) so the output is reproducible across
runs.

## Schema

**customers.csv** — ~10,000 rows
- customer_id (INT, PK, sequential starting at 1)
- customer_name (STRING)
- email (STRING)
- country (STRING)
- signup_date (DATE)
- customer_segment (STRING: Premium / Standard / Basic)
- lifetime_value (DECIMAL, 2dp)

**products.csv** — ~500 rows
- product_id (INT, PK, sequential starting at 1)
- product_name (STRING)
- category (STRING)
- price (DECIMAL, 2dp)
- cost (DECIMAL, 2dp, always less than price)
- stock_quantity (INT)
- reorder_level (INT)

**orders.csv** — ~100,000 rows
- order_id (INT, PK, sequential starting at 1)
- customer_id (INT, FK -> customers.customer_id)
- order_date (DATE)
- product_id (INT, FK -> products.product_id)
- quantity (INT)
- unit_price (DECIMAL, 2dp — should match the referenced product's price)
- total_amount (DECIMAL, 2dp = quantity * unit_price)
- order_status (STRING: Pending / Completed / Cancelled)
- payment_date (DATE, nullable — NULL if order_status is Pending or Cancelled)

Generate products.csv and customers.csv first (clean, valid data), then
generate orders.csv referencing valid customer_ids and product_ids by
default.

## Required intentional data quality issues

After generating clean base data, deliberately inject exactly these
issues (do this in a separate, clearly commented function so the
"dirty" logic is isolated from the "clean" generation logic):

- customers.csv: 50 rows with NULL email
- customers.csv: 10 rows that are exact duplicates of existing customer_id
  values (duplicate the full row, same customer_id)
- orders.csv: 100 rows with NULL customer_id
- orders.csv: 200 rows with NULL product_id
- orders.csv: 50 rows with a customer_id that does NOT exist in customers.csv
- orders.csv: 30 rows with a product_id that does NOT exist in products.csv
- orders.csv: 20 rows with duplicate order_id (reuse an existing order_id)

Print a summary at the end of the script showing: total rows generated
per file, and a breakdown of how many rows of each injected issue type
were created (so the counts can be verified against this list).

## Code requirements
- Add a module-level docstring explaining what the script does
- Add a docstring to every function (purpose, inputs, outputs)
- Use type hints
- Make row counts and issue counts configurable via constants at the
  top of the file (not hardcoded inline), so they're easy to verify or
  adjust later
- No hardcoded file paths — use a relative path to the data/ folder,
  computed from the script's own location
- Include a `if __name__ == "__main__":` entry point

After writing the script, run it and show me:
1. The row counts for each output file
2. The injected-issue summary printed by the script
3. Confirmation that data/customers.csv, data/orders.csv, and
   data/products.csv were created


Commands used for validation : 

# NULL emails in customers.csv (expect 50)
awk -F',' 'NR>1 && $3=="" {c++} END{print c}' customers.csv

# NULL customer_id in orders.csv (expect 100)
awk -F',' 'NR>1 && $2=="" {c++} END{print c}' orders.csv

# NULL product_id in orders.csv (expect 200)
awk -F',' 'NR>1 && $4=="" {c++} END{print c}' orders.csv

Prompt output :
============================================================
Sample Data Generation Summary
============================================================

Total rows generated:
  customers.csv: 10,010
  products.csv:  500
  orders.csv:    100,020

Injected data quality issues — customers.csv:
  NULL email:              50
  Duplicate customer_id:   10

Injected data quality issues — orders.csv:
  NULL customer_id:        100
  NULL product_id:         200
  Invalid customer_id:     50
  Invalid product_id:      30
  Duplicate order_id:      20
============================================================

. File confirmation
All three CSVs were created under data/:

data/customers.csv
data/products.csv
data/orders.csv
Each file has the correct header row and sample data (Faker-generated names, emails, countries, etc.).

Script design notes
Clean generation — generate_customers(), generate_products(), generate_orders()
Dirty injection — isolated in inject_customer_data_quality_issues() and inject_order_data_quality_issues()
Configurable constants at the top for row counts, issue counts, and RANDOM_SEED = 42
Relative paths — DATA_DIR is derived from Path(__file__)
Reproducible — random.seed(42) and Faker.seed(42)