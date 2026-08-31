# AI prompts used for dashboard and visualization development.
# Databricks SQL Dashboard Guide

This guide explains how to build the medallion pipeline analytics dashboard in the
Databricks UI using the queries in `dashboard_queries.sql`. The dashboard reads from
Gold Delta tables produced by `src/gold/create_gold_tables.py`.

**Prerequisites:** Bronze → Silver → Gold pipeline has been run successfully.

| Gold table | Purpose |
|------------|---------|
| `gold_sales_by_product` | Product-level revenue and order counts |
| `gold_revenue_by_customer` | Customer-level revenue and order counts |
| `gold_customer_segmentation` | Behavioral segment summary |

---

## 1. Create a new Databricks SQL Dashboard

1. Open your Databricks workspace.
2. Go to **SQL** → **Dashboards** in the left sidebar.
3. Click **Create dashboard**.
4. Name it (e.g. `Medallion E-Commerce Analytics`).
5. For each visualization below, create a **Query** first, then attach it to the dashboard as a **Visualization**.

To create a query:

1. Go to **SQL** → **SQL Editor** (or **Queries**).
2. Paste the relevant block from `src/dashboard/dashboard_queries.sql`.
3. Confirm the SQL warehouse can read the Gold tables (managed tables or Delta paths on your volume).
4. Click **Run** to validate, then **Save** the query with a descriptive name.

---

## 2. Required visualizations

### Visualization 1 — Bar chart: Top 10 products by revenue

| Setting | Value |
|---------|-------|
| **Query** | QUERY 1 from `dashboard_queries.sql` |
| **Chart type** | Bar |
| **X-axis** | `product_name` |
| **Y-axis** | `total_revenue` |
| **Optional color / group** | `category` |

**What it shows:** The ten highest-revenue products, helping viewers quickly see which products drive sales.

---

### Visualization 2 — Histogram: Customer revenue distribution

| Setting | Value |
|---------|-------|
| **Query** | QUERY 2 from `dashboard_queries.sql` |
| **Chart type** | Histogram |
| **Bin / value column** | `total_revenue` |
| **Count** | Rows (each row is one customer) |

**What it shows:** How revenue is distributed across customers who have at least one qualifying (PASS) order. Zero-revenue customers are excluded so the chart is not dominated by a spike at $0.

---

### Visualization 3 — Pie chart: Customer segmentation

| Setting | Value |
|---------|-------|
| **Query** | QUERY 3 from `dashboard_queries.sql` |
| **Chart type** | Pie |
| **Category / label** | `segment_type` |
| **Value** | `customer_count` |

**What it shows:** The proportion of customers in each behavioral segment (High-Value, Repeat, One-Time, Inactive).

---

## 3. How to verify the dashboard is correct

Run these checks after building the dashboard or after refreshing Gold data.

### Check A — Pie chart totals match customer dimension

The sum of `customer_count` across all segments in Query 3 should equal the total
row count of `gold_revenue_by_customer` (one row per unique customer):

```sql
SELECT SUM(customer_count) AS pie_total
FROM gold_customer_segmentation;

SELECT COUNT(*) AS customer_rows
FROM gold_revenue_by_customer;
```

Both numbers should match (e.g. 10,000).

### Check B — Top product revenue is plausible

The highest `total_revenue` in Query 1 should be less than or equal to the sum of
all product revenue:

```sql
SELECT SUM(total_revenue) FROM gold_sales_by_product;
```

### Check C — Histogram row count

Query 2 row count should equal customers with at least one PASS order:

```sql
SELECT COUNT(*) FROM gold_revenue_by_customer WHERE total_revenue > 0;
```

### Check D — Cross-layer revenue consistency

Total revenue from products should equal total revenue from customers (same PASS-order set in Gold):

```sql
SELECT SUM(total_revenue) FROM gold_sales_by_product;
SELECT SUM(total_revenue) FROM gold_revenue_by_customer;
```

These two sums should match within rounding tolerance.

---

## 4. Screenshot placeholder

> Screenshots have been placed in the below mentioned folder path:
databricks-medallion-pipeline/src/dashboard/
---

## Related files

- `src/dashboard/dashboard_queries.sql` — SQL for all three tiles
- `src/gold/create_gold_tables.py` — builds the Gold tables this dashboard reads
- `src/gold/04_customer_segmentation.sql` — segmentation logic documentation
