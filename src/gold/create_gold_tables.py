"""Gold layer orchestrator.

Reads Silver Delta tables, runs Gold SQL aggregations using PASS-only orders,
writes Gold Delta tables, and prints reconciliation checks.
"""

from __future__ import annotations

import inspect
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if os.environ.get("GOLD_SRC_DIR"):
    GOLD_DIR = Path(os.environ["GOLD_SRC_DIR"]).resolve()
else:
    try:
        GOLD_DIR = Path(__file__).resolve().parent
    except NameError:
        GOLD_DIR = Path(inspect.currentframe().f_code.co_filename).resolve().parent

if str(GOLD_DIR) not in sys.path:
    sys.path.insert(0, str(GOLD_DIR))

from gold_config import (  # noqa: E402
    GOLD_CUSTOMER_SEGMENTATION_TABLE,
    GOLD_REVENUE_BY_CUSTOMER_TABLE,
    GOLD_SALES_BY_PRODUCT_TABLE,
    HIGH_VALUE_N_TILES,
    HIGH_VALUE_TOP_PERCENT,
    QUALITY_RESULT_PASS,
    REPEAT_MIN_ORDERS,
    REVENUE_CROSS_CHECK_TOLERANCE,
    SILVER_CUSTOMERS_TABLE,
    SILVER_ORDERS_TABLE,
    SILVER_PRODUCTS_TABLE,
    GoldBuildResult,
    get_spark_session,
    load_sql_query,
    read_gold_table,
    read_silver_table,
    register_silver_temp_views,
    render_sql_template,
    write_gold_table,
)

GOLD_QUERY_SEQUENCE: tuple[tuple[str, str], ...] = (
    ("01_sales_by_product.sql", GOLD_SALES_BY_PRODUCT_TABLE),
    ("02_revenue_by_customer.sql", GOLD_REVENUE_BY_CUSTOMER_TABLE),
    ("04_customer_segmentation.sql", GOLD_CUSTOMER_SEGMENTATION_TABLE),
)


def run_gold_sql(spark: SparkSession, sql_filename: str):
    """Execute a Gold SQL file against registered Silver temp views.

    Args:
        spark: Active Spark session.
        sql_filename: SQL file name within ``src/gold/``.

    Returns:
        Query result DataFrame.
    """
    sql_path = GOLD_DIR / sql_filename
    sql_text = render_sql_template(load_sql_query(sql_path))
    return spark.sql(sql_text)


def sum_total_revenue(dataframe, revenue_column: str = "total_revenue") -> float:
    """Sum a revenue column from a Gold aggregation DataFrame.

    Args:
        dataframe: Gold result DataFrame.
        revenue_column: Name of the revenue column to sum.

    Returns:
        Total revenue as a float.
    """
    row = dataframe.select(F.sum(F.col(revenue_column)).alias("total")).collect()[0]
    return float(row["total"] or 0.0)


def create_gold_tables(spark: SparkSession) -> GoldBuildResult:
    """Build all Gold tables from Silver sources.

    Args:
        spark: Active Spark session.

    Returns:
        Summary containing row counts and revenue cross-check results.
    """
    pass_order_count = register_silver_temp_views(spark)
    print(
        f"Gold boundary: using {pass_order_count:,} Silver orders with "
        f"quality_check_result = {QUALITY_RESULT_PASS!r}."
    )

    table_row_counts: dict[str, int] = {}
    for sql_filename, gold_table_name in GOLD_QUERY_SEQUENCE:
        result_df = run_gold_sql(spark, sql_filename)
        table_row_counts[gold_table_name] = write_gold_table(spark, result_df, gold_table_name)

    sales_df = read_gold_table(spark, GOLD_SALES_BY_PRODUCT_TABLE)
    customer_df = read_gold_table(spark, GOLD_REVENUE_BY_CUSTOMER_TABLE)

    sales_revenue = sum_total_revenue(sales_df)
    customer_revenue = sum_total_revenue(customer_df)
    revenue_cross_check_passed = (
        abs(sales_revenue - customer_revenue) <= REVENUE_CROSS_CHECK_TOLERANCE
    )

    return GoldBuildResult(
        table_row_counts=table_row_counts,
        sales_by_product_revenue=sales_revenue,
        revenue_by_customer_total=customer_revenue,
        revenue_cross_check_passed=revenue_cross_check_passed,
    )


def print_row_count_summary(result: GoldBuildResult) -> None:
    """Print row counts for each Gold output table.

    Args:
        result: Gold build result.
    """
    print("\nGold Row Count Summary")
    print("----------------------")
    for table_name, row_count in result.table_row_counts.items():
        print(f"  - {table_name}: {row_count:,} rows")
    print("----------------------")


def print_revenue_cross_check(result: GoldBuildResult) -> None:
    """Print and warn on revenue reconciliation between Gold aggregations.

    Args:
        result: Gold build result.
    """
    print("\nGold Revenue Cross-Check")
    print("------------------------")
    print(f"  SUM(total_revenue) from {GOLD_SALES_BY_PRODUCT_TABLE}: {result.sales_by_product_revenue:,.2f}")
    print(
        f"  SUM(total_revenue) from {GOLD_REVENUE_BY_CUSTOMER_TABLE}: "
        f"{result.revenue_by_customer_total:,.2f}"
    )
    print(f"  Tolerance: +/- {REVENUE_CROSS_CHECK_TOLERANCE:.2f}")
    if result.revenue_cross_check_passed:
        print("  Result: MATCH")
    else:
        print("  Result: MISMATCH — possible join/filter bug in Gold SQL.")
    print("------------------------")


def print_segmentation_logic() -> None:
    """Print the customer segmentation rules used by Gold SQL."""
    print("\nCustomer Segmentation Logic")
    print("---------------------------")
    print("  Data source: PASS-only Silver orders (Gold boundary filter).")
    print(f"  High-Value: top {HIGH_VALUE_TOP_PERCENT:.0f}% of customers with >= 1 PASS order")
    print(f"              (NTILE({HIGH_VALUE_N_TILES}) = 1 by total_revenue DESC).")
    print(f"  Repeat:     >= {REPEAT_MIN_ORDERS} PASS orders, not High-Value.")
    print("  One-Time:   exactly 1 PASS order, not High-Value.")
    print("  Inactive:   0 PASS orders.")
    print("---------------------------")


def show_manual_verification_sample(spark: SparkSession) -> None:
    """Show one product and one customer Gold row with underlying Silver orders.

    Args:
        spark: Active Spark session.
    """
    sales_df = read_gold_table(spark, GOLD_SALES_BY_PRODUCT_TABLE)
    customer_df = read_gold_table(spark, GOLD_REVENUE_BY_CUSTOMER_TABLE)
    orders_df = read_silver_table(spark, SILVER_ORDERS_TABLE)

    sample_product = (
        sales_df.filter(F.col("total_orders") > 0)
        .orderBy(F.col("total_revenue").desc())
        .select("product_id", "product_name", "total_orders", "total_revenue", "avg_order_value")
        .first()
    )
    sample_customer = (
        customer_df.filter(F.col("total_orders") > 0)
        .orderBy(F.col("total_revenue").desc())
        .select(
            "customer_id",
            "customer_name",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "lifetime_value_actual",
        )
        .first()
    )

    if sample_product is None or sample_customer is None:
        print("\nManual verification sample skipped: no qualifying Gold rows found.")
        return

    product_id = sample_product["product_id"]
    customer_id = sample_customer["customer_id"]

    print("\nManual Verification Sample")
    print("--------------------------")
    print("Gold row — sales by product:")
    sales_df.filter(F.col("product_id") == product_id).show(truncate=False)

    product_orders = (
        orders_df.filter(
            (F.col("product_id") == product_id)
            & (F.col("quality_check_result") == QUALITY_RESULT_PASS)
        )
        .select("order_id", "customer_id", "product_id", "total_amount", "quality_check_result")
        .orderBy("order_id")
    )
    product_order_count = product_orders.count()
    product_revenue = product_orders.select(
        F.sum(F.col("total_amount").cast("decimal(18,2)")).alias("sum_revenue")
    ).collect()[0]["sum_revenue"]
    print(
        f"Underlying Silver PASS orders for product_id={product_id}: "
        f"{product_order_count} orders, revenue sum = {product_revenue}"
    )
    product_orders.show(10, truncate=False)
    if product_order_count > 10:
        print(f"  ... ({product_order_count - 10} more rows not shown)")

    print("Gold row — revenue by customer:")
    customer_df.filter(F.col("customer_id") == customer_id).show(truncate=False)

    customer_orders = (
        orders_df.filter(
            (F.col("customer_id") == customer_id)
            & (F.col("quality_check_result") == QUALITY_RESULT_PASS)
        )
        .select("order_id", "customer_id", "product_id", "total_amount", "quality_check_result")
        .orderBy("order_id")
    )
    customer_order_count = customer_orders.count()
    customer_revenue = customer_orders.select(
        F.sum(F.col("total_amount").cast("decimal(18,2)")).alias("sum_revenue")
    ).collect()[0]["sum_revenue"]
    print(
        f"Underlying Silver PASS orders for customer_id={customer_id}: "
        f"{customer_order_count} orders, revenue sum = {customer_revenue}"
    )
    customer_orders.show(10, truncate=False)
    if customer_order_count > 10:
        print(f"  ... ({customer_order_count - 10} more rows not shown)")
    print("--------------------------")


def main() -> int:
    """Run the Gold layer build end-to-end.

    Returns:
        Process exit code (0 on success, 1 on revenue mismatch).
    """
    spark = get_spark_session("gold-create-tables")
    result = create_gold_tables(spark)

    print_row_count_summary(result)
    print_revenue_cross_check(result)
    print_segmentation_logic()
    show_manual_verification_sample(spark)

    if not result.revenue_cross_check_passed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
