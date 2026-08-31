"""Silver layer orchestrator.

Reads Bronze Delta tables, applies all applicable quality checks, writes
Silver Delta tables without filtering rows, and prints a metrics report.
"""

from __future__ import annotations

import inspect
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if os.environ.get("SILVER_SRC_DIR"):
    SILVER_DIR = Path(os.environ["SILVER_SRC_DIR"]).resolve()
else:
    try:
        SILVER_DIR = Path(__file__).resolve().parent
    except NameError:
        SILVER_DIR = Path(inspect.currentframe().f_code.co_filename).resolve().parent

if str(SILVER_DIR) not in sys.path:
    sys.path.insert(0, str(SILVER_DIR))

from importlib import import_module  # noqa: E402

completeness = import_module("01_quality_completeness")
uniqueness = import_module("02_quality_uniqueness")
type_validation = import_module("03_quality_type_validation")
referential_integrity = import_module("04_quality_referential_integrity")

from silver_config import (  # noqa: E402
    BRONZE_CUSTOMERS_TABLE,
    BRONZE_ORDERS_TABLE,
    BRONZE_PRODUCTS_TABLE,
    COMPLETENESS_THRESHOLD_PCT,
    QUALITY_RESULT_FAIL,
    REFERENTIAL_INTEGRITY_THRESHOLD_PCT,
    SILVER_CUSTOMERS_TABLE,
    SILVER_ORDERS_TABLE,
    SILVER_PRODUCTS_TABLE,
    TYPE_VALIDATION_THRESHOLD_PCT,
    UNIQUENESS_THRESHOLD_PCT,
    QualityCheckMetric,
    add_not_applicable_check_columns,
    add_quality_check_result,
    compute_check_metric,
    get_spark_session,
    print_quality_metrics_report,
    read_bronze_table,
    write_silver_table,
)


@dataclass
class SilverBuildResult:
    """Summary of a Silver build execution.

    Attributes:
        bronze_counts: Bronze row counts per table.
        silver_counts: Silver row counts per table.
        metrics: Quality metrics for each applicable check.
    """

    bronze_counts: dict[str, int]
    silver_counts: dict[str, int]
    metrics: list[QualityCheckMetric]


def build_silver_customers(spark: SparkSession, bronze_customers_df) -> tuple:
    """Apply Silver quality checks to customers and return the enriched DataFrame.

    Args:
        spark: Active Spark session.
        bronze_customers_df: Bronze customers DataFrame.

    Returns:
        Tuple of (silver DataFrame, list of quality metrics).
    """
    silver_df = completeness.apply_completeness_check_customers(bronze_customers_df)
    silver_df = uniqueness.apply_uniqueness_check_customers(silver_df)
    silver_df = type_validation.apply_type_validation_customers(silver_df)
    silver_df = add_not_applicable_check_columns(silver_df, "referential_integrity")
    silver_df = add_quality_check_result(
        silver_df,
        [
            "completeness_check_passed",
            "uniqueness_check_passed",
            "type_validation_passed",
        ],
    )

    metrics = [
        compute_check_metric(
            silver_df,
            SILVER_CUSTOMERS_TABLE,
            "completeness",
            "completeness_check_passed",
            COMPLETENESS_THRESHOLD_PCT,
        ),
        compute_check_metric(
            silver_df,
            SILVER_CUSTOMERS_TABLE,
            "uniqueness",
            "uniqueness_check_passed",
            UNIQUENESS_THRESHOLD_PCT,
        ),
        compute_check_metric(
            silver_df,
            SILVER_CUSTOMERS_TABLE,
            "type_validation",
            "type_validation_passed",
            TYPE_VALIDATION_THRESHOLD_PCT,
        ),
    ]
    return silver_df, metrics


def build_silver_products(spark: SparkSession, bronze_products_df) -> tuple:
    """Apply Silver quality checks to products and return the enriched DataFrame.

    Args:
        spark: Active Spark session.
        bronze_products_df: Bronze products DataFrame.

    Returns:
        Tuple of (silver DataFrame, list of quality metrics).
    """
    silver_df = add_not_applicable_check_columns(bronze_products_df, "completeness")
    silver_df = add_not_applicable_check_columns(silver_df, "uniqueness")
    silver_df = type_validation.apply_type_validation_products(silver_df)
    silver_df = add_not_applicable_check_columns(silver_df, "referential_integrity")
    silver_df = add_quality_check_result(silver_df, ["type_validation_passed"])

    metrics = [
        compute_check_metric(
            silver_df,
            SILVER_PRODUCTS_TABLE,
            "type_validation",
            "type_validation_passed",
            TYPE_VALIDATION_THRESHOLD_PCT,
        ),
    ]
    return silver_df, metrics


def build_silver_orders(
    spark: SparkSession,
    bronze_orders_df,
    bronze_customers_df,
    bronze_products_df,
) -> tuple:
    """Apply Silver quality checks to orders and return the enriched DataFrame.

    Args:
        spark: Active Spark session.
        bronze_orders_df: Bronze orders DataFrame.
        bronze_customers_df: Bronze customers reference DataFrame.
        bronze_products_df: Bronze products reference DataFrame.

    Returns:
        Tuple of (silver DataFrame, list of quality metrics).
    """
    silver_df = completeness.apply_completeness_check_orders(bronze_orders_df)
    silver_df = uniqueness.apply_uniqueness_check_orders(silver_df)
    silver_df = type_validation.apply_type_validation_orders(silver_df)
    silver_df = referential_integrity.apply_referential_integrity_check_orders(
        silver_df,
        bronze_customers_df,
        bronze_products_df,
    )
    silver_df = add_quality_check_result(
        silver_df,
        [
            "completeness_check_passed",
            "uniqueness_check_passed",
            "type_validation_passed",
            "referential_integrity_passed",
        ],
    )

    metrics = [
        compute_check_metric(
            silver_df,
            SILVER_ORDERS_TABLE,
            "completeness",
            "completeness_check_passed",
            COMPLETENESS_THRESHOLD_PCT,
        ),
        compute_check_metric(
            silver_df,
            SILVER_ORDERS_TABLE,
            "uniqueness",
            "uniqueness_check_passed",
            UNIQUENESS_THRESHOLD_PCT,
        ),
        compute_check_metric(
            silver_df,
            SILVER_ORDERS_TABLE,
            "type_validation",
            "type_validation_passed",
            TYPE_VALIDATION_THRESHOLD_PCT,
        ),
        compute_check_metric(
            silver_df,
            SILVER_ORDERS_TABLE,
            "referential_integrity",
            "referential_integrity_passed",
            REFERENTIAL_INTEGRITY_THRESHOLD_PCT,
        ),
    ]
    return silver_df, metrics


def create_silver_tables(spark: SparkSession) -> SilverBuildResult:
    """Build all Silver tables from Bronze sources.

    Args:
        spark: Active Spark session.

    Returns:
        Summary containing row counts and quality metrics.
    """
    bronze_customers = read_bronze_table(spark, BRONZE_CUSTOMERS_TABLE)
    bronze_products = read_bronze_table(spark, BRONZE_PRODUCTS_TABLE)
    bronze_orders = read_bronze_table(spark, BRONZE_ORDERS_TABLE)

    bronze_counts = {
        BRONZE_CUSTOMERS_TABLE: bronze_customers.count(),
        BRONZE_PRODUCTS_TABLE: bronze_products.count(),
        BRONZE_ORDERS_TABLE: bronze_orders.count(),
    }

    silver_customers_df, customer_metrics = build_silver_customers(spark, bronze_customers)
    silver_products_df, product_metrics = build_silver_products(spark, bronze_products)
    silver_orders_df, order_metrics = build_silver_orders(
        spark,
        bronze_orders,
        bronze_customers,
        bronze_products,
    )

    silver_counts = {
        SILVER_CUSTOMERS_TABLE: write_silver_table(
            spark,
            silver_customers_df,
            SILVER_CUSTOMERS_TABLE,
            bronze_counts[BRONZE_CUSTOMERS_TABLE],
        ),
        SILVER_PRODUCTS_TABLE: write_silver_table(
            spark,
            silver_products_df,
            SILVER_PRODUCTS_TABLE,
            bronze_counts[BRONZE_PRODUCTS_TABLE],
        ),
        SILVER_ORDERS_TABLE: write_silver_table(
            spark,
            silver_orders_df,
            SILVER_ORDERS_TABLE,
            bronze_counts[BRONZE_ORDERS_TABLE],
        ),
    }

    return SilverBuildResult(
        bronze_counts=bronze_counts,
        silver_counts=silver_counts,
        metrics=customer_metrics + product_metrics + order_metrics,
    )


def print_row_count_summary(result: SilverBuildResult) -> None:
    """Print Bronze vs Silver row count summary.

    Args:
        result: Silver build result.
    """
    print("\nSilver Row Count Summary")
    print("------------------------+")
    print(f"{'Table':<22} {'Bronze In':>12} {'Silver Out':>12} {'Match':>8}")
    print("------------------------+")
    mapping = [
        (BRONZE_CUSTOMERS_TABLE, SILVER_CUSTOMERS_TABLE),
        (BRONZE_PRODUCTS_TABLE, SILVER_PRODUCTS_TABLE),
        (BRONZE_ORDERS_TABLE, SILVER_ORDERS_TABLE),
    ]
    for bronze_table, silver_table in mapping:
        bronze_count = result.bronze_counts[bronze_table]
        silver_count = result.silver_counts[silver_table]
        match = "YES" if bronze_count == silver_count else "NO"
        print(f"{silver_table:<22} {bronze_count:>12,} {silver_count:>12,} {match:>8}")
    print("------------------------+")


def show_failed_orders_sample(spark: SparkSession, sample_size: int = 5) -> None:
    """Display sample failed order rows with quality flag details.

    Args:
        spark: Active Spark session.
        sample_size: Number of rows to display.
    """
    from silver_config import read_delta_table, resolve_silver_table_path

    orders_df = read_delta_table(
        spark,
        SILVER_ORDERS_TABLE,
        resolve_silver_table_path(SILVER_ORDERS_TABLE),
    )
    failed_df = orders_df.filter(F.col("quality_check_result") == QUALITY_RESULT_FAIL).select(
        "order_id",
        "customer_id",
        "product_id",
        "quality_check_result",
        "completeness_check_passed",
        "completeness_check_details",
        "uniqueness_check_passed",
        "uniqueness_check_details",
        "type_validation_passed",
        "type_validation_details",
        "referential_integrity_passed",
        "referential_integrity_details",
    )

    print(f"\nSample silver_orders rows where quality_check_result = FAIL (up to {sample_size}):")
    failed_df.show(sample_size, truncate=False)


def main() -> int:
    """Run the Silver layer build end-to-end.

    Returns:
        Process exit code (0 when tables are built; threshold misses are warnings only).
    """
    spark = get_spark_session("silver-create-tables")
    result = create_silver_tables(spark)

    print_row_count_summary(result)
    print_quality_metrics_report(result.metrics)
    show_failed_orders_sample(spark)

    below_threshold = [metric for metric in result.metrics if not metric.meets_threshold]
    if below_threshold:
        print(
            "\nWARNING: One or more quality checks fell below the configured threshold "
            "(expected with injected test data). Silver tables were still written."
        )
        for metric in below_threshold:
            print(
                f"  - {metric.table_name}.{metric.check_name}: "
                f"{metric.pass_rate_pct:.2f}% < {metric.threshold_pct:.1f}%"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
