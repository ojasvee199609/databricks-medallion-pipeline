"""Shared configuration and helpers for Silver layer quality validation.

Reads Bronze Delta tables, applies row-level quality flag columns without
filtering, writes Silver Delta tables, and produces pass-rate metrics.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_DIR = Path(__file__).resolve().parent
REPO_ROOT = SILVER_DIR.parent.parent
BRONZE_DIR = REPO_ROOT / "src" / "bronze"

if str(BRONZE_DIR) not in sys.path:
    sys.path.insert(0, str(BRONZE_DIR))

from bronze_config import (  # noqa: E402
    BRONZE_BASE_PATH,
    DEFAULT_DATABRICKS_VOLUME_BASE,
    USE_MANAGED_TABLES,
    _is_databricks_runtime,
    get_spark_session,
)

# Bronze table names
BRONZE_CUSTOMERS_TABLE = os.environ.get("BRONZE_CUSTOMERS_TABLE", "bronze_customers")
BRONZE_ORDERS_TABLE = os.environ.get("BRONZE_ORDERS_TABLE", "bronze_orders")
BRONZE_PRODUCTS_TABLE = os.environ.get("BRONZE_PRODUCTS_TABLE", "bronze_products")

# Silver table names
SILVER_CUSTOMERS_TABLE = os.environ.get("SILVER_CUSTOMERS_TABLE", "silver_customers")
SILVER_ORDERS_TABLE = os.environ.get("SILVER_ORDERS_TABLE", "silver_orders")
SILVER_PRODUCTS_TABLE = os.environ.get("SILVER_PRODUCTS_TABLE", "silver_products")


def _default_silver_base_path() -> str:
    """Return the default base path for Silver Delta table storage."""
    if _is_databricks_runtime():
        return f"{DEFAULT_DATABRICKS_VOLUME_BASE}/silver"
    return str(REPO_ROOT / "delta" / "silver")


SILVER_BASE_PATH = os.environ.get("SILVER_BASE_PATH", _default_silver_base_path())

# Quality metric thresholds (percent passed)
COMPLETENESS_THRESHOLD_PCT = float(os.environ.get("SILVER_COMPLETENESS_THRESHOLD_PCT", "99.0"))
UNIQUENESS_THRESHOLD_PCT = float(os.environ.get("SILVER_UNIQUENESS_THRESHOLD_PCT", "100.0"))
REFERENTIAL_INTEGRITY_THRESHOLD_PCT = float(
    os.environ.get("SILVER_REFERENTIAL_INTEGRITY_THRESHOLD_PCT", "99.9")
)
TYPE_VALIDATION_THRESHOLD_PCT = float(
    os.environ.get("SILVER_TYPE_VALIDATION_THRESHOLD_PCT", "99.0")
)

VALID_ORDER_STATUSES = ("Pending", "Completed", "Cancelled")
VALID_CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")

QUALITY_RESULT_PASS = "PASS"
QUALITY_RESULT_FAIL = "FAIL"


@dataclass
class QualityCheckMetric:
    """Pass/fail statistics for a single quality check on one table.

    Attributes:
        table_name: Silver table evaluated.
        check_name: Quality check identifier.
        total_rows: Total rows evaluated.
        passed_rows: Rows where the check passed.
        failed_rows: Rows where the check failed.
        pass_rate_pct: Percentage of rows that passed.
        threshold_pct: Expected minimum pass rate, if applicable.
        meets_threshold: Whether the pass rate meets the threshold.
    """

    table_name: str
    check_name: str
    total_rows: int
    passed_rows: int
    failed_rows: int
    pass_rate_pct: float
    threshold_pct: float | None
    meets_threshold: bool


def resolve_bronze_table_path(table_name: str) -> str:
    """Build the Delta storage path for a Bronze table.

    Args:
        table_name: Bronze Delta table name.

    Returns:
        Filesystem or DBFS path for the Bronze table directory.
    """
    return f"{BRONZE_BASE_PATH.rstrip('/')}/{table_name}"


def resolve_silver_table_path(table_name: str) -> str:
    """Build the Delta storage path for a Silver table.

    Args:
        table_name: Silver Delta table name.

    Returns:
        Filesystem or DBFS path for the Silver table directory.
    """
    return f"{SILVER_BASE_PATH.rstrip('/')}/{table_name}"


def read_delta_table(spark: SparkSession, table_name: str, table_path: str) -> DataFrame:
    """Read a Delta table from a managed table or path.

    Args:
        spark: Active Spark session.
        table_name: Managed Delta table name.
        table_path: Delta path when managed tables are disabled.

    Returns:
        The Delta table as a DataFrame.

    Raises:
        RuntimeError: If the table cannot be read.
    """
    try:
        if USE_MANAGED_TABLES:
            return spark.read.table(table_name)
        return spark.read.format("delta").load(table_path)
    except Exception as exc:
        raise RuntimeError(f"Failed to read Delta table {table_name}: {exc}") from exc


def read_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Bronze Delta table and validate it is non-empty.

    Args:
        spark: Active Spark session.
        table_name: Bronze table name.

    Returns:
        Bronze table DataFrame.

    Raises:
        ValueError: If the Bronze table contains zero rows.
        RuntimeError: If the table cannot be read.
    """
    table_path = resolve_bronze_table_path(table_name)
    dataframe = read_delta_table(spark, table_name, table_path)
    row_count = dataframe.count()
    if row_count == 0:
        raise ValueError(f"Bronze table {table_name} is empty.")
    return dataframe


def write_silver_table(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    expected_row_count: int,
) -> int:
    """Write a Silver Delta table and validate row counts.

    Args:
        spark: Active Spark session.
        dataframe: Silver DataFrame to write.
        table_name: Target Silver table name.
        expected_row_count: Bronze row count that must be preserved.

    Returns:
        Number of rows written.

    Raises:
        RuntimeError: If written row count does not match the expected count.
    """
    input_count = dataframe.count()
    if input_count != expected_row_count:
        raise RuntimeError(
            f"Silver row count mismatch before write for {table_name}: "
            f"expected {expected_row_count:,}, got {input_count:,}."
        )

    table_path = resolve_silver_table_path(table_name)
    writer = dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

    if USE_MANAGED_TABLES:
        writer.saveAsTable(table_name)
        written_count = spark.read.table(table_name).count()
    else:
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        writer.save(table_path)
        written_count = spark.read.format("delta").load(table_path).count()

    if written_count != expected_row_count:
        raise RuntimeError(
            f"Silver row count mismatch after write for {table_name}: "
            f"expected {expected_row_count:,}, written {written_count:,}."
        )

    return written_count


def is_null_or_blank(column_name: str) -> Column:
    """Return a boolean column indicating NULL or blank string values.

    Args:
        column_name: Column to evaluate.

    Returns:
        Spark column expression that is true for NULL/blank values.
    """
    column = F.col(column_name)
    return column.isNull() | (F.trim(column) == "")


def join_detail_messages(parts: list[Column]) -> Column:
    """Join conditional detail message columns into one string.

    Args:
        parts: Conditional detail expressions (null when not applicable).

    Returns:
        Comma-separated detail string.
    """
    return F.trim(F.concat_ws(", ", *parts))


def add_not_applicable_check_columns(
    dataframe: DataFrame,
    prefix: str,
) -> DataFrame:
    """Add passing placeholder columns for checks that do not apply.

    Args:
        dataframe: Input DataFrame.
        prefix: Check prefix (for example, ``completeness``).

    Returns:
        DataFrame with ``{prefix}_check_passed`` and ``{prefix}_check_details``.
    """
    return (
        dataframe.withColumn(f"{prefix}_check_passed", F.lit(True))
        .withColumn(f"{prefix}_check_details", F.lit(""))
    )


def compute_check_metric(
    dataframe: DataFrame,
    table_name: str,
    check_name: str,
    passed_column: str,
    threshold_pct: float | None,
) -> QualityCheckMetric:
    """Compute pass/fail statistics for a single quality check column.

    Args:
        dataframe: Silver DataFrame containing the check column.
        table_name: Table being evaluated.
        check_name: Human-readable check name.
        passed_column: Boolean column indicating pass/fail.
        threshold_pct: Minimum acceptable pass rate, or None for reporting only.

    Returns:
        A ``QualityCheckMetric`` instance.
    """
    total_rows = dataframe.count()
    passed_rows = dataframe.filter(F.col(passed_column)).count()
    failed_rows = total_rows - passed_rows
    pass_rate_pct = (passed_rows / total_rows * 100.0) if total_rows else 0.0
    meets_threshold = True if threshold_pct is None else pass_rate_pct >= threshold_pct

    return QualityCheckMetric(
        table_name=table_name,
        check_name=check_name,
        total_rows=total_rows,
        passed_rows=passed_rows,
        failed_rows=failed_rows,
        pass_rate_pct=pass_rate_pct,
        threshold_pct=threshold_pct,
        meets_threshold=meets_threshold,
    )


def print_quality_metrics_report(metrics: Iterable[QualityCheckMetric]) -> None:
    """Print a formatted quality metrics report.

    Args:
        metrics: Quality check metrics to display.
    """
    headers = (
        "Table",
        "Check",
        "Total",
        "Passed",
        "Failed",
        "% Passed",
        "Threshold",
        "Meets Threshold",
    )
    rows: list[tuple[str, ...]] = []
    for metric in metrics:
        threshold = "" if metric.threshold_pct is None else f"{metric.threshold_pct:.1f}%"
        rows.append(
            (
                metric.table_name,
                metric.check_name,
                f"{metric.total_rows:,}",
                f"{metric.passed_rows:,}",
                f"{metric.failed_rows:,}",
                f"{metric.pass_rate_pct:.2f}%",
                threshold,
                "YES" if metric.meets_threshold else "NO",
            )
        )

    widths = [max(len(headers[i]), *(len(row[i]) for row in rows)) for i in range(len(headers))]
    separator = "-+-".join("-" * width for width in widths)
    header_line = " | ".join(headers[index].ljust(widths[index]) for index in range(len(headers)))

    print("\nSilver Quality Metrics Report")
    print(separator)
    print(header_line)
    print(separator)
    for row in rows:
        print(" | ".join(row[index].ljust(widths[index]) for index in range(len(headers))))
    print(separator)

    below_threshold = [metric for metric in metrics if not metric.meets_threshold]
    if below_threshold:
        print("\nChecks below threshold:")
        for metric in below_threshold:
            print(
                f"  - {metric.table_name}.{metric.check_name}: "
                f"{metric.pass_rate_pct:.2f}% < {metric.threshold_pct:.1f}%"
            )


def add_quality_check_result(
    dataframe: DataFrame,
    applicable_passed_columns: list[str],
) -> DataFrame:
    """Add the combined ``quality_check_result`` column.

    Args:
        dataframe: Silver DataFrame with individual check pass columns.
        applicable_passed_columns: Boolean pass columns that must all be true.

    Returns:
        DataFrame with ``quality_check_result`` set to PASS or FAIL.
    """
    combined_passed = F.lit(True)
    for column_name in applicable_passed_columns:
        combined_passed = combined_passed & F.col(column_name)

    return dataframe.withColumn(
        "quality_check_result",
        F.when(combined_passed, F.lit(QUALITY_RESULT_PASS)).otherwise(F.lit(QUALITY_RESULT_FAIL)),
    )
