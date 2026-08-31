"""Shared configuration and helpers for Gold layer aggregations.

Reads Silver Delta tables, applies the Gold-boundary PASS-only filter for
trustworthy reporting, and writes Gold Delta tables.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

GOLD_DIR = Path(__file__).resolve().parent
REPO_ROOT = GOLD_DIR.parent.parent
SILVER_DIR = REPO_ROOT / "src" / "silver"

if str(SILVER_DIR) not in sys.path:
    sys.path.insert(0, str(SILVER_DIR))

from silver_config import (  # noqa: E402
    DEFAULT_DATABRICKS_VOLUME_BASE,
    QUALITY_RESULT_PASS,
    SILVER_CUSTOMERS_TABLE,
    SILVER_ORDERS_TABLE,
    SILVER_PRODUCTS_TABLE,
    USE_MANAGED_TABLES,
    _is_databricks_runtime,
    get_spark_session,
    read_delta_table,
    resolve_silver_table_path,
)

# Gold table names
GOLD_SALES_BY_PRODUCT_TABLE = os.environ.get(
    "GOLD_SALES_BY_PRODUCT_TABLE",
    "gold_sales_by_product",
)
GOLD_REVENUE_BY_CUSTOMER_TABLE = os.environ.get(
    "GOLD_REVENUE_BY_CUSTOMER_TABLE",
    "gold_revenue_by_customer",
)
GOLD_CUSTOMER_SEGMENTATION_TABLE = os.environ.get(
    "GOLD_CUSTOMER_SEGMENTATION_TABLE",
    "gold_customer_segmentation",
)

# Segmentation constants (documented in 04_customer_segmentation.sql as well)
HIGH_VALUE_TOP_PERCENT = float(os.environ.get("GOLD_HIGH_VALUE_TOP_PERCENT", "20.0"))
HIGH_VALUE_N_TILES = int(os.environ.get("GOLD_HIGH_VALUE_N_TILES", "5"))
REPEAT_MIN_ORDERS = int(os.environ.get("GOLD_REPEAT_MIN_ORDERS", "2"))

# Cross-check tolerance for revenue reconciliation (absolute currency units)
REVENUE_CROSS_CHECK_TOLERANCE = float(
    os.environ.get("GOLD_REVENUE_CROSS_CHECK_TOLERANCE", "0.01")
)


def _default_gold_base_path() -> str:
    """Return the default base path for Gold Delta table storage."""
    if _is_databricks_runtime():
        return f"{DEFAULT_DATABRICKS_VOLUME_BASE}/gold"
    return str(REPO_ROOT / "delta" / "gold")


GOLD_BASE_PATH = os.environ.get("GOLD_BASE_PATH", _default_gold_base_path())


@dataclass
class GoldBuildResult:
    """Summary of a Gold build execution.

    Attributes:
        table_row_counts: Row counts per Gold output table.
        sales_by_product_revenue: Sum of total_revenue from sales-by-product.
        revenue_by_customer_total: Sum of total_revenue from revenue-by-customer.
        revenue_cross_check_passed: Whether the two revenue totals reconcile.
    """

    table_row_counts: dict[str, int]
    sales_by_product_revenue: float
    revenue_by_customer_total: float
    revenue_cross_check_passed: bool


def resolve_gold_table_path(table_name: str) -> str:
    """Build the Delta storage path for a Gold table.

    Args:
        table_name: Gold Delta table name.

    Returns:
        Filesystem or DBFS path for the Gold table directory.
    """
    return f"{GOLD_BASE_PATH.rstrip('/')}/{table_name}"


def read_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Silver Delta table and validate it is non-empty.

    Args:
        spark: Active Spark session.
        table_name: Silver table name.

    Returns:
        Silver table DataFrame.

    Raises:
        ValueError: If the Silver table contains zero rows.
        RuntimeError: If the table cannot be read.
    """
    table_path = resolve_silver_table_path(table_name)
    dataframe = read_delta_table(spark, table_name, table_path)
    row_count = dataframe.count()
    if row_count == 0:
        raise ValueError(f"Silver table {table_name} is empty.")
    return dataframe


def validate_pass_orders_exist(orders_df: DataFrame) -> int:
    """Ensure at least one Silver order passed all quality checks.

    Args:
        orders_df: Silver orders DataFrame.

    Returns:
        Count of PASS orders available for Gold aggregations.

    Raises:
        ValueError: If no PASS orders exist after the Gold-boundary filter.
    """
    pass_count = orders_df.filter(orders_df.quality_check_result == QUALITY_RESULT_PASS).count()
    if pass_count == 0:
        raise ValueError(
            "No Silver orders with quality_check_result = PASS. "
            "Gold aggregations require at least one trustworthy order row."
        )
    return pass_count


def register_silver_temp_views(spark: SparkSession) -> int:
    """Load Silver tables and register them as temporary views for SQL.

    Args:
        spark: Active Spark session.

    Returns:
        Count of PASS orders available for Gold aggregations.

    Raises:
        ValueError: If a Silver table is empty or no PASS orders exist.
    """
    customers_df = read_silver_table(spark, SILVER_CUSTOMERS_TABLE)
    products_df = read_silver_table(spark, SILVER_PRODUCTS_TABLE)
    orders_df = read_silver_table(spark, SILVER_ORDERS_TABLE)

    customers_df.createOrReplaceTempView(SILVER_CUSTOMERS_TABLE)
    products_df.createOrReplaceTempView(SILVER_PRODUCTS_TABLE)
    orders_df.createOrReplaceTempView(SILVER_ORDERS_TABLE)

    return validate_pass_orders_exist(orders_df)


def load_sql_query(sql_path: Path) -> str:
    """Load a Gold SQL file from disk.

    Args:
        sql_path: Path to the SQL file.

    Returns:
        SQL query text.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"Gold SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


def render_sql_template(sql_text: str) -> str:
    """Substitute configurable table and constant tokens in Gold SQL.

    Args:
        sql_text: Raw SQL with template placeholders.

    Returns:
        SQL ready for execution against registered Silver temp views.
    """
    replacements = {
        "{{SILVER_CUSTOMERS_TABLE}}": SILVER_CUSTOMERS_TABLE,
        "{{SILVER_ORDERS_TABLE}}": SILVER_ORDERS_TABLE,
        "{{SILVER_PRODUCTS_TABLE}}": SILVER_PRODUCTS_TABLE,
        "{{QUALITY_RESULT_PASS}}": QUALITY_RESULT_PASS,
        "{{HIGH_VALUE_TOP_PERCENT}}": str(HIGH_VALUE_TOP_PERCENT),
        "{{HIGH_VALUE_N_TILES}}": str(HIGH_VALUE_N_TILES),
        "{{REPEAT_MIN_ORDERS}}": str(REPEAT_MIN_ORDERS),
    }
    rendered = sql_text
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    return rendered


def write_gold_table(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
) -> int:
    """Write a Gold Delta table and return the row count.

    Args:
        spark: Active Spark session.
        dataframe: Gold aggregation DataFrame.
        table_name: Target Gold table name.

    Returns:
        Number of rows written.

    Raises:
        ValueError: If the Gold result set is empty.
        RuntimeError: If the write fails.
    """
    row_count = dataframe.count()
    if row_count == 0:
        raise ValueError(f"Gold table {table_name} would be empty after build.")

    table_path = resolve_gold_table_path(table_name)
    writer = dataframe.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

    try:
        if USE_MANAGED_TABLES:
            writer.saveAsTable(table_name)
            return spark.read.table(table_name).count()
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        writer.save(table_path)
        return spark.read.format("delta").load(table_path).count()
    except Exception as exc:
        raise RuntimeError(f"Failed to write Gold table {table_name}: {exc}") from exc


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    """Read a Gold Delta table from a managed table or path.

    Args:
        spark: Active Spark session.
        table_name: Gold table name.

    Returns:
        Gold table DataFrame.
    """
    table_path = resolve_gold_table_path(table_name)
    return read_delta_table(spark, table_name, table_path)
