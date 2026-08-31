"""Assert Silver quality flag counts match known injected defect counts.

Purpose: programmatic validation that Silver checks detect all seven injection
categories from generate_sample_data.py (seed 42).

Inputs: Silver Delta tables (run Bronze + Silver pipeline first).
Outputs: exit code 0 if all assertions pass, 1 otherwise.

Usage:
    python3 test_data_quality.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src" / "data_generation"))
sys.path.insert(0, str(REPO_ROOT / "src" / "silver"))

from generate_sample_data import (  # noqa: E402
    CUSTOMERS_DUPLICATE_ID,
    CUSTOMERS_NULL_EMAIL,
    ORDERS_DUPLICATE_ORDER_ID,
    ORDERS_INVALID_CUSTOMER_ID,
    ORDERS_INVALID_PRODUCT_ID,
    ORDERS_NULL_CUSTOMER_ID,
    ORDERS_NULL_PRODUCT_ID,
)
from silver_config import (  # noqa: E402
    SILVER_CUSTOMERS_TABLE,
    SILVER_ORDERS_TABLE,
    get_spark_session,
    read_delta_table,
    resolve_silver_table_path,
)


def main() -> int:
    """Run injected-issue count assertions against Silver flag columns."""
    spark = get_spark_session("test-data-quality")
    customers = read_delta_table(
        spark, SILVER_CUSTOMERS_TABLE, resolve_silver_table_path(SILVER_CUSTOMERS_TABLE)
    )
    orders = read_delta_table(spark, SILVER_ORDERS_TABLE, resolve_silver_table_path(SILVER_ORDERS_TABLE))

    expectations = [
        ("customers: NULL email (completeness)", ~F.col("completeness_check_passed"), customers, CUSTOMERS_NULL_EMAIL),
        ("customers: duplicate customer_id (uniqueness)", ~F.col("uniqueness_check_passed"), customers, CUSTOMERS_DUPLICATE_ID * 2),
        ("orders: NULL customer_id/product_id (completeness)", ~F.col("completeness_check_passed"), orders, ORDERS_NULL_CUSTOMER_ID + ORDERS_NULL_PRODUCT_ID),
        ("orders: orphan FKs (referential integrity)", ~F.col("referential_integrity_passed"), orders, ORDERS_INVALID_CUSTOMER_ID + ORDERS_INVALID_PRODUCT_ID),
        ("orders: duplicate order_id (uniqueness)", ~F.col("uniqueness_check_passed"), orders, ORDERS_DUPLICATE_ORDER_ID * 2),
    ]

    failures: list[str] = []
    for label, condition, dataframe, expected in expectations:
        actual = dataframe.filter(condition).count()
        if actual != expected:
            failures.append(f"{label}: expected {expected:,}, got {actual:,}")

    if failures:
        print("test_data_quality.py FAILED:")
        for message in failures:
            print(f"  - {message}")
        return 1

    print("test_data_quality.py PASSED — all 7 injection categories match Silver flags.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
