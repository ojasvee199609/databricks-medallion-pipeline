"""Silver layer uniqueness quality check.

Flags duplicate primary-key values without removing rows. All rows sharing
a duplicated customer_id or order_id are flagged.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_uniqueness_check_customers(customers_df: DataFrame) -> DataFrame:
    """Apply uniqueness validation to customer_id values.

    Args:
        customers_df: Customers DataFrame (may already include other check columns).

    Returns:
        Customers DataFrame with uniqueness flag columns appended.
    """
    window = Window.partitionBy("customer_id")
    duplicate = F.count(F.lit(1)).over(window) > 1

    return (
        customers_df.withColumn("uniqueness_check_passed", ~duplicate)
        .withColumn(
            "uniqueness_check_details",
            F.when(
                duplicate,
                F.concat(F.lit("duplicate customer_id: "), F.col("customer_id")),
            ).otherwise(F.lit("")),
        )
    )


def apply_uniqueness_check_orders(orders_df: DataFrame) -> DataFrame:
    """Apply uniqueness validation to order_id values.

    Args:
        orders_df: Orders DataFrame (may already include other check columns).

    Returns:
        Orders DataFrame with uniqueness flag columns appended.
    """
    window = Window.partitionBy("order_id")
    duplicate = F.count(F.lit(1)).over(window) > 1

    return (
        orders_df.withColumn("uniqueness_check_passed", ~duplicate)
        .withColumn(
            "uniqueness_check_details",
            F.when(
                duplicate,
                F.concat(F.lit("duplicate order_id: "), F.col("order_id")),
            ).otherwise(F.lit("")),
        )
    )
