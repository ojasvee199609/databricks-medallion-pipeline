"""Silver layer completeness quality check.

Flags NULL or blank values in required fields without removing rows.
Customers: email. Orders: customer_id, product_id.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_config import is_null_or_blank, join_detail_messages


def apply_completeness_check_customers(customers_df: DataFrame) -> DataFrame:
    """Apply completeness validation to the customers Bronze table.

    Args:
        customers_df: Bronze customers DataFrame.

    Returns:
        Customers DataFrame with completeness flag columns appended.
    """
    email_null = is_null_or_blank("email")
    details = join_detail_messages([F.when(email_null, F.lit("email is NULL"))])

    return (
        customers_df.withColumn("completeness_check_passed", ~email_null)
        .withColumn("completeness_check_details", F.when(email_null, details).otherwise(F.lit("")))
    )


def apply_completeness_check_orders(orders_df: DataFrame) -> DataFrame:
    """Apply completeness validation to the orders Bronze table.

    Args:
        orders_df: Bronze orders DataFrame.

    Returns:
        Orders DataFrame with completeness flag columns appended.
    """
    customer_null = is_null_or_blank("customer_id")
    product_null = is_null_or_blank("product_id")
    failed = customer_null | product_null
    details = join_detail_messages(
        [
            F.when(customer_null, F.lit("customer_id is NULL")),
            F.when(product_null, F.lit("product_id is NULL")),
        ]
    )

    return (
        orders_df.withColumn("completeness_check_passed", ~failed)
        .withColumn("completeness_check_details", F.when(failed, details).otherwise(F.lit("")))
    )
