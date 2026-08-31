"""Silver layer referential integrity quality check.

Flags orphan non-NULL foreign keys in orders without removing rows. NULL
foreign keys are excluded from this check because completeness handles them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from silver_config import is_null_or_blank, join_detail_messages


def apply_referential_integrity_check_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    """Validate order foreign keys against customer and product Bronze tables.

    Args:
        orders_df: Orders DataFrame.
        customers_df: Customers Bronze reference DataFrame.
        products_df: Products Bronze reference DataFrame.

    Returns:
        Orders DataFrame with referential integrity flag columns appended.
    """
    valid_customer_ids = customers_df.select(
        F.col("customer_id").alias("customer_id_ref")
    ).distinct()
    valid_product_ids = products_df.select(
        F.col("product_id").alias("product_id_ref")
    ).distinct()

    orders_with_refs = (
        orders_df.join(
            valid_customer_ids,
            orders_df["customer_id"] == valid_customer_ids["customer_id_ref"],
            how="left",
        )
        .join(
            valid_product_ids,
            orders_df["product_id"] == valid_product_ids["product_id_ref"],
            how="left",
        )
    )

    customer_present = ~is_null_or_blank("customer_id")
    product_present = ~is_null_or_blank("product_id")
    orphan_customer = customer_present & F.col("customer_id_ref").isNull()
    orphan_product = product_present & F.col("product_id_ref").isNull()
    failed = orphan_customer | orphan_product

    details = join_detail_messages(
        [
            F.when(
                orphan_customer,
                F.concat(F.lit("orphan customer_id: "), F.col("customer_id")),
            ),
            F.when(
                orphan_product,
                F.concat(F.lit("orphan product_id: "), F.col("product_id")),
            ),
        ]
    )

    return (
        orders_with_refs.withColumn("referential_integrity_passed", ~failed)
        .withColumn(
            "referential_integrity_details",
            F.when(failed, details).otherwise(F.lit("")),
        )
        .drop("customer_id_ref", "product_id_ref")
    )
