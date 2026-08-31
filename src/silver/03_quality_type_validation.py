"""Silver layer type validation quality check.

Validates that Bronze string columns can be cast to expected types and
allowed value sets without removing rows.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from silver_config import VALID_CUSTOMER_SEGMENTS, VALID_ORDER_STATUSES, join_detail_messages


def _is_present(column_name: str) -> Column:
    """Return whether a column contains a non-null, non-blank value."""
    column = F.col(column_name)
    return column.isNotNull() & (F.trim(column) != "")


def _valid_non_negative_int(column_name: str) -> Column:
    """Validate that a present value is a non-negative integer string."""
    present = _is_present(column_name)
    casted = F.col(column_name).cast("int")
    return ~present | (casted.isNotNull() & (casted >= 0))


def _valid_non_negative_decimal(column_name: str) -> Column:
    """Validate that a present value is a non-negative decimal string."""
    present = _is_present(column_name)
    casted = F.col(column_name).cast("decimal(18,2)")
    return ~present | (casted.isNotNull() & (casted >= F.lit(0)))


def _valid_date_or_null(column_name: str) -> Column:
    """Validate that a present value parses as a date."""
    present = _is_present(column_name)
    parsed = F.to_date(F.col(column_name))
    return ~present | parsed.isNotNull()


def _valid_enum_or_null(column_name: str, allowed_values: tuple[str, ...]) -> Column:
    """Validate that a present value is within an allowed set."""
    present = _is_present(column_name)
    allowed = F.col(column_name).isin(list(allowed_values))
    return ~present | allowed


def _build_type_validation(
    dataframe: DataFrame,
    rules: list[tuple[str, Column, str]],
) -> DataFrame:
    """Apply a list of type-validation rules and append flag columns.

    Args:
        dataframe: Input DataFrame.
        rules: Tuples of (field name, validation expression, failure message).

    Returns:
        DataFrame with ``type_validation_passed`` and ``type_validation_details``.
    """
    passed_expression = F.lit(True)
    detail_parts: list[Column] = []

    for _field_name, rule_passed, failure_message in rules:
        passed_expression = passed_expression & rule_passed
        detail_parts.append(F.when(~rule_passed, F.lit(failure_message)))

    details = join_detail_messages(detail_parts)
    failed = ~passed_expression

    return (
        dataframe.withColumn("type_validation_passed", passed_expression)
        .withColumn("type_validation_details", F.when(failed, details).otherwise(F.lit("")))
    )


def apply_type_validation_customers(customers_df: DataFrame) -> DataFrame:
    """Apply type validation to the customers table.

    Args:
        customers_df: Customers DataFrame.

    Returns:
        Customers DataFrame with type validation flag columns appended.
    """
    rules = [
        ("customer_id", _valid_non_negative_int("customer_id"), "customer_id is invalid"),
        (
            "lifetime_value",
            _valid_non_negative_decimal("lifetime_value"),
            "lifetime_value is invalid",
        ),
        ("signup_date", _valid_date_or_null("signup_date"), "signup_date is invalid"),
        (
            "customer_segment",
            _valid_enum_or_null("customer_segment", VALID_CUSTOMER_SEGMENTS),
            "customer_segment is invalid",
        ),
    ]
    return _build_type_validation(customers_df, rules)


def apply_type_validation_products(products_df: DataFrame) -> DataFrame:
    """Apply type validation to the products table.

    Args:
        products_df: Products DataFrame.

    Returns:
        Products DataFrame with type validation flag columns appended.
    """
    rules = [
        ("product_id", _valid_non_negative_int("product_id"), "product_id is invalid"),
        ("price", _valid_non_negative_decimal("price"), "price is invalid"),
        ("cost", _valid_non_negative_decimal("cost"), "cost is invalid"),
        (
            "stock_quantity",
            _valid_non_negative_int("stock_quantity"),
            "stock_quantity is invalid",
        ),
        (
            "reorder_level",
            _valid_non_negative_int("reorder_level"),
            "reorder_level is invalid",
        ),
    ]
    return _build_type_validation(products_df, rules)


def apply_type_validation_orders(orders_df: DataFrame) -> DataFrame:
    """Apply type validation to the orders table.

    Args:
        orders_df: Orders DataFrame.

    Returns:
        Orders DataFrame with type validation flag columns appended.
    """
    rules = [
        ("order_id", _valid_non_negative_int("order_id"), "order_id is invalid"),
        ("customer_id", _valid_non_negative_int("customer_id"), "customer_id is invalid"),
        ("product_id", _valid_non_negative_int("product_id"), "product_id is invalid"),
        ("quantity", _valid_non_negative_int("quantity"), "quantity is invalid"),
        ("unit_price", _valid_non_negative_decimal("unit_price"), "unit_price is invalid"),
        (
            "total_amount",
            _valid_non_negative_decimal("total_amount"),
            "total_amount is invalid",
        ),
        ("order_date", _valid_date_or_null("order_date"), "order_date is invalid"),
        (
            "order_status",
            _valid_enum_or_null("order_status", VALID_ORDER_STATUSES),
            "order_status is invalid",
        ),
        ("payment_date", _valid_date_or_null("payment_date"), "payment_date is invalid"),
    ]
    return _build_type_validation(orders_df, rules)
