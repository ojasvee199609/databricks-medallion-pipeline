"""Generate synthetic e-commerce sample data for the medallion pipeline.

Creates customers.csv, products.csv, and orders.csv in the data/ folder
using Faker for realistic values. Injects deliberate data quality issues
so the Silver layer has realistic defects to detect and flag.
"""

from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from faker import Faker

RANDOM_SEED = 42

NUM_CUSTOMERS = 10_000
NUM_PRODUCTS = 500
NUM_ORDERS = 100_000

# Data quality issue counts (intentional defects for Silver layer testing)
CUSTOMERS_NULL_EMAIL = 50
CUSTOMERS_DUPLICATE_ID = 10

ORDERS_NULL_CUSTOMER_ID = 100
ORDERS_NULL_PRODUCT_ID = 200
ORDERS_INVALID_CUSTOMER_ID = 50
ORDERS_INVALID_PRODUCT_ID = 30
ORDERS_DUPLICATE_ORDER_ID = 20

INVALID_CUSTOMER_ID = 99_999
INVALID_PRODUCT_ID = 99_999

CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
CUSTOMER_SEGMENT_WEIGHTS = (0.15, 0.55, 0.30)

ORDER_STATUSES = ("Pending", "Completed", "Cancelled")
ORDER_STATUS_WEIGHTS = (0.10, 0.80, 0.10)

PRODUCT_CATEGORIES = (
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Beauty",
    "Toys",
    "Automotive",
)

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"

CUSTOMER_FIELDNAMES = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
]

PRODUCT_FIELDNAMES = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
]

ORDER_FIELDNAMES = [
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_status",
    "payment_date",
]


def round_decimal(value: Decimal, places: int = 2) -> Decimal:
    """Round a Decimal to the given number of decimal places.

    Args:
        value: The decimal value to round.
        places: Number of digits after the decimal point.

    Returns:
        The rounded Decimal value.
    """
    quantizer = Decimal("1").scaleb(-places)
    return value.quantize(quantizer, rounding=ROUND_HALF_UP)


def format_decimal(value: Decimal | None) -> str:
    """Format a Decimal for CSV output, or an empty string for NULL.

    Args:
        value: Decimal value to format, or None for a NULL field.

    Returns:
        A string with two decimal places, or an empty string if value is None.
    """
    if value is None:
        return ""
    return f"{round_decimal(value):.2f}"


def format_int(value: int | None) -> str:
    """Format an integer for CSV output, or an empty string for NULL.

    Args:
        value: Integer value to format, or None for a NULL field.

    Returns:
        A string representation of the integer, or an empty string if None.
    """
    if value is None:
        return ""
    return str(value)


def format_date(value: date | None) -> str:
    """Format a date for CSV output, or an empty string for NULL.

    Args:
        value: Date value to format, or None for a NULL field.

    Returns:
        An ISO-8601 date string, or an empty string if value is None.
    """
    if value is None:
        return ""
    return value.isoformat()


def generate_customers(fake: Faker) -> list[dict[str, Any]]:
    """Generate clean customer records with no data quality issues.

    Args:
        fake: Configured Faker instance for realistic field values.

    Returns:
        A list of customer record dictionaries.
    """
    customers: list[dict[str, Any]] = []

    for customer_id in range(1, NUM_CUSTOMERS + 1):
        segment = random.choices(CUSTOMER_SEGMENTS, weights=CUSTOMER_SEGMENT_WEIGHTS, k=1)[0]
        segment_multiplier = {"Premium": 3.5, "Standard": 1.5, "Basic": 0.8}[segment]
        lifetime_value = round_decimal(
            Decimal(str(random.uniform(50, 500) * segment_multiplier))
        )

        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": fake.name(),
                "email": fake.unique.email(),
                "country": fake.country(),
                "signup_date": fake.date_between(start_date="-5y", end_date="today"),
                "customer_segment": segment,
                "lifetime_value": lifetime_value,
            }
        )

    return customers


def generate_products(fake: Faker) -> list[dict[str, Any]]:
    """Generate clean product records with no data quality issues.

    Args:
        fake: Configured Faker instance for realistic field values.

    Returns:
        A list of product record dictionaries.
    """
    products: list[dict[str, Any]] = []

    for product_id in range(1, NUM_PRODUCTS + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        price = round_decimal(Decimal(str(random.uniform(5.0, 500.0))))
        cost_ratio = Decimal(str(random.uniform(0.40, 0.85)))
        cost = round_decimal(price * cost_ratio)
        stock_quantity = random.randint(0, 1000)
        reorder_level = random.randint(10, 100)

        products.append(
            {
                "product_id": product_id,
                "product_name": fake.catch_phrase(),
                "category": category,
                "price": price,
                "cost": cost,
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
            }
        )

    return products


def generate_orders(
    customers: list[dict[str, Any]],
    products: list[dict[str, Any]],
    fake: Faker,
) -> list[dict[str, Any]]:
    """Generate clean order records referencing valid customers and products.

    Args:
        customers: Clean customer records used for foreign-key references.
        products: Clean product records used for foreign-key references.
        fake: Configured Faker instance for realistic field values.

    Returns:
        A list of order record dictionaries with valid referential integrity.
    """
    product_price_lookup = {product["product_id"]: product["price"] for product in products}
    orders: list[dict[str, Any]] = []

    for order_id in range(1, NUM_ORDERS + 1):
        customer_id = random.randint(1, len(customers))
        product_id = random.randint(1, len(products))
        order_date = fake.date_between(start_date="-2y", end_date="today")
        quantity = random.randint(1, 10)
        unit_price = product_price_lookup[product_id]
        total_amount = round_decimal(Decimal(quantity) * unit_price)
        order_status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

        if order_status == "Completed":
            max_offset = max(0, (date.today() - order_date).days)
            offset_days = random.randint(0, max_offset) if max_offset else 0
            payment_date = order_date + timedelta(days=offset_days)
        else:
            payment_date = None

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "order_status": order_status,
                "payment_date": payment_date,
            }
        )

    return orders


def inject_customer_data_quality_issues(
    customers: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Inject deliberate data quality issues into customer records.

    Intentionally corrupts otherwise clean customer data so the Silver layer
    can detect NULL emails and duplicate primary keys.

    Args:
        customers: Clean customer records generated by generate_customers().

    Returns:
        A tuple of the modified customer list and a dict of injected issue counts.
    """
    issue_counts = {
        "null_email": 0,
        "duplicate_customer_id": 0,
    }

    # NULL email: overwrite email on randomly selected existing rows.
    null_email_indices = random.sample(range(len(customers)), CUSTOMERS_NULL_EMAIL)
    for index in null_email_indices:
        customers[index]["email"] = None
    issue_counts["null_email"] = len(null_email_indices)

    # Duplicate customer_id: append exact copies of randomly selected rows.
    duplicate_indices = random.sample(range(len(customers)), CUSTOMERS_DUPLICATE_ID)
    for index in duplicate_indices:
        customers.append(dict(customers[index]))
    issue_counts["duplicate_customer_id"] = len(duplicate_indices)

    return customers, issue_counts


def inject_order_data_quality_issues(
    orders: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Inject deliberate data quality issues into order records.

    Intentionally corrupts otherwise clean order data so the Silver layer
    can detect NULL foreign keys, invalid references, and duplicate keys.

    Args:
        orders: Clean order records generated by generate_orders().

    Returns:
        A tuple of the modified order list and a dict of injected issue counts.
    """
    issue_counts = {
        "null_customer_id": 0,
        "null_product_id": 0,
        "invalid_customer_id": 0,
        "invalid_product_id": 0,
        "duplicate_order_id": 0,
    }

    all_indices = list(range(len(orders)))
    random.shuffle(all_indices)

    null_customer_indices = set(all_indices[:ORDERS_NULL_CUSTOMER_ID])
    remaining_after_null_customer = all_indices[ORDERS_NULL_CUSTOMER_ID:]
    null_product_indices = set(remaining_after_null_customer[:ORDERS_NULL_PRODUCT_ID])
    remaining_after_null_product = remaining_after_null_customer[ORDERS_NULL_PRODUCT_ID:]
    invalid_customer_indices = set(
        remaining_after_null_product[:ORDERS_INVALID_CUSTOMER_ID]
    )
    invalid_product_indices = set(
        remaining_after_null_product[
            ORDERS_INVALID_CUSTOMER_ID : ORDERS_INVALID_CUSTOMER_ID
            + ORDERS_INVALID_PRODUCT_ID
        ]
    )

    for index in null_customer_indices:
        orders[index]["customer_id"] = None
    issue_counts["null_customer_id"] = len(null_customer_indices)

    for index in null_product_indices:
        orders[index]["product_id"] = None
    issue_counts["null_product_id"] = len(null_product_indices)

    for index in invalid_customer_indices:
        orders[index]["customer_id"] = INVALID_CUSTOMER_ID
    issue_counts["invalid_customer_id"] = len(invalid_customer_indices)

    for index in invalid_product_indices:
        orders[index]["product_id"] = INVALID_PRODUCT_ID
    issue_counts["invalid_product_id"] = len(invalid_product_indices)

    # Duplicate order_id: append exact copies of randomly selected rows.
    duplicate_indices = random.sample(range(len(orders)), ORDERS_DUPLICATE_ORDER_ID)
    for index in duplicate_indices:
        orders.append(dict(orders[index]))
    issue_counts["duplicate_order_id"] = len(duplicate_indices)

    return orders, issue_counts


def write_customers_csv(customers: list[dict[str, Any]], output_path: Path) -> None:
    """Write customer records to a CSV file.

    Args:
        customers: Customer records to write.
        output_path: Destination CSV file path.
    """
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CUSTOMER_FIELDNAMES)
        writer.writeheader()
        for customer in customers:
            writer.writerow(
                {
                    "customer_id": customer["customer_id"],
                    "customer_name": customer["customer_name"],
                    "email": customer["email"] or "",
                    "country": customer["country"],
                    "signup_date": format_date(customer["signup_date"]),
                    "customer_segment": customer["customer_segment"],
                    "lifetime_value": format_decimal(customer["lifetime_value"]),
                }
            )


def write_products_csv(products: list[dict[str, Any]], output_path: Path) -> None:
    """Write product records to a CSV file.

    Args:
        products: Product records to write.
        output_path: Destination CSV file path.
    """
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=PRODUCT_FIELDNAMES)
        writer.writeheader()
        for product in products:
            writer.writerow(
                {
                    "product_id": product["product_id"],
                    "product_name": product["product_name"],
                    "category": product["category"],
                    "price": format_decimal(product["price"]),
                    "cost": format_decimal(product["cost"]),
                    "stock_quantity": product["stock_quantity"],
                    "reorder_level": product["reorder_level"],
                }
            )


def write_orders_csv(orders: list[dict[str, Any]], output_path: Path) -> None:
    """Write order records to a CSV file.

    Args:
        orders: Order records to write.
        output_path: Destination CSV file path.
    """
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=ORDER_FIELDNAMES)
        writer.writeheader()
        for order in orders:
            writer.writerow(
                {
                    "order_id": order["order_id"],
                    "customer_id": format_int(order["customer_id"]),
                    "order_date": format_date(order["order_date"]),
                    "product_id": format_int(order["product_id"]),
                    "quantity": order["quantity"],
                    "unit_price": format_decimal(order["unit_price"]),
                    "total_amount": format_decimal(order["total_amount"]),
                    "order_status": order["order_status"],
                    "payment_date": format_date(order["payment_date"]),
                }
            )


def print_summary(
    customers: list[dict[str, Any]],
    products: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    customer_issues: dict[str, int],
    order_issues: dict[str, int],
    output_paths: dict[str, Path],
) -> None:
    """Print row counts and a breakdown of injected data quality issues.

    Args:
        customers: Final customer records written to CSV.
        products: Final product records written to CSV.
        orders: Final order records written to CSV.
        customer_issues: Counts of injected customer data quality issues.
        order_issues: Counts of injected order data quality issues.
        output_paths: Mapping of dataset names to written file paths.
    """
    print("=" * 60)
    print("Sample Data Generation Summary")
    print("=" * 60)
    print("\nOutput files:")
    for name, path in output_paths.items():
        print(f"  {name}: {path}")

    print("\nTotal rows generated:")
    print(f"  customers.csv: {len(customers):,}")
    print(f"  products.csv:  {len(products):,}")
    print(f"  orders.csv:    {len(orders):,}")

    print("\nInjected data quality issues — customers.csv:")
    print(f"  NULL email:              {customer_issues['null_email']:,}")
    print(f"  Duplicate customer_id:   {customer_issues['duplicate_customer_id']:,}")

    print("\nInjected data quality issues — orders.csv:")
    print(f"  NULL customer_id:        {order_issues['null_customer_id']:,}")
    print(f"  NULL product_id:         {order_issues['null_product_id']:,}")
    print(f"  Invalid customer_id:     {order_issues['invalid_customer_id']:,}")
    print(f"  Invalid product_id:      {order_issues['invalid_product_id']:,}")
    print(f"  Duplicate order_id:      {order_issues['duplicate_order_id']:,}")
    print("=" * 60)


def main() -> None:
    """Generate synthetic data files and write them to the data/ folder."""
    random.seed(RANDOM_SEED)
    fake = Faker()
    Faker.seed(RANDOM_SEED)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    products = generate_products(fake)
    customers = generate_customers(fake)
    orders = generate_orders(customers, products, fake)

    customers, customer_issues = inject_customer_data_quality_issues(customers)
    orders, order_issues = inject_order_data_quality_issues(orders)

    customers_path = DATA_DIR / "customers.csv"
    products_path = DATA_DIR / "products.csv"
    orders_path = DATA_DIR / "orders.csv"

    write_products_csv(products, products_path)
    write_customers_csv(customers, customers_path)
    write_orders_csv(orders, orders_path)

    print_summary(
        customers=customers,
        products=products,
        orders=orders,
        customer_issues=customer_issues,
        order_issues=order_issues,
        output_paths={
            "customers.csv": customers_path,
            "products.csv": products_path,
            "orders.csv": orders_path,
        },
    )


if __name__ == "__main__":
    main()
