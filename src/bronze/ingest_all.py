"""Bronze layer orchestrator for all source ingestions.

Runs customers, products, and orders ingestions in sequence from the DBFS
to_process folder, archives successful files to processed, continues on
individual failures, and prints a final summary with row counts and status.
"""

from __future__ import annotations

import importlib.util
import inspect
import os
import sys
from pathlib import Path
from typing import Callable

from pyspark.sql import SparkSession

if os.environ.get("BRONZE_SRC_DIR"):
    BRONZE_DIR = Path(os.environ["BRONZE_SRC_DIR"]).resolve()
else:
    try:
        BRONZE_DIR = Path(__file__).resolve().parent
    except NameError:
        BRONZE_DIR = Path(inspect.currentframe().f_code.co_filename).resolve().parent

if str(BRONZE_DIR) not in sys.path:
    sys.path.insert(0, str(BRONZE_DIR))

from bronze_config import (  # noqa: E402
    DBFS_BASE_PATH,
    PROCESSED_FOLDER,
    TO_PROCESS_FOLDER,
    IngestResult,
    get_spark_session,
    read_bronze_table,
)

INGESTION_SEQUENCE: tuple[tuple[str, str], ...] = (
    ("01_ingest_customers.py", "ingest_customers"),
    ("03_ingest_products.py", "ingest_products"),
    ("02_ingest_orders.py", "ingest_orders"),
)


def _load_ingest_function(script_name: str, function_name: str) -> Callable[[SparkSession], IngestResult]:
    """Dynamically load an ingestion function from a numbered Bronze script.

    Args:
        script_name: Bronze script filename (for example, ``01_ingest_customers.py``).
        function_name: Ingestion function name exported by that script.

    Returns:
        Callable that accepts a Spark session and returns an ``IngestResult``.
    """
    script_path = BRONZE_DIR / script_name
    module_name = script_name.replace(".py", "")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load Bronze ingestion module: {script_name}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    ingest_fn = getattr(module, function_name, None)
    if ingest_fn is None:
        raise AttributeError(f"{function_name} not found in {script_name}")
    return ingest_fn


def print_summary_table(results: list[IngestResult]) -> None:
    """Print a formatted summary table for all ingestion jobs.

    Args:
        results: Ingestion outcomes for each source.
    """
    headers = ("Source", "Rows Read", "Rows Written", "Status", "Duration (s)")
    rows = [
        (
            result.source_name,
            f"{result.rows_read:,}",
            f"{result.rows_written:,}",
            result.status,
            f"{result.duration_seconds:.2f}",
        )
        for result in results
    ]

    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]

    header_line = " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    separator = "-+-".join("-" * widths[index] for index in range(len(headers)))

    print("\nBronze Ingestion Summary")
    print(separator)
    print(header_line)
    print(separator)
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))
    print(separator)

    failures = [result for result in results if result.status == "fail"]
    if failures:
        print("\nFailures:")
        for result in failures:
            print(f"  - {result.source_name}: {result.error_message}")


def run_all_ingestions(spark: SparkSession) -> list[IngestResult]:
    """Run all Bronze ingestions in the configured sequence.

    Args:
        spark: Active Spark session shared across ingestion jobs.

    Returns:
        A list of ``IngestResult`` objects, one per source.
    """
    results: list[IngestResult] = []

    for script_name, function_name in INGESTION_SEQUENCE:
        ingest_fn = _load_ingest_function(script_name, function_name)
        print(f"\nStarting Bronze ingestion: {function_name} ({script_name})")
        result = ingest_fn(spark)
        results.append(result)

        if result.status == "fail":
            print(
                f"WARNING: {result.source_name} ingestion failed; "
                "continuing with remaining sources."
            )

    return results


def verify_delta_tables_exist(spark: SparkSession, results: list[IngestResult]) -> None:
    """Confirm successful Bronze Delta tables are readable after ingestion.

    Args:
        spark: Active Spark session.
        results: Ingestion outcomes used to determine which tables to verify.

    Raises:
        RuntimeError: If a successful ingestion's Delta table cannot be read.
    """
    from bronze_config import USE_MANAGED_TABLES, resolve_table_path

    print("\nDelta table verification:")
    for result in results:
        if result.status != "success":
            continue

        table_path = resolve_table_path(result.table_name)
        try:
            table_df = read_bronze_table(spark, result.table_name, table_path)
            row_count = table_df.count()
            location = result.table_name if USE_MANAGED_TABLES else table_path
            print(f"  - {result.table_name}: exists at {location} ({row_count:,} rows)")
        except Exception as exc:
            raise RuntimeError(
                f"Delta table verification failed for {result.table_name}: {exc}"
            ) from exc


def show_orders_anomaly_sample(spark: SparkSession) -> None:
    """Display sample bronze_orders rows that still contain injected anomalies.

    Args:
        spark: Active Spark session.
    """
    from bronze_config import resolve_table_path

    table_path = resolve_table_path("bronze_orders")
    orders_df = read_bronze_table(spark, "bronze_orders", table_path)

    anomaly_df = orders_df.where(
        "customer_id IS NULL OR product_id IS NULL OR customer_id = '99999' OR product_id = '99999'"
    ).select(
        "order_id",
        "customer_id",
        "product_id",
        "order_status",
        "payment_date",
        "_source_file",
    )

    print("\nSample bronze_orders rows with injected anomalies (up to 5):")
    anomaly_df.show(5, truncate=False)


def main() -> int:
    """Orchestrate all Bronze ingestions and print a final status summary.

    Returns:
        Process exit code (0 if all sources succeed, 1 otherwise).
    """
    spark = get_spark_session("bronze-ingest-all")
    print(
        "DBFS staging configuration: "
        f"base={DBFS_BASE_PATH}, to_process={TO_PROCESS_FOLDER}, "
        f"processed={PROCESSED_FOLDER}"
    )
    results = run_all_ingestions(spark)
    print_summary_table(results)

    successes = [result for result in results if result.status == "success"]
    if successes:
        verify_delta_tables_exist(spark, results)
        show_orders_anomaly_sample(spark)

    if any(result.status == "fail" for result in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
