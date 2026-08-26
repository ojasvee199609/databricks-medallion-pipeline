"""Bronze ingestion for order source data.

Reads the oldest pending orders*.csv from to_process (pattern-matched),
writes bronze_orders, then archives the file to processed with its original
timestamped filename preserved.
"""

from __future__ import annotations

import inspect
import os
import sys
import time
from pathlib import Path

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
    ORDERS_SOURCE_FILENAME_PATTERN,
    ORDERS_SOURCE_SCHEMA,
    IngestResult,
    add_ingestion_metadata,
    find_pending_source_file,
    move_source_to_processed,
    read_csv_with_schema,
    resolve_table_path,
    write_bronze_table,
)

SOURCE_NAME = "orders"
BRONZE_TABLE_NAME = "bronze_orders"
BRONZE_TABLE_PATH = resolve_table_path(BRONZE_TABLE_NAME)


def ingest_orders(spark: SparkSession) -> IngestResult:
    """Ingest raw order CSV data into the bronze_orders Delta table.

    Args:
        spark: Active Spark session.

    Returns:
        An ``IngestResult`` describing rows read/written and job status.
    """
    start_time = time.perf_counter()
    source_file = "unknown"
    try:
        source_path, source_file = find_pending_source_file(
            spark,
            ORDERS_SOURCE_FILENAME_PATTERN,
            SOURCE_NAME,
        )
        print(f"[{SOURCE_NAME}] Processing file: {source_file}")

        raw_df = read_csv_with_schema(spark, source_path, ORDERS_SOURCE_SCHEMA)
        rows_read = raw_df.count()
        bronze_df = add_ingestion_metadata(raw_df, source_file)
        rows_written = write_bronze_table(
            spark,
            bronze_df,
            BRONZE_TABLE_NAME,
            BRONZE_TABLE_PATH,
        )

        print(f"[{SOURCE_NAME}] Rows read from CSV: {rows_read:,}")
        print(f"[{SOURCE_NAME}] Rows written to Delta: {rows_written:,}")

        archived_path = move_source_to_processed(spark, source_file)
        print(f"[{SOURCE_NAME}] Archived source file to: {archived_path}")

        return IngestResult(
            source_name=SOURCE_NAME,
            source_file=source_file,
            table_name=BRONZE_TABLE_NAME,
            rows_read=rows_read,
            rows_written=rows_written,
            status="success",
            duration_seconds=time.perf_counter() - start_time,
        )
    except Exception as exc:
        return IngestResult(
            source_name=SOURCE_NAME,
            source_file=source_file,
            table_name=BRONZE_TABLE_NAME,
            rows_read=0,
            rows_written=0,
            status="fail",
            duration_seconds=time.perf_counter() - start_time,
            error_message=str(exc),
        )


def main() -> int:
    """Run order Bronze ingestion as a standalone script.

    Returns:
        Process exit code (0 for success, 1 for failure).
    """
    from bronze_config import get_spark_session

    spark = get_spark_session("bronze-ingest-orders")
    result = ingest_orders(spark)

    if result.status == "fail":
        print(f"[{SOURCE_NAME}] Ingestion failed: {result.error_message}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
