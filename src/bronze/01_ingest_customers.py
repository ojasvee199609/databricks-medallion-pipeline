"""Bronze ingestion for customer source data.

Reads customers.csv from the DBFS to_process folder as-is (including NULL
emails and duplicate IDs), writes bronze_customers, then archives the file
to the DBFS processed folder.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

BRONZE_DIR = Path(__file__).resolve().parent
if str(BRONZE_DIR) not in sys.path:
    sys.path.insert(0, str(BRONZE_DIR))

from bronze_config import (  # noqa: E402
    CUSTOMERS_SOURCE_SCHEMA,
    IngestResult,
    add_ingestion_metadata,
    move_source_to_processed,
    read_csv_with_schema,
    resolve_table_path,
    resolve_to_process_path,
    write_bronze_table,
)

SOURCE_FILE = "customers.csv"
SOURCE_NAME = "customers"
BRONZE_TABLE_NAME = "bronze_customers"
SOURCE_PATH = resolve_to_process_path(SOURCE_FILE)
BRONZE_TABLE_PATH = resolve_table_path(BRONZE_TABLE_NAME)


def ingest_customers(spark: SparkSession) -> IngestResult:
    """Ingest raw customer CSV data into the bronze_customers Delta table.

    Args:
        spark: Active Spark session.

    Returns:
        An ``IngestResult`` describing rows read/written and job status.
    """
    start_time = time.perf_counter()
    try:
        raw_df = read_csv_with_schema(spark, SOURCE_PATH, CUSTOMERS_SOURCE_SCHEMA)
        rows_read = raw_df.count()
        bronze_df = add_ingestion_metadata(raw_df, SOURCE_FILE)
        rows_written = write_bronze_table(
            spark,
            bronze_df,
            BRONZE_TABLE_NAME,
            BRONZE_TABLE_PATH,
        )

        print(f"[{SOURCE_NAME}] Rows read from CSV: {rows_read:,}")
        print(f"[{SOURCE_NAME}] Rows written to Delta: {rows_written:,}")

        archived_path = move_source_to_processed(spark, SOURCE_FILE)
        print(f"[{SOURCE_NAME}] Archived source file to: {archived_path}")

        return IngestResult(
            source_name=SOURCE_NAME,
            source_file=SOURCE_FILE,
            table_name=BRONZE_TABLE_NAME,
            rows_read=rows_read,
            rows_written=rows_written,
            status="success",
            duration_seconds=time.perf_counter() - start_time,
        )
    except Exception as exc:
        return IngestResult(
            source_name=SOURCE_NAME,
            source_file=SOURCE_FILE,
            table_name=BRONZE_TABLE_NAME,
            rows_read=0,
            rows_written=0,
            status="fail",
            duration_seconds=time.perf_counter() - start_time,
            error_message=str(exc),
        )


def main() -> int:
    """Run customer Bronze ingestion as a standalone script.

    Returns:
        Process exit code (0 for success, 1 for failure).
    """
    from bronze_config import get_spark_session

    spark = get_spark_session("bronze-ingest-customers")
    result = ingest_customers(spark)

    if result.status == "fail":
        print(f"[{SOURCE_NAME}] Ingestion failed: {result.error_message}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
