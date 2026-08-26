"""Shared configuration and helpers for Bronze layer ingestion.

Provides Unity Catalog volume / DBFS path resolution for CSV staging
(to_process/processed), CSV schemas, ingestion metadata helpers, Delta
write utilities, and row-count validation.
"""

from __future__ import annotations

import os
import posixpath
import re
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Pattern

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

if TYPE_CHECKING:
    from pyspark.sql.types import StructType as StructTypeType

BRONZE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BRONZE_DIR.parent.parent

# Unity Catalog volume defaults (override via environment variables if needed).
DEFAULT_VOLUME_CATALOG = os.environ.get("BRONZE_VOLUME_CATALOG", "workspace")
DEFAULT_VOLUME_SCHEMA = os.environ.get("BRONZE_VOLUME_SCHEMA", "default")
DEFAULT_VOLUME_NAME = os.environ.get("BRONZE_VOLUME_NAME", "medalion")

DEFAULT_DATABRICKS_VOLUME_BASE = (
    f"/Volumes/{DEFAULT_VOLUME_CATALOG}/{DEFAULT_VOLUME_SCHEMA}/{DEFAULT_VOLUME_NAME}"
)
DEFAULT_LOCAL_INGESTION_BASE = "dbfs:/FileStore/medallion/ingestion"
DEFAULT_LOCAL_BRONZE_BASE = str(REPO_ROOT / "delta" / "bronze")


def _is_databricks_runtime() -> bool:
    """Return whether the code is executing on a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _default_ingestion_base_path() -> str:
    """Return the default base path for CSV staging (to_process / processed).

    Returns:
        Unity Catalog volume path on Databricks, local DBFS simulation path otherwise.
    """
    if _is_databricks_runtime():
        return DEFAULT_DATABRICKS_VOLUME_BASE
    return DEFAULT_LOCAL_INGESTION_BASE


def _default_bronze_base_path() -> str:
    """Return the default base path for Bronze Delta table storage.

    Returns:
        Volume-backed bronze folder on Databricks, local delta folder otherwise.
    """
    if _is_databricks_runtime():
        return f"{DEFAULT_DATABRICKS_VOLUME_BASE}/bronze"
    return DEFAULT_LOCAL_BRONZE_BASE


# Ingestion staging paths — override via environment variables when needed.
DBFS_BASE_PATH = os.environ.get("BRONZE_DBFS_BASE_PATH", _default_ingestion_base_path())
TO_PROCESS_FOLDER = os.environ.get("BRONZE_TO_PROCESS_FOLDER", "to_process")
PROCESSED_FOLDER = os.environ.get("BRONZE_PROCESSED_FOLDER", "processed")

BRONZE_BASE_PATH = os.environ.get("BRONZE_BASE_PATH", _default_bronze_base_path())
USE_MANAGED_TABLES = os.environ.get(
    "BRONZE_USE_MANAGED_TABLES",
    "true" if _is_databricks_runtime() else "false",
).lower() == "true"

# Source filename patterns — timestamped batches from generate_sample_data.py.
# Examples: customers_20260826_143045.csv, orders_20260826_143045.csv
FILE_TIMESTAMP_TOKEN = r"\d{8}_\d{6}"
CUSTOMERS_SOURCE_FILENAME_PATTERN = re.compile(
    os.environ.get(
        "BRONZE_CUSTOMERS_FILE_PATTERN",
        rf"^customers(?:_{FILE_TIMESTAMP_TOKEN})?\.csv$",
    )
)
PRODUCTS_SOURCE_FILENAME_PATTERN = re.compile(
    os.environ.get(
        "BRONZE_PRODUCTS_FILE_PATTERN",
        rf"^products(?:_{FILE_TIMESTAMP_TOKEN})?\.csv$",
    )
)
ORDERS_SOURCE_FILENAME_PATTERN = re.compile(
    os.environ.get(
        "BRONZE_ORDERS_FILE_PATTERN",
        rf"^orders(?:_{FILE_TIMESTAMP_TOKEN})?\.csv$",
    )
)

CUSTOMERS_SOURCE_SCHEMA: StructTypeType = StructType(
    [
        StructField("customer_id", StringType(), nullable=True),
        StructField("customer_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("signup_date", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("lifetime_value", StringType(), nullable=True),
    ]
)

PRODUCTS_SOURCE_SCHEMA: StructTypeType = StructType(
    [
        StructField("product_id", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("price", StringType(), nullable=True),
        StructField("cost", StringType(), nullable=True),
        StructField("stock_quantity", StringType(), nullable=True),
        StructField("reorder_level", StringType(), nullable=True),
    ]
)

ORDERS_SOURCE_SCHEMA: StructTypeType = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("order_date", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", StringType(), nullable=True),
        StructField("unit_price", StringType(), nullable=True),
        StructField("total_amount", StringType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("payment_date", StringType(), nullable=True),
    ]
)


@dataclass
class IngestResult:
    """Outcome of a single Bronze ingestion job.

    Attributes:
        source_name: Human-readable source label for reporting.
        source_file: Source CSV filename.
        table_name: Target Delta table name.
        rows_read: Number of rows read from the CSV.
        rows_written: Number of rows written to the Delta table.
        status: ``success`` or ``fail``.
        duration_seconds: Wall-clock runtime in seconds.
        error_message: Error details when status is ``fail``.
    """

    source_name: str
    source_file: str
    table_name: str
    rows_read: int
    rows_written: int
    status: str
    duration_seconds: float
    error_message: str = ""


def get_spark_session(app_name: str = "bronze-ingestion") -> SparkSession:
    """Create or return an active Spark session configured for Delta Lake.

    Args:
        app_name: Spark application name.

    Returns:
        An active ``SparkSession`` with Delta Lake extensions enabled.
    """
    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder.appName(app_name)

    # Local runs use a dedicated warehouse directory under the repo.
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        warehouse_dir = str(REPO_ROOT / "spark-warehouse")
        builder = (
            builder.config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        )
        builder = configure_spark_with_delta_pip(builder)

    builder = builder.config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )

    return builder.getOrCreate()


def is_dbfs_path(path: str) -> bool:
    """Return whether a path uses DBFS or Unity Catalog volume storage.

    Args:
        path: Filesystem path or URI.

    Returns:
        True when the path should be accessed via ``dbutils`` on Databricks.
    """
    return (
        path.startswith("dbfs:")
        or path.startswith("/dbfs/")
        or path.startswith("/Volumes/")
    )


def is_databricks_runtime() -> bool:
    """Return whether the code is executing on a Databricks cluster.

    Returns:
        True when ``DATABRICKS_RUNTIME_VERSION`` is present in the environment.
    """
    return _is_databricks_runtime()


def to_local_filesystem_path(path: str) -> Path:
    """Map a DBFS or Volume URI to a local filesystem path for offline development.

    Args:
        path: DBFS URI, Unity Catalog volume path, or filesystem path.

    Returns:
        A local ``Path`` under the repository for simulated Databricks storage.
    """
    if path.startswith("dbfs:"):
        relative_path = path[len("dbfs:") :].lstrip("/")
        return REPO_ROOT / "dbfs" / relative_path
    if path.startswith("/dbfs/"):
        return REPO_ROOT / "dbfs" / path[len("/dbfs/") :]
    if path.startswith("/Volumes/"):
        return REPO_ROOT / "dbfs" / "volumes" / path[len("/Volumes/") :]
    return Path(path)


def get_dbutils(spark: SparkSession) -> Any:
    """Return a ``dbutils`` handle when running on Databricks.

    Args:
        spark: Active Spark session.

    Returns:
        A ``DBUtils`` instance for filesystem operations.

    Raises:
        RuntimeError: If called outside a Databricks runtime.
    """
    if not is_databricks_runtime():
        raise RuntimeError("dbutils is only available on Databricks runtimes.")

    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)
    except ImportError as exc:
        raise RuntimeError("Unable to initialize dbutils on Databricks.") from exc


def resolve_to_process_folder() -> str:
    """Build the DBFS path to the to_process staging folder.

    Returns:
        Full DBFS path to the pending-ingest folder.
    """
    return posixpath.join(DBFS_BASE_PATH.rstrip("/"), TO_PROCESS_FOLDER)


def resolve_to_process_path(source_file: str) -> str:
    """Build the DBFS path to a CSV file in the to_process staging folder.

    Args:
        source_file: CSV filename (for example, ``customers.csv``).

    Returns:
        Full DBFS path to the pending source file.
    """
    return posixpath.join(
        DBFS_BASE_PATH.rstrip("/"),
        TO_PROCESS_FOLDER,
        source_file,
    )


def list_to_process_filenames(spark: SparkSession) -> list[str]:
    """List CSV filenames currently waiting in the to_process folder.

    Args:
        spark: Active Spark session.

    Returns:
        Sorted list of CSV basenames in the to_process folder.
    """
    folder = resolve_to_process_folder()

    if is_databricks_runtime() and is_dbfs_path(folder):
        dbutils = get_dbutils(spark)
        try:
            entries = dbutils.fs.ls(folder)
        except Exception:
            return []

        filenames = [
            entry.name
            for entry in entries
            if not entry.isDir() and entry.name.lower().endswith(".csv")
        ]
        return sorted(filenames)

    local_folder = to_local_filesystem_path(folder)
    if not local_folder.is_dir():
        return []

    return sorted(
        path.name for path in local_folder.iterdir() if path.is_file() and path.suffix.lower() == ".csv"
    )


def find_pending_source_file(
    spark: SparkSession,
    filename_pattern: Pattern[str],
    source_label: str,
) -> tuple[str, str]:
    """Find the oldest pending CSV in to_process that matches a source pattern.

    Args:
        spark: Active Spark session.
        filename_pattern: Regex matched against CSV basenames.
        source_label: Human-readable source label for error messages.

    Returns:
        Tuple of (full source path, source filename basename).

    Raises:
        FileNotFoundError: If no matching CSV exists in to_process.
    """
    matching_files = [
        filename
        for filename in list_to_process_filenames(spark)
        if filename_pattern.match(filename)
    ]

    if not matching_files:
        folder = resolve_to_process_folder()
        raise FileNotFoundError(
            f"No pending {source_label} file matching pattern "
            f"'{filename_pattern.pattern}' in {folder}."
        )

    # Oldest batch first — timestamp suffix sorts lexicographically.
    selected_file = matching_files[0]
    if len(matching_files) > 1:
        print(
            f"[{source_label}] Multiple pending files matched; processing oldest: "
            f"{selected_file} (queue size: {len(matching_files)})"
        )

    return resolve_to_process_path(selected_file), selected_file


def resolve_processed_path(source_file: str) -> str:
    """Build the DBFS path to a CSV file in the processed archive folder.

    Args:
        source_file: CSV filename (for example, ``customers_20260826_143045.csv``).

    Returns:
        Full DBFS path to the archived source file.
    """
    return posixpath.join(
        DBFS_BASE_PATH.rstrip("/"),
        PROCESSED_FOLDER,
        source_file,
    )


def resolve_processed_folder() -> str:
    """Build the DBFS path to the processed archive folder.

    Returns:
        Full DBFS path to the processed folder.
    """
    return posixpath.join(DBFS_BASE_PATH.rstrip("/"), PROCESSED_FOLDER)


def to_spark_readable_path(path: str) -> str:
    """Convert a DBFS URI to a path Spark can read in the current runtime.

    Args:
        path: DBFS URI or filesystem path.

    Returns:
        A path string suitable for ``spark.read.csv``.
    """
    if is_databricks_runtime() or not is_dbfs_path(path):
        return path

    local_path = to_local_filesystem_path(path).resolve()
    return local_path.as_uri()


def resolve_source_path(source_file: str) -> str:
    """Build the DBFS to_process path for a source CSV file.

    Args:
        source_file: CSV filename (for example, ``customers.csv``).

    Returns:
        Full DBFS path to the pending source file.
    """
    return resolve_to_process_path(source_file)


def resolve_table_path(table_name: str) -> str:
    """Build the Delta storage path for a Bronze table.

    Args:
        table_name: Bronze Delta table name.

    Returns:
        Filesystem or DBFS path for the Delta table directory.
    """
    return f"{BRONZE_BASE_PATH.rstrip('/')}/{table_name}"


def source_file_exists(spark: SparkSession, source_path: str) -> bool:
    """Check whether a source CSV exists at the given path.

    Args:
        spark: Active Spark session.
        source_path: Full DBFS or local path to the CSV file.

    Returns:
        True when the source file is present.
    """
    if is_databricks_runtime() and is_dbfs_path(source_path):
        dbutils = get_dbutils(spark)
        try:
            dbutils.fs.ls(source_path)
            return True
        except Exception:
            return False

    return to_local_filesystem_path(source_path).is_file()


def validate_source_path(spark: SparkSession, source_path: str) -> None:
    """Ensure the source CSV exists before attempting ingestion.

    Args:
        spark: Active Spark session.
        source_path: Full path to the source CSV.

    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    if not source_file_exists(spark, source_path):
        raise FileNotFoundError(f"Source file not found in to_process folder: {source_path}")


def move_source_to_processed(spark: SparkSession, source_file: str) -> str:
    """Move a successfully ingested CSV from to_process to processed on DBFS.

    Args:
        spark: Active Spark session.
        source_file: CSV filename that was ingested.

    Returns:
        The DBFS path where the file was moved.

    Raises:
        FileNotFoundError: If the source file is missing from to_process.
        RuntimeError: If the move operation fails.
    """
    source_path = resolve_to_process_path(source_file)
    destination_path = resolve_processed_path(source_file)
    processed_folder = resolve_processed_folder()

    if is_databricks_runtime():
        dbutils = get_dbutils(spark)
        if not source_file_exists(spark, source_path):
            raise FileNotFoundError(
                f"Cannot archive source file; not found in to_process: {source_path}"
            )
        try:
            dbutils.fs.mkdirs(processed_folder)
            dbutils.fs.mv(source_path, destination_path)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to move {source_path} to {destination_path}: {exc}"
            ) from exc
        return destination_path

    local_source = to_local_filesystem_path(source_path)
    local_destination = to_local_filesystem_path(destination_path)
    if not local_source.is_file():
        raise FileNotFoundError(
            f"Cannot archive source file; not found in to_process: {source_path}"
        )

    local_destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(local_source), str(local_destination))
    except OSError as exc:
        raise RuntimeError(
            f"Failed to move {source_path} to {destination_path}: {exc}"
        ) from exc

    return destination_path


def validate_dataframe_columns(df: DataFrame, expected_schema: StructTypeType) -> None:
    """Verify that a DataFrame matches the expected Bronze source schema.

    Args:
        df: DataFrame read from the source CSV.
        expected_schema: Expected column names and order.

    Raises:
        ValueError: If column names or order do not match the expected schema.
    """
    expected_columns = [field.name for field in expected_schema.fields]
    actual_columns = df.columns
    if actual_columns != expected_columns:
        raise ValueError(
            "Schema mismatch between CSV and expected Bronze schema. "
            f"Expected columns {expected_columns}, got {actual_columns}."
        )


def read_csv_with_schema(
    spark: SparkSession,
    source_path: str,
    schema: StructTypeType,
) -> DataFrame:
    """Read a CSV file using an explicit schema and clear error handling.

    Args:
        spark: Active Spark session.
        source_path: Full path to the CSV file.
        schema: Explicit schema for the source file.

    Returns:
        A DataFrame containing the raw CSV rows.

    Raises:
        FileNotFoundError: If the source file is missing.
        ValueError: If the file is empty or columns do not match the schema.
    """
    validate_source_path(spark, source_path)
    spark_source_path = to_spark_readable_path(source_path)

    try:
        df = (
            spark.read.option("header", True)
            .option("nullValue", "")
            .schema(schema)
            .csv(spark_source_path)
        )
    except Exception as exc:
        message = str(exc)
        if "Path does not exist" in message or "No such file or directory" in message:
            raise FileNotFoundError(f"Source file not found: {source_path}") from exc
        raise RuntimeError(f"Failed to read source file {source_path}: {message}") from exc

    validate_dataframe_columns(df, schema)

    rows_read = df.count()
    if rows_read == 0:
        raise ValueError(f"Source file is empty (0 data rows): {source_path}")

    return df


def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Append Bronze ingestion metadata columns without altering source fields.

    Args:
        df: Raw source DataFrame.
        source_file: Source CSV filename written to ``_source_file``.

    Returns:
        The input DataFrame with ``_ingested_at`` and ``_source_file`` appended.
    """
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "_source_file",
        F.lit(source_file),
    )


def read_bronze_table(
    spark: SparkSession,
    table_name: str,
    table_path: str,
) -> DataFrame:
    """Read a Bronze Delta table from a managed table or path.

    Args:
        spark: Active Spark session.
        table_name: Managed Delta table name.
        table_path: Delta path used when managed tables are disabled.

    Returns:
        The Bronze Delta table as a DataFrame.
    """
    if USE_MANAGED_TABLES:
        return spark.read.table(table_name)
    return spark.read.format("delta").load(table_path)


def write_bronze_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    table_path: str,
) -> int:
    """Write a DataFrame to a Bronze Delta table and validate row counts.

    Args:
        spark: Active Spark session.
        df: DataFrame including source columns and ingestion metadata.
        table_name: Target managed Delta table name.
        table_path: Target Delta path when managed tables are disabled.

    Returns:
        The number of rows written to the Delta table.

    Raises:
        RuntimeError: If rows read and rows written do not match exactly.
    """
    rows_read = df.count()
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

    if USE_MANAGED_TABLES:
        writer.saveAsTable(table_name)
        rows_written = spark.read.table(table_name).count()
    else:
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        writer.save(table_path)
        rows_written = spark.read.format("delta").load(table_path).count()

    if rows_read != rows_written:
        raise RuntimeError(
            f"Row count mismatch for {table_name}: read {rows_read:,}, "
            f"written {rows_written:,}. Bronze ingestion aborted."
        )

    return rows_written


def ensure_bronze_dir_on_path() -> None:
    """Add the Bronze package directory to ``sys.path`` for script imports."""
    bronze_path = str(BRONZE_DIR)
    if bronze_path not in sys.path:
        sys.path.insert(0, bronze_path)
