"""
MediPulse Big Data Processing Engine
Data Ingestion Layer with Explicit Schemas (Phase 6 / HDFS Integration)

Supports Dual Ingestion Modes:
1. LOCAL: Ingests CSV files from backend/data/*.csv on the local filesystem.
2. HDFS: Ingests CSV files from hdfs://localhost:9000/medipulse/raw/production/*.csv.

The active mode is resolved via:
- Explicit loader parameter `source='local'|'hdfs'`
- Or the MEDIPULSE_DATA_SOURCE environment variable (defaults to 'local').

CRITICAL ARCHITECTURE NOTE (Viva / Evaluation):
- Python's `os.path.exists()` only operates on the local OS filesystem.
- When an HDFS URI (hdfs://...) is passed, `os.path.exists()` returns False.
- Therefore, `os.path.exists()` is ONLY invoked for local filesystem paths.
- For HDFS paths, path resolution, validation, and distributed block streaming
  are delegated to Apache Spark's Hadoop FileSystem client.
"""

import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "backend", "data")
HDFS_ROOT = "hdfs://localhost:9000/medipulse"
HDFS_BASE_DIR = f"{HDFS_ROOT}/raw/production"
HDFS_OUTPUT_BASE = HDFS_ROOT
SOURCE_MODE = os.getenv("MEDIPULSE_DATA_SOURCE", "local").lower()


def get_data_path(filename: str, source: str = None) -> str:
    """
    Resolves the dataset path based on the requested source.

    Precedence:
      1. Explicit `source` argument ('local' or 'hdfs')
      2. Environment variable MEDIPULSE_DATA_SOURCE ('local' or 'hdfs')
      3. Default fallback: 'local'

    Parameters:
      filename (str): CSV filename (e.g. 'hospitals.csv')
      source (str, optional): 'local' or 'hdfs'

    Returns:
      str: Absolute local path or HDFS URI.
    """
    mode = (source or os.getenv("MEDIPULSE_DATA_SOURCE", "local")).strip().lower()
    if mode == "hdfs":
        # Form HDFS URI with forward slashes (never use os.path.join on Windows for URIs)
        return f"{HDFS_BASE_DIR.rstrip('/')}/{filename}"
    return os.path.join(DATA_DIR, filename)


def get_hospital_schema():
    """Returns the explicit Spark StructType schema for hospitals dataset."""
    return StructType([
        StructField("hospital_id", StringType(), False),
        StructField("hospital_name", StringType(), False),
        StructField("location", StringType(), True),
        StructField("district", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("specialty", StringType(), True),
        StructField("total_beds", IntegerType(), True),
        StructField("available_beds", IntegerType(), True),
        StructField("icu_beds", IntegerType(), True),
        StructField("available_icu_beds", IntegerType(), True),
        StructField("queue_length", IntegerType(), True),
        StructField("emergency_load", StringType(), True),
        StructField("average_wait_time", IntegerType(), True),
        StructField("estimated_cost", IntegerType(), True),
        StructField("emergency_services", StringType(), True),
        StructField("rating", DoubleType(), True),
    ])


def get_surgery_schema():
    """Returns the explicit Spark StructType schema for surgeries dataset."""
    return StructType([
        StructField("surgery_id", StringType(), False),
        StructField("hospital_id", StringType(), False),
        StructField("surgery_name", StringType(), False),
        StructField("specialty", StringType(), True),
        StructField("estimated_cost", IntegerType(), True),
        StructField("waiting_days", IntegerType(), True),
        StructField("available_slots", IntegerType(), True),
        StructField("location", StringType(), True),
    ])


def get_blood_schema():
    """Returns the explicit Spark StructType schema for blood banks dataset."""
    return StructType([
        StructField("blood_bank_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("location", StringType(), True),
        StructField("blood_group", StringType(), True),
        StructField("available_units", IntegerType(), True),
        StructField("contact", StringType(), True),
        StructField("last_updated", StringType(), True),
    ])


def get_resource_schema():
    """Returns the explicit Spark StructType schema for hospital resources dataset."""
    return StructType([
        StructField("id", StringType(), False),
        StructField("hospital_id", StringType(), False),
        StructField("resource_name", StringType(), False),
        StructField("resource_type", StringType(), True),
        StructField("total_quantity", IntegerType(), True),
        StructField("available_quantity", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("last_updated", StringType(), True),
    ])


def _load_dataset(spark, file_path, source, filename, schema, dataset_name):
    """
    Internal loader implementing path resolution, conditional local existence check,
    and Spark schema enforcement.
    """
    # 1. Resolve path based on precedence: file_path > source > MEDIPULSE_DATA_SOURCE
    path = file_path if file_path is not None else get_data_path(filename, source=source)

    # 2. Local vs. HDFS existence check:
    # Only check os.path.exists on local files; HDFS URIs must be handled by Spark's Hadoop FS reader
    if not path.startswith("hdfs://"):
        if not os.path.exists(path):
            raise FileNotFoundError(f"{dataset_name} dataset not found at local path: {path}")

    # 3. Read dataset with explicit schema
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(path)
    )


def load_hospitals(spark, file_path=None, source=None):
    """
    Ingests hospitals CSV into a Spark DataFrame using explicit schema.

    Parameters:
      spark: Active SparkSession
      file_path (str, optional): Custom file path or URI (local or HDFS).
      source (str, optional): 'local' or 'hdfs'. Overrides MEDIPULSE_DATA_SOURCE.
    """
    return _load_dataset(
        spark=spark,
        file_path=file_path,
        source=source,
        filename="hospitals.csv",
        schema=get_hospital_schema(),
        dataset_name="Hospitals"
    )


def load_surgeries(spark, file_path=None, source=None):
    """
    Ingests surgeries CSV into a Spark DataFrame using explicit schema.

    Parameters:
      spark: Active SparkSession
      file_path (str, optional): Custom file path or URI (local or HDFS).
      source (str, optional): 'local' or 'hdfs'. Overrides MEDIPULSE_DATA_SOURCE.
    """
    return _load_dataset(
        spark=spark,
        file_path=file_path,
        source=source,
        filename="surgeries.csv",
        schema=get_surgery_schema(),
        dataset_name="Surgeries"
    )


def load_blood_banks(spark, file_path=None, source=None):
    """
    Ingests blood banks CSV into a Spark DataFrame using explicit schema.

    Parameters:
      spark: Active SparkSession
      file_path (str, optional): Custom file path or URI (local or HDFS).
      source (str, optional): 'local' or 'hdfs'. Overrides MEDIPULSE_DATA_SOURCE.
    """
    return _load_dataset(
        spark=spark,
        file_path=file_path,
        source=source,
        filename="blood_banks.csv",
        schema=get_blood_schema(),
        dataset_name="Blood banks"
    )


def load_resources(spark, file_path=None, source=None):
    """
    Ingests resources CSV into a Spark DataFrame using explicit schema.

    Parameters:
      spark: Active SparkSession
      file_path (str, optional): Custom file path or URI (local or HDFS).
      source (str, optional): 'local' or 'hdfs'. Overrides MEDIPULSE_DATA_SOURCE.
    """
    return _load_dataset(
        spark=spark,
        file_path=file_path,
        source=source,
        filename="resources.csv",
        schema=get_resource_schema(),
        dataset_name="Resources"
    )
