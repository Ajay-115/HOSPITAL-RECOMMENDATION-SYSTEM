"""
MediPulse Big Data Processing Engine
SparkSession Factory & Lifecycle Management (Phase 6)
"""

import os
import sys

# Directory anchors
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE_DIR = os.path.join(BASE_DIR, "spark", "warehouse")

# Auto-detect local JDK 17 for PySpark JVM initialization
JAVA_CANDIDATES = [
    os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "jdk-17.0.12+7"),
    r"C:\Users\ajays\AppData\Local\Programs\jdk-17.0.12+7",
    r"C:\Program Files\Java\jdk-17",
    r"C:\Program Files\Microsoft\jdk-17"
]

if "JAVA_HOME" not in os.environ:
    for candidate in JAVA_CANDIDATES:
        if candidate and os.path.exists(candidate):
            os.environ["JAVA_HOME"] = candidate
            break

if "JAVA_HOME" in os.environ:
    bin_dir = os.path.join(os.environ["JAVA_HOME"], "bin")
    if bin_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")

# Auto-detect local winutils and HADOOP_HOME for Windows FileOutputCommitter
HADOOP_DIR = os.path.join(BASE_DIR, "hadoop")
if os.path.exists(os.path.join(HADOOP_DIR, "bin", "winutils.exe")):
    os.environ["HADOOP_HOME"] = HADOOP_DIR
    os.environ["hadoop.home.dir"] = HADOOP_DIR
    h_bin = os.path.join(HADOOP_DIR, "bin")
    if h_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = h_bin + os.pathsep + os.environ.get("PATH", "")

_ACTIVE_SPARK = None


def get_spark_session(app_name="MediPulse-BigData-Analytics", master="local[*]"):
    """
    Initializes and returns a configured local Apache Spark Session.
    Configured for local development on Windows systems with multi-threading.
    """
    global _ACTIVE_SPARK
    if _ACTIVE_SPARK is not None:
        try:
            # Check if existing session is alive
            if not _ACTIVE_SPARK._jsc.sc().isStopped():
                return _ACTIVE_SPARK
        except Exception:
            _ACTIVE_SPARK = None

    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise ImportError(
            "PySpark is not installed or accessible in current Python environment. "
            "Please run 'pip install pyspark' to enable Big Data processing."
        ) from e

    os.makedirs(WAREHOUSE_DIR, exist_ok=True)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )

    _ACTIVE_SPARK = builder.getOrCreate()
    # Suppress verbose JVM log levels for clean output
    try:
        _ACTIVE_SPARK.sparkContext.setLogLevel("ERROR")
    except Exception:
        pass

    return _ACTIVE_SPARK


def stop_spark_session(spark=None):
    """
    Safely stops an active SparkSession and releases driver memory.
    """
    global _ACTIVE_SPARK
    target = spark or _ACTIVE_SPARK
    if target is not None:
        try:
            target.stop()
        except Exception:
            pass
    _ACTIVE_SPARK = None
