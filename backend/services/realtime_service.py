"""
MediPulse Phase 9 — Real-Time Analytics Service

Reads the latest Spark Structured Streaming Parquet output from HDFS,
applies window deduplication (latest window per hospital+resource),
enriches records with SQLite hospital metadata, and caches results
with a configurable TTL to avoid per-request Spark overhead.

IMPORTANT:
  - Does NOT start a new Spark session per HTTP request.
  - Does NOT collect the entire historical Parquet dataset.
  - Does NOT modify SQLite records.
  - Does NOT expose stack traces to the frontend.
"""

import os
import sys
import time
import logging
import threading

logger = logging.getLogger(__name__)

# Ensure the project root (parent of backend/) is on sys.path so that
# "spark.*" packages are importable when Flask is started from any CWD.
_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_BACKEND_DIR, ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# ---------------------------------------------------------------------------
# TTL cache (thread-safe)
# ---------------------------------------------------------------------------
_cache_lock = threading.Lock()
_cached_result: dict | None = None
_cache_timestamp: float = 0.0
CACHE_TTL: int = int(os.getenv("REALTIME_CACHE_TTL", "10"))

# ---------------------------------------------------------------------------
# HDFS source path (from Phase 8 streaming constants)
# ---------------------------------------------------------------------------
try:
    from spark.streaming.streaming_schema import STREAMING_HDFS_OUTPUT
    _HDFS_REALTIME_PATH = STREAMING_HDFS_OUTPUT
except ImportError:
    _HDFS_REALTIME_PATH = "hdfs://localhost:9000/medipulse/analytics/realtime_resource_metrics"

# ---------------------------------------------------------------------------
# Module-level imports so they are patchable by tests
# ---------------------------------------------------------------------------
try:
    from spark.spark_session import get_spark_session
except ImportError:  # spark package not on sys.path in some test envs
    get_spark_session = None  # type: ignore[assignment]

try:
    from database.database import get_all_hospitals
except ImportError:
    get_all_hospitals = None  # type: ignore[assignment]

# PySpark window helpers — imported lazily to allow tests without PySpark
try:
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, col as spark_col
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False


# ---------------------------------------------------------------------------
# Status classification helper
# ---------------------------------------------------------------------------
def _classify_status(avg_utilization: float | None) -> str:
    """Classify resource utilization into a human-readable status tier."""
    if avg_utilization is None:
        return "Unknown"
    if avg_utilization >= 85:
        return "Critical"
    if avg_utilization >= 70:
        return "Warning"
    return "Normal"


# ---------------------------------------------------------------------------
# Core service function
# ---------------------------------------------------------------------------
def get_realtime_resource_metrics() -> tuple[dict, int]:
    """
    Return the latest real-time resource metrics as a JSON-serializable dict.

    Uses a thread-safe TTL cache (default 10 s) to avoid running a Spark
    query on every HTTP request.

    Returns:
        (response_dict, http_status_code)

    On success  → ({"success": True, "count": N, "data": [...], ...}, 200)
    On HDFS error → ({"success": False, "error": "REALTIME_DATA_UNAVAILABLE"}, 503)
    On empty    → ({"success": True, "count": 0, "data": [], ...}, 200)
    """
    global _cached_result, _cache_timestamp

    # --- Cache hit ---
    now = time.monotonic()
    with _cache_lock:
        if _cached_result is not None and (now - _cache_timestamp) < CACHE_TTL:
            logger.debug("[Phase9] Cache hit — returning cached result.")
            return _cached_result, 200

    # --- Cache miss: fetch from HDFS via Spark ---
    logger.info("[Phase9] Cache miss — reading HDFS Parquet from %s", _HDFS_REALTIME_PATH)

    try:
        if get_spark_session is None:
            raise RuntimeError("Spark session factory is not available.")

        spark = get_spark_session("MediPulse-RealTime-API")

        # Read parquet — raises AnalysisException if path not found
        raw_df = spark.read.parquet(_HDFS_REALTIME_PATH)

        # Deduplicate: keep only the latest window per hospital+resource
        window_spec = (
            Window
            .partitionBy("hospital_id", "resource_type")
            .orderBy(spark_col("window_end").desc())
        )
        latest_df = (
            raw_df
            .withColumn("_rank", row_number().over(window_spec))
            .filter(spark_col("_rank") == 1)
            .drop("_rank")
        )

        rows = latest_df.collect()

    except Exception as exc:  # noqa: BLE001
        logger.error("[Phase9] HDFS read failed: %s", exc, exc_info=True)
        error_response = {
            "success": False,
            "error": "REALTIME_DATA_UNAVAILABLE",
            "message": "Real-time metrics are temporarily unavailable. Please try again later.",
        }
        return error_response, 503

    # --- SQLite metadata lookup ---
    hospital_lookup: dict[str, dict] = {}
    try:
        if get_all_hospitals is not None:
            for hosp in get_all_hospitals():
                hid = hosp.get("hospital_id") or hosp.get("id")
                if hid:
                    info = {
                        "hospital_name": hosp.get("hospital_name", "Unknown"),
                        "location": hosp.get("location", "Unknown"),
                    }
                    hospital_lookup[str(hid)] = info
                    # Support unpadded or alternate formatting (e.g. H101 -> H0101)
                    s_hid = str(hid).strip()
                    if s_hid.upper().startswith("H") and s_hid[1:].isdigit():
                        num = int(s_hid[1:])
                        hospital_lookup[f"H{num}"] = info
                        hospital_lookup[f"H{num:04d}"] = info
    except Exception as exc:  # noqa: BLE001
        # Non-fatal — enrich what we can; missing hospitals get defaults
        logger.warning("[Phase9] SQLite lookup failed (non-fatal): %s", exc)

    # --- Serialise Spark Row objects to plain Python dicts ---
    records = []
    for row in rows:
        row_dict = row.asDict()

        hospital_id = row_dict.get("hospital_id", "")
        meta = hospital_lookup.get(str(hospital_id), {})

        avg_util = row_dict.get("avg_utilization")
        try:
            avg_util_float = float(avg_util) if avg_util is not None else None
        except (TypeError, ValueError):
            avg_util_float = None

        record = {
            "hospital_id": hospital_id,
            "hospital_name": meta.get("hospital_name", "Unknown"),
            "location": meta.get("location", "Unknown"),
            "resource_type": row_dict.get("resource_type", ""),
            "avg_utilization": round(avg_util_float, 2) if avg_util_float is not None else None,
            "avg_occupied": (
                round(float(row_dict["avg_occupied"]), 2)
                if row_dict.get("avg_occupied") is not None else None
            ),
            "avg_available": (
                round(float(row_dict["avg_available"]), 2)
                if row_dict.get("avg_available") is not None else None
            ),
            "max_emergency_load": row_dict.get("max_emergency_load"),
            "event_count": row_dict.get("event_count"),
            "window_start": (
                row_dict["window_start"].isoformat()
                if row_dict.get("window_start") is not None else None
            ),
            "window_end": (
                row_dict["window_end"].isoformat()
                if row_dict.get("window_end") is not None else None
            ),
            "status": _classify_status(avg_util_float),
        }
        records.append(record)

    # --- Summary statistics ---
    total = len(records)
    critical_count = sum(1 for r in records if r["status"] == "Critical")
    warning_count = sum(1 for r in records if r["status"] == "Warning")
    normal_count = sum(1 for r in records if r["status"] == "Normal")

    response = {
        "success": True,
        "count": total,
        "data": records,
        "summary": {
            "total_records": total,
            "critical": critical_count,
            "warning": warning_count,
            "normal": normal_count,
        },
        "source": "hdfs_realtime_parquet",
        "cache_ttl_seconds": CACHE_TTL,
    }

    # --- Update cache ---
    with _cache_lock:
        _cached_result = response
        _cache_timestamp = time.monotonic()

    logger.info(
        "[Phase9] Returning %d real-time records (%d critical, %d warning, %d normal).",
        total, critical_count, warning_count, normal_count,
    )
    return response, 200


def invalidate_cache() -> None:
    """Force-invalidate the TTL cache (useful for testing)."""
    global _cached_result, _cache_timestamp
    with _cache_lock:
        _cached_result = None
        _cache_timestamp = 0.0
