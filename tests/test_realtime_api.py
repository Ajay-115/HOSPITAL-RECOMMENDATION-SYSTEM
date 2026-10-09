"""
MediPulse Phase 9 — Real-Time API Test Suite

Tests for GET /api/realtime/resources.

Test isolation:
  - Tests that mock Spark/HDFS never touch live HDFS or the production DB.
  - All Spark/PySpark module-level names in realtime_service are patched
    to avoid requiring a live SparkContext during unit tests.

All tests follow the same setUp/tearDown pattern as tests/test_api.py.
"""

import os
import sys
import json
import unittest
from unittest.mock import patch, MagicMock

# Resolve backend package root so imports work from the project root
BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "backend")
sys.path.insert(0, os.path.abspath(BACKEND_DIR))

from app import app
from database.database import init_db, DEFAULT_DB_PATH, set_active_db_path
import tempfile


# ---------------------------------------------------------------------------
# Helper — mock PySpark Row
# ---------------------------------------------------------------------------
class _MockRow:
    """Minimal stand-in for a PySpark Row that supports .asDict()."""

    def __init__(self, **kwargs):
        self._data = kwargs

    def asDict(self):
        return dict(self._data)


def _make_mock_rows(n: int = 3):
    from datetime import datetime
    rows = []
    for i in range(n):
        rows.append(_MockRow(
            hospital_id=f"H10{i + 1}",
            resource_type="ICU",
            avg_utilization=60.0 + i * 15,   # 60 → Normal, 75 → Warning, 90 → Critical
            avg_occupied=10.0 + i,
            avg_available=5.0 - i,
            max_emergency_load="High",
            event_count=10 + i,
            window_start=datetime(2024, 1, 1, 0, 0, 0),
            window_end=datetime(2024, 1, 1, 0, 5, 0),
        ))
    return rows


def _build_mock_spark(rows):
    """Return a mock SparkSession whose read.parquet().withColumn().filter().drop() chain
    produces the given rows from .collect()."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.collect.return_value = rows
    # Chain: withColumn → filter → drop → all return mock_df
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = mock_df
    mock_spark.read.parquet.return_value = mock_df
    return mock_spark, mock_df


# Patch targets — all names must be module-level in realtime_service
_SVC = "services.realtime_service"
_PYSPARK_PATCHES = {
    f"{_SVC}.Window": MagicMock(),
    f"{_SVC}.row_number": MagicMock(return_value=MagicMock()),
    f"{_SVC}.spark_col": MagicMock(return_value=MagicMock()),
}


def _with_pyspark_mocks(fn):
    """Decorator: patches Window, row_number, spark_col before calling fn."""
    def wrapper(self, mock_get_spark):
        with patch(f"{_SVC}.Window") as mock_window, \
             patch(f"{_SVC}.row_number") as mock_rn, \
             patch(f"{_SVC}.spark_col") as mock_col:
            mock_window.partitionBy.return_value.orderBy.return_value = MagicMock()
            mock_rn.return_value.over.return_value = MagicMock()
            mock_col.return_value = MagicMock()
            fn(self, mock_get_spark)
    wrapper.__name__ = fn.__name__
    return wrapper


# ---------------------------------------------------------------------------
# Test Suite
# ---------------------------------------------------------------------------
class TestRealtimeAPI(unittest.TestCase):
    """Phase 9 — Real-Time API endpoint tests."""

    @classmethod
    def setUpClass(cls):
        """Create an isolated temp SQLite DB; never touch production DB."""
        cls._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        cls._tmp.close()
        cls._test_db_path = cls._tmp.name

        init_db(cls._test_db_path, force_reseed=True)
        app.config["DATABASE_PATH"] = cls._test_db_path
        set_active_db_path(cls._test_db_path)
        app.config["TESTING"] = True

        cls.client = app.test_client()

        # Record production DB size to verify it is never modified
        cls._prod_db_size = (
            os.path.getsize(DEFAULT_DB_PATH)
            if os.path.exists(DEFAULT_DB_PATH)
            else 0
        )

    @classmethod
    def tearDownClass(cls):
        """Restore production DB path and clean up temp file."""
        set_active_db_path(DEFAULT_DB_PATH)
        app.config["DATABASE_PATH"] = DEFAULT_DB_PATH

        os.unlink(cls._test_db_path)

        # Assert production DB was NOT modified
        if os.path.exists(DEFAULT_DB_PATH):
            current_size = os.path.getsize(DEFAULT_DB_PATH)
            assert current_size == cls._prod_db_size, (
                f"Production DB was modified by tests! "
                f"Before: {cls._prod_db_size} bytes, After: {current_size} bytes"
            )

    def setUp(self):
        """Invalidate the realtime service cache before each test."""
        from services.realtime_service import invalidate_cache
        invalidate_cache()

    # ------------------------------------------------------------------
    # Shared helper for success-path tests
    # ------------------------------------------------------------------
    def _mock_success_response(self, n: int = 3):
        """
        Returns the response from GET /api/realtime/resources with all
        PySpark internals mocked and n synthetic rows.
        """
        mock_spark, mock_df = _build_mock_spark(_make_mock_rows(n))
        with patch(f"{_SVC}.get_spark_session", return_value=mock_spark), \
             patch(f"{_SVC}.Window") as mock_window, \
             patch(f"{_SVC}.row_number") as mock_rn, \
             patch(f"{_SVC}.spark_col") as mock_col:
            mock_window.partitionBy.return_value.orderBy.return_value = MagicMock()
            mock_rn.return_value = MagicMock()
            mock_col.return_value = MagicMock()
            resp = self.client.get("/api/realtime/resources")
        return resp

    # ------------------------------------------------------------------
    # Test 01 — Route exists and returns valid JSON
    # ------------------------------------------------------------------
    def test_01_route_exists_and_returns_json(self):
        """GET /api/realtime/resources must exist and return JSON."""
        resp = self._mock_success_response(1)
        self.assertIn(resp.status_code, [200, 503])
        data = json.loads(resp.data)
        self.assertIsInstance(data, dict)

    # ------------------------------------------------------------------
    # Test 02 — Success response schema validation
    # ------------------------------------------------------------------
    def test_02_success_response_schema(self):
        """A successful response must contain all required top-level keys."""
        resp = self._mock_success_response(2)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertTrue(data.get("success"))
        self.assertIn("count", data)
        self.assertIn("data", data)
        self.assertIn("summary", data)
        self.assertIsInstance(data["data"], list)

    # ------------------------------------------------------------------
    # Test 03 — Each record contains required fields
    # ------------------------------------------------------------------
    def test_03_record_required_fields(self):
        """Every record in 'data' must contain the 12 required fields."""
        resp = self._mock_success_response(3)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)

        required_fields = {
            "hospital_id", "hospital_name", "location", "resource_type",
            "avg_utilization", "avg_occupied", "avg_available",
            "max_emergency_load", "event_count",
            "window_start", "window_end", "status",
        }
        for record in data["data"]:
            for field in required_fields:
                self.assertIn(field, record, f"Missing field: {field}")

    # ------------------------------------------------------------------
    # Test 04 — count matches len(data)
    # ------------------------------------------------------------------
    def test_04_count_matches_data_length(self):
        """'count' in response must equal len(data)."""
        resp = self._mock_success_response(5)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertEqual(data["count"], len(data["data"]))

    # ------------------------------------------------------------------
    # Test 05 — Status classification correctness
    # ------------------------------------------------------------------
    def test_05_status_classification(self):
        """Status must be 'Normal' (<70%), 'Warning' (70–84%), 'Critical' (>=85%)."""
        resp = self._mock_success_response(3)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        statuses = [r["status"] for r in data["data"]]
        # rows: util=60 (Normal), 75 (Warning), 90 (Critical)
        self.assertIn("Normal", statuses)
        self.assertIn("Warning", statuses)
        self.assertIn("Critical", statuses)

    # ------------------------------------------------------------------
    # Test 06 — Summary fields are correct
    # ------------------------------------------------------------------
    def test_06_summary_fields(self):
        """summary must contain total_records, critical, warning, normal and sum correctly."""
        resp = self._mock_success_response(3)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        summary = data["summary"]
        for key in ("total_records", "critical", "warning", "normal"):
            self.assertIn(key, summary)
        self.assertEqual(
            summary["critical"] + summary["warning"] + summary["normal"],
            summary["total_records"],
        )

    # ------------------------------------------------------------------
    # Test 07 — HDFS failure returns 503
    # ------------------------------------------------------------------
    def test_07_hdfs_failure_returns_503(self):
        """When HDFS is unavailable the endpoint must return 503."""
        mock_spark = MagicMock()
        mock_spark.read.parquet.side_effect = Exception("HDFS connection refused")
        with patch(f"{_SVC}.get_spark_session", return_value=mock_spark):
            resp = self.client.get("/api/realtime/resources")
        self.assertEqual(resp.status_code, 503)
        data = json.loads(resp.data)
        self.assertFalse(data.get("success"))
        self.assertEqual(data.get("error"), "REALTIME_DATA_UNAVAILABLE")

    # ------------------------------------------------------------------
    # Test 08 — No stack traces exposed in error responses
    # ------------------------------------------------------------------
    def test_08_no_stack_trace_in_error_response(self):
        """Error response must NOT contain 'Traceback' or internal module paths."""
        mock_spark = MagicMock()
        mock_spark.read.parquet.side_effect = RuntimeError("Internal Spark error")
        with patch(f"{_SVC}.get_spark_session", return_value=mock_spark):
            resp = self.client.get("/api/realtime/resources")
        self.assertEqual(resp.status_code, 503)
        body = resp.data.decode("utf-8")
        self.assertNotIn("Traceback", body)
        self.assertNotIn("site-packages", body)

    # ------------------------------------------------------------------
    # Test 09 — Empty dataset returns 200 with count=0
    # ------------------------------------------------------------------
    def test_09_empty_dataset_returns_200(self):
        """When HDFS exists but has 0 records, return 200 with count=0."""
        mock_spark, mock_df = _build_mock_spark([])
        with patch(f"{_SVC}.get_spark_session", return_value=mock_spark), \
             patch(f"{_SVC}.Window") as mock_window, \
             patch(f"{_SVC}.row_number") as mock_rn, \
             patch(f"{_SVC}.spark_col") as mock_col:
            mock_window.partitionBy.return_value.orderBy.return_value = MagicMock()
            mock_rn.return_value = MagicMock()
            mock_col.return_value = MagicMock()
            resp = self.client.get("/api/realtime/resources")
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertTrue(data.get("success"))
        self.assertEqual(data["count"], 0)
        self.assertEqual(data["data"], [])

    # ------------------------------------------------------------------
    # Test 10 — Cache behavior (second call must not hit Spark again)
    # ------------------------------------------------------------------
    def test_10_cache_prevents_repeated_spark_calls(self):
        """The TTL cache must prevent Spark from being called on every request."""
        mock_spark, mock_df = _build_mock_spark(_make_mock_rows(2))
        with patch(f"{_SVC}.get_spark_session", return_value=mock_spark) as mock_gss, \
             patch(f"{_SVC}.Window") as mock_window, \
             patch(f"{_SVC}.row_number") as mock_rn, \
             patch(f"{_SVC}.spark_col") as mock_col:
            mock_window.partitionBy.return_value.orderBy.return_value = MagicMock()
            mock_rn.return_value = MagicMock()
            mock_col.return_value = MagicMock()

            # First request — should call Spark
            resp1 = self.client.get("/api/realtime/resources")
            self.assertEqual(resp1.status_code, 200)

            # Second request (within TTL) — cache should serve the result
            resp2 = self.client.get("/api/realtime/resources")
            self.assertEqual(resp2.status_code, 200)

            # parquet() called exactly once; 2nd request served from cache
            self.assertEqual(mock_spark.read.parquet.call_count, 1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    unittest.main(verbosity=2)
