"""
MediPulse Big Data Processing Engine
Automated Test Suite: Phase 8 Spark Structured Streaming Simulation

Tests:
1. Streaming module imports
2. Explicit StructType schema definitions
3. Required schema columns and types
4. HDFS target paths (Input, Checkpoint, Output)
5. Synthetic event generation (valid values, no PII)
6. Event validation filter logic
7. Utilization percentage calculation
8. Live Structured Streaming query lifecycle (start, micro-batch processing, stop)
"""

import os
import sys
import unittest
import tempfile
import shutil
import time
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.streaming.streaming_schema import (
    get_streaming_schema,
    STREAMING_HDFS_INPUT,
    STREAMING_HDFS_CHECKPOINT,
    STREAMING_HDFS_OUTPUT,
    RESOURCE_TYPES,
    HOSPITAL_IDS
)
from spark.streaming.event_generator import (
    generate_synthetic_event,
    generate_event_batch,
    write_events_to_json_file
)
from spark.streaming.streaming_pipeline import (
    build_resource_streaming_pipeline,
    start_streaming_query
)


class TestSparkStructuredStreaming(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # 1. Setup isolated temporary directories for streaming tests
        cls.test_dir = tempfile.mkdtemp(prefix="spark_streaming_test_")
        cls.input_dir = os.path.join(cls.test_dir, "input")
        cls.checkpoint_dir = os.path.join(cls.test_dir, "checkpoint")
        cls.output_dir = os.path.join(cls.test_dir, "output")
        os.makedirs(cls.input_dir, exist_ok=True)
        os.makedirs(cls.checkpoint_dir, exist_ok=True)
        os.makedirs(cls.output_dir, exist_ok=True)

        # 2. Re-use or initialize SparkSession
        cls.spark = get_spark_session(
            app_name="MediPulse-StreamingTest",
            master="local[2]"
        )

    @classmethod
    def tearDownClass(cls):
        # 1. Stop SparkSession
        stop_spark_session(cls.spark)

        # 2. Cleanup test directory
        if os.path.exists(cls.test_dir):
            try:
                shutil.rmtree(cls.test_dir)
            except OSError:
                pass

    def test_01_module_imports(self):
        """1. Verify streaming module and components import successfully"""
        import spark.streaming as ss
        self.assertTrue(hasattr(ss, "get_streaming_schema"))
        self.assertTrue(hasattr(ss, "build_resource_streaming_pipeline"))
        self.assertTrue(hasattr(ss, "generate_synthetic_event"))

    def test_02_streaming_schema_definition(self):
        """2. Verify streaming schema exists with correct types"""
        schema = get_streaming_schema()
        self.assertIsNotNone(schema)
        self.assertEqual(len(schema.fields), 8)

    def test_03_required_columns_exist(self):
        """3. Verify all 8 required event columns exist in schema"""
        schema = get_streaming_schema()
        field_names = [f.name for f in schema.fields]

        expected_fields = [
            "event_id",
            "timestamp",
            "hospital_id",
            "resource_type",
            "total_capacity",
            "occupied",
            "available",
            "emergency_load"
        ]
        for field in expected_fields:
            self.assertIn(field, field_names, f"Field '{field}' must exist in streaming schema.")

    def test_04_hdfs_paths_configuration(self):
        """4. Verify HDFS input, output, and checkpoint paths match specifications"""
        self.assertTrue(STREAMING_HDFS_INPUT.startswith("hdfs://localhost:9000/medipulse/raw/streaming"))
        self.assertTrue(STREAMING_HDFS_CHECKPOINT.startswith("hdfs://localhost:9000/medipulse/checkpoints/resource_streaming"))
        self.assertTrue(STREAMING_HDFS_OUTPUT.startswith("hdfs://localhost:9000/medipulse/analytics/realtime_resource_metrics"))

    def test_05_synthetic_event_generation(self):
        """5. Verify synthetic event generation produces realistic non-PII values"""
        event = generate_synthetic_event()
        self.assertIsNotNone(event["event_id"])
        self.assertIn(event["hospital_id"], HOSPITAL_IDS)
        self.assertIn(event["resource_type"], RESOURCE_TYPES)
        self.assertGreater(event["total_capacity"], 0)
        self.assertGreaterEqual(event["occupied"], 0)
        self.assertGreaterEqual(event["available"], 0)
        self.assertEqual(event["occupied"] + event["available"], event["total_capacity"])

        # Verify no PII fields
        for forbidden in ["patient_name", "phone", "email", "ssn", "mrn", "address"]:
            self.assertNotIn(forbidden, event)

        # Verify batch generation
        batch = generate_event_batch(num_events=10)
        self.assertEqual(len(batch), 10)

    def test_06_event_validation_logic(self):
        """6. Verify validation rules discard malformed records"""
        now = datetime.now(timezone.utc)
        test_events = [
            # Valid event
            {"event_id": "EV1", "timestamp": now, "hospital_id": "H101", "resource_type": "ICU", "total_capacity": 50, "occupied": 30, "available": 20, "emergency_load": "Medium"},
            # Invalid: occupied > total_capacity
            {"event_id": "EV2", "timestamp": now, "hospital_id": "H101", "resource_type": "ICU", "total_capacity": 50, "occupied": 60, "available": 10, "emergency_load": "High"},
            # Invalid: negative available
            {"event_id": "EV3", "timestamp": now, "hospital_id": "H102", "resource_type": "Ventilator", "total_capacity": 20, "occupied": 15, "available": -5, "emergency_load": "Low"},
            # Invalid: null hospital_id
            {"event_id": "EV4", "timestamp": now, "hospital_id": None, "resource_type": "ICU", "total_capacity": 50, "occupied": 20, "available": 30, "emergency_load": "Low"}
        ]

        df = self.spark.createDataFrame(test_events, schema=get_streaming_schema())
        from pyspark.sql.functions import col
        valid_df = df.filter(
            col("hospital_id").isNotNull() &
            col("resource_type").isNotNull() &
            col("timestamp").isNotNull() &
            (col("occupied") >= 0) &
            (col("available") >= 0) &
            (col("total_capacity") >= 0) &
            (col("occupied") <= col("total_capacity")) &
            (col("available") <= col("total_capacity"))
        )

        self.assertEqual(valid_df.count(), 1, "Only the 1 valid record must survive the filter.")
        row = valid_df.collect()[0]
        self.assertEqual(row["event_id"], "EV1")

    def test_07_utilization_calculation(self):
        """7. Verify utilization percentage formula: (occupied / total_capacity) * 100"""
        now = datetime.now(timezone.utc)
        data = [
            {"event_id": "EV1", "timestamp": now, "hospital_id": "H101", "resource_type": "ICU", "total_capacity": 50, "occupied": 25, "available": 25, "emergency_load": "Medium"},
            {"event_id": "EV2", "timestamp": now, "hospital_id": "H102", "resource_type": "Ventilator", "total_capacity": 0, "occupied": 0, "available": 0, "emergency_load": "Low"}
        ]
        df = self.spark.createDataFrame(data, schema=get_streaming_schema())
        from pyspark.sql.functions import col, when, round as spark_round
        res = df.withColumn(
            "utilization_percentage",
            when(
                col("total_capacity") > 0,
                spark_round((col("occupied") / col("total_capacity")) * 100.0, 2)
            ).otherwise(0.0)
        ).collect()

        # 25 / 50 * 100 = 50.0%
        self.assertEqual(res[0]["utilization_percentage"], 50.0)
        # Safe zero division -> 0.0%
        self.assertEqual(res[1]["utilization_percentage"], 0.0)

    def test_08_live_structured_streaming_query(self):
        """8. Verify real file-based Structured Streaming query starts, processes files, and stops"""
        # Create initial test JSON file in input directory
        events = generate_event_batch(num_events=5)
        write_events_to_json_file(events, os.path.join(self.input_dir, "batch_1.json"))

        # Build streaming pipeline on temporary directory
        streaming_df = build_resource_streaming_pipeline(
            spark=self.spark,
            input_path=self.input_dir,
            watermark_delay="1 minute",
            window_duration="1 minute"
        )

        self.assertTrue(streaming_df.isStreaming, "DataFrame must be an active streaming source.")

        # Start streaming query writing to memory or parquet
        query = start_streaming_query(
            streaming_df=streaming_df,
            output_path=self.output_dir,
            checkpoint_path=self.checkpoint_dir,
            query_name="test-resource-stream",
            trigger_processing_time="1 second"
        )

        try:
            self.assertTrue(query.isActive, "Streaming query must be active after start.")
            self.assertEqual(query.name, "test-resource-stream")

            # Let the query process the first micro-batch
            time.sleep(3)

            # Drop another file into input directory to simulate real-time ingestion
            events_2 = generate_event_batch(num_events=5)
            write_events_to_json_file(events_2, os.path.join(self.input_dir, "batch_2.json"))

            # Allow query to pick up batch 2
            query.processAllAvailable()
            time.sleep(2)

        finally:
            query.stop()
            self.assertFalse(query.isActive, "Streaming query must be stopped cleanly.")


if __name__ == "__main__":
    unittest.main()
