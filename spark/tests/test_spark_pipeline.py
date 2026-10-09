"""
Automated Test Suite: Apache Spark / PySpark Big Data Processing (Phase 6)
Verifies SparkSession creation, dataset ingestion, cleaning rules, transformations,
aggregations, multi-table joins, and Parquet/CSV output exports.
"""

import sys
import os
import unittest
import tempfile
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.data_ingestion import (
    load_hospitals,
    load_surgeries,
    load_blood_banks,
    load_resources,
    get_hospital_schema,
    get_data_path,
    DATA_DIR,
    HDFS_BASE_DIR,
    HDFS_OUTPUT_BASE
)
from spark.pipeline import write_hdfs_outputs
from spark.data_cleaning import (
    clean_hospitals,
    clean_surgeries,
    clean_blood_banks,
    clean_resources
)
from spark.transformations import (
    add_bed_occupancy,
    add_icu_occupancy,
    add_resource_utilization,
    join_hospitals_and_resources,
    join_hospitals_and_surgeries
)
from spark.hospital_analytics import (
    compute_district_analytics,
    compute_specialty_analytics,
    compute_macro_kpis
)
from spark.resource_analytics import (
    compute_resource_type_analytics,
    compute_hospital_resource_summary
)
from spark.recommendation_analytics import (
    compute_surgery_analytics,
    compute_blood_analytics,
    compute_combined_hospital_summary
)


class TestSparkPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # 1. Setup isolated temporary output directory
        cls.test_output_dir = tempfile.mkdtemp(prefix="spark_test_output_")

        # 2. Initialize local SparkSession
        cls.spark = get_spark_session(
            app_name="MediPulse-SparkTest",
            master="local[2]"
        )

    @classmethod
    def tearDownClass(cls):
        # 1. Stop SparkSession
        stop_spark_session(cls.spark)

        # 2. Remove temporary test outputs
        if os.path.exists(cls.test_output_dir):
            try:
                shutil.rmtree(cls.test_output_dir)
            except OSError:
                pass

    def test_01_sparksession_creation(self):
        """1. Verify SparkSession is created and running in local mode"""
        self.assertIsNotNone(self.spark)
        self.assertIn("local", self.spark.conf.get("spark.master"))
        self.assertEqual(self.spark.conf.get("spark.app.name"), "MediPulse-SparkTest")

    def test_02_hospital_csv_ingestion(self):
        """2. Verify hospital CSV ingestion with explicit StructType schema"""
        df = load_hospitals(self.spark)
        self.assertIsNotNone(df)
        self.assertIn("hospital_id", df.columns)
        self.assertIn("specialty", df.columns)
        self.assertIn("total_beds", df.columns)
        self.assertIn("available_beds", df.columns)
        self.assertEqual(len(df.schema.fields), len(get_hospital_schema().fields))

    def test_03_correct_row_count(self):
        """3. Verify correct row count from baseline datasets"""
        h_df = load_hospitals(self.spark)
        s_df = load_surgeries(self.spark)
        b_df = load_blood_banks(self.spark)
        r_df = load_resources(self.spark)

        self.assertEqual(h_df.count(), 1000, "Hospitals dataset must contain 1000 records.")
        self.assertEqual(s_df.count(), 3051, "Surgeries dataset must contain 3051 records.")
        self.assertEqual(b_df.count(), 368, "Blood banks dataset must contain 368 records.")
        self.assertEqual(r_df.count(), 6000, "Hospital resources dataset must contain 6000 records.")

    def test_04_data_cleaning_and_trimming(self):
        """4. Verify data cleaning and text trimming operations"""
        # Create test data with messy whitespace
        data = [
            (" H901 ", "  Care Hospital  ", " North Zone ", " North Zone ", 12.9, 77.5, "Cardiology ", 100, 20, 20, 5, 10, "Medium", 30, 40000, "yes", 4.5)
        ]
        test_df = self.spark.createDataFrame(data, schema=get_hospital_schema())
        cleaned = clean_hospitals(test_df).collect()[0]

        self.assertEqual(cleaned["hospital_id"], "H901")
        self.assertEqual(cleaned["hospital_name"], "Care Hospital")
        self.assertEqual(cleaned["location"], "North Zone")
        self.assertEqual(cleaned["specialty"], "Cardiology")
        self.assertEqual(cleaned["emergency_services"], "Yes")

    def test_05_duplicate_removal(self):
        """5. Verify duplicate removal on primary keys"""
        data = [
            ("H101", "Hospital A", "Central District", "Central District", 12.9, 77.5, "Cardiology", 100, 20, 20, 5, 10, "Medium", 30, 40000, "Yes", 4.5),
            ("H101", "Hospital A Duplicate", "Central District", "Central District", 12.9, 77.5, "Cardiology", 100, 20, 20, 5, 10, "Medium", 30, 40000, "Yes", 4.5)
        ]
        test_df = self.spark.createDataFrame(data, schema=get_hospital_schema())
        cleaned = clean_hospitals(test_df)
        self.assertEqual(cleaned.count(), 1, "Duplicate hospital_id must be pruned to exactly 1 record.")

    def test_06_invalid_value_handling(self):
        """6. Verify invalid numeric values, overflows, and negative values are clamped"""
        data = [
            # available_beds (150) > total_beds (100) -> must clamp to 100
            # available_icu_beds (-5) < 0 -> must clamp to 0
            # rating (8.8) > 5.0 -> must clamp to 5.0
            # estimated_cost (-2000) -> must clamp to 0
            # average_wait_time (1) < 5 -> must clamp to 5
            ("H902", "Stress Hospital", "South Zone", "South Zone", 12.9, 77.5, "General Medicine", 100, 150, 20, -5, -2, "Unknown", 1, -2000, "maybe", 8.8)
        ]
        test_df = self.spark.createDataFrame(data, schema=get_hospital_schema())
        cleaned = clean_hospitals(test_df).collect()[0]

        self.assertEqual(cleaned["available_beds"], 100, "Available beds must not exceed total beds.")
        self.assertEqual(cleaned["available_icu_beds"], 0, "Available ICU beds must not be negative.")
        self.assertEqual(cleaned["rating"], 5.0, "Rating must be capped at 5.0.")
        self.assertEqual(cleaned["estimated_cost"], 0, "Cost must not be negative.")
        self.assertEqual(cleaned["average_wait_time"], 5, "Wait time must be at least 5 minutes.")
        self.assertEqual(cleaned["queue_length"], 0, "Queue length must not be negative.")
        self.assertEqual(cleaned["emergency_load"], "Medium", "Invalid emergency load must default to Medium.")

    def test_07_hospital_aggregation(self):
        """7. Verify hospital district and specialty aggregations"""
        h_df = clean_hospitals(load_hospitals(self.spark))
        dist_df = compute_district_analytics(h_df)
        spec_df = compute_specialty_analytics(h_df)
        kpi_df = compute_macro_kpis(h_df)

        self.assertGreater(dist_df.count(), 0)
        self.assertGreater(spec_df.count(), 0)

        kpis = kpi_df.collect()[0]
        self.assertEqual(kpis["total_hospitals"], 1000)
        self.assertGreater(kpis["total_registered_beds"], 5000)
        self.assertGreater(kpis["total_available_beds"], 1000)

    def test_08_resource_aggregation(self):
        """8. Verify resource utilization aggregations"""
        r_df = clean_resources(load_resources(self.spark))
        r_trans = add_resource_utilization(r_df)
        type_df = compute_resource_type_analytics(r_trans)

        self.assertGreater(type_df.count(), 0)
        row = type_df.collect()[0]
        self.assertIn("utilization_percentage", type_df.columns)
        self.assertGreaterEqual(row["utilization_percentage"], 0.0)
        self.assertLessEqual(row["utilization_percentage"], 100.0)

    def test_09_surgery_aggregation(self):
        """9. Verify surgery aggregations by clinical specialty"""
        s_df = clean_surgeries(load_surgeries(self.spark))
        s_agg = compute_surgery_analytics(s_df)

        self.assertGreater(s_agg.count(), 0)
        cols = s_agg.columns
        self.assertIn("specialty", cols)
        self.assertIn("surgery_types_offered", cols)
        self.assertIn("avg_surgery_cost", cols)
        self.assertIn("total_ot_slots_open", cols)

    def test_10_blood_aggregation(self):
        """10. Verify blood reserve analytics and low-stock alerts"""
        b_df = clean_blood_banks(load_blood_banks(self.spark))
        group_df, district_df = compute_blood_analytics(b_df, low_stock_threshold=15)

        self.assertGreaterEqual(group_df.count(), 8, "All 8 ABO/Rh groups must be represented.")
        self.assertGreater(district_df.count(), 0)
        self.assertIn("low_stock_centers_count", group_df.columns)

    def test_11_bed_occupancy_calculation(self):
        """11. Verify bed occupancy percentage formula: ((total - avail) / total) * 100"""
        data = [
            ("H801", "Alpha Clinic", "Central District", "Central District", 12.9, 77.5, "Cardiology", 100, 25, 20, 5, 10, "Medium", 30, 40000, "Yes", 4.5),
            ("H802", "Zero Bed Clinic", "South Zone", "South Zone", 12.9, 77.5, "General Medicine", 0, 0, 0, 0, 0, "Low", 15, 10000, "No", 4.0)
        ]
        test_df = self.spark.createDataFrame(data, schema=get_hospital_schema())
        trans = add_bed_occupancy(test_df).collect()

        # Row 1: total=100, avail=25 -> occupancy = 75.0%
        self.assertEqual(trans[0]["bed_occupancy_percentage"], 75.0)

        # Row 2: total=0 -> safe zero handling
        self.assertEqual(trans[1]["bed_occupancy_percentage"], 0.0)

    def test_12_output_generation(self):
        """12. Verify Parquet and CSV output generation on disk"""
        h_df = clean_hospitals(load_hospitals(self.spark))
        r_df = clean_resources(load_resources(self.spark))
        s_df = clean_surgeries(load_surgeries(self.spark))

        summary_df = compute_combined_hospital_summary(h_df, r_df, s_df)

        parquet_path = os.path.join(self.test_output_dir, "test_hospital_summary")
        csv_path = os.path.join(self.test_output_dir, "test_hospital_summary_csv")

        summary_df.write.mode("overwrite").parquet(parquet_path)
        summary_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)

        self.assertTrue(os.path.exists(parquet_path), "Parquet output directory must exist.")
        self.assertTrue(os.path.exists(csv_path), "CSV summary output directory must exist.")

        # Verify output contains data files
        parquet_files = [f for f in os.listdir(parquet_path) if f.endswith(".parquet")]
        csv_files = [f for f in os.listdir(csv_path) if f.endswith(".csv")]

        self.assertGreater(len(parquet_files), 0, "Must generate valid Parquet part files.")
        self.assertGreater(len(csv_files), 0, "Must generate valid CSV part files.")

    def test_13_get_data_path_resolution(self):
        """13. Verify get_data_path resolves local and HDFS paths with correct precedence"""
        # Explicit local
        local_path = get_data_path("hospitals.csv", source="local")
        self.assertEqual(local_path, os.path.join(DATA_DIR, "hospitals.csv"))

        # Explicit HDFS
        hdfs_path = get_data_path("hospitals.csv", source="hdfs")
        self.assertEqual(hdfs_path, f"{HDFS_BASE_DIR}/hospitals.csv")

        # Environment variable fallback
        old_env = os.environ.get("MEDIPULSE_DATA_SOURCE")
        try:
            os.environ["MEDIPULSE_DATA_SOURCE"] = "hdfs"
            self.assertEqual(get_data_path("surgeries.csv"), f"{HDFS_BASE_DIR}/surgeries.csv")

            os.environ["MEDIPULSE_DATA_SOURCE"] = "local"
            self.assertEqual(get_data_path("surgeries.csv"), os.path.join(DATA_DIR, "surgeries.csv"))
        finally:
            if old_env is not None:
                os.environ["MEDIPULSE_DATA_SOURCE"] = old_env
            else:
                os.environ.pop("MEDIPULSE_DATA_SOURCE", None)

    def test_14_local_mode_explicit_ingestion(self):
        """14. Verify explicit local mode ingestion for all 4 datasets"""
        h_df = load_hospitals(self.spark, source="local")
        s_df = load_surgeries(self.spark, source="local")
        b_df = load_blood_banks(self.spark, source="local")
        r_df = load_resources(self.spark, source="local")

        self.assertEqual(h_df.count(), 1000, "Local hospitals count must be 1000.")
        self.assertEqual(s_df.count(), 3051, "Local surgeries count must be 3051.")
        self.assertEqual(b_df.count(), 368, "Local blood banks count must be 368.")
        self.assertEqual(r_df.count(), 6000, "Local resources count must be 6000.")

    def test_15_hdfs_mode_ingestion(self):
        """15. Verify real HDFS mode ingestion for all 4 datasets via hdfs://localhost:9000"""
        try:
            h_df = load_hospitals(self.spark, source="hdfs")
            s_df = load_surgeries(self.spark, source="hdfs")
            b_df = load_blood_banks(self.spark, source="hdfs")
            r_df = load_resources(self.spark, source="hdfs")

            self.assertEqual(h_df.count(), 1000, "HDFS hospitals count must be 1000.")
            self.assertEqual(s_df.count(), 3051, "HDFS surgeries count must be 3051.")
            self.assertEqual(b_df.count(), 368, "HDFS blood banks count must be 368.")
            self.assertEqual(r_df.count(), 6000, "HDFS resources count must be 6000.")
        except Exception as e:
            self.fail(
                f"Failed to ingest datasets from HDFS (hdfs://localhost:9000/medipulse/raw/production/). "
                f"Ensure WSL2 Hadoop NameNode and DataNode are active on port 9000. Error: {e}"
            )

    def test_16_local_file_not_found(self):
        """16. Verify missing local file raises FileNotFoundError (without os.path.exists on HDFS)"""
        with self.assertRaises(FileNotFoundError):
            load_hospitals(self.spark, file_path="C:/nonexistent_dir_12345/missing.csv")

    def test_17_hdfs_output_generation(self):
        """17. Verify writing processed and analytics DataFrames to HDFS and reading them back"""
        try:
            from spark.transformations import add_cost_and_wait_tiers

            # Prepare data
            h_df = clean_hospitals(load_hospitals(self.spark, source="hdfs"))
            r_df = clean_resources(load_resources(self.spark, source="hdfs"))
            s_df = clean_surgeries(load_surgeries(self.spark, source="hdfs"))
            b_df = clean_blood_banks(load_blood_banks(self.spark, source="hdfs"))

            trans_hosp = add_cost_and_wait_tiers(add_icu_occupancy(add_bed_occupancy(h_df)))
            trans_res = add_resource_utilization(r_df)

            district_analytics = compute_district_analytics(trans_hosp)
            specialty_analytics = compute_specialty_analytics(trans_hosp)
            resource_type_analytics = compute_resource_type_analytics(trans_res)
            surgery_analytics = compute_surgery_analytics(s_df)
            blood_group_analytics, _ = compute_blood_analytics(b_df, low_stock_threshold=15)
            combined_summary = compute_combined_hospital_summary(trans_hosp, r_df, s_df)

            # Write outputs to HDFS
            write_hdfs_outputs(
                combined_summary=combined_summary,
                district_analytics=district_analytics,
                specialty_analytics=specialty_analytics,
                resource_type_analytics=resource_type_analytics,
                surgery_analytics=surgery_analytics,
                blood_group_analytics=blood_group_analytics
            )

            # Read back from HDFS via PySpark
            df_hosp = self.spark.read.parquet(f"{HDFS_OUTPUT_BASE}/processed/hospital_summary")
            df_dist = self.spark.read.parquet(f"{HDFS_OUTPUT_BASE}/analytics/district_analytics")
            df_spec = self.spark.read.parquet(f"{HDFS_OUTPUT_BASE}/analytics/specialty_analytics")
            df_res = self.spark.read.parquet(f"{HDFS_OUTPUT_BASE}/analytics/resource_analytics")
            df_surg = self.spark.read.parquet(f"{HDFS_OUTPUT_BASE}/analytics/surgery_analytics")
            df_bld = self.spark.read.parquet(f"{HDFS_OUTPUT_BASE}/analytics/blood_analytics")

            # Validate row counts and schemas
            self.assertEqual(df_hosp.count(), 1000, "HDFS hospital_summary count must be 1000.")
            self.assertGreater(df_dist.count(), 0, "HDFS district_analytics must have rows.")
            self.assertGreater(df_spec.count(), 0, "HDFS specialty_analytics must have rows.")
            self.assertGreater(df_res.count(), 0, "HDFS resource_analytics must have rows.")
            self.assertGreater(df_surg.count(), 0, "HDFS surgery_analytics must have rows.")
            self.assertGreater(df_bld.count(), 0, "HDFS blood_analytics must have rows.")

            self.assertIn("hospital_id", df_hosp.columns)
            self.assertIn("location", df_dist.columns)
            self.assertIn("specialty", df_spec.columns)
            self.assertIn("resource_type", df_res.columns)
            self.assertIn("specialty", df_surg.columns)
            self.assertIn("blood_group", df_bld.columns)

        except Exception as e:
            self.fail(
                f"Failed HDFS output verification at {HDFS_OUTPUT_BASE}. "
                f"Ensure WSL2 Hadoop is active on port 9000. Error: {e}"
            )


if __name__ == "__main__":
    unittest.main()


