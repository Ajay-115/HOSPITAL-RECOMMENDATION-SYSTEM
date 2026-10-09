"""
MediPulse Big Data Processing Engine
End-to-End PySpark Analytics Pipeline Driver (Phase 6)

Supports dual data sources via MEDIPULSE_DATA_SOURCE:
- 'local': Ingests from backend/data/*.csv
- 'hdfs': Ingests from hdfs://localhost:9000/medipulse/raw/production/*.csv

Execution:
    # Local mode (default):
    python -m spark.pipeline

    # HDFS mode:
    $env:MEDIPULSE_DATA_SOURCE="hdfs"
    python -m spark.pipeline
"""

import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.data_ingestion import (
    load_hospitals,
    load_surgeries,
    load_blood_banks,
    load_resources,
    DATA_DIR,
    HDFS_BASE_DIR,
    HDFS_OUTPUT_BASE
)
from spark.data_cleaning import clean_hospitals, clean_surgeries, clean_blood_banks, clean_resources
from spark.transformations import add_bed_occupancy, add_icu_occupancy, add_cost_and_wait_tiers, add_resource_utilization
from spark.hospital_analytics import compute_district_analytics, compute_specialty_analytics, compute_emergency_analytics, compute_macro_kpis
from spark.resource_analytics import compute_resource_type_analytics, compute_hospital_resource_summary
from spark.recommendation_analytics import compute_surgery_analytics, compute_blood_analytics, compute_combined_hospital_summary


def write_hdfs_outputs(
    combined_summary,
    district_analytics,
    specialty_analytics,
    resource_type_analytics,
    surgery_analytics,
    blood_group_analytics,
    hdfs_base=None
):
    """
    Exports processed and analytical DataFrames as Parquet partitions to HDFS.

    HDFS Target Structure:
      hdfs://localhost:9000/medipulse/processed/hospital_summary
      hdfs://localhost:9000/medipulse/analytics/district_analytics
      hdfs://localhost:9000/medipulse/analytics/specialty_analytics
      hdfs://localhost:9000/medipulse/analytics/resource_analytics
      hdfs://localhost:9000/medipulse/analytics/surgery_analytics
      hdfs://localhost:9000/medipulse/analytics/blood_analytics

    Note: HDFS output uses columnar Parquet format (Snappy compressed) without
    unnecessary CSV duplicates, adhering to enterprise big data best practices.
    """
    base = (hdfs_base or HDFS_OUTPUT_BASE).rstrip("/")
    processed_dir = f"{base}/processed"
    analytics_dir = f"{base}/analytics"

    print(f"[SPARK] Writing HDFS processed output to {processed_dir}/hospital_summary...")
    combined_summary.write.mode("overwrite").parquet(f"{processed_dir}/hospital_summary")

    print(f"[SPARK] Writing HDFS analytics outputs to {analytics_dir}...")
    district_analytics.write.mode("overwrite").parquet(f"{analytics_dir}/district_analytics")
    specialty_analytics.write.mode("overwrite").parquet(f"{analytics_dir}/specialty_analytics")
    resource_type_analytics.write.mode("overwrite").parquet(f"{analytics_dir}/resource_analytics")
    surgery_analytics.write.mode("overwrite").parquet(f"{analytics_dir}/surgery_analytics")
    blood_group_analytics.write.mode("overwrite").parquet(f"{analytics_dir}/blood_analytics")


def run_pipeline(output_dir=None, source=None):
    """
    Orchestrates the complete MediPulse PySpark Big Data Analytics pipeline:
    1. Create SparkSession (local[*])
    2. Determine data source (Local vs. HDFS)
    3. Ingest CSV datasets with explicit schemas
    4. Clean and validate records
    5. Apply analytical transformations
    6. Generate hospital, resource, surgery, and blood analytics
    7. Generate combined relational hospital summary
    8. Write outputs to Parquet & CSV summaries
    9. Stop SparkSession
    """
    start_time = time.time()
    print("==================================================================")
    print(" [SPARK] Starting MediPulse Big Data Analytics Pipeline (Phase 6) ")
    print("==================================================================")

    source_mode = (source or os.getenv("MEDIPULSE_DATA_SOURCE", "local")).strip().lower()
    if source_mode == "hdfs":
        print("[SPARK] Data source: HDFS")
        print(f"[SPARK] HDFS base: {HDFS_BASE_DIR}")
    else:
        print("[SPARK] Data source: LOCAL")
        print(f"[SPARK] Local data directory: {DATA_DIR}")

    out_base = output_dir or os.path.join(BASE_DIR, "spark", "output")
    os.makedirs(out_base, exist_ok=True)

    # 1. Spark Session
    print("[SPARK] Initializing local SparkSession (master='local[*]')...")
    spark = get_spark_session("MediPulse-BigData-Analytics")
    print(f"[SPARK] SparkSession active: Version {spark.version}")

    try:
        # 2. Ingestion
        print("[SPARK] Loading hospitals dataset...")
        raw_hospitals = load_hospitals(spark, source=source_mode)

        print("[SPARK] Loading surgeries dataset...")
        raw_surgeries = load_surgeries(spark, source=source_mode)

        print("[SPARK] Loading blood data...")
        raw_blood = load_blood_banks(spark, source=source_mode)

        print("[SPARK] Loading resources dataset...")
        raw_resources = load_resources(spark, source=source_mode)

        print(f"[SPARK] Datasets loaded: {raw_hospitals.count()} hospitals, {raw_surgeries.count()} surgeries, {raw_blood.count()} blood banks, {raw_resources.count()} resources.")

        # 3. Data Cleaning
        print("[SPARK] Cleaning datasets (pruning invalid records, bounding capacities, deduplicating IDs)...")
        clean_hosp = clean_hospitals(raw_hospitals)
        clean_surg = clean_surgeries(raw_surgeries)
        clean_bld = clean_blood_banks(raw_blood)
        clean_res = clean_resources(raw_resources)

        # 4. Transformations
        print("[SPARK] Applying feature engineering & analytical transformations...")
        trans_hosp = add_cost_and_wait_tiers(add_icu_occupancy(add_bed_occupancy(clean_hosp)))
        trans_res = add_resource_utilization(clean_res)

        # 5. Hospital Analytics
        print("[SPARK] Generating hospital analytics (district capacity, specialty distributions, emergency profiles)...")
        district_analytics = compute_district_analytics(trans_hosp)
        specialty_analytics = compute_specialty_analytics(trans_hosp)
        emergency_analytics = compute_emergency_analytics(trans_hosp)
        macro_kpis = compute_macro_kpis(trans_hosp)

        # 6. Resource Analytics
        print("[SPARK] Generating resource analytics (equipment utilization percentages)...")
        resource_type_analytics = compute_resource_type_analytics(trans_res)
        hospital_res_summary = compute_hospital_resource_summary(trans_res)

        # 7. Surgery & Blood Analytics
        print("[SPARK] Generating surgery analytics & blood reserve supply metrics...")
        surgery_analytics = compute_surgery_analytics(clean_surg)
        blood_group_analytics, blood_district_analytics = compute_blood_analytics(clean_bld, low_stock_threshold=15)

        # 8. Combined Hospital Summary
        print("[SPARK] Generating combined multi-table relational hospital summary...")
        combined_summary = compute_combined_hospital_summary(trans_hosp, clean_res, clean_surg)

        # 9. Write local outputs
        print(f"[SPARK] Writing local outputs to {out_base}...")

        combined_summary.write.mode("overwrite").parquet(os.path.join(out_base, "hospital_summary"))
        district_analytics.write.mode("overwrite").parquet(os.path.join(out_base, "district_analytics"))
        specialty_analytics.write.mode("overwrite").parquet(os.path.join(out_base, "specialty_analytics"))
        resource_type_analytics.write.mode("overwrite").parquet(os.path.join(out_base, "resource_analytics"))
        surgery_analytics.write.mode("overwrite").parquet(os.path.join(out_base, "surgery_analytics"))
        blood_group_analytics.write.mode("overwrite").parquet(os.path.join(out_base, "blood_analytics"))

        # Small CSV summary exports for inspection
        csv_dir = os.path.join(out_base, "csv_summary")
        os.makedirs(csv_dir, exist_ok=True)
        combined_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(csv_dir, "hospital_summary"))
        district_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(csv_dir, "district_analytics"))
        specialty_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(csv_dir, "specialty_analytics"))
        resource_type_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(csv_dir, "resource_analytics"))
        surgery_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(csv_dir, "surgery_analytics"))
        blood_group_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(csv_dir, "blood_analytics"))

        # 10. Write HDFS outputs (only when running in HDFS mode)
        if source_mode == "hdfs":
            write_hdfs_outputs(
                combined_summary=combined_summary,
                district_analytics=district_analytics,
                specialty_analytics=specialty_analytics,
                resource_type_analytics=resource_type_analytics,
                surgery_analytics=surgery_analytics,
                blood_group_analytics=blood_group_analytics
            )

        elapsed = round(time.time() - start_time, 2)
        print("==================================================================")
        print(f" [SPARK] Pipeline completed successfully in {elapsed}s")
        print("==================================================================")

        return {
            "success": True,
            "source_mode": source_mode,
            "elapsed_seconds": elapsed,
            "hospitals_processed": trans_hosp.count(),
            "districts_analyzed": district_analytics.count(),
            "specialties_analyzed": specialty_analytics.count(),
            "resource_types_analyzed": resource_type_analytics.count(),
            "surgeries_analyzed": surgery_analytics.count(),
            "blood_groups_analyzed": blood_group_analytics.count()
        }

    finally:
        print("[SPARK] Stopping SparkSession...")
        stop_spark_session(spark)


if __name__ == "__main__":
    run_pipeline()
