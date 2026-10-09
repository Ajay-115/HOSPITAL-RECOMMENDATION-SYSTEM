"""
MediPulse Big Data Processing Engine
Job: Process Hospitals & Regional Healthcare Analytics (Phase 6)
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.data_ingestion import load_hospitals
from spark.data_cleaning import clean_hospitals
from spark.transformations import add_bed_occupancy, add_icu_occupancy, add_cost_and_wait_tiers
from spark.hospital_analytics import compute_district_analytics, compute_specialty_analytics, compute_macro_kpis


def run_process_hospitals(spark=None, output_dir=None):
    """
    Executes the hospital data processing job:
    1. Ingestion with explicit schema
    2. Data cleaning & validation
    3. Occupancy and tier transformations
    4. Aggregation by district and specialty
    5. Export to Parquet and CSV summary
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("MediPulse-ProcessHospitals")
        own_spark = True

    out_base = output_dir or os.path.join(BASE_DIR, "spark", "output")
    os.makedirs(out_base, exist_ok=True)

    print("[JOB: Hospitals] 1. Ingesting hospitals dataset...")
    raw_df = load_hospitals(spark)

    print(f"[JOB: Hospitals] 2. Ingested {raw_df.count()} raw records. Cleaning dataset...")
    cleaned_df = clean_hospitals(raw_df)

    print("[JOB: Hospitals] 3. Applying bed occupancy and analytical tier transformations...")
    transformed_df = add_cost_and_wait_tiers(add_icu_occupancy(add_bed_occupancy(cleaned_df)))

    print("[JOB: Hospitals] 4. Computing district and specialty aggregations...")
    district_df = compute_district_analytics(transformed_df)
    specialty_df = compute_specialty_analytics(transformed_df)
    macro_kpi_df = compute_macro_kpis(transformed_df)

    # 5. Writing Outputs
    hosp_out = os.path.join(out_base, "hospitals_processed")
    district_out = os.path.join(out_base, "district_analytics")
    specialty_out = os.path.join(out_base, "specialty_analytics")

    print(f"[JOB: Hospitals] 5. Writing output datasets to {out_base}...")
    transformed_df.write.mode("overwrite").parquet(hosp_out)
    district_df.write.mode("overwrite").parquet(district_out)
    specialty_df.write.mode("overwrite").parquet(specialty_out)

    # Also export small CSV summaries for immediate human verification
    transformed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "hospitals"))
    district_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "districts"))

    print("[JOB: Hospitals] Completed successfully.")

    if own_spark:
        stop_spark_session(spark)

    return transformed_df, district_df, specialty_df, macro_kpi_df


if __name__ == "__main__":
    run_process_hospitals()
