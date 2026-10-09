"""
MediPulse Big Data Processing Engine
Job: Process Blood Reserves & Emergency Supply Analytics (Phase 6)
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.data_ingestion import load_blood_banks
from spark.data_cleaning import clean_blood_banks
from spark.recommendation_analytics import compute_blood_analytics


def run_process_blood(spark=None, output_dir=None, low_stock_threshold=15):
    """
    Executes blood reserve batch job:
    1. Ingestion of blood_banks CSV
    2. Data cleaning & ABO/Rh standardization
    3. Aggregations by blood group and district
    4. Low stock reserve alerts
    5. Parquet & CSV export
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("MediPulse-ProcessBlood")
        own_spark = True

    out_base = output_dir or os.path.join(BASE_DIR, "spark", "output")
    os.makedirs(out_base, exist_ok=True)

    print("[JOB: Blood] 1. Ingesting blood banks dataset...")
    raw_df = load_blood_banks(spark)

    print(f"[JOB: Blood] 2. Ingested {raw_df.count()} blood bank records. Cleaning data...")
    cleaned_df = clean_blood_banks(raw_df)

    print(f"[JOB: Blood] 3. Computing blood reserve analytics (low-stock threshold = {low_stock_threshold} units)...")
    group_df, district_df = compute_blood_analytics(cleaned_df, low_stock_threshold)

    # 4. Output Export
    blood_out = os.path.join(out_base, "blood_processed")
    group_out = os.path.join(out_base, "blood_group_analytics")
    district_out = os.path.join(out_base, "blood_district_analytics")

    print(f"[JOB: Blood] 4. Writing output datasets to {out_base}...")
    cleaned_df.write.mode("overwrite").parquet(blood_out)
    group_df.write.mode("overwrite").parquet(group_out)
    district_df.write.mode("overwrite").parquet(district_out)

    group_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "blood_groups"))
    district_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "blood_districts"))

    print("[JOB: Blood] Completed successfully.")

    if own_spark:
        stop_spark_session(spark)

    return cleaned_df, group_df, district_df


if __name__ == "__main__":
    run_process_blood()
