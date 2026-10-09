"""
MediPulse Big Data Processing Engine
Job: Process Surgeries & Operating Theater Analytics (Phase 6)
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.data_ingestion import load_surgeries
from spark.data_cleaning import clean_surgeries
from spark.recommendation_analytics import compute_surgery_analytics


def run_process_surgeries(spark=None, output_dir=None):
    """
    Executes surgical analytics batch job:
    1. Ingestion of surgeries CSV
    2. Data cleaning & bounds enforcement
    3. Specialty procedure distribution & cost aggregation
    4. Parquet & CSV export
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("MediPulse-ProcessSurgeries")
        own_spark = True

    out_base = output_dir or os.path.join(BASE_DIR, "spark", "output")
    os.makedirs(out_base, exist_ok=True)

    print("[JOB: Surgeries] 1. Ingesting surgeries dataset...")
    raw_df = load_surgeries(spark)

    print(f"[JOB: Surgeries] 2. Ingested {raw_df.count()} surgery records. Cleaning data...")
    cleaned_df = clean_surgeries(raw_df)

    print("[JOB: Surgeries] 3. Computing surgical specialty metrics and slot analytics...")
    surgery_analytics_df = compute_surgery_analytics(cleaned_df)

    # 4. Output Export
    surg_out = os.path.join(out_base, "surgeries_processed")
    analytics_out = os.path.join(out_base, "surgery_analytics")

    print(f"[JOB: Surgeries] 4. Writing output datasets to {out_base}...")
    cleaned_df.write.mode("overwrite").parquet(surg_out)
    surgery_analytics_df.write.mode("overwrite").parquet(analytics_out)

    surgery_analytics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "surgeries"))

    print("[JOB: Surgeries] Completed successfully.")

    if own_spark:
        stop_spark_session(spark)

    return cleaned_df, surgery_analytics_df


if __name__ == "__main__":
    run_process_surgeries()
