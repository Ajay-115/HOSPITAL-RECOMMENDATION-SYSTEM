"""
MediPulse Big Data Processing Engine
Job: Process Resources & Equipment Utilization (Phase 6)
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.data_ingestion import load_resources
from spark.data_cleaning import clean_resources
from spark.transformations import add_resource_utilization
from spark.resource_analytics import compute_resource_type_analytics, compute_hospital_resource_summary


def run_process_resources(spark=None, output_dir=None):
    """
    Executes the hospital resources batch job:
    1. Ingests resources CSV
    2. Cleans & bounds quantities
    3. Calculates utilization percentage
    4. Aggregates by resource type and by facility
    5. Exports Parquet and CSV summary
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("MediPulse-ProcessResources")
        own_spark = True

    out_base = output_dir or os.path.join(BASE_DIR, "spark", "output")
    os.makedirs(out_base, exist_ok=True)

    print("[JOB: Resources] 1. Ingesting hospital resources dataset...")
    raw_df = load_resources(spark)

    print(f"[JOB: Resources] 2. Ingested {raw_df.count()} equipment rows. Cleaning records...")
    cleaned_df = clean_resources(raw_df)

    print("[JOB: Resources] 3. Computing utilization percentages...")
    transformed_df = add_resource_utilization(cleaned_df)

    print("[JOB: Resources] 4. Aggregating by equipment type and hospital...")
    type_analytics = compute_resource_type_analytics(transformed_df)
    hospital_res_summary = compute_hospital_resource_summary(transformed_df)

    # 5. Output export
    res_out = os.path.join(out_base, "resources_processed")
    type_out = os.path.join(out_base, "resource_type_analytics")
    hosp_res_out = os.path.join(out_base, "hospital_resource_summary")

    print(f"[JOB: Resources] 5. Writing output datasets to {out_base}...")
    transformed_df.write.mode("overwrite").parquet(res_out)
    type_analytics.write.mode("overwrite").parquet(type_out)
    hospital_res_summary.write.mode("overwrite").parquet(hosp_res_out)

    transformed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "resources"))
    type_analytics.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(out_base, "csv_summary", "resource_types"))

    print("[JOB: Resources] Completed successfully.")

    if own_spark:
        stop_spark_session(spark)

    return transformed_df, type_analytics, hospital_res_summary


if __name__ == "__main__":
    run_process_resources()
