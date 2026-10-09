"""
MediPulse Big Data Processing Engine
Resource Analytics & Utilization Layer (Phase 6)
"""

from pyspark.sql import functions as F


def compute_resource_type_analytics(res_df):
    """
    Computes system-wide resource availability and utilization metrics grouped by equipment type.
    """
    if res_df is None:
        return None

    return (
        res_df.groupBy("resource_name", "resource_type")
        .agg(
            F.count("id").alias("hospital_instances"),
            F.sum("total_quantity").alias("total_units_installed"),
            F.sum("available_quantity").alias("available_units_ready"),
            F.sum(F.col("total_quantity") - F.col("available_quantity")).alias("in_use_units")
        )
        .withColumn(
            "utilization_percentage",
            F.round(
                F.when(
                    F.col("total_units_installed") > 0,
                    (F.col("in_use_units") / F.col("total_units_installed")) * 100.0
                ).otherwise(0.0),
                1
            )
        )
        .orderBy(F.col("utilization_percentage").desc(), F.col("total_units_installed").desc())
    )


def compute_hospital_resource_summary(res_df):
    """
    Computes resource totals and composite equipment utilization for each individual hospital.
    """
    if res_df is None:
        return None

    return (
        res_df.groupBy("hospital_id")
        .agg(
            F.count("id").alias("monitored_equipment_types"),
            F.sum("total_quantity").alias("hospital_total_equipment"),
            F.sum("available_quantity").alias("hospital_available_equipment")
        )
        .withColumn(
            "hospital_equipment_utilization_pct",
            F.round(
                F.when(
                    F.col("hospital_total_equipment") > 0,
                    ((F.col("hospital_total_equipment") - F.col("hospital_available_equipment")) / F.col("hospital_total_equipment")) * 100.0
                ).otherwise(0.0),
                1
            )
        )
        .orderBy(F.col("hospital_total_equipment").desc())
    )
