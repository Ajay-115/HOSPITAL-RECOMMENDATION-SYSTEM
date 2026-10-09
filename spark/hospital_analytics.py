"""
MediPulse Big Data Processing Engine
Hospital Analytics & Aggregation Layer (Phase 6)
"""

from pyspark.sql import functions as F


def compute_district_analytics(hosp_df):
    """
    Computes regional healthcare capacity and metrics grouped by district using Spark groupBy() and agg().
    """
    if hosp_df is None:
        return None

    group_col = "district" if "district" in hosp_df.columns else "location"

    df_agg = (
        hosp_df.groupBy(group_col)
        .agg(
            F.count("hospital_id").alias("hospital_count"),
            F.sum("total_beds").alias("total_district_beds"),
            F.sum("available_beds").alias("available_district_beds"),
            F.sum("icu_beds").alias("total_district_icu"),
            F.sum("available_icu_beds").alias("available_district_icu"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.avg("estimated_cost"), 0).alias("avg_treatment_cost"),
            F.round(F.avg("average_wait_time"), 1).alias("avg_wait_minutes")
        )
        .withColumn(
            "district_bed_occupancy_pct",
            F.round(
                F.when(
                    F.col("total_district_beds") > 0,
                    ((F.col("total_district_beds") - F.col("available_district_beds")) / F.col("total_district_beds")) * 100.0
                ).otherwise(0.0),
                1
            )
        )
        .orderBy(F.col("hospital_count").desc(), F.col("total_district_beds").desc())
    )
    if "district" in hosp_df.columns:
        df_agg = df_agg.withColumn("location", F.col("district"))
    return df_agg


def compute_specialty_analytics(hosp_df):
    """
    Computes clinical department distributions, costs, and waiting profiles across specialties.
    """
    if hosp_df is None:
        return None

    return (
        hosp_df.groupBy("specialty")
        .agg(
            F.count("hospital_id").alias("center_count"),
            F.sum("available_beds").alias("specialty_avail_beds"),
            F.sum("available_icu_beds").alias("specialty_avail_icu"),
            F.round(F.avg("estimated_cost"), 0).alias("avg_estimated_cost"),
            F.min("estimated_cost").alias("min_cost"),
            F.max("estimated_cost").alias("max_cost"),
            F.round(F.avg("average_wait_time"), 1).alias("avg_wait_minutes"),
            F.round(F.avg("rating"), 2).alias("avg_rating")
        )
        .orderBy(F.col("center_count").desc(), F.col("specialty").asc())
    )


def compute_emergency_analytics(hosp_df):
    """
    Computes emergency load distribution and 24/7 trauma capability counts.
    """
    if hosp_df is None:
        return None

    return (
        hosp_df.groupBy("emergency_load", "emergency_services")
        .agg(
            F.count("hospital_id").alias("facility_count"),
            F.sum("available_icu_beds").alias("icu_surge_capacity"),
            F.round(F.avg("average_wait_time"), 1).alias("avg_triage_wait_minutes")
        )
        .orderBy(F.col("facility_count").desc())
    )


def compute_macro_kpis(hosp_df):
    """
    Calculates overall system-wide executive KPIs using Spark aggregate operations.
    """
    if hosp_df is None:
        return None

    return (
        hosp_df.agg(
            F.count("hospital_id").alias("total_hospitals"),
            F.sum("total_beds").alias("total_registered_beds"),
            F.sum("available_beds").alias("total_available_beds"),
            F.sum("icu_beds").alias("total_icu_beds"),
            F.sum("available_icu_beds").alias("total_available_icu"),
            F.round(F.avg("rating"), 2).alias("system_avg_rating"),
            F.round(F.avg("estimated_cost"), 0).alias("system_avg_cost"),
            F.round(F.avg("average_wait_time"), 1).alias("system_avg_wait_minutes"),
            F.sum(F.when(F.col("emergency_services") == "Yes", 1).otherwise(0)).alias("emergency_enabled_facilities")
        )
    )
