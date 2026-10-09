"""
MediPulse Big Data Processing Engine
Surgery, Blood Bank & Combined Recommendation Analytics (Phase 6)
"""

from pyspark.sql import functions as F


def compute_surgery_analytics(surg_df):
    """
    Computes surgery procedure counts, costs, and availability across clinical specialties.
    """
    if surg_df is None:
        return None

    return (
        surg_df.groupBy("specialty")
        .agg(
            F.count("surgery_id").alias("surgery_types_offered"),
            F.round(F.avg("estimated_cost"), 0).alias("avg_surgery_cost"),
            F.min("estimated_cost").alias("min_surgery_cost"),
            F.max("estimated_cost").alias("max_surgery_cost"),
            F.round(F.avg("waiting_days"), 1).alias("avg_waiting_days"),
            F.sum("available_slots").alias("total_ot_slots_open")
        )
        .orderBy(F.col("surgery_types_offered").desc(), F.col("avg_surgery_cost").desc())
    )


def compute_blood_analytics(blood_df, low_stock_threshold=15):
    """
    Computes blood group availability distributions, regional reserves, and low-stock facility alerts.
    """
    if blood_df is None:
        return None

    # Group availability
    group_summary = (
        blood_df.groupBy("blood_group")
        .agg(
            F.count("blood_bank_id").alias("reporting_centers"),
            F.sum("available_units").alias("total_available_units"),
            F.round(F.avg("available_units"), 1).alias("avg_units_per_center"),
            F.min("available_units").alias("min_reserve_units"),
            F.max("available_units").alias("max_reserve_units"),
            F.sum(
                F.when(F.col("available_units") < low_stock_threshold, 1).otherwise(0)
            ).alias("low_stock_centers_count")
        )
        .orderBy(F.col("total_available_units").desc())
    )

    # District availability
    district_summary = (
        blood_df.groupBy("location")
        .agg(
            F.count("blood_bank_id").alias("center_count"),
            F.sum("available_units").alias("total_district_units"),
            F.round(F.avg("available_units"), 1).alias("avg_district_units")
        )
        .orderBy(F.col("total_district_units").desc())
    )

    return group_summary, district_summary


def compute_combined_hospital_summary(hosp_df, res_df, surg_df):
    """
    Performs multi-table relational joins to generate a comprehensive
    macro analytics DataFrame combining beds, ICUs, equipment, and surgical capacity.
    """
    if hosp_df is None:
        return None

    # 1. Base hospital metrics
    cols = [
        "hospital_id",
        "hospital_name",
        "location",
        "specialty",
        "rating",
        "total_beds",
        "available_beds",
        "icu_beds",
        "available_icu_beds",
        "queue_length",
        "emergency_load",
        "emergency_services",
        "average_wait_time",
        "estimated_cost"
    ]
    if "district" in hosp_df.columns:
        cols.insert(3, "district")

    base = (
        hosp_df.select(*cols)
        .withColumn(
            "bed_occupancy_percentage",
            F.round(
                F.when(
                    F.col("total_beds") > 0,
                    ((F.col("total_beds") - F.col("available_beds")) / F.col("total_beds")) * 100.0
                ).otherwise(0.0),
                1
            )
        )
        .withColumn(
            "icu_occupancy_percentage",
            F.round(
                F.when(
                    F.col("icu_beds") > 0,
                    ((F.col("icu_beds") - F.col("available_icu_beds")) / F.col("icu_beds")) * 100.0
                ).otherwise(0.0),
                1
            )
        )
    )

    # 2. Join aggregated resources
    if res_df is not None:
        res_agg = res_df.groupBy("hospital_id").agg(
            F.sum("total_quantity").alias("total_equipment_installed"),
            F.sum("available_quantity").alias("available_equipment_ready")
        ).withColumn(
            "resource_utilization_percentage",
            F.round(
                F.when(
                    F.col("total_equipment_installed") > 0,
                    ((F.col("total_equipment_installed") - F.col("available_equipment_ready")) / F.col("total_equipment_installed")) * 100.0
                ).otherwise(0.0),
                1
            )
        )
        base = base.join(res_agg, on="hospital_id", how="left")

    # 3. Join aggregated surgeries
    if surg_df is not None:
        surg_agg = surg_df.groupBy("hospital_id").agg(
            F.count("surgery_id").alias("active_surgeries_count"),
            F.sum("available_slots").alias("open_ot_slots")
        )
        base = base.join(surg_agg, on="hospital_id", how="left")

    return base.orderBy(F.col("rating").desc(), F.col("available_beds").desc())
