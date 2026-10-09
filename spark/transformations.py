"""
MediPulse Big Data Processing Engine
Spark Feature Transformations & Relational Joins (Phase 6)
"""

from pyspark.sql import functions as F


def add_bed_occupancy(df):
    """
    Computes bed occupancy percentage:
    Formula: ((total_beds - available_beds) / total_beds) * 100
    Safely handles total_beds <= 0 by defaulting to 0.0%.
    """
    if df is None:
        return None

    return df.withColumn(
        "bed_occupancy_percentage",
        F.round(
            F.when(
                F.col("total_beds") > 0,
                ((F.col("total_beds") - F.col("available_beds")) / F.col("total_beds")) * 100.0
            ).otherwise(0.0),
            1
        )
    )


def add_icu_occupancy(df):
    """
    Computes ICU bed occupancy percentage:
    Formula: ((icu_beds - available_icu_beds) / icu_beds) * 100
    Safely handles icu_beds <= 0.
    """
    if df is None:
        return None

    return df.withColumn(
        "icu_occupancy_percentage",
        F.round(
            F.when(
                F.col("icu_beds") > 0,
                ((F.col("icu_beds") - F.col("available_icu_beds")) / F.col("icu_beds")) * 100.0
            ).otherwise(0.0),
            1
        )
    )


def add_resource_utilization(df):
    """
    Computes resource utilization percentage on hospital resources:
    Formula: ((total_quantity - available_quantity) / total_quantity) * 100
    Safely handles total_quantity <= 0.
    """
    if df is None:
        return None

    return df.withColumn(
        "resource_utilization_pct",
        F.round(
            F.when(
                F.col("total_quantity") > 0,
                ((F.col("total_quantity") - F.col("available_quantity")) / F.col("total_quantity")) * 100.0
            ).otherwise(0.0),
            1
        )
    )


def add_cost_and_wait_tiers(df):
    """
    Categorizes treatment costs and waiting times into analytical tiers.
    """
    if df is None:
        return None

    return (
        df
        .withColumn(
            "cost_tier",
            F.when(F.col("estimated_cost") < 25000, F.lit("Affordable"))
            .when(F.col("estimated_cost") < 50000, F.lit("Standard"))
            .otherwise(F.lit("Premium"))
        )
        .withColumn(
            "wait_tier",
            F.when(F.col("average_wait_time") <= 25, F.lit("Rapid"))
            .when(F.col("average_wait_time") <= 45, F.lit("Moderate"))
            .otherwise(F.lit("Congested"))
        )
    )


def join_hospitals_and_resources(hosp_df, res_df):
    """
    Demonstrates Spark distributed JOIN operation between hospitals and aggregated resources.
    Groups resources by hospital_id before joining to preserve 1:1 hospital granularity.
    """
    if hosp_df is None or res_df is None:
        return None

    # Aggregate resources by hospital
    res_agg = res_df.groupBy("hospital_id").agg(
        F.count("id").alias("total_resource_types"),
        F.sum("total_quantity").alias("total_equipment_units"),
        F.sum("available_quantity").alias("available_equipment_units")
    )

    # Join on hospital_id
    joined = hosp_df.join(res_agg, on="hospital_id", how="left")

    # Add utilization of joined equipment
    joined = joined.withColumn(
        "equipment_utilization_pct",
        F.round(
            F.when(
                F.col("total_equipment_units") > 0,
                ((F.col("total_equipment_units") - F.col("available_equipment_units")) / F.col("total_equipment_units")) * 100.0
            ).otherwise(0.0),
            1
        )
    )

    return joined


def join_hospitals_and_surgeries(hosp_df, surg_df):
    """
    Demonstrates Spark distributed JOIN operation between hospitals and aggregated surgeries.
    """
    if hosp_df is None or surg_df is None:
        return None

    surg_agg = surg_df.groupBy("hospital_id").agg(
        F.count("surgery_id").alias("total_surgeries_offered"),
        F.avg("estimated_cost").alias("avg_surgery_cost"),
        F.sum("available_slots").alias("total_ot_slots_available"),
        F.avg("waiting_days").alias("avg_surgery_wait_days")
    )

    return hosp_df.join(surg_agg, on="hospital_id", how="left")
