"""
MediPulse Big Data Processing Engine
Spark Data Cleaning & Validation Layer (Phase 6)
"""

from pyspark.sql import functions as F


def clean_hospitals(df):
    """
    Cleans and validates the hospitals DataFrame using Spark distributed operations.

    DOCUMENTED CLEANING RULES:
    1. Null / Empty String Pruning: Drops rows where primary hospital_id or hospital_name is missing or whitespace.
    2. Deduplication: Eliminates duplicate records sharing the same hospital_id using dropDuplicates().
    3. String Trimming: Trims whitespace from all categorical text columns (name, location, specialty).
    4. Capacity Constraints:
       - total_beds: Clamped to at least 1. Missing values imputed to baseline 50.
       - available_beds: Bounded strictly between 0 and total_beds (cannot have negative or overflow vacancies).
       - icu_beds: Clamped to non-negative integers.
       - available_icu_beds: Bounded strictly between 0 and icu_beds.
    5. Cost & Triage Bounds:
       - estimated_cost: Clamped to non-negative values.
       - average_wait_time: Bounded to minimum clinically realistic triage of 5 minutes.
       - queue_length: Clamped to non-negative patient count.
    6. Categorical Normalization:
       - emergency_services: Standardized to 'Yes' / 'No'.
       - emergency_load: Standardized to valid levels ('Low', 'Medium', 'High', 'Critical'), default 'Medium'.
       - rating: Clamped strictly between 1.0 and 5.0.
    """
    if df is None:
        return None

    # 1. Filter out null or empty hospital_id or hospital_name
    cleaned = df.filter(
        F.col("hospital_id").isNotNull() &
        (F.length(F.trim(F.col("hospital_id"))) > 0) &
        F.col("hospital_name").isNotNull() &
        (F.length(F.trim(F.col("hospital_name"))) > 0)
    )

    # 2. Deduplicate on primary key
    cleaned = cleaned.dropDuplicates(["hospital_id"])

    # 3. String trimming & normalization
    cleaned = (
        cleaned
        .withColumn("hospital_id", F.trim(F.col("hospital_id")))
        .withColumn("hospital_name", F.trim(F.col("hospital_name")))
        .withColumn("location", F.trim(F.col("location")))
        .withColumn("specialty", F.trim(F.col("specialty")))
    )
    if "district" in cleaned.columns:
        cleaned = cleaned.withColumn("district", F.trim(F.col("district")))

    # 4. Total and available beds bounding
    cleaned = cleaned.withColumn(
        "total_beds",
        F.when(F.col("total_beds").isNull() | (F.col("total_beds") < 1), F.lit(50))
        .otherwise(F.col("total_beds"))
    )

    cleaned = cleaned.withColumn(
        "available_beds",
        F.when(F.col("available_beds").isNull() | (F.col("available_beds") < 0), F.lit(0))
        .when(F.col("available_beds") > F.col("total_beds"), F.col("total_beds"))
        .otherwise(F.col("available_beds"))
    )

    # 5. ICU total and available bounding
    cleaned = cleaned.withColumn(
        "icu_beds",
        F.when(F.col("icu_beds").isNull() | (F.col("icu_beds") < 0), F.lit(0))
        .otherwise(F.col("icu_beds"))
    )

    cleaned = cleaned.withColumn(
        "available_icu_beds",
        F.when(F.col("available_icu_beds").isNull() | (F.col("available_icu_beds") < 0), F.lit(0))
        .when(F.col("available_icu_beds") > F.col("icu_beds"), F.col("icu_beds"))
        .otherwise(F.col("available_icu_beds"))
    )

    # 6. Cost, wait time, queue length
    cleaned = cleaned.withColumn(
        "estimated_cost",
        F.when(F.col("estimated_cost").isNull() | (F.col("estimated_cost") < 0), F.lit(0))
        .otherwise(F.col("estimated_cost"))
    )

    cleaned = cleaned.withColumn(
        "average_wait_time",
        F.when(F.col("average_wait_time").isNull() | (F.col("average_wait_time") < 5), F.lit(5))
        .otherwise(F.col("average_wait_time"))
    )

    cleaned = cleaned.withColumn(
        "queue_length",
        F.when(F.col("queue_length").isNull() | (F.col("queue_length") < 0), F.lit(0))
        .otherwise(F.col("queue_length"))
    )

    # 7. Categorical & Rating normalization
    valid_loads = ["Low", "Medium", "High", "Critical"]
    cleaned = cleaned.withColumn(
        "emergency_load",
        F.when(F.col("emergency_load").isin(valid_loads), F.col("emergency_load"))
        .otherwise(F.lit("Medium"))
    )

    cleaned = cleaned.withColumn(
        "emergency_services",
        F.when(F.lower(F.trim(F.col("emergency_services"))) == "yes", F.lit("Yes"))
        .otherwise(F.lit("No"))
    )

    cleaned = cleaned.withColumn(
        "rating",
        F.when(F.col("rating").isNull() | (F.col("rating") < 1.0), F.lit(1.0))
        .when(F.col("rating") > 5.0, F.lit(5.0))
        .otherwise(F.col("rating"))
    )

    return cleaned


def clean_surgeries(df):
    """
    Cleans and validates the surgeries DataFrame.

    DOCUMENTED CLEANING RULES:
    1. Null surgery_id or hospital_id rows dropped.
    2. Deduplicated on surgery_id.
    3. Trim string columns.
    4. estimated_cost clamped >= 0.
    5. waiting_days clamped >= 0.
    6. available_slots clamped >= 0.
    """
    if df is None:
        return None

    cleaned = df.filter(
        F.col("surgery_id").isNotNull() &
        (F.length(F.trim(F.col("surgery_id"))) > 0) &
        F.col("hospital_id").isNotNull()
    )

    cleaned = cleaned.dropDuplicates(["surgery_id"])

    cleaned = (
        cleaned
        .withColumn("surgery_id", F.trim(F.col("surgery_id")))
        .withColumn("hospital_id", F.trim(F.col("hospital_id")))
        .withColumn("surgery_name", F.trim(F.col("surgery_name")))
        .withColumn("specialty", F.trim(F.col("specialty")))
        .withColumn("location", F.trim(F.col("location")))
    )

    cleaned = cleaned.withColumn(
        "estimated_cost",
        F.when(F.col("estimated_cost").isNull() | (F.col("estimated_cost") < 0), F.lit(0))
        .otherwise(F.col("estimated_cost"))
    )

    cleaned = cleaned.withColumn(
        "waiting_days",
        F.when(F.col("waiting_days").isNull() | (F.col("waiting_days") < 0), F.lit(0))
        .otherwise(F.col("waiting_days"))
    )

    cleaned = cleaned.withColumn(
        "available_slots",
        F.when(F.col("available_slots").isNull() | (F.col("available_slots") < 0), F.lit(0))
        .otherwise(F.col("available_slots"))
    )

    return cleaned


def clean_blood_banks(df):
    """
    Cleans and validates the blood banks DataFrame.

    DOCUMENTED CLEANING RULES:
    1. Null blood_bank_id dropped.
    2. Deduplicated on blood_bank_id.
    3. Blood group uppercase & trimmed.
    4. available_units clamped >= 0.
    """
    if df is None:
        return None

    cleaned = df.filter(
        F.col("blood_bank_id").isNotNull() &
        (F.length(F.trim(F.col("blood_bank_id"))) > 0)
    )

    cleaned = cleaned.dropDuplicates(["blood_bank_id"])

    cleaned = (
        cleaned
        .withColumn("blood_bank_id", F.trim(F.col("blood_bank_id")))
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("location", F.trim(F.col("location")))
        .withColumn("blood_group", F.upper(F.trim(F.col("blood_group"))))
    )

    cleaned = cleaned.withColumn(
        "available_units",
        F.when(F.col("available_units").isNull() | (F.col("available_units") < 0), F.lit(0))
        .otherwise(F.col("available_units"))
    )

    return cleaned


def clean_resources(df):
    """
    Cleans and validates the hospital resources DataFrame.

    DOCUMENTED CLEANING RULES:
    1. Null resource id dropped.
    2. Deduplicated on id.
    3. total_quantity clamped >= 0.
    4. available_quantity bounded between 0 and total_quantity.
    """
    if df is None:
        return None

    cleaned = df.filter(
        F.col("id").isNotNull() &
        (F.length(F.trim(F.col("id"))) > 0) &
        F.col("hospital_id").isNotNull()
    )

    cleaned = cleaned.dropDuplicates(["id"])

    cleaned = (
        cleaned
        .withColumn("id", F.trim(F.col("id")))
        .withColumn("hospital_id", F.trim(F.col("hospital_id")))
        .withColumn("resource_name", F.trim(F.col("resource_name")))
        .withColumn("resource_type", F.trim(F.col("resource_type")))
    )

    cleaned = cleaned.withColumn(
        "total_quantity",
        F.when(F.col("total_quantity").isNull() | (F.col("total_quantity") < 0), F.lit(0))
        .otherwise(F.col("total_quantity"))
    )

    cleaned = cleaned.withColumn(
        "available_quantity",
        F.when(F.col("available_quantity").isNull() | (F.col("available_quantity") < 0), F.lit(0))
        .when(F.col("available_quantity") > F.col("total_quantity"), F.col("total_quantity"))
        .otherwise(F.col("available_quantity"))
    )

    return cleaned
