"""
MediPulse Big Data Processing Engine
Phase 8: Spark Structured Streaming Event Schemas & Constants
"""

import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

# HDFS Streaming Paths
HDFS_NAMENODE = "hdfs://localhost:9000"
STREAMING_HDFS_INPUT = f"{HDFS_NAMENODE}/medipulse/raw/streaming"
STREAMING_HDFS_CHECKPOINT = f"{HDFS_NAMENODE}/medipulse/checkpoints/resource_streaming"
STREAMING_HDFS_OUTPUT = f"{HDFS_NAMENODE}/medipulse/analytics/realtime_resource_metrics"

# Local Fallback Paths for Development & Offline Testing
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STREAMING_LOCAL_INPUT = os.path.join(BASE_DIR, "spark", "streaming_input")
STREAMING_LOCAL_CHECKPOINT = os.path.join(BASE_DIR, "spark", "streaming_checkpoint")
STREAMING_LOCAL_OUTPUT = os.path.join(BASE_DIR, "spark", "streaming_output")

# Standard Domain Constants
RESOURCE_TYPES = [
    "ICU",
    "General Bed",
    "Ventilator",
    "Operation Theatre",
    "Blood Unit"
]

HOSPITAL_IDS = [f"H{i:04d}" for i in range(1, 1001)] + [f"H{i}" for i in range(101, 125)]


EMERGENCY_LOADS = ["Low", "Medium", "High", "Critical"]


def get_streaming_schema() -> StructType:
    """
    Returns the explicit StructType schema for synthetic hospital resource events.

    Schema:
      event_id: Unique event identifier (UUID)
      timestamp: Event generation timestamp
      hospital_id: Associated hospital identifier (H101-H124)
      resource_type: Resource category (ICU, General Bed, Ventilator, OT, Blood)
      total_capacity: Total units installed/configured
      occupied: Currently occupied or in-use units
      available: Available units ready for allocation
      emergency_load: Current emergency saturation level
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("hospital_id", StringType(), True),
        StructField("resource_type", StringType(), True),
        StructField("total_capacity", IntegerType(), True),
        StructField("occupied", IntegerType(), True),
        StructField("available", IntegerType(), True),
        StructField("emergency_load", StringType(), True),
    ])
