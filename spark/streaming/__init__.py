"""
MediPulse Big Data Processing Engine
Phase 8: Spark Structured Streaming Layer (File-Based Simulation)
"""

from spark.streaming.streaming_schema import (
    get_streaming_schema,
    STREAMING_HDFS_INPUT,
    STREAMING_HDFS_CHECKPOINT,
    STREAMING_HDFS_OUTPUT,
    RESOURCE_TYPES,
    HOSPITAL_IDS
)
from spark.streaming.event_generator import (
    generate_synthetic_event,
    generate_event_batch,
    emit_events_to_hdfs
)
from spark.streaming.streaming_pipeline import (
    build_resource_streaming_pipeline,
    start_streaming_query,
    run_streaming_simulation
)

__all__ = [
    "get_streaming_schema",
    "STREAMING_HDFS_INPUT",
    "STREAMING_HDFS_CHECKPOINT",
    "STREAMING_HDFS_OUTPUT",
    "RESOURCE_TYPES",
    "HOSPITAL_IDS",
    "generate_synthetic_event",
    "generate_event_batch",
    "emit_events_to_hdfs",
    "build_resource_streaming_pipeline",
    "start_streaming_query",
    "run_streaming_simulation"
]
