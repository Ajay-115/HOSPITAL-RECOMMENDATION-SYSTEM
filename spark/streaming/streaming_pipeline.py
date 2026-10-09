"""
MediPulse Big Data Processing Engine
Phase 8: Spark Structured Streaming Pipeline Driver

Architecture:
  Synthetic Event Files (JSON)
          ↓
  Spark Structured Streaming (spark.readStream)
          ↓
  Cleaning + Validation (null checks, capacity invariants)
          ↓
  Feature Engineering (utilization_percentage) + Watermark
          ↓
  1-Minute Tumbling Window Aggregation (hospital_id, resource_type)
          ↓
  Real-Time Metrics Parquet Output (HDFS append mode)
          ↓
  HDFS Checkpoint Storage
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from pyspark.sql.functions import (
    col,
    when,
    round as spark_round,
    window,
    avg,
    max,
    count
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.spark_session import get_spark_session, stop_spark_session
from spark.streaming.streaming_schema import (
    get_streaming_schema,
    STREAMING_HDFS_INPUT,
    STREAMING_HDFS_CHECKPOINT,
    STREAMING_HDFS_OUTPUT,
    STREAMING_LOCAL_INPUT,
    STREAMING_LOCAL_CHECKPOINT,
    STREAMING_LOCAL_OUTPUT
)
from spark.streaming.event_generator import (
    generate_event_batch,
    emit_events_to_hdfs,
    generate_streaming_files_series
)


def build_resource_streaming_pipeline(
    spark,
    input_path: str = STREAMING_HDFS_INPUT,
    watermark_delay: str = "1 minute",
    window_duration: str = "1 minute"
):
    """
    Constructs the end-to-end Spark Structured Streaming transformation pipeline:
    1. File-based stream ingestion with explicit StructType schema
    2. Data validation & bounds filtering
    3. Real-time utilization percentage calculation
    4. Watermarking on timestamp
    5. 1-minute tumbling window aggregation by hospital_id and resource_type
    """
    schema = get_streaming_schema()

    # 1. Ingestion: Monitor directory for arriving JSON micro-batches
    raw_stream = (
        spark.readStream
        .schema(schema)
        .json(input_path)
    )

    # 2. Validation: Filter out malformed records and negative/overflow capacities
    valid_stream = raw_stream.filter(
        col("hospital_id").isNotNull() &
        col("resource_type").isNotNull() &
        col("timestamp").isNotNull() &
        (col("occupied") >= 0) &
        (col("available") >= 0) &
        (col("total_capacity") >= 0) &
        (col("occupied") <= col("total_capacity")) &
        (col("available") <= col("total_capacity"))
    )

    # 3. Transformation: Compute resource utilization percentage
    enriched_stream = valid_stream.withColumn(
        "utilization_percentage",
        when(
            col("total_capacity") > 0,
            spark_round((col("occupied") / col("total_capacity")) * 100.0, 2)
        ).otherwise(0.0)
    )

    # 4. Watermarking: Bound state store retention and handle late data
    watermarked_stream = enriched_stream.withWatermark("timestamp", watermark_delay)

    # 5. Window Aggregation: 1-minute tumbling windows grouped by hospital and resource
    windowed_metrics = watermarked_stream.groupBy(
        window(col("timestamp"), window_duration),
        col("hospital_id"),
        col("resource_type")
    ).agg(
        spark_round(avg("utilization_percentage"), 2).alias("avg_utilization"),
        spark_round(avg("occupied"), 1).alias("avg_occupied"),
        spark_round(avg("available"), 1).alias("avg_available"),
        max("emergency_load").alias("max_emergency_load"),
        count("*").alias("event_count")
    )

    # 6. Flatten window struct into top-level columns for Parquet storage
    flattened_stream = windowed_metrics.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("hospital_id"),
        col("resource_type"),
        col("avg_utilization"),
        col("avg_occupied"),
        col("avg_available"),
        col("max_emergency_load"),
        col("event_count")
    )

    return flattened_stream


def start_streaming_query(
    streaming_df,
    output_path: str = STREAMING_HDFS_OUTPUT,
    checkpoint_path: str = STREAMING_HDFS_CHECKPOINT,
    query_name: str = "medipulse-resource-stream",
    trigger_processing_time: Optional[str] = "2 seconds",
    trigger_available_now: bool = False
):
    """
    Configures and starts the Structured Streaming query writing Parquet outputs.
    """
    writer = (
        streaming_df.writeStream
        .queryName(query_name)
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
    )

    if trigger_available_now:
        writer = writer.trigger(availableNow=True)
    elif trigger_processing_time:
        writer = writer.trigger(processingTime=trigger_processing_time)

    query = writer.start()
    return query


def run_streaming_simulation(
    duration_seconds: int = 15,
    num_batches: int = 3,
    events_per_batch: int = 15,
    source: str = "hdfs",
    spark=None
) -> Dict[str, Any]:
    """
    Executes a complete, bounded simulation of the Spark Structured Streaming pipeline:
    1. Ingests or generates synthetic events
    2. Starts Structured Streaming query
    3. Feeds event micro-batches over time
    4. Logs streaming status and query progress
    5. Gracefully stops query after duration_seconds
    """
    own_spark = False
    if spark is None:
        spark = get_spark_session("MediPulse-Resource-Streaming")
        own_spark = True

    is_hdfs = (source.lower() == "hdfs")
    input_path = STREAMING_HDFS_INPUT if is_hdfs else STREAMING_LOCAL_INPUT
    output_path = STREAMING_HDFS_OUTPUT if is_hdfs else STREAMING_LOCAL_OUTPUT
    checkpoint_path = STREAMING_HDFS_CHECKPOINT if is_hdfs else STREAMING_LOCAL_CHECKPOINT

    print("==================================================================")
    print(" [STREAM] Starting MediPulse Spark Structured Streaming (Phase 8) ")
    print("==================================================================")
    print(f"[STREAM] Spark version: {spark.version}")
    print(f"[STREAM] Input: {input_path}")
    print(f"[STREAM] Output: {output_path}")
    print(f"[STREAM] Checkpoint: {checkpoint_path}")

    # Ensure local directories exist if running locally
    if not is_hdfs:
        os.makedirs(input_path, exist_ok=True)
        os.makedirs(output_path, exist_ok=True)
        os.makedirs(checkpoint_path, exist_ok=True)

    # 1. Pre-generate initial event batch with advancing timestamps
    print("[STREAM] Generating synthetic telemetry event batches...")
    now = datetime.now(timezone.utc)
    for b in range(1, num_batches + 1):
        batch_time = now + timedelta(seconds=(b - 1) * 65)
        batch = generate_event_batch(num_events=events_per_batch, base_time=batch_time)
        if is_hdfs:
            emit_events_to_hdfs(spark, batch, hdfs_dir=input_path)
        else:
            filename = f"batch_{b}_{int(batch_time.timestamp())}.json"
            from spark.streaming.event_generator import write_events_to_json_file
            write_events_to_json_file(batch, os.path.join(input_path, filename))
    print(f"[STREAM] Dispatched {num_batches} micro-batches ({num_batches * events_per_batch} events).")

    # 2. Build streaming pipeline
    streaming_df = build_resource_streaming_pipeline(
        spark=spark,
        input_path=input_path,
        watermark_delay="1 minute",
        window_duration="1 minute"
    )

    # 3. Start streaming query
    query = start_streaming_query(
        streaming_df=streaming_df,
        output_path=output_path,
        checkpoint_path=checkpoint_path,
        query_name="medipulse-resource-stream",
        trigger_processing_time="2 seconds"
    )
    print(f"[STREAM] Streaming query started: {query.name} (id: {query.id})")

    # 4. Monitor streaming for duration_seconds
    start_t = time.time()
    processed_batches = 0
    try:
        while time.time() - start_t < duration_seconds:
            if not query.isActive:
                break
            recent_progress = query.lastProgress
            if recent_progress:
                batch_id = recent_progress.get("batchId", processed_batches)
                num_input_rows = recent_progress.get("numInputRows", 0)
                print(f"[STREAM] Batch processed: Batch #{batch_id}, Input Rows: {num_input_rows}")
                processed_batches += 1
            time.sleep(2)

        # Emit one final future event batch to push watermark and finalize windows
        future_time = now + timedelta(seconds=(num_batches + 2) * 65)
        final_batch = generate_event_batch(num_events=events_per_batch, base_time=future_time)
        if is_hdfs:
            emit_events_to_hdfs(spark, final_batch, hdfs_dir=input_path)
        else:
            filename = f"batch_finalize_{int(future_time.timestamp())}.json"
            from spark.streaming.event_generator import write_events_to_json_file
            write_events_to_json_file(final_batch, os.path.join(input_path, filename))

        query.processAllAvailable()
        time.sleep(2)

    finally:
        print("[STREAM] Stopping streaming query...")
        if query.isActive:
            query.stop()
        print(f"[STREAM] Streaming test completed in {round(time.time() - start_t, 2)}s")

        if own_spark:
            stop_spark_session(spark)

    return {
        "success": True,
        "query_name": query.name,
        "is_hdfs": is_hdfs,
        "input_path": input_path,
        "output_path": output_path,
        "batches_processed": processed_batches
    }


if __name__ == "__main__":
    src = os.getenv("MEDIPULSE_DATA_SOURCE", "hdfs").lower()
    run_streaming_simulation(duration_seconds=12, source=src)
