"""
MediPulse Big Data Processing Engine
Phase 8: Synthetic Hospital Resource Event Generator

Generates synthetic JSON event files simulating continuous hospital resource telemetry:
- Real-time ICU admissions and discharges
- Inpatient bed occupancy changes
- Critical ventilator usage
- Operation theatre slot status
- Blood bank stock consumption

IMPORTANT COMPLIANCE & PRIVACY NOTE:
- All generated telemetry is purely synthetic.
- No personal identifying information (PII), patient names, phone numbers,
  addresses, or medical record numbers (MRNs) are ever generated.
"""

import os
import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

from spark.streaming.streaming_schema import (
    RESOURCE_TYPES,
    HOSPITAL_IDS,
    EMERGENCY_LOADS,
    STREAMING_HDFS_INPUT,
    STREAMING_LOCAL_INPUT,
    get_streaming_schema
)

# Capacity boundaries per resource category
RESOURCE_CAPACITIES = {
    "ICU": (20, 50),
    "General Bed": (100, 300),
    "Ventilator": (10, 30),
    "Operation Theatre": (4, 12),
    "Blood Unit": (50, 200)
}


def generate_synthetic_event(
    hospital_id: Optional[str] = None,
    resource_type: Optional[str] = None,
    timestamp: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Constructs a single valid, realistic synthetic hospital resource telemetry event.
    """
    h_id = hospital_id or random.choice(HOSPITAL_IDS)
    r_type = resource_type or random.choice(RESOURCE_TYPES)
    ts = timestamp or datetime.now(timezone.utc)

    min_cap, max_cap = RESOURCE_CAPACITIES.get(r_type, (20, 100))
    total_capacity = random.randint(min_cap, max_cap)

    # Generate realistic occupancy (30% to 95% saturation)
    utilization_rate = random.uniform(0.30, 0.95)
    occupied = int(round(total_capacity * utilization_rate))
    occupied = max(0, min(occupied, total_capacity))
    available = total_capacity - occupied

    # Emergency load loosely correlated with utilization
    if utilization_rate > 0.85:
        emergency_load = random.choice(["High", "Critical"])
    elif utilization_rate > 0.60:
        emergency_load = random.choice(["Medium", "High"])
    else:
        emergency_load = random.choice(["Low", "Medium"])

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts),
        "hospital_id": h_id,
        "resource_type": r_type,
        "total_capacity": total_capacity,
        "occupied": occupied,
        "available": available,
        "emergency_load": emergency_load
    }


def generate_event_batch(
    num_events: int = 20,
    base_time: Optional[datetime] = None
) -> List[Dict[str, Any]]:
    """
    Generates a list of synthetic resource telemetry events.
    """
    events = []
    base = base_time or datetime.now(timezone.utc)
    for i in range(num_events):
        # Slightly jitter timestamps within a 30-second window
        event_time = base + timedelta(seconds=random.uniform(-15, 15))
        events.append(generate_synthetic_event(timestamp=event_time))
    return events


def write_events_to_json_file(events: List[Dict[str, Any]], filepath: str) -> str:
    """
    Writes events to a newline-delimited JSON file on the local filesystem.
    """
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    return filepath


def emit_events_to_hdfs(
    spark,
    events: List[Dict[str, Any]],
    hdfs_dir: str = STREAMING_HDFS_INPUT
) -> int:
    """
    Emits a batch of synthetic events directly to HDFS as a JSON micro-batch file
    using an active SparkSession.
    """
    # Convert string timestamps to datetime objects for StructType compatibility
    converted = []
    for ev in events:
        item = dict(ev)
        if isinstance(item["timestamp"], str):
            # Parse ISO string
            item["timestamp"] = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
        converted.append(item)

    df = spark.createDataFrame(converted, schema=get_streaming_schema())
    df.write.mode("append").json(hdfs_dir)
    return len(events)


def generate_streaming_files_series(
    target_dir: str,
    num_files: int = 3,
    events_per_file: int = 15,
    start_time: Optional[datetime] = None
) -> List[str]:
    """
    Generates a numbered series of JSON files on disk. Useful for file-based streaming.
    """
    created_files = []
    t = start_time or datetime.now(timezone.utc)
    for b in range(1, num_files + 1):
        batch_events = generate_event_batch(num_events=events_per_file, base_time=t)
        filename = f"events_batch_{b}_{int(t.timestamp())}.json"
        path = os.path.join(target_dir, filename)
        write_events_to_json_file(batch_events, path)
        created_files.append(path)
        # Advance timestamp for subsequent batch to simulate real temporal progression
        t += timedelta(seconds=65)
    return created_files
