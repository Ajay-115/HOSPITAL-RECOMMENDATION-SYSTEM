"""
MediPulse Phase 9 — Real-Time Analytics API Blueprint

Exposes:
  GET /api/realtime/resources  — latest streaming resource metrics from HDFS
"""

from flask import Blueprint, jsonify
from services.realtime_service import get_realtime_resource_metrics

realtime_bp = Blueprint("realtime", __name__)


@realtime_bp.route("/api/realtime/resources", methods=["GET"])
def get_realtime_resources():
    """
    GET /api/realtime/resources

    Returns the latest windowed resource utilization metrics per hospital and
    resource type, sourced from the Spark Structured Streaming Parquet output
    stored in HDFS.

    Response (200):
      {
        "success": true,
        "count": <int>,
        "data": [
          {
            "hospital_id": "H101",
            "hospital_name": "...",
            "location": "...",
            "resource_type": "ICU",
            "avg_utilization": 78.5,
            "avg_occupied": 15.7,
            "avg_available": 4.3,
            "max_emergency_load": "High",
            "event_count": 12,
            "window_start": "2024-01-01T00:00:00",
            "window_end": "2024-01-01T00:05:00",
            "status": "Warning"
          },
          ...
        ],
        "summary": {
          "total_records": <int>,
          "critical": <int>,
          "warning": <int>,
          "normal": <int>
        },
        "source": "hdfs_realtime_parquet",
        "cache_ttl_seconds": 10
      }

    Response (503) — HDFS unavailable:
      {
        "success": false,
        "error": "REALTIME_DATA_UNAVAILABLE",
        "message": "..."
      }
    """
    result, status_code = get_realtime_resource_metrics()
    return jsonify(result), status_code
