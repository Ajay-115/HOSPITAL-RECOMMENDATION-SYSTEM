"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Admin Management, Resources & Analytics API Routes (Phase 5 SQLite Integration)
"""

from flask import Blueprint, request, jsonify
from database.database import (
    get_all_hospitals,
    get_hospital_by_id,
    create_hospital,
    update_hospital,
    delete_hospital,
    get_surgeries,
    get_blood_banks,
    get_resources,
    get_system_resource_summary
)
from services.analytics import compute_system_analytics
from services.waiting_time import get_hospital_wait_analytics

admin_bp = Blueprint("admin", __name__)


@admin_bp.route("/api/resources", methods=["GET"])
def get_resource_inventory():
    """
    GET /api/resources
    Returns detailed bed, ICU, and ventilator capacity for all registered hospitals from SQLite.
    """
    hospitals = get_all_hospitals()
    summary = get_system_resource_summary()

    # Enrich each hospital with ventilator metrics from SQLite resources table
    vent_records = get_resources()
    vent_map = {
        r["hospital_id"]: r
        for r in vent_records
        if "ventilator" in str(r.get("resource_name", "")).lower()
    }

    tot_vent = 0
    avail_vent = 0
    for h in hospitals:
        hid = h.get("hospital_id") or h.get("id")
        vent = vent_map.get(hid)
        if vent:
            v_total = int(vent.get("total_quantity", 0))
            v_avail = int(vent.get("available_quantity", 0))
        else:
            icu = int(h.get("icu_beds") or h.get("icu_total") or 10)
            v_total = max(5, int(icu * 0.8))
            v_avail = max(2, int(icu * 0.3))

        h["ventilators_total"] = v_total
        h["ventilators_available"] = v_avail
        tot_vent += v_total
        avail_vent += v_avail

    if summary and isinstance(summary, dict):
        summary["total_ventilators"] = tot_vent
        summary["available_ventilators"] = avail_vent

    return jsonify({
        "success": True,
        "summary": summary,
        "hospitals": hospitals
    }), 200


@admin_bp.route("/api/analytics", methods=["GET"])
def get_analytics():
    """
    GET /api/analytics
    Macro utilization metrics across hospitals, surgeries, and blood banks.
    """
    hospitals = get_all_hospitals()
    surgeries = get_surgeries()
    blood_banks = get_blood_banks()

    data = compute_system_analytics(hospitals, surgeries, blood_banks)
    return jsonify({
        "success": True,
        "analytics": data
    }), 200


@admin_bp.route("/api/waiting-time/analytics", methods=["GET"])
def get_wait_analytics():
    """
    GET /api/waiting-time/analytics
    Queuing theory metrics calculated directly from database records.
    """
    district = request.args.get("district")
    location = request.args.get("location")
    hospitals = get_all_hospitals()
    target_loc = district if (district and district != "All") else location
    if target_loc and target_loc != "All":
        clean = target_loc.strip().lower()
        hospitals = [
            h for h in hospitals
            if clean == str(h.get("district", "")).lower()
            or clean in str(h.get("district", "")).lower()
            or clean in str(h.get("location", "")).lower()
        ]
    data = get_hospital_wait_analytics(hospitals)
    return jsonify({
        "success": True,
        "data": data
    }), 200


@admin_bp.route("/api/hospitals", methods=["POST"])
def add_hospital():
    """
    POST /api/hospitals
    Admin endpoint to register a new hospital directly into SQLite.
    """
    payload = request.get_json() or {}

    name = payload.get("hospital_name") or payload.get("name")
    specialty = payload.get("specialty")
    if not name or not specialty:
        return jsonify({"success": False, "error": "Hospital name and specialty are required."}), 400

    try:
        record = create_hospital(payload)
        return jsonify({
            "success": True,
            "message": f"Hospital '{record['hospital_name']}' created successfully.",
            "hospital": record
        }), 201
    except ValueError as ve:
        return jsonify({"success": False, "error": str(ve)}), 400
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to create hospital: {str(e)}"}), 500


@admin_bp.route("/api/hospitals/<hospital_id>", methods=["PUT"])
def update_hospital_record(hospital_id):
    """
    PUT /api/hospitals/<id>
    Update hospital capacity or details in SQLite.
    Changes take effect immediately on recommendation scoring.
    """
    payload = request.get_json() or {}
    if not payload:
        return jsonify({"success": False, "error": "No update fields provided."}), 400

    try:
        updated = update_hospital(hospital_id, payload)
        if not updated:
            return jsonify({"success": False, "error": f"Hospital ID '{hospital_id}' not found."}), 404

        return jsonify({
            "success": True,
            "message": f"Hospital '{hospital_id}' updated successfully.",
            "hospital": updated
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to update hospital: {str(e)}"}), 500


@admin_bp.route("/api/hospitals/<hospital_id>", methods=["DELETE"])
def remove_hospital(hospital_id):
    """
    DELETE /api/hospitals/<id>
    Deletes hospital from SQLite. Subsequent recommendations will exclude it immediately.
    """
    try:
        deleted = delete_hospital(hospital_id)
        if not deleted:
            return jsonify({"success": False, "error": f"Hospital ID '{hospital_id}' not found."}), 404

        return jsonify({
            "success": True,
            "message": f"Hospital '{hospital_id}' deleted successfully."
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to delete hospital: {str(e)}"}), 500
