"""
Blood Bank Resource & Donor API Routes
"""

from flask import Blueprint, request, jsonify
from services.data_loader import get_blood_banks

blood_bp = Blueprint("blood", __name__)


@blood_bp.route("/api/blood", methods=["GET"])
def list_blood_banks():
    """
    GET /api/blood
    """
    blood_banks = get_blood_banks()
    group = request.args.get("group")
    if group:
        group = group.replace(" ", "+").strip()
    location = request.args.get("location")
    district = request.args.get("district")

    results = blood_banks
    if group and group != "All":
        results = [b for b in results if str(b.get("blood_group", "")).upper() == group.upper()]
    if district and district != "All":
        d_clean = district.strip().lower()
        results = [b for b in results if str(b.get("district", "")).lower() == d_clean or d_clean in str(b.get("location", "")).lower()]
    elif location and location != "All":
        loc_clean = location.strip().lower()
        results = [b for b in results if str(b.get("district", "")).lower() == loc_clean or str(b.get("location", "")).lower() == loc_clean or loc_clean in str(b.get("location", "")).lower()]

    return jsonify({
        "success": True,
        "count": len(results),
        "data": results
    }), 200


@blood_bp.route("/api/blood/search", methods=["POST"])
def search_blood():
    """
    POST /api/blood/search
    Request Body:
    {
      "blood_group": "O+",
      "location": "Thrissur",
      "units_required": 2
    }
    """
    criteria = request.get_json() or {}
    group = criteria.get("blood_group", "All")
    location = criteria.get("location", "All")
    district = criteria.get("district", "All")
    units_req = int(criteria.get("units_required", 1))

    banks = get_blood_banks()
    matched = []
    for b in banks:
        if group != "All" and str(b.get("blood_group", "")).upper() != group.upper():
            continue
        if district != "All":
            d_clean = district.strip().lower()
            if str(b.get("district", "")).lower() != d_clean and d_clean not in str(b.get("location", "")).lower():
                continue
        elif location != "All":
            loc_clean = location.strip().lower()
            if str(b.get("district", "")).lower() != loc_clean and str(b.get("location", "")).lower() != loc_clean and loc_clean not in str(b.get("location", "")).lower():
                continue

        units = int(b.get("available_units", 0))
        status = "Adequate"
        if units == 0:
            status = "Out of Stock"
        elif units < units_req or units < 5:
            status = "Critical Shortage"
        elif units < 15:
            status = "Low Reserve"

        entry = dict(b)
        entry["stock_status"] = status
        entry["meets_requirement"] = units >= units_req
        matched.append(entry)

    matched.sort(key=lambda x: int(x.get("available_units", 0)), reverse=True)

    return jsonify({
        "success": True,
        "count": len(matched),
        "criteria": criteria,
        "data": matched
    }), 200
