"""
Hospital Resource & Recommendation API Routes
"""

from flask import Blueprint, request, jsonify
from services.data_loader import get_hospitals, get_surgeries, get_blood_banks
from services.recommendation import rank_hospitals, compute_hospital_score

hospital_bp = Blueprint("hospitals", __name__)


@hospital_bp.route("/api/hospitals", methods=["GET"])
def list_hospitals():
    """
    GET /api/hospitals
    Query params: specialty, location, emergency_only
    """
    hospitals = get_hospitals()
    specialty = request.args.get("specialty")
    location = request.args.get("location")
    district = request.args.get("district")
    emergency = request.args.get("emergency")

    results = hospitals
    if specialty and specialty != "All":
        results = [h for h in results if str(h.get("specialty", "")).lower() == specialty.lower()]
    if district and district != "All":
        d_clean = district.strip().lower()
        results = [
            h for h in results
            if str(h.get("district", "")).lower() == d_clean or d_clean in str(h.get("location", "")).lower()
        ]
    elif location and location != "All":
        loc_clean = location.strip().lower()
        results = [
            h for h in results
            if str(h.get("district", "")).lower() == loc_clean or str(h.get("location", "")).lower() == loc_clean or loc_clean in str(h.get("location", "")).lower()
        ]
    if emergency and emergency.lower() == "true":
        results = [h for h in results if str(h.get("emergency_services", "")).lower() == "yes"]

    return jsonify({
        "success": True,
        "count": len(results),
        "data": results
    }), 200


@hospital_bp.route("/api/hospitals/<hospital_id>", methods=["GET"])
def get_hospital_detail(hospital_id):
    """
    GET /api/hospitals/<id>
    """
    req_id = str(hospital_id).strip()
    hospitals = get_hospitals()
    hospital = next((h for h in hospitals if str(h.get("hospital_id", "")).upper() == req_id.upper()), None)
    
    if not hospital and req_id.upper().startswith("H") and req_id[1:].isdigit():
        num = int(req_id[1:])
        alt_id = f"H{num:04d}" if len(req_id) != 5 else f"H{num}"
        hospital = next((h for h in hospitals if str(h.get("hospital_id", "")).upper() == alt_id.upper()), None)

    if not hospital:
        return jsonify({
            "success": False,
            "error": f"Hospital with ID '{hospital_id}' not found"
        }), 404

    actual_id = str(hospital.get("hospital_id") or hospital.get("id", ""))
    surgeries = [s for s in get_surgeries() if str(s.get("hospital_id", "")).upper() in (req_id.upper(), actual_id.upper())]
    blood_banks = [b for b in get_blood_banks() if str(b.get("location", "")).lower() == str(hospital.get("location", "")).lower()]

    score_result = compute_hospital_score(hospital, {
        "specialty": hospital.get("specialty"),
        "location": hospital.get("location")
    })

    resp_hospital = dict(hospital)
    if req_id.upper().startswith("H") and req_id.upper() != actual_id.upper():
        resp_hospital["hospital_id"] = req_id
        resp_hospital["id"] = req_id

    return jsonify({
        "success": True,
        "data": {
            "hospital": resp_hospital,
            "baseline_recommendation": score_result,
            "available_surgeries": surgeries,
            "district_blood_banks": blood_banks
        }
    }), 200


@hospital_bp.route("/api/recommend", methods=["POST"])
def get_recommendations():
    """
    POST /api/recommend
    Request Body JSON:
    {
      "location": "Central District",
      "specialty": "Cardiology",
      "max_cost": 60000,
      "max_wait": 45,
      "emergency_only": false,
      "min_available_beds": 5,
      "min_available_icu": 2,
      "sort_by": "score_desc"
    }
    """
    criteria = request.get_json()
    if criteria is None:
        criteria = {}

    hospitals = get_hospitals()
    result = rank_hospitals(hospitals, criteria)

    if not result.get("success", False):
        return jsonify(result), 400

    return jsonify(result), 200

