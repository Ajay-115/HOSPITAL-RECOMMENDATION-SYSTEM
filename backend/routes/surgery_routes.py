"""
Surgery Resource & Scheduling API Routes
"""

from flask import Blueprint, request, jsonify
from services.data_loader import get_surgeries, get_hospitals

surgery_bp = Blueprint("surgeries", __name__)


@surgery_bp.route("/api/surgeries", methods=["GET"])
def list_surgeries():
    """
    GET /api/surgeries
    """
    surgeries = get_surgeries()
    specialty = request.args.get("specialty")
    location = request.args.get("location")
    district = request.args.get("district")

    results = surgeries
    if specialty and specialty != "All":
        results = [s for s in results if str(s.get("specialty", "")).lower() == specialty.lower()]
    loc_target = district if (district and district != "All") else location
    if loc_target and loc_target != "All":
        loc_clean = loc_target.strip().lower()
        results = [s for s in results if str(s.get("location", "")).lower() == loc_clean or loc_clean in str(s.get("location", "")).lower()]

    return jsonify({
        "success": True,
        "count": len(results),
        "data": results
    }), 200


@surgery_bp.route("/api/surgery/recommend", methods=["POST"])
def recommend_surgeries():
    """
    POST /api/surgery/recommend
    Request Body:
    {
      "surgery_name": "Coronary Angioplasty",
      "specialty": "Cardiology",
      "location": "Central District",
      "max_budget": 300000,
      "max_waiting_days": 5,
      "sort_by": "match"
    }
    """
    criteria = request.get_json() or {}
    surgeries = get_surgeries()
    hospitals = {h["hospital_id"]: h for h in get_hospitals()}

    name_filter = criteria.get("surgery_name", "All")
    spec_filter = criteria.get("specialty", "All")
    loc_filter = criteria.get("location", "All")
    dist_filter = criteria.get("district", "All")
    target_loc = dist_filter if dist_filter != "All" else loc_filter
    max_budget = float(criteria.get("max_budget", 1000000))
    max_days = float(criteria.get("max_waiting_days", 100))
    sort_by = criteria.get("sort_by", "match")

    scored = []
    for s in surgeries:
        if name_filter != "All" and str(s.get("surgery_name", "")).lower() != name_filter.lower():
            continue
        if spec_filter != "All" and str(s.get("specialty", "")).lower() != spec_filter.lower():
            continue
        if target_loc != "All":
            s_loc = str(s.get("location", "")).lower()
            t_loc = target_loc.strip().lower()
            if s_loc != t_loc and t_loc not in s_loc:
                continue
        cost = float(s.get("estimated_cost", 0))
        days = float(s.get("waiting_days", 0))
        if cost > max_budget or days > max_days:
            continue

        h = hospitals.get(s.get("hospital_id"), {})
        rating = float(h.get("rating", 4.2))

        cost_factor = max(0, 100 - (cost / 400000.0) * 100)
        wait_factor = max(0, 100 - (days / 10.0) * 100)
        slot_factor = min(100, float(s.get("available_slots", 1)) * 12)
        hosp_factor = (rating / 5.0) * 100

        score = round(cost_factor * 0.35 + wait_factor * 0.30 + slot_factor * 0.20 + hosp_factor * 0.15)
        score = max(30, min(99, score))

        entry = dict(s)
        entry["hospital_name"] = h.get("hospital_name", "Partner Hospital")
        entry["hospital_rating"] = rating
        entry["match_score"] = score
        entry["district"] = s.get("district") or s.get("location") or h.get("district") or h.get("location") or "Kerala"
        scored.append(entry)

    if sort_by == "cost_asc":
        scored.sort(key=lambda x: float(x.get("estimated_cost", 0)))
    elif sort_by == "wait_asc":
        scored.sort(key=lambda x: float(x.get("waiting_days", 0)))
    elif sort_by == "slots_desc":
        scored.sort(key=lambda x: float(x.get("available_slots", 0)), reverse=True)
    else:  # match
        scored.sort(key=lambda x: x["match_score"], reverse=True)

    return jsonify({
        "success": True,
        "count": len(scored),
        "data": scored
    }), 200
