"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Backend Service: Advanced Explainable Multi-Factor Recommendation Engine (Phase 3)

SCIENTIFIC METHODOLOGY & ALGORITHM SPECIFICATION
------------------------------------------------
1. Multi-Criteria Linear Weighting:
   Final Score = w_s * S + w_d * D + w_a * A + w_w * W + w_c * C + w_e * E

   Weights:
   - Specialty match (w_s):        0.30 (30%)
   - Distance proximity (w_d):     0.20 (20%)
   - Resource availability (w_a):  0.20 (20%)
   - Waiting time (w_w):           0.15 (15%)
   - Treatment cost (w_c):         0.10 (10%)
   - Emergency load headroom (w_e):0.05 (5%)
   Sum of weights = 1.00 (100%)

2. Strict Medical Eligibility Filtering:
   - Non-negotiable medical specialties: hospitals not certified in the requested
     specialty are filtered out before scoring.
   - Emergency services requirement: if emergency care is required, facilities without
     24/7 emergency accreditation or in complete critical lockdown are excluded.
   - Resource thresholds: minimum free general beds or ICU beds.

3. Feature Normalization:
   Every factor is normalized to a continuous [0, 100] interval using bounded linear scaling.
"""

import math

# Strict weights configuration (Sum = 1.00)
RECOMMENDATION_WEIGHTS = {
    "specialty_match": 0.30,
    "distance": 0.20,
    "resource_availability": 0.20,
    "waiting_time": 0.15,
    "cost": 0.10,
    "emergency_load": 0.05
}

# Verify weights sum to 100%
assert abs(sum(RECOMMENDATION_WEIGHTS.values()) - 1.0) < 1e-6, "Weights must sum to 1.0"

# Regional district distance lookup matrix (in km)
DISTRICT_DISTANCES = {
    "Central District": { "Central District": 2.1, "North Zone": 8.4, "East Metro": 6.8, "South Zone": 7.2, "West District": 5.9 },
    "North Zone": { "Central District": 8.4, "North Zone": 2.5, "East Metro": 11.2, "South Zone": 15.6, "West District": 9.8 },
    "East Metro": { "Central District": 6.8, "North Zone": 11.2, "East Metro": 2.2, "South Zone": 9.4, "West District": 12.3 },
    "South Zone": { "Central District": 7.2, "North Zone": 15.6, "East Metro": 9.4, "South Zone": 2.0, "West District": 8.5 },
    "West District": { "Central District": 5.9, "North Zone": 9.8, "East Metro": 12.3, "South Zone": 8.5, "West District": 2.3 }
}

# Regional cost bounds for normalization
MIN_COST_CEILING = 10000.0  # Regional lowest typical inpatient cost
MAX_COST_CEILING = 80000.0  # Regional standard upper cost ceiling


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate great-circle distance between two GPS coordinates in kilometers.
    """
    try:
        R = 6371.0  # Earth's radius in kilometers
        dlat = math.radians(float(lat2) - float(lat1))
        dlon = math.radians(float(lon2) - float(lon1))
        a = math.sin(dlat / 2.0) ** 2 + math.cos(math.radians(float(lat1))) * math.cos(math.radians(float(lat2))) * math.sin(dlon / 2.0) ** 2
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return round(R * c, 2)
    except (ValueError, TypeError):
        return 5.0


def calculate_distance(user_location, hospital):
    """
    Computes distance using geographic coordinates if available,
    otherwise resolves via regional district distance matrix.
    """
    if not user_location:
        return 4.5

    # Check for coordinate dictionary
    if isinstance(user_location, dict) and "latitude" in user_location and "longitude" in user_location:
        try:
            h_lat = float(hospital.get("latitude", 0))
            h_lon = float(hospital.get("longitude", 0))
            if h_lat != 0 and h_lon != 0:
                return haversine_distance(user_location["latitude"], user_location["longitude"], h_lat, h_lon)
        except (ValueError, TypeError):
            pass

    # District zone matrix lookup
    user_zone = str(user_location).strip()
    h_zone = str(hospital.get("location", "")).strip()

    if user_zone in DISTRICT_DISTANCES and h_zone in DISTRICT_DISTANCES[user_zone]:
        return DISTRICT_DISTANCES[user_zone][h_zone]

    # Substring matching fallback
    for zone in DISTRICT_DISTANCES:
        if zone.lower() in user_zone.lower():
            if h_zone in DISTRICT_DISTANCES[zone]:
                return DISTRICT_DISTANCES[zone][h_zone]

    return 2.5 if user_zone.lower() == h_zone.lower() else 8.5


def validate_criteria(criteria):
    """
    Validates input criteria for recommendation requests.
    Returns: (is_valid: bool, sanitized_criteria: dict, errors: list)
    """
    if not isinstance(criteria, dict):
        return False, {}, ["Input criteria must be a valid JSON object."]

    sanitized = {}
    errors = []

    # 1. Location
    loc = criteria.get("location")
    if loc is not None:
        if isinstance(loc, (str, dict)):
            sanitized["location"] = loc
        else:
            errors.append("Field 'location' must be a district string or {latitude, longitude} object.")
    else:
        sanitized["location"] = "Central District"

    dist = criteria.get("district")
    if dist is not None:
        if isinstance(dist, str):
            sanitized["district"] = dist.strip()
        else:
            errors.append("Field 'district' must be a string.")

    # 2. Specialty
    spec = criteria.get("specialty", "All")
    if isinstance(spec, str):
        sanitized["specialty"] = spec.strip()
    else:
        errors.append("Field 'specialty' must be a string.")

    # 3. Maximum Cost
    cost = criteria.get("max_cost")
    if cost is not None:
        try:
            cost_val = float(cost)
            if cost_val < 0:
                errors.append("Field 'max_cost' cannot be negative.")
            else:
                sanitized["max_cost"] = cost_val
        except (ValueError, TypeError):
            errors.append("Field 'max_cost' must be a valid numeric value.")
    else:
        sanitized["max_cost"] = 1000000.0

    # 4. Maximum Wait
    wait = criteria.get("max_wait")
    if wait is not None:
        try:
            wait_val = float(wait)
            if wait_val < 0:
                errors.append("Field 'max_wait' cannot be negative.")
            else:
                sanitized["max_wait"] = wait_val
        except (ValueError, TypeError):
            errors.append("Field 'max_wait' must be a valid numeric value.")
    else:
        sanitized["max_wait"] = 1000.0

    # 5. Emergency Only
    em = criteria.get("emergency_only")
    if em is not None:
        sanitized["emergency_only"] = bool(em)
    else:
        sanitized["emergency_only"] = False

    # 6. Minimum Resource Constraints
    min_beds = criteria.get("min_available_beds")
    if min_beds is not None:
        try:
            sanitized["min_available_beds"] = max(0, int(min_beds))
        except (ValueError, TypeError):
            errors.append("Field 'min_available_beds' must be an integer.")
    else:
        sanitized["min_available_beds"] = 0

    min_icu = criteria.get("min_available_icu")
    if min_icu is not None:
        try:
            sanitized["min_available_icu"] = max(0, int(min_icu))
        except (ValueError, TypeError):
            errors.append("Field 'min_available_icu' must be an integer.")
    else:
        sanitized["min_available_icu"] = 0

    # 7. Sorting
    sort_by = criteria.get("sort_by", "score_desc")
    valid_sorts = ["score_desc", "distance_asc", "cost_asc", "wait_asc", "beds_desc"]
    if sort_by in valid_sorts:
        sanitized["sort_by"] = sort_by
    else:
        sanitized["sort_by"] = "score_desc"

    return len(errors) == 0, sanitized, errors


def normalize_features(hospital, criteria, distance):
    """
    Normalizes all 6 recommendation factors strictly to 0-100 scales.
    Handles missing data safely using domain-grounded defaults.
    """
    # 1. Specialty Match Normalization (0-100)
    req_spec = criteria.get("specialty", "All")
    hosp_spec = str(hospital.get("specialty", "")).strip()

    if not req_spec or req_spec == "All":
        norm_specialty = 80  # General matching overview baseline
    elif req_spec.lower() == hosp_spec.lower():
        norm_specialty = 100  # Exact match
    else:
        norm_specialty = 0  # Incompatible specialty

    # 2. Distance Normalization (0-100)
    # 0 km -> 100, 20 km -> 0; linear decay, clamped to [0, 100]
    norm_distance = max(0, min(100, round(100.0 - (distance / 20.0) * 100.0)))

    # 3. Resource Availability Normalization (0-100)
    try:
        tot_beds = float(hospital.get("total_beds", 100) or 100)
        avail_beds = float(hospital.get("available_beds", 0) or 0)
        tot_icu = float(hospital.get("icu_beds", 20) or 20)
        avail_icu = float(hospital.get("available_icu_beds", 0) or 0)
    except (ValueError, TypeError):
        tot_beds, avail_beds, tot_icu, avail_icu = 100.0, 10.0, 20.0, 2.0

    bed_ratio = (avail_beds / tot_beds) if tot_beds > 0 else 0.0
    icu_ratio = (avail_icu / tot_icu) if tot_icu > 0 else 0.0
    # Blend: 60% bed availability + 40% ICU reserve scaled by capacity headroom
    raw_res = (bed_ratio * 60.0 + icu_ratio * 40.0) * 1.5
    norm_resource = max(0, min(100, round(raw_res)))

    # 4. Waiting Time Normalization (0-100)
    # Inverted: <= 10 mins -> 100, >= 90 mins -> 0
    try:
        avg_wait = float(hospital.get("average_wait_time", 40) or 40)
    except (ValueError, TypeError):
        avg_wait = 40.0

    if avg_wait <= 10.0:
        norm_wait = 100
    elif avg_wait >= 90.0:
        norm_wait = 0
    else:
        norm_wait = round(100.0 - ((avg_wait - 10.0) / 80.0) * 100.0)
    norm_wait = max(0, min(100, norm_wait))

    # 5. Cost Affordability Normalization (0-100)
    # Inverted: <= MIN_COST_CEILING (10k) -> 100, >= MAX_COST_CEILING (80k) -> 0
    try:
        cost = float(hospital.get("estimated_cost", 40000) or 40000)
    except (ValueError, TypeError):
        cost = 40000.0

    if cost <= MIN_COST_CEILING:
        norm_cost = 100
    elif cost >= MAX_COST_CEILING:
        norm_cost = 0
    else:
        norm_cost = round(100.0 - ((cost - MIN_COST_CEILING) / (MAX_COST_CEILING - MIN_COST_CEILING)) * 100.0)
    norm_cost = max(0, min(100, norm_cost))

    # 6. Emergency Load Normalization (0-100)
    load = str(hospital.get("emergency_load", "Medium")).strip().lower()
    if load == "low":
        norm_emergency = 100
    elif load == "medium":
        norm_emergency = 75
    elif load == "high":
        norm_emergency = 45
    elif load == "critical":
        norm_emergency = 15
    else:
        norm_emergency = 70

    return {
        "specialty_match": norm_specialty,
        "distance": norm_distance,
        "resource_availability": norm_resource,
        "waiting_time": norm_wait,
        "cost": norm_cost,
        "emergency_load": norm_emergency
    }


def generate_explanation(hospital, total_score, breakdown, distance):
    """
    Generates human-readable, transparent justification for the recommendation score.
    """
    spec = hospital.get("specialty", "Medical Care")
    beds = hospital.get("available_beds", 0)
    icus = hospital.get("available_icu_beds", 0)
    wait = hospital.get("average_wait_time", 35)
    load = hospital.get("emergency_load", "Moderate")

    highlights = []
    if breakdown["specialty_match"] == 100:
        highlights.append(f"Certified Center of Excellence in {spec}")
    if breakdown["distance"] >= 80:
        highlights.append(f"close proximity ({distance:.1f} km)")
    else:
        highlights.append(f"located {distance:.1f} km away")

    if icus >= 8:
        highlights.append(f"ample critical care capacity ({icus} free ICUs, {beds} beds)")
    else:
        highlights.append(f"{beds} beds and {icus} ICUs available")

    if wait <= 25:
        highlights.append(f"short expected wait of ~{wait} mins")
    else:
        highlights.append(f"expected wait of ~{wait} mins under {load} trauma pressure")

    return f"Ranked with {total_score}% match score: " + "; ".join(highlights) + "."


def compute_hospital_score(hospital, criteria=None, weights=None):
    """
    Computes normalized score and factor breakdown for a single hospital.
    """
    if criteria is None:
        criteria = {}
    if weights is None:
        weights = RECOMMENDATION_WEIGHTS

    dist = calculate_distance(criteria.get("location"), hospital)
    breakdown = normalize_features(hospital, criteria, dist)

    raw_total = (
        breakdown["specialty_match"] * weights["specialty_match"] +
        breakdown["distance"] * weights["distance"] +
        breakdown["resource_availability"] * weights["resource_availability"] +
        breakdown["waiting_time"] * weights["waiting_time"] +
        breakdown["cost"] * weights["cost"] +
        breakdown["emergency_load"] * weights["emergency_load"]
    )

    total_score = max(5, min(99, round(raw_total)))
    explanation = generate_explanation(hospital, total_score, breakdown, dist)

    return {
        "total_score": total_score,
        "distance": round(dist, 1),
        "breakdown": breakdown,
        "explanation": explanation
    }


def rank_hospitals(hospitals, criteria=None):
    """
    Applies strict medical eligibility filters, calculates explainable scores,
    and returns ranked hospitals along with zero-result diagnostics.
    """
    if criteria is None:
        criteria = {}

    is_valid, validated_criteria, validation_errors = validate_criteria(criteria)
    if not is_valid:
        return {
            "success": False,
            "error": "Validation failed: " + "; ".join(validation_errors),
            "recommendations": [],
            "count": 0
        }

    crit = validated_criteria
    req_spec = crit.get("specialty", "All")
    req_district = crit.get("district")
    max_cost = crit.get("max_cost", 1000000.0)
    max_wait = crit.get("max_wait", 1000.0)
    emergency_only = crit.get("emergency_only", False)
    min_beds = crit.get("min_available_beds", 0)
    min_icu = crit.get("min_available_icu", 0)
    sort_by = crit.get("sort_by", "score_desc")

    eligible = []
    rejection_reasons = {"specialty_mismatch": 0, "district_mismatch": 0, "cost_exceeded": 0, "wait_exceeded": 0, "no_emergency": 0, "insufficient_beds": 0, "insufficient_icu": 0}

    for h in hospitals:
        # STRICT FILTER 0: District compatibility (if explicitly requested)
        if req_district and req_district != "All":
            h_dist = str(h.get("district", "")).strip().lower()
            h_loc = str(h.get("location", "")).strip().lower()
            target_dist = req_district.strip().lower()
            if target_dist != h_dist and target_dist not in h_loc:
                rejection_reasons["district_mismatch"] += 1
                continue

        # STRICT FILTER 1: Specialty compatibility
        h_spec = str(h.get("specialty", "")).strip()
        if req_spec and req_spec != "All" and h_spec.lower() != req_spec.lower():
            rejection_reasons["specialty_mismatch"] += 1
            continue

        # STRICT FILTER 2: Emergency requirement
        if emergency_only:
            has_em = str(h.get("emergency_services", "")).strip().lower() == "yes"
            em_load = str(h.get("emergency_load", "")).strip().lower()
            if not has_em or em_load == "critical":
                rejection_reasons["no_emergency"] += 1
                continue

        # STRICT FILTER 3: Budget maximum
        try:
            h_cost = float(h.get("estimated_cost", 0))
            if h_cost > max_cost:
                rejection_reasons["cost_exceeded"] += 1
                continue
        except (ValueError, TypeError):
            pass

        # STRICT FILTER 4: Waiting time maximum
        try:
            h_wait = float(h.get("average_wait_time", 0))
            if h_wait > max_wait:
                rejection_reasons["wait_exceeded"] += 1
                continue
        except (ValueError, TypeError):
            pass

        # STRICT FILTER 5: Minimum available beds
        try:
            h_beds = int(h.get("available_beds", 0))
            if h_beds < min_beds:
                rejection_reasons["insufficient_beds"] += 1
                continue
        except (ValueError, TypeError):
            pass

        # STRICT FILTER 6: Minimum available ICU beds
        try:
            h_icus = int(h.get("available_icu_beds", 0))
            if h_icus < min_icu:
                rejection_reasons["insufficient_icu"] += 1
                continue
        except (ValueError, TypeError):
            pass

        score_res = compute_hospital_score(h, crit)
        entry = dict(h)
        entry["recommendation_score"] = score_res["total_score"]
        entry["calculated_distance"] = score_res["distance"]
        entry["factor_breakdown"] = score_res["breakdown"]
        entry["recommendation_explanation"] = score_res["explanation"]
        eligible.append(entry)

    # Sorting
    if sort_by == "distance_asc":
        eligible.sort(key=lambda x: x["calculated_distance"])
    elif sort_by == "cost_asc":
        eligible.sort(key=lambda x: float(x.get("estimated_cost", 0)))
    elif sort_by == "wait_asc":
        eligible.sort(key=lambda x: float(x.get("average_wait_time", 0)))
    elif sort_by == "beds_desc":
        eligible.sort(key=lambda x: float(x.get("available_beds", 0)), reverse=True)
    else:  # default score_desc
        eligible.sort(key=lambda x: x["recommendation_score"], reverse=True)

    # Zero result handling & helpful diagnostic hints
    suggestions = []
    if len(eligible) == 0:
        if rejection_reasons["cost_exceeded"] > 0:
            suggestions.append(f"Consider increasing maximum cost ceiling (excluded {rejection_reasons['cost_exceeded']} facilities).")
        if rejection_reasons["wait_exceeded"] > 0:
            suggestions.append(f"Consider relaxing waiting tolerance (excluded {rejection_reasons['wait_exceeded']} facilities).")
        if rejection_reasons["no_emergency"] > 0:
            suggestions.append("Uncheck 'Emergency Care Only' if condition is non-critical to access outpatient facilities.")
        if rejection_reasons["specialty_mismatch"] > 0 and req_spec != "All":
            suggestions.append(f"No hospital found specializing in '{req_spec}' with current constraints. Try searching for 'General Medicine' or expanding regional zones.")
        if not suggestions:
            suggestions.append("Try relaxing resource filters or selecting 'All' for specialty and location.")

    msg = f"Found {len(eligible)} matching hospital(s)." if len(eligible) > 0 else "No hospitals match the given criteria."

    return {
        "success": True,
        "count": len(eligible),
        "message": msg,
        "criteria_applied": crit,
        "recommendations": eligible,
        "suggestions": suggestions
    }
