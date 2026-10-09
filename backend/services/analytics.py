"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Backend Service: System Resource Analytics Engine
"""


def compute_system_analytics(hospitals, surgeries, blood_banks):
    """
    Compute macro-level healthcare capacity and utilization metrics.
    """
    tot_hosp = len(hospitals)
    tot_beds = sum(int(h.get("total_beds", 0) or 0) for h in hospitals)
    avail_beds = sum(int(h.get("available_beds", 0) or 0) for h in hospitals)
    tot_icu = sum(int(h.get("icu_beds", 0) or 0) for h in hospitals)
    avail_icu = sum(int(h.get("available_icu_beds", 0) or 0) for h in hospitals)

    bed_utilization = round(((tot_beds - avail_beds) / tot_beds * 100), 1) if tot_beds > 0 else 0
    icu_utilization = round(((tot_icu - avail_icu) / tot_icu * 100), 1) if tot_icu > 0 else 0

    tot_blood = sum(int(b.get("available_units", 0) or 0) for b in blood_banks)

    # Emergency load counts
    emergency_dist = {"Low": 0, "Medium": 0, "High": 0, "Critical": 0}
    for h in hospitals:
        load = str(h.get("emergency_load", "Medium")).capitalize()
        if load in emergency_dist:
            emergency_dist[load] += 1
        else:
            emergency_dist["Medium"] += 1

    # Specialty distribution
    specialty_dist = {}
    for h in hospitals:
        s = h.get("specialty", "General")
        specialty_dist[s] = specialty_dist.get(s, 0) + 1

    return {
        "overview": {
            "total_hospitals": tot_hosp,
            "total_beds": tot_beds,
            "available_beds": avail_beds,
            "bed_utilization_pct": bed_utilization,
            "total_icu_beds": tot_icu,
            "available_icu_beds": avail_icu,
            "icu_utilization_pct": icu_utilization,
            "total_blood_units": tot_blood,
            "total_surgeries_registered": len(surgeries)
        },
        "emergency_load_distribution": emergency_dist,
        "specialty_distribution": specialty_dist
    }
