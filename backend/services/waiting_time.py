"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Backend Service: Waiting Time Estimation Engine

MATHEMATICAL MODEL & QUEUING THEORY DOCUMENTATION (ACADEMIC VIVA DEFENSE)
-------------------------------------------------------------------------
Formula:
    Dynamic Wait Time (W_dynamic) = ((Q * S) / C) * Gamma_emergency
    Final Estimated Wait Time = (alpha * W_dynamic) + ((1 - alpha) * W_historical)

Where:
    - Q  = Current outpatient / triage queue length (number of waiting patients)
    - S  = Mean triage consultation / service duration (minutes per patient, default: 8 mins)
    - C  = Active triage service channels / consulting physicians (default: 4 channels)
    - Gamma_emergency = Trauma pressure multiplier:
        * Low emergency load:      1.00x
        * Medium emergency load:   1.25x
        * High emergency load:     1.60x
        * Critical trauma load:    2.20x
    - W_historical = Historical baseline average wait time for the facility/time slot (minutes)
    - alpha = Dynamic real-time blending coefficient (default: 0.75, i.e., 75% real-time, 25% historical)

Assumptions:
    1. Multi-server M/M/c queuing approximation with parallel service stations (C channels).
    2. Non-preemptive queuing discipline for standard outpatient consultations;
       emergency arrivals preempt standard triage and scale queue delays by Gamma_emergency.
    3. Patient arrival rate lambda and service rate mu are locally stationary during the evaluation interval.

Limitations:
    1. Does not account for sudden mass-casualty surge anomalies beyond the emergency load categories.
    2. Does not incorporate individual clinical acuity scoring (e.g., ESI 1-5 triage levels)
       without live Electronic Health Record (EHR) integration.
    3. THIS IS NOT A MACHINE LEARNING MODEL. It is a deterministic queuing-theory formulation
       engineered for 100% academic explainability and viva defense.
"""

EMERGENCY_LOAD_FACTORS = {
    "low": 1.00,
    "medium": 1.25,
    "high": 1.60,
    "critical": 2.20
}

DEFAULT_TRIAGE_CHANNELS = 4
DEFAULT_SERVICE_TIME = 8.0  # minutes per patient
DEFAULT_DYNAMIC_WEIGHT = 0.75  # alpha blend


def estimate_waiting_time(
    queue_length,
    avg_service_time=DEFAULT_SERVICE_TIME,
    emergency_load="Medium",
    channels=DEFAULT_TRIAGE_CHANNELS,
    historical_avg_wait=None,
    alpha=DEFAULT_DYNAMIC_WEIGHT
):
    """
    Computes deterministic waiting time estimation using M/M/c queuing approximation
    with emergency trauma scaling and historical baseline blending.

    :param queue_length: int >= 0, current patients in queue
    :param avg_service_time: float > 0, minutes per consultation
    :param emergency_load: str in ('low', 'medium', 'high', 'critical')
    :param channels: int >= 1, parallel triage doctors
    :param historical_avg_wait: optional float, historical baseline wait in minutes
    :param alpha: float in (0.0, 1.0], weight given to dynamic queue vs historical baseline
    :return: int, estimated waiting time in minutes
    """
    try:
        q = max(0, int(queue_length if queue_length is not None else 10))
    except (ValueError, TypeError):
        q = 10

    try:
        s = max(1.0, float(avg_service_time if avg_service_time is not None else DEFAULT_SERVICE_TIME))
    except (ValueError, TypeError):
        s = DEFAULT_SERVICE_TIME

    try:
        c = max(1, int(channels if channels is not None else DEFAULT_TRIAGE_CHANNELS))
    except (ValueError, TypeError):
        c = DEFAULT_TRIAGE_CHANNELS

    load_key = str(emergency_load or "medium").strip().lower()
    gamma = EMERGENCY_LOAD_FACTORS.get(load_key, 1.25)

    # Dynamic queuing formulation: (Q * S / C) * Gamma
    dynamic_wait = (q * s / float(c)) * gamma

    # If historical average is provided, blend using exponential smoothing coefficient alpha
    if historical_avg_wait is not None:
        try:
            h_wait = max(5.0, float(historical_avg_wait))
            blended = (alpha * dynamic_wait) + ((1.0 - alpha) * h_wait)
            final_wait = round(blended)
        except (ValueError, TypeError):
            final_wait = round(dynamic_wait)
    else:
        final_wait = round(dynamic_wait)

    return max(5, final_wait)


def get_hospital_wait_analytics(hospitals):
    """
    Computes system-wide queuing statistics, queue length distributions,
    and average wait times across all facilities.
    """
    if not hospitals:
        return {
            "average_wait_minutes": 0,
            "total_queue_patients": 0,
            "hospitals_analyzed": 0,
            "records": []
        }

    total_wait = 0
    total_queue = 0
    records = []

    for h in hospitals:
        try:
            q = int(h.get("queue_length", 10) or 10)
        except (ValueError, TypeError):
            q = 10

        load = str(h.get("emergency_load", "Medium"))
        hist_wait = h.get("average_wait_time", None)

        est_wait = estimate_waiting_time(
            queue_length=q,
            avg_service_time=DEFAULT_SERVICE_TIME,
            emergency_load=load,
            channels=DEFAULT_TRIAGE_CHANNELS,
            historical_avg_wait=hist_wait
        )

        total_wait += est_wait
        total_queue += q

        records.append({
            "hospital_id": h.get("hospital_id"),
            "hospital_name": h.get("hospital_name"),
            "location": h.get("location"),
            "district": h.get("district") or h.get("location"),
            "queue_length": q,
            "emergency_load": load,
            "historical_baseline_wait": hist_wait,
            "estimated_wait_time": est_wait,
            "service_channels": DEFAULT_TRIAGE_CHANNELS
        })

    avg_wait = round(total_wait / len(hospitals), 1)

    return {
        "average_wait_minutes": avg_wait,
        "total_queue_patients": total_queue,
        "hospitals_analyzed": len(hospitals),
        "records": records,
        "methodology": {
            "model_type": "Deterministic M/M/c Queuing Approximation with Emergency Load Scaling",
            "channels": DEFAULT_TRIAGE_CHANNELS,
            "avg_service_time_mins": DEFAULT_SERVICE_TIME,
            "emergency_multipliers": EMERGENCY_LOAD_FACTORS
        }
    }
