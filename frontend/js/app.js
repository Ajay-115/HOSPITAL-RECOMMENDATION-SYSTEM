/**
 * Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
 * Core Shared Application Framework & REST API Client (Phase 3/4 Hardening)
 * 
 * ARCHITECTURE PRINCIPLE:
 * The Python Flask backend (backend/services/recommendation.py) is the SINGLE
 * AUTHORITATIVE SOURCE OF TRUTH for recommendation scores, feature normalization,
 * and queuing estimations. No duplicate recommendation algorithms are maintained in JS.
 */

const App = (function () {
  const API_BASE = (typeof window !== "undefined" && window.location && window.location.protocol.startsWith("http"))
    ? `${window.location.origin}/api`
    : "http://localhost:5000/api";

  // Official weights documented for transparency (sum = 100%)
  const ALGORITHM_WEIGHTS = {
    specialtyMatch: 0.30,
    distance: 0.20,
    resourceAvailability: 0.20,
    waitingTime: 0.15,
    cost: 0.10,
    emergencyLoad: 0.05
  };

  /**
   * Generic REST API Callers
   */
  async function apiGet(endpoint, params = {}) {
    try {
      const url = new URL(API_BASE + endpoint);
      Object.keys(params).forEach(k => {
        if (params[k] !== undefined && params[k] !== null && params[k] !== "") {
          url.searchParams.append(k, params[k]);
        }
      });
      const res = await fetch(url.toString());
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.warn(`[App.apiGet] Backend unavailable for ${endpoint}:`, err.message);
      return null;
    }
  }

  async function apiPost(endpoint, body = {}) {
    try {
      const res = await fetch(API_BASE + endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        const errJson = await res.json().catch(() => ({}));
        throw new Error(errJson.error || `HTTP ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      console.warn(`[App.apiPost] Backend unavailable for ${endpoint}:`, err.message);
      return null;
    }
  }

  async function apiPut(endpoint, body = {}) {
    try {
      const res = await fetch(API_BASE + endpoint, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      });
      if (!res.ok) {
        const errJson = await res.json().catch(() => ({}));
        throw new Error(errJson.error || `HTTP ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      console.warn(`[App.apiPut] Backend unavailable for ${endpoint}:`, err.message);
      return null;
    }
  }

  async function apiDelete(endpoint) {
    try {
      const res = await fetch(API_BASE + endpoint, {
        method: "DELETE"
      });
      if (!res.ok) {
        const errJson = await res.json().catch(() => ({}));
        throw new Error(errJson.error || `HTTP ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      console.warn(`[App.apiDelete] Backend unavailable for ${endpoint}:`, err.message);
      return null;
    }
  }

  /**
   * Data Mapping Layer: Maps Backend snake_case to Clean Unified Frontend Model
   * Guarantees consistent property access across all views without mixing formats.
   */
  function normalizeHospitalModel(h) {
    if (!h) return null;
    const bd = h.factor_breakdown || h.breakdown || {};
    return {
      id: h.hospital_id || "",
      name: h.hospital_name || "Unknown Facility",
      location: h.location || "Kerala",
      district: h.district || h.location || "Kerala",
      latitude: parseFloat(h.latitude || 0),
      longitude: parseFloat(h.longitude || 0),
      specialty: h.specialty || "General Medicine",
      totalBeds: parseInt(h.total_beds || 0, 10),
      availableBeds: parseInt(h.available_beds || 0, 10),
      icuBeds: parseInt(h.icu_beds || 0, 10),
      availableIcuBeds: parseInt(h.available_icu_beds || 0, 10),
      queueLength: parseInt(h.queue_length || 0, 10),
      emergencyLoad: h.emergency_load || "Medium",
      averageWaitTime: parseInt(h.average_wait_time || 30, 10),
      estimatedCost: parseFloat(h.estimated_cost || 0),
      emergencyServices: h.emergency_services || "Yes",
      rating: parseFloat(h.rating || 4.0),
      contact: h.contact || "+91-80-2000-0000",
      address: h.address || `${h.location || "Kerala"}, Kerala`,
      ventilatorsTotal: parseInt(h.ventilators_total || 10, 10),
      ventilatorsAvailable: parseInt(h.ventilators_available || 3, 10),
      bloodBankAttached: Boolean(h.blood_bank_attached),
      // Authoritative scores directly from Backend
      recommendationScore: h.recommendation_score !== undefined ? h.recommendation_score : (h.score !== undefined ? h.score : 0),
      calculatedDistance: h.calculated_distance !== undefined ? h.calculated_distance : (h.distance !== undefined ? h.distance : 0),
      explanation: h.recommendation_explanation || h.explanation || "",
      factors: {
        specialtyMatch: bd.specialty_match !== undefined ? bd.specialty_match : 0,
        distance: bd.distance !== undefined ? bd.distance : 0,
        resourceAvailability: bd.resource_availability !== undefined ? bd.resource_availability : 0,
        waitingTime: bd.waiting_time !== undefined ? bd.waiting_time : 0,
        cost: bd.cost !== undefined ? bd.cost : 0,
        emergencyLoad: bd.emergency_load !== undefined ? bd.emergency_load : 0
      },
      raw: h
    };
  }

  /**
   * Authoritative Hospital Recommendation Service
   * Sends criteria to POST /api/recommend.
   */
  async function fetchRecommendations(criteria = {}) {
    const apiRes = await apiPost("/recommend", criteria);
    if (apiRes && apiRes.success && Array.isArray(apiRes.recommendations)) {
      return {
        isLive: true,
        count: apiRes.count,
        recommendations: apiRes.recommendations.map(normalizeHospitalModel),
        suggestions: apiRes.suggestions || []
      };
    }

    // Explicit Fallback Mode when Backend is unreachable:
    // DOES NOT calculate a fake/divergent score in JavaScript.
    // Explicitly flags demonstration data status.
    const fallbackList = window.SAMPLE_DATA ? window.SAMPLE_DATA.hospitals : [];
    return {
      isLive: false,
      count: fallbackList.length,
      recommendations: fallbackList.map(h => normalizeHospitalModel({
        ...h,
        recommendation_score: 70, // Fixed demo baseline indicator
        calculated_distance: 5.0,
        recommendation_explanation: "Demonstration catalog record. Backend server offline.",
        factor_breakdown: {
          specialty_match: 70,
          distance: 70,
          resource_availability: 70,
          waiting_time: 70,
          cost: 70,
          emergency_load: 70
        }
      })),
      suggestions: ["Backend API is offline or unreachable. Please launch 'python app.py' to activate real-time recommendation scoring."]
    };
  }

  /**
   * Helper to format currency
   */
  function formatCurrency(amount) {
    return "₹" + Number(amount || 0).toLocaleString("en-IN");
  }

  /**
   * Render emergency badge
   */
  function getEmergencyBadge(load) {
    const l = (load || "").toLowerCase();
    if (l === "low") return `<span class="badge badge-emergency-low"><i class="bi bi-shield-check me-1"></i>Low Load</span>`;
    if (l === "medium") return `<span class="badge badge-emergency-medium"><i class="bi bi-activity me-1"></i>Moderate</span>`;
    if (l === "high") return `<span class="badge badge-emergency-high"><i class="bi bi-exclamation-triangle me-1"></i>High Load</span>`;
    return `<span class="badge badge-emergency-critical"><i class="bi bi-exclamation-octagon me-1"></i>Critical Load</span>`;
  }

  /**
   * Render wait time badge
   */
  function getWaitTimeBadge(minutes) {
    const m = parseInt(minutes || 30, 10);
    if (m <= 25) return `<span class="text-success fw-bold"><i class="bi bi-clock me-1"></i>~${m} mins</span>`;
    if (m <= 45) return `<span class="text-warning fw-bold"><i class="bi bi-clock me-1"></i>~${m} mins</span>`;
    return `<span class="text-danger fw-bold"><i class="bi bi-clock me-1"></i>~${m} mins</span>`;
  }

  /**
   * Render backend connection status alert banner
   */
  function renderBackendStatusBanner(isLive, containerId) {
    const target = document.getElementById(containerId);
    if (!target) return;
    if (isLive) {
      target.innerHTML = `
        <div class="alert alert-success py-2 px-3 small mb-3 border-success border-opacity-25 d-flex align-items-center justify-content-between">
          <span><i class="bi bi-check-circle-fill text-success me-2"></i><strong>Connected to Flask REST API:</strong> Recommendations computed via Python scoring engine.</span>
          <span class="badge bg-success">Live Engine</span>
        </div>
      `;
    } else {
      target.innerHTML = `
        <div class="alert alert-warning py-2 px-3 small mb-3 border-warning border-opacity-50 d-flex align-items-center justify-content-between">
          <span><i class="bi bi-exclamation-triangle-fill text-warning me-2"></i><strong>Backend unavailable:</strong> Showing demo / offline data. Start <code>python app.py</code> for live SQLite database & ranking.</span>
          <span class="badge bg-warning text-dark">Demo / Offline Data</span>
        </div>
      `;
    }
  }

  /**
   * Open Explainable Recommendation Breakdown Modal
   * Reads authoritative factor breakdown from the hospital model (NO CLIENT-SIDE RESCORING).
   */
  function showExplainableModal(hospitalData) {
    if (!hospitalData) return;
    const h = normalizeHospitalModel(hospitalData.raw || hospitalData);

    const modalTitle = document.getElementById("explainModalTitle");
    const modalBody = document.getElementById("explainModalBody");

    if (modalTitle && modalBody) {
      modalTitle.innerHTML = `<i class="bi bi-cpu me-2 text-primary"></i>Recommendation Score Analysis: ${h.name}`;
      modalBody.innerHTML = `
        <div class="row align-items-center mb-4 p-3 bg-light rounded-3">
          <div class="col-sm-4 text-center border-end">
            <div class="display-4 fw-bold text-primary">${h.recommendationScore}%</div>
            <div class="text-muted small fw-semibold text-uppercase">Overall Match Score</div>
            <div class="badge bg-success mt-1">Backend Python Scoring Engine</div>
          </div>
          <div class="col-sm-8 ps-sm-4">
            <h6 class="fw-bold mb-1">Mathematical Ranking Formula (Normalized 0-100)</h6>
            <p class="small text-muted mb-2">
              <code>Score = (S &times; 30%) + (D &times; 20%) + (A &times; 20%) + (W &times; 15%) + (C &times; 10%) + (E &times; 5%)</code>
            </p>
            <div class="small mb-1"><i class="bi bi-geo-alt text-danger me-1"></i>Calculated Distance: <strong>${h.calculatedDistance.toFixed(1)} km</strong> from searched sector</div>
            ${h.explanation ? `<div class="p-2 bg-white rounded border small text-dark mt-2"><i class="bi bi-chat-left-quote me-1 text-primary"></i><strong>Recommendation Summary:</strong> ${h.explanation}</div>` : ''}
          </div>
        </div>

        <h6 class="fw-bold mb-3"><i class="bi bi-bar-chart-steps me-1 text-primary"></i>Individual Factor Breakdown (Normalized 0-100)</h6>

        <div class="mb-3">
          <div class="d-flex justify-content-between small fw-semibold mb-1">
            <span>1. Medical Specialty Match (Weight: 30%)</span>
            <span class="text-primary">${h.factors.specialtyMatch}/100</span>
          </div>
          <div class="progress progress-thin">
            <div class="progress-bar bg-primary" style="width: ${h.factors.specialtyMatch}%"></div>
          </div>
          <div class="small text-muted mt-1">Specialty: <strong>${h.specialty}</strong></div>
        </div>

        <div class="mb-3">
          <div class="d-flex justify-content-between small fw-semibold mb-1">
            <span>2. Distance Proximity (Weight: 20%)</span>
            <span class="text-info">${h.factors.distance}/100</span>
          </div>
          <div class="progress progress-thin">
            <div class="progress-bar bg-info" style="width: ${h.factors.distance}%"></div>
          </div>
          <div class="small text-muted mt-1">Distance factor normalized from ${h.calculatedDistance.toFixed(1)} km</div>
        </div>

        <div class="mb-3">
          <div class="d-flex justify-content-between small fw-semibold mb-1">
            <span>3. Bed & ICU Availability (Weight: 20%)</span>
            <span class="text-success">${h.factors.resourceAvailability}/100</span>
          </div>
          <div class="progress progress-thin">
            <div class="progress-bar bg-success" style="width: ${h.factors.resourceAvailability}%"></div>
          </div>
          <div class="small text-muted mt-1">${h.availableBeds} beds free of ${h.totalBeds}, ${h.availableIcuBeds} ICU units free</div>
        </div>

        <div class="mb-3">
          <div class="d-flex justify-content-between small fw-semibold mb-1">
            <span>4. Estimated Waiting Time (Weight: 15%)</span>
            <span class="text-warning">${h.factors.waitingTime}/100</span>
          </div>
          <div class="progress progress-thin">
            <div class="progress-bar bg-warning" style="width: ${h.factors.waitingTime}%"></div>
          </div>
          <div class="small text-muted mt-1">Expected wait: ${h.averageWaitTime} mins (Queuing formula) | Queue: ${h.queueLength} patients</div>
        </div>

        <div class="mb-3">
          <div class="d-flex justify-content-between small fw-semibold mb-1">
            <span>5. Estimated Cost Affordability (Weight: 10%)</span>
            <span class="text-secondary">${h.factors.cost}/100</span>
          </div>
          <div class="progress progress-thin">
            <div class="progress-bar bg-secondary" style="width: ${h.factors.cost}%"></div>
          </div>
          <div class="small text-muted mt-1">Standard inpatient base rate: ${formatCurrency(h.estimatedCost)}</div>
        </div>

        <div class="mb-3">
          <div class="d-flex justify-content-between small fw-semibold mb-1">
            <span>6. Emergency Load Headroom (Weight: 5%)</span>
            <span class="text-danger">${h.factors.emergencyLoad}/100</span>
          </div>
          <div class="progress progress-thin">
            <div class="progress-bar bg-danger" style="width: ${h.factors.emergencyLoad}%"></div>
          </div>
          <div class="small text-muted mt-1">Current emergency triage load: ${h.emergencyLoad}</div>
        </div>

        <div class="alert alert-info py-2 px-3 small mt-3 mb-0">
          <i class="bi bi-info-circle me-1"></i>
          <strong>Single Source of Truth:</strong> Values computed by Python backend (backend/services/recommendation.py). No client-side recalculations.
        </div>
      `;

      const modalElement = document.getElementById("explainModal");
      if (modalElement && window.bootstrap) {
        const bsModal = new bootstrap.Modal(modalElement);
        bsModal.show();
      }
    }
  }

  /**
   * Dedicated handler for hospital card double-click (desktop) and double-tap (touch)
   * Opens hospital details page (/pages/hospital-details.html?id=<id>).
   *
   * Ignores interaction with child buttons, links, inputs, selects, checkboxes, toggles.
   */
  function attachHospitalCardDoubleAction(container, getUrl) {
    if (!container) return;

    const resolveUrl = typeof getUrl === "function"
      ? getUrl
      : (id) => {
          const prefix = (typeof window !== "undefined" && window.location && window.location.pathname.includes("/pages/")) ? "" : "pages/";
          return `${prefix}hospital-details.html?id=${encodeURIComponent(id)}`;
        };

    const isInteractive = (target) => {
      return !!target.closest("a, button, input, select, textarea, label, .form-check, [onclick], .explainable-title a");
    };

    // Desktop: dblclick
    container.addEventListener("dblclick", function (e) {
      if (isInteractive(e.target)) return;
      const card = e.target.closest(".hospital-card");
      if (!card) return;
      const hospId = card.getAttribute("data-hospital-id");
      if (hospId) {
        window.location.href = resolveUrl(hospId);
      }
    });

    // Touch: double-tap detection (interval between 50ms and 500ms)
    let lastTapTime = 0;
    let lastTapCard = null;

    container.addEventListener("touchend", function (e) {
      if (isInteractive(e.target)) return;
      const card = e.target.closest(".hospital-card");
      if (!card) return;

      const currentTime = Date.now();
      const tapInterval = currentTime - lastTapTime;

      if (card === lastTapCard && tapInterval >= 50 && tapInterval <= 500) {
        e.preventDefault();
        lastTapTime = 0;
        lastTapCard = null;
        const hospId = card.getAttribute("data-hospital-id");
        if (hospId) {
          window.location.href = resolveUrl(hospId);
        }
      } else {
        lastTapTime = currentTime;
        lastTapCard = card;
      }
    });
  }

  return {
    weights: ALGORITHM_WEIGHTS,
    normalizeHospitalModel,
    fetchRecommendations,
    formatCurrency,
    getEmergencyBadge,
    getWaitTimeBadge,
    renderBackendStatusBanner,
    showExplainableModal,
    attachHospitalCardDoubleAction,
    apiGet,
    apiPost,
    apiPut,
    apiDelete
  };
})();

// Attach globally
window.App = App;
