/**
 * Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
 * Hospital Search & Recommendation Module (Phase 3/4 Hardening)
 * 
 * All recommendation scores, distances, and factor breakdowns are retrieved
 * directly from the Python Flask backend via POST /api/recommend.
 */

let currentHospitalResults = [];

function openHospitalExplainModal(idx) {
  if (currentHospitalResults && currentHospitalResults[idx]) {
    App.showExplainableModal(currentHospitalResults[idx]);
  }
}

document.addEventListener("DOMContentLoaded", function () {
  const hospitalsGrid = document.getElementById("hospitalsGrid");
  const resultCount = document.getElementById("resultCount");
  const filterForm = document.getElementById("hospitalFilterForm");
  const resetBtn = document.getElementById("resetFilterBtn");

  // Filter form elements
  const locationSelect = document.getElementById("filterLocation");
  const specialtySelect = document.getElementById("filterSpecialty");
  const maxCostRange = document.getElementById("filterMaxCost");
  const maxCostVal = document.getElementById("maxCostValue");
  const maxWaitRange = document.getElementById("filterMaxWait");
  const maxWaitVal = document.getElementById("maxWaitValue");
  const emergencyCheck = document.getElementById("filterEmergencyOnly");
  const sortSelect = document.getElementById("sortResults");

  // Read URL search params (e.g. from homepage CTA buttons)
  const urlParams = new URLSearchParams(window.location.search);
  if (urlParams.has("specialty") && specialtySelect) {
    specialtySelect.value = urlParams.get("specialty");
  }
  const districtParam = urlParams.get("district") || urlParams.get("location");
  if (districtParam && locationSelect) {
    locationSelect.value = districtParam;
  }
  if (urlParams.has("emergency") && emergencyCheck) {
    emergencyCheck.checked = urlParams.get("emergency") === "true";
  }

  // Cost Slider Display Update
  if (maxCostRange && maxCostVal) {
    maxCostRange.addEventListener("input", function () {
      maxCostVal.textContent = App.formatCurrency(this.value);
    });
  }

  // Wait Time Slider Display Update
  if (maxWaitRange && maxWaitVal) {
    maxWaitRange.addEventListener("input", function () {
      maxWaitVal.textContent = `${this.value} mins`;
    });
  }

  // Authoritative Backend Recommendation Query
  async function applyFiltersAndRender() {
    if (!hospitalsGrid) return;

    // Show loading state
    hospitalsGrid.innerHTML = `
      <div class="col-12 text-center py-5">
        <div class="spinner-border text-primary" role="status">
          <span class="visually-hidden">Calculating recommendation scores...</span>
        </div>
        <p class="text-muted small mt-2">Computing multi-factor recommendation ranking via backend Python engine...</p>
      </div>
    `;

    const locVal = locationSelect ? locationSelect.value : "All";
    const criteria = {
      location: locVal,
      district: locVal,
      specialty: specialtySelect ? specialtySelect.value : "All",
      max_cost: maxCostRange ? parseInt(maxCostRange.value, 10) : 80000,
      max_wait: maxWaitRange ? parseInt(maxWaitRange.value, 10) : 90,
      emergency_only: emergencyCheck ? emergencyCheck.checked : false,
      sort_by: sortSelect ? sortSelect.value : "score_desc"
    };

    // Query Backend Single Source of Truth
    const result = await App.fetchRecommendations(criteria);
    App.renderBackendStatusBanner(result.isLive, "backendStatusContainer");

    currentHospitalResults = result.recommendations || [];

    // Update Result Count
    if (resultCount) {
      resultCount.textContent = `Showing ${currentHospitalResults.length} ranked hospital${currentHospitalResults.length !== 1 ? 's' : ''}`;
    }

    // Zero-Result Handling
    if (currentHospitalResults.length === 0) {
      const suggestionsHtml = (result.suggestions && result.suggestions.length > 0)
        ? `<div class="p-3 bg-light rounded text-start mt-3 border"><div class="fw-bold small mb-1"><i class="bi bi-lightbulb text-warning me-1"></i>Relaxation Suggestions:</div><ul class="mb-0 small text-muted ps-3">${result.suggestions.map(s => `<li>${s}</li>`).join('')}</ul></div>`
        : '';

      hospitalsGrid.innerHTML = `
        <div class="col-12">
          <div class="empty-state">
            <i class="bi bi-hospital text-muted"></i>
            <h4 class="fw-bold mt-2">No hospitals match your exact criteria</h4>
            <p class="text-muted">Strict eligibility filters excluded facilities that cannot satisfy the requested requirements.</p>
            ${suggestionsHtml}
            <button class="btn btn-outline-primary mt-3" onclick="document.getElementById('resetFilterBtn').click()">
              <i class="bi bi-arrow-counterclockwise me-1"></i>Reset All Filters
            </button>
          </div>
        </div>
      `;
      return;
    }

    // Render Cards using authoritative Backend values
    hospitalsGrid.innerHTML = currentHospitalResults.map((h, idx) => {
      const isTopPick = idx === 0 && criteria.sort_by === "score_desc";

      return `
        <div class="col-lg-6 col-xl-4 mb-4">
          <div class="hospital-card ${isTopPick ? 'border-primary shadow-md' : ''}" data-hospital-id="${h.id}" title="Double-click or double-tap to view full hospital details">
            ${isTopPick ? '<div class="badge bg-primary text-white py-1 px-3 position-absolute top-0 end-0 rounded-0 rounded-bottom-start small fw-bold"><i class="bi bi-star-fill me-1"></i>Top Ranked Recommendation</div>' : ''}
            
            <div class="hospital-card-header ${isTopPick ? 'pt-4' : ''}">
              <div>
                <span class="hospital-specialty-pill mb-1">${h.specialty}</span>
                <h5 class="hospital-title">${h.name}</h5>
                <div class="d-flex align-items-center gap-2 small text-muted">
                  <span><i class="bi bi-geo-alt-fill text-danger me-1"></i>${h.district || h.location} (${h.calculatedDistance.toFixed(1)} km)</span>
                  <span>&bull;</span>
                  <span class="rating-pill"><i class="bi bi-star-fill text-warning"></i>${h.rating}</span>
                </div>
              </div>

              <div class="score-badge-circle" title="Authoritative Backend Recommendation Score">
                ${h.recommendationScore}%
                <small>Match</small>
              </div>
            </div>

            <div class="hospital-card-body">
              <div class="meta-grid">
                <div class="meta-item">
                  <span class="meta-item-label">Available Beds</span>
                  <span class="meta-item-value text-primary">
                    <i class="bi bi-hospital-fill"></i> ${h.availableBeds} <small class="text-muted fw-normal">/ ${h.totalBeds}</small>
                  </span>
                </div>
                <div class="meta-item">
                  <span class="meta-item-label">ICU Availability</span>
                  <span class="meta-item-value ${h.availableIcuBeds > 5 ? 'text-success' : 'text-danger'}">
                    <i class="bi bi-heart-pulse-fill"></i> ${h.availableIcuBeds} Free <small class="text-muted fw-normal">/ ${h.icuBeds}</small>
                  </span>
                </div>
                <div class="meta-item">
                  <span class="meta-item-label">Estimated Wait</span>
                  <span class="meta-item-value">
                    ${App.getWaitTimeBadge(h.averageWaitTime)}
                  </span>
                </div>
                <div class="meta-item">
                  <span class="meta-item-label">Standard Base Cost</span>
                  <span class="meta-item-value text-dark">
                    <i class="bi bi-cash-stack text-success"></i> ${App.formatCurrency(h.estimatedCost)}
                  </span>
                </div>
              </div>

              <div class="d-flex align-items-center justify-content-between mb-2">
                <span class="small text-muted fw-semibold">Emergency Status:</span>
                ${App.getEmergencyBadge(h.emergencyLoad)}
              </div>

              <!-- Authoritative Factor Breakdown from Flask -->
              <div class="explainable-breakdown">
                <div class="explainable-title">
                  <span>Backend Factor Breakdown</span>
                  <a href="javascript:void(0)" onclick="openHospitalExplainModal(${idx})" class="text-primary text-decoration-none fw-bold">
                    <i class="bi bi-eye me-1"></i>Why this score?
                  </a>
                </div>
                <div class="factor-meter">
                  <div class="factor-label-row">
                    <span>Specialty Match (30%)</span>
                    <span class="fw-bold">${h.factors.specialtyMatch}/100</span>
                  </div>
                  <div class="progress progress-thin">
                    <div class="progress-bar bg-primary" style="width: ${h.factors.specialtyMatch}%"></div>
                  </div>
                </div>
                <div class="factor-meter">
                  <div class="factor-label-row">
                    <span>Distance & Resources (40%)</span>
                    <span class="fw-bold">${Math.round((h.factors.distance + h.factors.resourceAvailability) / 2)}/100</span>
                  </div>
                  <div class="progress progress-thin">
                    <div class="progress-bar bg-info" style="width: ${Math.round((h.factors.distance + h.factors.resourceAvailability) / 2)}%"></div>
                  </div>
                </div>
              </div>
            </div>

            <div class="hospital-card-footer">
              <a href="hospital-details.html?id=${h.id}" class="btn btn-sm btn-outline-primary w-100">
                <i class="bi bi-info-circle me-1"></i>View Full Details & Resources
              </a>
              <div class="card-dblclick-hint text-center text-muted mt-1" style="font-size: 0.72rem;">
                <i class="bi bi-cursor-fill me-1"></i>Double-click to view details
              </div>
            </div>
          </div>
        </div>
      `;
    }).join("");
  }

  // Bind change listeners
  if (filterForm) {
    filterForm.addEventListener("submit", function (e) {
      e.preventDefault();
      applyFiltersAndRender();
    });
  }

  if (locationSelect) locationSelect.addEventListener("change", applyFiltersAndRender);
  if (specialtySelect) specialtySelect.addEventListener("change", applyFiltersAndRender);
  if (maxCostRange) maxCostRange.addEventListener("change", applyFiltersAndRender);
  if (maxWaitRange) maxWaitRange.addEventListener("change", applyFiltersAndRender);
  if (emergencyCheck) emergencyCheck.addEventListener("change", applyFiltersAndRender);
  if (sortSelect) sortSelect.addEventListener("change", applyFiltersAndRender);

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      if (filterForm) filterForm.reset();
      if (maxCostVal) maxCostVal.textContent = "₹80,000";
      if (maxWaitVal) maxWaitVal.textContent = "90 mins";
      applyFiltersAndRender();
    });
  }

  // Initial execution
  applyFiltersAndRender();
  if (hospitalsGrid && App.attachHospitalCardDoubleAction) {
    App.attachHospitalCardDoubleAction(hospitalsGrid);
  }
});
