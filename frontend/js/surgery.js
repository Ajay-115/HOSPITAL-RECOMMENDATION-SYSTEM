/**
 * Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
 * Surgery Recommendation Module (Phase 1)
 */

document.addEventListener("DOMContentLoaded", function () {
  const surgeryGrid = document.getElementById("surgeryGrid");
  const surgeryCount = document.getElementById("surgeryCount");
  const filterForm = document.getElementById("surgeryFilterForm");
  const resetBtn = document.getElementById("resetSurgeryBtn");

  const procedureSelect = document.getElementById("filterSurgeryType");
  const specialtySelect = document.getElementById("filterSurgerySpecialty");
  const locationSelect = document.getElementById("filterSurgeryLocation");
  const budgetRange = document.getElementById("filterSurgeryBudget");
  const budgetVal = document.getElementById("budgetValDisplay");
  const maxWaitDays = document.getElementById("filterSurgeryWaitDays");
  const sortSelect = document.getElementById("sortSurgery");

  // Update budget display
  if (budgetRange && budgetVal) {
    budgetRange.addEventListener("input", function () {
      budgetVal.textContent = App.formatCurrency(this.value);
    });
  }

  // Populate dynamic procedure dropdown based on API or sample surgeries
  async function populateProcedureDropdown() {
    if (!procedureSelect) return;
    let surgeryList = [];

    const apiSurgeries = await App.apiGet("/surgeries");
    if (apiSurgeries && apiSurgeries.success && Array.isArray(apiSurgeries.data)) {
      surgeryList = apiSurgeries.data;
    } else if (window.SAMPLE_DATA) {
      surgeryList = window.SAMPLE_DATA.surgeries;
    }

    const uniqueSurgeries = [...new Set(surgeryList.map(s => s.surgery_name))].sort();
    procedureSelect.innerHTML = '<option value="All">All Procedures</option>';
    uniqueSurgeries.forEach(name => {
      const opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      procedureSelect.appendChild(opt);
    });
  }

  async function applySurgeryFilters() {
    if (!surgeryGrid) return;

    const payload = {
      surgery_name: procedureSelect ? procedureSelect.value : "All",
      specialty: specialtySelect ? specialtySelect.value : "All",
      location: locationSelect ? locationSelect.value : "All",
      district: locationSelect ? locationSelect.value : "All",
      max_budget: budgetRange ? parseInt(budgetRange.value, 10) : 400000,
      max_waiting_days: maxWaitDays ? parseInt(maxWaitDays.value, 10) : 10,
      sort_by: sortSelect ? sortSelect.value : "match"
    };

    let scored = null;

    // Try live Flask API
    const apiRes = await App.apiPost("/surgery/recommend", payload);
    if (apiRes && apiRes.success && Array.isArray(apiRes.data)) {
      scored = apiRes.data.map(s => ({
        ...s,
        score: s.match_score
      }));
    } else if (window.SAMPLE_DATA) {
      // Local fallback
      const surgeries = window.SAMPLE_DATA.surgeries;
      let filtered = surgeries.filter(s => {
        if (payload.surgery_name !== "All" && s.surgery_name !== payload.surgery_name) return false;
        if (payload.specialty !== "All" && s.specialty !== payload.specialty) return false;
        if (payload.location !== "All" && s.location !== payload.location) return false;
        if (s.estimated_cost > payload.max_budget) return false;
        if (s.waiting_days > payload.max_waiting_days) return false;
        return true;
      });

      scored = filtered.map(s => {
        const hospital = window.SAMPLE_DATA.hospitals.find(h => h.hospital_id === s.hospital_id) || {};
        const rating = hospital.rating || 4.2;
        const costFactor = Math.max(0, 100 - (s.estimated_cost / 400000) * 100);
        const waitFactor = Math.max(0, 100 - (s.waiting_days / 10) * 100);
        const slotFactor = Math.min(100, s.available_slots * 12);
        const hospitalScore = (rating / 5) * 100;
        const score = Math.round(costFactor * 0.35 + waitFactor * 0.30 + slotFactor * 0.20 + hospitalScore * 0.15);
        return {
          ...s,
          hospitalRating: rating,
          score: Math.min(99, Math.max(30, score)),
          costFactor,
          waitFactor
        };
      });

      scored.sort((a, b) => {
        if (payload.sort_by === "match") return b.score - a.score;
        if (payload.sort_by === "cost_asc") return a.estimated_cost - b.estimated_cost;
        if (payload.sort_by === "wait_asc") return a.waiting_days - b.waiting_days;
        if (payload.sort_by === "slots_desc") return b.available_slots - a.available_slots;
        return b.score - a.score;
      });
    }

    if (!scored) return;

    if (surgeryCount) {
      surgeryCount.textContent = `Showing ${scored.length} matching surgical procedures`;
    }

    if (scored.length === 0) {
      surgeryGrid.innerHTML = `
        <div class="col-12">
          <div class="empty-state">
            <i class="bi bi-heart-pulse"></i>
            <h4 class="fw-bold">No surgery slots match the selected criteria</h4>
            <p class="text-muted">Try increasing your budget range or waiting period tolerance.</p>
            <button class="btn btn-outline-primary mt-2" onclick="document.getElementById('resetSurgeryBtn').click()">
              <i class="bi bi-arrow-counterclockwise me-1"></i>Reset Surgery Filters
            </button>
          </div>
        </div>
      `;
      return;
    }

    const activeSort = payload.sort_by;
    surgeryGrid.innerHTML = scored.map((s, idx) => {
      const isTop = idx === 0 && activeSort === "match";

      return `
        <div class="col-lg-6 col-xl-4 mb-4">
          <div class="custom-card h-100 ${isTop ? 'border-primary' : ''}">
            ${isTop ? '<span class="badge bg-primary mb-2"><i class="bi bi-award-fill me-1"></i>Best Match Value</span>' : ''}
            <div class="d-flex justify-content-between align-items-start mb-2">
              <div>
                <span class="badge bg-light text-primary border">${s.specialty}</span>
                <h5 class="fw-bold mt-2 mb-1">${s.surgery_name}</h5>
                <div class="text-muted small"><i class="bi bi-building me-1"></i>${s.hospital_name}</div>
              </div>
              <div class="text-end">
                <span class="badge bg-success bg-opacity-10 text-success fs-6 fw-bold border border-success border-opacity-25 px-2 py-1">
                  ${s.score}% Match
                </span>
              </div>
            </div>

            <hr class="my-3 opacity-25">

            <div class="row g-2 mb-3 small">
              <div class="col-6">
                <span class="text-muted d-block">Estimated Cost:</span>
                <strong class="fs-6 text-dark">${App.formatCurrency(s.estimated_cost)}</strong>
              </div>
              <div class="col-6">
                <span class="text-muted d-block">Waiting Window:</span>
                <strong class="text-primary"><i class="bi bi-calendar-event me-1"></i>${s.waiting_days} ${s.waiting_days === 1 ? 'day' : 'days'}</strong>
              </div>
              <div class="col-6">
                <span class="text-muted d-block">Available Slots:</span>
                <span class="badge bg-info bg-opacity-25 text-info-emphasis">${s.available_slots} Slots Available</span>
              </div>
              <div class="col-6">
                <span class="text-muted d-block">District:</span>
                <span class="text-secondary"><i class="bi bi-geo-alt me-1"></i>${s.district || s.location}</span>
              </div>
            </div>

            <div class="mt-auto pt-2">
              <a href="hospital-details.html?id=${s.hospital_id}" class="btn btn-sm btn-outline-primary w-100">
                <i class="bi bi-hospital me-1"></i>View Hospital Facilities
              </a>
            </div>
          </div>
        </div>
      `;
    }).join("");
  }

  if (filterForm) {
    filterForm.addEventListener("submit", function (e) {
      e.preventDefault();
      applySurgeryFilters();
    });
  }

  if (procedureSelect) procedureSelect.addEventListener("change", applySurgeryFilters);
  if (specialtySelect) specialtySelect.addEventListener("change", applySurgeryFilters);
  if (locationSelect) locationSelect.addEventListener("change", applySurgeryFilters);
  if (budgetRange) budgetRange.addEventListener("change", applySurgeryFilters);
  if (maxWaitDays) maxWaitDays.addEventListener("change", applySurgeryFilters);
  if (sortSelect) sortSelect.addEventListener("change", applySurgeryFilters);

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      if (filterForm) filterForm.reset();
      if (budgetVal) budgetVal.textContent = "₹4,00,000";
      applySurgeryFilters();
    });
  }

  populateProcedureDropdown();
  applySurgeryFilters();
});
