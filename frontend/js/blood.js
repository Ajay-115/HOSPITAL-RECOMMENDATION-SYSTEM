/**
 * Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
 * Blood Availability & Donation Module (Phase 1)
 */

document.addEventListener("DOMContentLoaded", function () {
  const bloodTableBody = document.getElementById("bloodTableBody");
  const bloodCountDisplay = document.getElementById("bloodCountDisplay");
  const filterForm = document.getElementById("bloodFilterForm");
  const resetBtn = document.getElementById("resetBloodBtn");

  const groupSelect = document.getElementById("filterBloodGroup");
  const locationSelect = document.getElementById("filterBloodLocation");
  const unitsInput = document.getElementById("filterUnitsRequired");

  // Donors Container
  const donorsTableBody = document.getElementById("donorsTableBody");

  function getStatusPill(units, required) {
    if (units === 0) {
      return `<span class="stock-pill stock-critical"><i class="bi bi-x-circle me-1"></i>Out of Stock</span>`;
    }
    if (units < required || units < 5) {
      return `<span class="stock-pill stock-critical"><i class="bi bi-exclamation-octagon me-1"></i>Critical Shortage</span>`;
    }
    if (units < 15) {
      return `<span class="stock-pill stock-low"><i class="bi bi-exclamation-triangle me-1"></i>Low Reserve</span>`;
    }
    return `<span class="stock-pill stock-adequate"><i class="bi bi-check-circle me-1"></i>Adequate Reserve</span>`;
  }

  async function applyBloodFilters() {
    if (!bloodTableBody) return;

    const selectedGroup = groupSelect ? groupSelect.value : "All";
    const selectedLoc = locationSelect ? locationSelect.value : "All";
    const reqUnits = unitsInput ? parseInt(unitsInput.value, 10) || 1 : 1;

    let filtered = null;

    // Try live Flask API
    const apiRes = await App.apiPost("/blood/search", {
      blood_group: selectedGroup,
      location: selectedLoc,
      district: selectedLoc,
      units_required: reqUnits
    });

    if (apiRes && apiRes.success && Array.isArray(apiRes.data)) {
      filtered = apiRes.data.map(b => ({
        ...b,
        stockStatus: b.stock_status
      }));
    } else if (window.SAMPLE_DATA) {
      // Local fallback
      const bloodBanks = window.SAMPLE_DATA.bloodBanks;
      filtered = bloodBanks.filter(b => {
        if (selectedGroup !== "All" && b.blood_group !== selectedGroup) return false;
        if (selectedLoc !== "All" && b.location !== selectedLoc) return false;
        return true;
      });
      filtered.sort((a, b) => b.available_units - a.available_units);
    }

    if (!filtered) return;

    if (bloodCountDisplay) {
      bloodCountDisplay.textContent = `Showing ${filtered.length} blood bank records (Synthetic Live Inventory)`;
    }

    if (filtered.length === 0) {
      bloodTableBody.innerHTML = `
        <tr>
          <td colspan="7" class="text-center py-4 text-muted">
            <i class="bi bi-droplet-half fs-2 text-danger d-block mb-2"></i>
            No certified blood banks matched your query. Try selecting "All Blood Groups" or checking adjacent zones.
          </td>
        </tr>
      `;
      return;
    }

    bloodTableBody.innerHTML = filtered.map(b => {
      const units = b.available_units;
      const statusHtml = b.stockStatus ?
        (b.stockStatus === "Adequate" ? `<span class="stock-pill stock-adequate"><i class="bi bi-check-circle me-1"></i>Adequate Reserve</span>` :
         b.stockStatus === "Low Reserve" ? `<span class="stock-pill stock-low"><i class="bi bi-exclamation-triangle me-1"></i>Low Reserve</span>` :
         `<span class="stock-pill stock-critical"><i class="bi bi-exclamation-octagon me-1"></i>Critical Shortage</span>`) :
        getStatusPill(units, reqUnits);

      return `
        <tr>
          <td>
            <div class="d-flex align-items-center gap-2">
              <span class="blood-unit-badge">${b.blood_group}</span>
              <div>
                <strong>${b.name}</strong>
                <div class="small text-muted"><i class="bi bi-geo-alt me-1"></i>${b.location}</div>
              </div>
            </div>
          </td>
          <td>
            <span class="badge bg-secondary fs-6">${b.blood_group}</span>
          </td>
          <td>
            <strong class="fs-5 ${units >= reqUnits ? 'text-success' : 'text-danger'}">${units}</strong>
            <span class="text-muted small">units</span>
          </td>
          <td>
            ${statusHtml}
          </td>
          <td class="small text-muted">
            <i class="bi bi-clock-history me-1"></i>${b.last_updated}
          </td>
          <td>
            <a href="tel:${b.contact}" class="btn btn-sm btn-outline-danger">
              <i class="bi bi-telephone-fill me-1"></i>${b.contact}
            </a>
          </td>
          <td>
            <button class="btn btn-sm btn-primary" onclick="alert('Reserve Blood Request submitted for ${b.name} (${b.blood_group}, ${reqUnits} units). Telemetry routed to certified dispatch.')">
              <i class="bi bi-shield-plus me-1"></i>Request
            </button>
          </td>
        </tr>
      `;
    }).join("");
  }

  function renderDonors() {
    if (!donorsTableBody || !window.SAMPLE_DATA) return;
    const donors = window.SAMPLE_DATA.donors || [];

    // Anonymize private donor records for academic data privacy compliance
    donorsTableBody.innerHTML = donors.map((d, i) => `
      <tr>
        <td>
          <div class="d-flex align-items-center gap-2">
            <i class="bi bi-person-badge text-secondary"></i>
            <div>
              <strong>Donor ID #REG-${d.id}</strong>
              <div class="text-muted" style="font-size: 0.72rem;"><i class="bi bi-shield-check text-success me-1"></i>Identity Anonymized (Privacy Compliant)</div>
            </div>
          </div>
        </td>
        <td><span class="badge bg-danger fs-6">${d.blood_group}</span></td>
        <td><i class="bi bi-geo-alt text-muted me-1"></i>${d.location}</td>
        <td><span class="small text-muted">${d.last_donated}</span></td>
        <td><span class="badge bg-success-subtle text-success border border-success-subtle">${d.status}</span></td>
        <td>
          <button class="btn btn-sm btn-outline-secondary" onclick="alert('Notification dispatched securely to anonymized Donor #REG-${d.id} through system messaging gateway.')">
            <i class="bi bi-send me-1"></i>Alert Donor
          </button>
        </td>
      </tr>
    `).join("");
  }

  if (filterForm) {
    filterForm.addEventListener("submit", function (e) {
      e.preventDefault();
      applyBloodFilters();
    });
  }

  if (groupSelect) groupSelect.addEventListener("change", applyBloodFilters);
  if (locationSelect) locationSelect.addEventListener("change", applyBloodFilters);
  if (unitsInput) unitsInput.addEventListener("input", applyBloodFilters);

  if (resetBtn) {
    resetBtn.addEventListener("click", function () {
      if (filterForm) filterForm.reset();
      applyBloodFilters();
    });
  }

  applyBloodFilters();
  renderDonors();
});
