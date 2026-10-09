/**
 * Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
 * Healthcare Resource Monitoring & Inventory Module (Phase 1 & Phase 9.5)
 *
 * Sourced from:
 *   1. SQLite Operational DB: GET /api/resources (Operational Bed & Care Matrix)
 *   2. HDFS Spark Streaming:  GET /api/realtime/resources (Real-Time Telemetry)
 */

document.addEventListener("DOMContentLoaded", function () {
  // Operational table elements
  const resourceTableBody = document.getElementById("resourceTableBody");
  const resourceFilterInput = document.getElementById("resourceSearchInput");
  const filterDistrictSelect = document.getElementById("resourceDistrictFilter");
  const statusFilterSelect = document.getElementById("resourceStatusFilter");

  // Summary counter cards (Operational)
  const totalBedsEl = document.getElementById("resTotalBeds");
  const availBedsEl = document.getElementById("resAvailBeds");
  const availIcuEl = document.getElementById("resAvailIcu");
  const availVentEl = document.getElementById("resAvailVent");

  // Real-Time Streaming Telemetry elements (Phase 9.5)
  const realtimeTableBody = document.getElementById("realtimeTableBody");
  const realtimeLastUpdated = document.getElementById("realtimeLastUpdated");
  const realtimeAlertContainer = document.getElementById("realtimeAlertContainer");
  const rtNormalBadge = document.getElementById("rtNormalBadge");
  const rtWarningBadge = document.getElementById("rtWarningBadge");
  const rtCriticalBadge = document.getElementById("rtCriticalBadge");

  // =========================================================================
  // Section 1: Real-Time Resource Monitoring (Spark Streaming → HDFS)
  // =========================================================================

  /**
   * Render real-time status badge using the authoritative status returned by Flask.
   * NOTE: Does NOT calculate status in JavaScript. Uses r.status directly.
   */
  function getRealtimeStatusBadge(status) {
    if (status === "Critical") {
      return `<span class="badge bg-danger-subtle text-danger border border-danger-subtle fw-semibold"><i class="bi bi-exclamation-octagon-fill me-1"></i>Critical</span>`;
    }
    if (status === "Warning") {
      return `<span class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle fw-semibold"><i class="bi bi-exclamation-triangle-fill me-1"></i>Warning</span>`;
    }
    return `<span class="badge bg-success-subtle text-success border border-success-subtle fw-semibold"><i class="bi bi-check-circle-fill me-1"></i>Normal</span>`;
  }

  /**
   * Format ISO window start/end timestamps into readable local time strings.
   */
  function formatWindowInterval(startIso, endIso) {
    if (!startIso || !endIso) return "--";
    try {
      const s = new Date(startIso);
      const e = new Date(endIso);
      if (isNaN(s.getTime()) || isNaN(e.getTime())) {
        return `${startIso.substring(11, 16)} - ${endIso.substring(11, 16)}`;
      }
      const opts = { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false };
      return `${s.toLocaleTimeString([], opts)} &rarr; ${e.toLocaleTimeString([], opts)}`;
    } catch {
      return `${startIso} - ${endIso}`;
    }
  }

  /**
   * Fetch and render real-time streaming resource telemetry from Flask API.
   * Endpoint: GET /api/realtime/resources
   * Refreshed every 10 seconds (backend protected by 10s TTL cache).
   */
  async function fetchRealtimeResources() {
    if (!realtimeTableBody) return;

    try {
      const res = await App.apiGet("/realtime/resources");

      // Handle HTTP 503 or backend unreachable gracefully
      if (!res || !res.success) {
        showRealtimeOfflineAlert();
        return;
      }

      // Hide offline alert if previously shown
      if (realtimeAlertContainer) {
        realtimeAlertContainer.className = "d-none";
        realtimeAlertContainer.innerHTML = "";
      }

      // Update Last Updated Timestamp
      if (realtimeLastUpdated) {
        const now = new Date();
        realtimeLastUpdated.textContent = now.toLocaleTimeString();
      }

      // Update Summary KPI Badges
      if (res.summary) {
        if (rtNormalBadge) rtNormalBadge.textContent = `Normal: ${res.summary.normal ?? 0}`;
        if (rtWarningBadge) rtWarningBadge.textContent = `Warning: ${res.summary.warning ?? 0}`;
        if (rtCriticalBadge) rtCriticalBadge.textContent = `Critical: ${res.summary.critical ?? 0}`;
      }

      // Render records
      const records = Array.isArray(res.data) ? res.data : [];
      if (records.length === 0) {
        realtimeTableBody.innerHTML = `
          <tr>
            <td colspan="9" class="text-center py-4 text-muted">
              <i class="bi bi-info-circle fs-3 text-secondary d-block mb-1"></i>
              No active streaming telemetry windows available in HDFS.
            </td>
          </tr>
        `;
        return;
      }

      realtimeTableBody.innerHTML = records.map(r => {
        const utilVal = r.avg_utilization != null ? Number(r.avg_utilization) : 0;
        const progressBg = r.status === "Critical" ? "bg-danger" : (r.status === "Warning" ? "bg-warning" : "bg-success");

        return `
          <tr>
            <td>
              <strong>${r.hospital_name || "Unknown Facility"}</strong>
              <div class="small text-muted"><i class="bi bi-geo-alt me-1"></i>${r.location || "Unknown"} &bull; <span class="badge bg-light text-secondary border">${r.hospital_id}</span></div>
            </td>
            <td>
              <span class="fw-semibold text-dark"><i class="bi bi-tag me-1 text-primary"></i>${r.resource_type}</span>
            </td>
            <td>
              <div class="d-flex justify-content-between small mb-1">
                <span class="fw-bold">${r.avg_utilization != null ? r.avg_utilization + "%" : "N/A"}</span>
              </div>
              <div class="progress" style="height: 6px;">
                <div class="progress-bar ${progressBg}" style="width: ${Math.min(100, Math.max(0, utilVal))}%"></div>
              </div>
            </td>
            <td>
              <span class="fw-semibold">${r.avg_occupied != null ? r.avg_occupied : "--"}</span>
            </td>
            <td>
              <span class="fw-semibold text-success">${r.avg_available != null ? r.avg_available : "--"}</span>
            </td>
            <td>
              ${App.getEmergencyBadge(r.max_emergency_load)}
            </td>
            <td>
              <span class="badge bg-light text-dark border px-2 py-1">${r.event_count ?? 0}</span>
            </td>
            <td class="small text-muted">
              ${formatWindowInterval(r.window_start, r.window_end)}
            </td>
            <td>
              ${getRealtimeStatusBadge(r.status)}
            </td>
          </tr>
        `;
      }).join("");

    } catch (err) {
      console.warn("[RealtimeResources] Error updating telemetry:", err);
      showRealtimeOfflineAlert();
    }
  }

  function showRealtimeOfflineAlert() {
    if (realtimeAlertContainer) {
      realtimeAlertContainer.className = "alert alert-warning py-2 px-3 small mb-3 border-warning border-opacity-50 d-flex align-items-center justify-content-between";
      realtimeAlertContainer.innerHTML = `
        <div class="d-flex align-items-center">
          <i class="bi bi-exclamation-triangle-fill text-warning me-2 fs-5"></i>
          <div>
            <strong>Real-time analytics temporarily unavailable.</strong>
            <span> Spark Structured Streaming or HDFS feed is currently offline or unreachable. Operational inventory below remains accessible.</span>
          </div>
        </div>
        <span class="badge bg-warning text-dark">HTTP 503 / Offline</span>
      `;
    }
    if (realtimeLastUpdated) {
      realtimeLastUpdated.textContent = "Unavailable";
    }
    if (rtNormalBadge) rtNormalBadge.textContent = "Normal: --";
    if (rtWarningBadge) rtWarningBadge.textContent = "Warning: --";
    if (rtCriticalBadge) rtCriticalBadge.textContent = "Critical: --";

    if (realtimeTableBody) {
      realtimeTableBody.innerHTML = `
        <tr>
          <td colspan="9" class="text-center py-4 text-muted">
            <i class="bi bi-exclamation-circle fs-3 text-warning d-block mb-1"></i>
            Real-time analytics temporarily unavailable.
          </td>
        </tr>
      `;
    }
  }

  // =========================================================================
  // Section 2: Operational Resource Directory (SQLite Database)
  // =========================================================================

  function getBedStatusBadge(avail, total) {
    const ratio = total > 0 ? (avail / total) : 0;
    if (avail === 0) return `<span class="badge bg-danger">Unavailable</span>`;
    if (ratio < 0.15) return `<span class="badge bg-danger-subtle text-danger border border-danger-subtle">Critical (<15%)</span>`;
    if (ratio < 0.30) return `<span class="badge bg-warning-subtle text-warning-emphasis border border-warning-subtle">Low Stock</span>`;
    return `<span class="badge bg-success-subtle text-success border border-success-subtle">Available</span>`;
  }

  async function renderResourceTable() {
    if (!resourceTableBody) return;

    let hospitals = [];
    let summary = null;

    // Fetch from Flask REST API
    const apiRes = await App.apiGet("/resources");
    if (apiRes && apiRes.success && apiRes.hospitals) {
      hospitals = apiRes.hospitals;
      summary = apiRes.summary;
    } else if (window.SAMPLE_DATA) {
      hospitals = window.SAMPLE_DATA.hospitals;
    }

    const query = (resourceFilterInput ? resourceFilterInput.value : "").toLowerCase().trim();
    const district = filterDistrictSelect ? filterDistrictSelect.value : "All";
    const statusFilter = statusFilterSelect ? statusFilterSelect.value : "All";

    // Set macro counts
    if (summary) {
      if (totalBedsEl) totalBedsEl.textContent = summary.total_registered_beds.toLocaleString();
      if (availBedsEl) availBedsEl.textContent = summary.available_beds.toLocaleString();
      if (availIcuEl) availIcuEl.textContent = summary.available_icu_beds.toLocaleString();
    } else {
      let totBeds = 0, freeBeds = 0, freeIcu = 0;
      hospitals.forEach(h => {
        totBeds += h.total_beds || 0;
        freeBeds += h.available_beds || 0;
        freeIcu += h.available_icu_beds || 0;
      });
      if (totalBedsEl) totalBedsEl.textContent = totBeds.toLocaleString();
      if (availBedsEl) availBedsEl.textContent = freeBeds.toLocaleString();
      if (availIcuEl) availIcuEl.textContent = freeIcu.toLocaleString();
    }

    let freeVent = (summary && summary.available_ventilators != null)
      ? summary.available_ventilators
      : hospitals.reduce((sum, h) => sum + (h.ventilators_available != null ? Number(h.ventilators_available) : (h.available_ventilators != null ? Number(h.available_ventilators) : 5)), 0);
    if (availVentEl) availVentEl.textContent = freeVent.toLocaleString();

    let filtered = hospitals.filter(h => {
      if (query && !h.hospital_name.toLowerCase().includes(query) && !h.specialty.toLowerCase().includes(query)) {
        return false;
      }
      if (district !== "All") {
        const hDist = (h.district || "").toLowerCase();
        const hLoc = (h.location || "").toLowerCase();
        const dLower = district.toLowerCase();
        if (hDist !== dLower && !hLoc.includes(dLower)) {
          return false;
        }
      }
      if (statusFilter !== "All") {
        const ratio = h.total_beds > 0 ? (h.available_beds / h.total_beds) : 0;
        if (statusFilter === "Critical" && ratio >= 0.15) return false;
        if (statusFilter === "Low" && (ratio < 0.15 || ratio >= 0.30)) return false;
        if (statusFilter === "Available" && ratio < 0.30) return false;
      }
      return true;
    });

    if (filtered.length === 0) {
      resourceTableBody.innerHTML = `
        <tr>
          <td colspan="7" class="text-center py-4 text-muted">
            <i class="bi bi-search fs-3 text-secondary d-block mb-1"></i>
            No hospital resource records matched your query.
          </td>
        </tr>
      `;
      return;
    }

    resourceTableBody.innerHTML = filtered.map(h => {
      const bedPct = Math.round(((h.total_beds - h.available_beds) / h.total_beds) * 100);
      const icuPct = Math.round(((h.icu_beds - h.available_icu_beds) / h.icu_beds) * 100);

      return `
        <tr>
          <td>
            <strong>${h.hospital_name}</strong>
            <div class="small text-muted"><i class="bi bi-geo-alt me-1"></i>${h.district || h.location} &bull; ${h.specialty}</div>
          </td>
          <td>
            <div class="d-flex justify-content-between small mb-1">
              <span>${h.available_beds} Free</span>
              <span class="text-muted">${h.total_beds} Total</span>
            </div>
            <div class="progress" style="height: 6px;">
              <div class="progress-bar ${bedPct > 85 ? 'bg-danger' : bedPct > 65 ? 'bg-warning' : 'bg-success'}" style="width: ${bedPct}%"></div>
            </div>
          </td>
          <td>
            <div class="d-flex justify-content-between small mb-1">
              <span class="fw-bold ${h.available_icu_beds < 5 ? 'text-danger' : 'text-primary'}">${h.available_icu_beds} Free</span>
              <span class="text-muted">${h.icu_beds} Total</span>
            </div>
            <div class="progress" style="height: 6px;">
              <div class="progress-bar ${icuPct > 85 ? 'bg-danger' : 'bg-primary'}" style="width: ${icuPct}%"></div>
            </div>
          </td>
          <td>
            <span class="fw-semibold ${(h.ventilators_available != null ? h.ventilators_available : 5) <= 3 ? 'text-danger' : 'text-dark'}">
              <i class="bi bi-wind me-1 text-info"></i>${h.ventilators_available != null ? h.ventilators_available : (h.available_ventilators != null ? h.available_ventilators : 0)} <small class="text-muted">/ ${h.ventilators_total != null ? h.ventilators_total : (h.total_ventilators != null ? h.total_ventilators : 0)}</small>
            </span>
          </td>
          <td>
            ${App.getEmergencyBadge(h.emergency_load)}
          </td>
          <td>
            ${getBedStatusBadge(h.available_beds, h.total_beds)}
          </td>
          <td>
            <a href="hospital-details.html?id=${h.hospital_id}" class="btn btn-sm btn-outline-primary">
              <i class="bi bi-eye"></i> View
            </a>
          </td>
        </tr>
      `;
    }).join("");
  }

  // Filter event listeners for operational inventory
  if (resourceFilterInput) resourceFilterInput.addEventListener("input", renderResourceTable);
  if (filterDistrictSelect) filterDistrictSelect.addEventListener("change", renderResourceTable);
  if (statusFilterSelect) statusFilterSelect.addEventListener("change", renderResourceTable);

  // Initial loads
  renderResourceTable();
  fetchRealtimeResources();

  // Automatic 10-second refresh for the real-time section (aligned with 10s backend TTL cache)
  const realtimeTimer = setInterval(fetchRealtimeResources, 10000);

  // Clean up interval if navigating away
  window.addEventListener("beforeunload", function () {
    clearInterval(realtimeTimer);
  });
});
