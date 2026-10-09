/**
 * Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
 * Admin Operations & Resource Management Module (Flask REST API Integration)
 */

document.addEventListener("DOMContentLoaded", async function () {
  const adminHospitalsTable = document.getElementById("adminHospitalsTable");
  const addHospitalForm = document.getElementById("addHospitalForm");

  // Metric elements
  const adminTotalHospitals = document.getElementById("admTotHosp");
  const adminTotalBeds = document.getElementById("admTotBeds");
  const adminAvailBeds = document.getElementById("admAvailBeds");
  const adminAvailIcu = document.getElementById("admAvailIcu");
  const adminBloodUnits = document.getElementById("admBloodUnits");

  let currentHospitals = [];
  let isBackendConnected = false;

  async function loadAdminData() {
    try {
      const [hospRes, resSummary, bloodRes] = await Promise.all([
        App.apiGet("/hospitals"),
        App.apiGet("/resources"),
        App.apiGet("/blood")
      ]);

      if (hospRes && hospRes.success && hospRes.data) {
        isBackendConnected = true;
        App.renderBackendStatusBanner(true);
        currentHospitals = hospRes.data;

        if (adminTotalHospitals) adminTotalHospitals.textContent = hospRes.count;

        if (resSummary && resSummary.success && resSummary.summary) {
          const s = resSummary.summary;
          if (adminTotalBeds) adminTotalBeds.textContent = (s.total_registered_beds || 0).toLocaleString();
          if (adminAvailBeds) adminAvailBeds.textContent = (s.available_beds || 0).toLocaleString();
          if (adminAvailIcu) adminAvailIcu.textContent = (s.available_icu_beds || 0).toLocaleString();
        }

        const bloodList = (bloodRes && bloodRes.success) ? (bloodRes.data || bloodRes.blood_banks || []) : [];
        if (bloodList.length > 0) {
          const totalBlood = bloodList.reduce((sum, b) => {
            const avail = b.units_available || b.units || 0;
            return sum + (parseInt(avail, 10) || 0);
          }, 0);
          if (adminBloodUnits) adminBloodUnits.textContent = totalBlood.toLocaleString();
        }

        renderAdminTable(currentHospitals);
        return;
      }
      throw new Error("Failed to load hospitals from backend");
    } catch (err) {
      console.warn("Backend unavailable for Admin operations, using fallback data:", err);
      isBackendConnected = false;
      App.renderBackendStatusBanner(false);

      if (window.SAMPLE_DATA) {
        currentHospitals = window.SAMPLE_DATA.hospitals;
        const metrics = App.getSystemMetrics();
        if (adminTotalHospitals) adminTotalHospitals.textContent = metrics.totalHospitals;
        if (adminTotalBeds) adminTotalBeds.textContent = metrics.totalBeds.toLocaleString();
        if (adminAvailBeds) adminAvailBeds.textContent = metrics.availableBeds.toLocaleString();
        if (adminAvailIcu) adminAvailIcu.textContent = metrics.availableIcuBeds.toLocaleString();
        if (adminBloodUnits) adminBloodUnits.textContent = metrics.totalBloodUnits.toLocaleString();
        renderAdminTable(currentHospitals);
      }
    }
  }

  function renderAdminTable(hospitals) {
    if (!adminHospitalsTable) return;
    if (!hospitals || hospitals.length === 0) {
      adminHospitalsTable.innerHTML = `<tr><td colspan="8" class="text-center py-4 text-muted">No hospitals currently registered.</td></tr>`;
      return;
    }

    adminHospitalsTable.innerHTML = hospitals.map(h => `
      <tr>
        <td><code>${h.hospital_id}</code></td>
        <td>
          <strong>${h.hospital_name}</strong>
          <div class="small text-muted"><i class="bi bi-geo-alt me-1"></i>${h.district || h.location}</div>
        </td>
        <td><span class="badge bg-primary-subtle text-primary border">${h.specialty}</span></td>
        <td>
          <strong>${h.available_beds}</strong> / ${h.total_beds}
        </td>
        <td>
          <span class="${parseInt(h.available_icu_beds, 10) < 5 ? 'text-danger fw-bold' : 'text-success'}">
            ${h.available_icu_beds} / ${h.icu_beds}
          </span>
        </td>
        <td>${App.getEmergencyBadge(h.emergency_load)}</td>
        <td>${App.formatCurrency(h.estimated_cost)}</td>
        <td>
          <div class="btn-group btn-group-sm">
            <button class="btn btn-outline-secondary" onclick="editHospitalModal('${h.hospital_id}')" title="Quick Edit Available Beds">
              <i class="bi bi-pencil-square"></i>
            </button>
            <button class="btn btn-outline-danger" onclick="deleteHospitalMock('${h.hospital_id}')" title="Delete Hospital">
              <i class="bi bi-trash"></i>
            </button>
          </div>
        </td>
      </tr>
    `).join("");
  }

  // Handle Add Hospital Form Submission (Sends POST /api/hospitals)
  if (addHospitalForm) {
    addHospitalForm.addEventListener("submit", async function (e) {
      e.preventDefault();

      const locVal = document.getElementById("newHospLocation").value.trim();
      const newHospital = {
        hospital_name: document.getElementById("newHospName").value.trim(),
        location: locVal,
        district: locVal,
        latitude: 12.9700,
        longitude: 77.5900,
        specialty: document.getElementById("newHospSpecialty").value.trim(),
        total_beds: parseInt(document.getElementById("newHospTotalBeds").value, 10) || 100,
        available_beds: parseInt(document.getElementById("newHospAvailBeds").value, 10) || 20,
        icu_beds: parseInt(document.getElementById("newHospIcuBeds").value, 10) || 20,
        available_icu_beds: parseInt(document.getElementById("newHospAvailIcu").value, 10) || 5,
        queue_length: 10,
        emergency_load: document.getElementById("newHospEmergencyLoad").value,
        average_wait_time: parseInt(document.getElementById("newHospWaitTime").value, 10) || 30,
        estimated_cost: parseInt(document.getElementById("newHospCost").value, 10) || 35000,
        emergency_services: "Yes",
        rating: 4.5
      };

      if (isBackendConnected) {
        try {
          const res = await App.apiPost("/hospitals", newHospital);
          if (res && res.success) {
            // Close modal
            const modalEl = document.getElementById("addHospitalModal");
            if (modalEl && window.bootstrap) {
              const modal = bootstrap.Modal.getInstance(modalEl);
              if (modal) modal.hide();
            }
            addHospitalForm.reset();
            alert(`Hospital "${newHospital.hospital_name}" successfully saved to backend database!`);
            await loadAdminData();
            return;
          } else {
            alert(`Error adding hospital: ${res.error || "Unknown server response"}`);
          }
        } catch (err) {
          alert(`Network error communicating with backend: ${err.message}`);
        }
      } else {
        // Fallback local update
        newHospital.hospital_id = "H" + (window.SAMPLE_DATA.hospitals.length + 101);
        window.SAMPLE_DATA.hospitals.unshift(newHospital);

        const modalEl = document.getElementById("addHospitalModal");
        if (modalEl && window.bootstrap) {
          const modal = bootstrap.Modal.getInstance(modalEl);
          if (modal) modal.hide();
        }
        addHospitalForm.reset();
        alert(`Hospital added in Demo / Offline Data mode. Start Flask backend for persistent SQLite storage.`);
        loadAdminData();
      }
    });
  }

  window.deleteHospitalMock = async function (id) {
    if (!confirm(`Are you sure you want to remove hospital ${id}?`)) return;

    if (isBackendConnected) {
      try {
        const res = await App.apiDelete(`/hospitals/${encodeURIComponent(id)}`);
        if (res && res.success) {
          alert(`Hospital ${id} removed successfully from database.`);
          await loadAdminData();
        } else {
          alert(`Failed to delete hospital: ${res.error || "Unknown server response"}`);
        }
      } catch (err) {
        alert(`Error communicating with backend: ${err.message}`);
      }
    } else {
      window.SAMPLE_DATA.hospitals = window.SAMPLE_DATA.hospitals.filter(h => h.hospital_id !== id);
      alert(`Hospital ${id} removed in Demo / Offline Data mode.`);
      loadAdminData();
    }
  };

  window.editHospitalModal = async function (id) {
    const hospital = currentHospitals.find(h => String(h.hospital_id).toUpperCase() === String(id).toUpperCase());
    if (!hospital) {
      alert("Hospital not found.");
      return;
    }

    const currentVal = hospital.available_beds;
    const totalBeds = hospital.total_beds;
    const input = prompt(`Update Available Beds for ${hospital.hospital_name} (Total Capacity: ${totalBeds}):`, currentVal);
    if (input === null) return;

    const newAvail = parseInt(input, 10);
    if (isNaN(newAvail) || newAvail < 0 || newAvail > totalBeds) {
      alert(`Please enter a valid number between 0 and ${totalBeds}.`);
      return;
    }

    if (isBackendConnected) {
      try {
        const res = await App.apiPut(`/hospitals/${encodeURIComponent(id)}`, {
          available_beds: newAvail
        });
        if (res && res.success) {
          alert(`Updated ${hospital.hospital_name} available beds to ${newAvail}.`);
          await loadAdminData();
        } else {
          alert(`Failed to update hospital: ${res.error || "Unknown server response"}`);
        }
      } catch (err) {
        alert(`Error communicating with backend: ${err.message}`);
      }
    } else {
      hospital.available_beds = newAvail;
      alert(`Available beds updated locally (Demo / Offline Data mode).`);
      loadAdminData();
    }
  };

  await loadAdminData();
});
