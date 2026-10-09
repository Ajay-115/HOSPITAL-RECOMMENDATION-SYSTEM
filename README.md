# Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
**Final-Year B.Tech Computer Science (Data Science) Project**

---

### Academic Disclaimer
> **IMPORTANT:**
> Information shown by this academic system is for demonstration and informational purposes only. Availability and healthcare decisions should be confirmed directly with the healthcare provider. This is an academic healthcare information and recommendation project, NOT a provider of medical diagnosis or clinical advice.

---

## 1. Project Overview
This project is an intelligent healthcare resource recommendation platform designed to optimize patient routing and hospital resource utilization. It analyzes multiple constraints:
- **Location & Proximity** (zone/district distance matrices)
- **Medical Specialties** (Cardiology, Neurology, Orthopedics, Oncology, etc.)
- **Surgery Scheduling & Affordability**
- **Bed & ICU Availability**
- **Queue Length & Waiting Time Estimation**
- **Emergency Room Load & Trauma Capacity**
- **Blood Bank Reserves across all 8 ABO/Rh blood groups**

---

## 2. Recommendation Algorithm (Explainable Linear Model)
To ensure the scoring model is 100% explainable and defensible during viva evaluation, the recommendation engine calculates scores using normalized multi-variable linear weighting:

$$\text{Recommendation Score} = 0.30 S + 0.20 D + 0.20 A + 0.15 W + 0.10 C + 0.05 E$$

Where:
- **$S$ (Specialty Match, 30%)**: Exact match (100) vs related vs general care baseline.
- **$D$ (Distance Proximity, 20%)**: Normalized inverted route distance ($0\text{ km} \to 100$, $20\text{ km} \to 0$).
- **$A$ (Resource Availability, 20%)**: Available beds ratio ($60\%$) + ICU availability ratio ($40\%$).
- **$W$ (Estimated Waiting Time, 15%)**: Normalized inverted queue wait ($10\text{ mins} \to 100$, $90\text{ mins} \to 0$).
- **$C$ (Cost Affordability, 10%)**: Standard care cost relative to regional maximum ceiling.
- **$E$ (Emergency Load Headroom, 5%)**: Low ($95$) / Medium ($75$) / High ($45$) / Critical ($20$).

Every hospital card features an interactive **"Why this score?"** breakdown modal displaying individual normalized factor scores.

---

## 3. Technology Stack & Planned Architecture
```
[ Frontend: HTML5 / CSS3 / Bootstrap 5 / JS ]
                      │
                      ▼
        [ Backend: Python Flask REST API ]
                      │
                      ▼
        [ Recommendation Engine Service ]
                      │
                      ▼
[ Big Data Processing: PySpark / Apache Spark & HDFS ]
```

---

## 4. Current Status: Phase 1 & Phase 2 Completed
- [x] **Phase 1: Professional Responsive Frontend UI with Synthetic Datasets**
  - Modern medical technology design system (`css/style.css`)
  - Landing / Dashboard with system-wide live statistics (`index.html`)
  - Hospital search & explainable recommendation ranking (`pages/hospitals.html`)
  - Hospital details & resource breakdown (`pages/hospital-details.html`)
  - Surgery recommendation & procedure filtering (`pages/surgery.html`)
  - Blood bank inventory (8 groups) & voluntary donor registry (`pages/blood.html`)
  - Bed, ICU, and ventilator capacity monitoring (`pages/resources.html`)
  - Transparent waiting-time queuing theory simulator (`pages/waiting-time.html`)
  - Admin management console & hospital registry CRUD (`pages/admin.html`)
  - Academic demo portal authentication (`pages/login.html`)
  - Clean synthetic datasets (`hospitals.csv`, `surgeries.csv`, `blood_banks.csv`)

- [x] **Phase 2: Flask Backend, CORS & REST APIs**
  - Modular Flask application architecture (`backend/app.py`) with Blueprint routing
  - Ingestion and data services (`backend/services/data_loader.py`)
  - Explainable recommendation scoring service (`backend/services/recommendation.py`)
  - Transparent queuing waiting time service (`backend/services/waiting_time.py`)
  - System capacity analytics aggregation service (`backend/services/analytics.py`)
  - Full REST API endpoints with CORS support:
    - `GET  /api/health`
    - `GET  /api/hospitals` & `GET /api/hospitals/<id>`
    - `POST /api/recommend`
    - `GET  /api/surgeries` & `POST /api/surgery/recommend`
    - `GET  /api/blood` & `POST /api/blood/search`
    - `GET  /api/resources`
    - `GET  /api/analytics`
    - `GET  /api/waiting-time/analytics`
    - `POST /api/hospitals`, `PUT /api/hospitals/<id>`, `DELETE /api/hospitals/<id>`
  - Unified server serving both frontend HTML/CSS/JS and REST APIs simultaneously on port 5000

---

## 5. How to Run the Application (Full Stack)

### Step 1: Start the Flask Backend Server
In PowerShell or Command Prompt:
```powershell
cd "c:\Users\ajays\Downloads\HOSPITAL RECOMMENDATION\hospital_recommendation_system\backend"
python app.py
```

### Step 2: Open in Web Browser
Open your browser at:
**`http://localhost:5000`**

Both the responsive frontend UI and the REST API endpoints are served concurrently on port 5000 with CORS enabled.

---

## 6. Testing the REST API Endpoints

You can verify the backend endpoints using PowerShell:
```powershell
# Health Check
Invoke-RestMethod -Uri "http://localhost:5000/api/health"

# Get Cardiology Hospitals
Invoke-RestMethod -Uri "http://localhost:5000/api/hospitals?specialty=Cardiology"

# Calculate Multi-Factor Recommendations
$body = @{ location = "Central District"; specialty = "Cardiology"; max_cost = 60000; max_wait = 50; sort_by = "score_desc" } | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:5000/api/recommend" -Method Post -Body $body -ContentType "application/json"

# Macro Bed & ICU Capacity
Invoke-RestMethod -Uri "http://localhost:5000/api/resources"
```

---

## 6. Roadmap for Next Phases
- **Phase 2**: Flask Backend setup (`app.py`, `requirements.txt`, modular routes, and REST API endpoints: `/api/hospitals`, `/api/recommend`, `/api/surgeries`, `/api/blood`, `/api/resources`).
- **Phase 3**: Backend Recommendation Engine service (`backend/services/recommendation.py`).
- **Phase 4**: Full dynamic REST integration for surgery, blood, and waiting-time modules.
- **Phase 5**: Full Admin database synchronization.
- **Phase 6**: PySpark batch data cleaning and aggregation analytics jobs.
- **Phase 7**: Hadoop HDFS dataset upload and retrieval scripts.
- **Phase 8**: Streaming simulation for dynamic resource fluctuations.
