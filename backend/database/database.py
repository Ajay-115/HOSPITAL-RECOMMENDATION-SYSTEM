"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
SQLite Database Access Layer & Persistent Storage Engine (Phase 5)
"""

import sqlite3
import os
import csv

from contextlib import contextmanager

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR = os.path.join(BASE_DIR, "database")
DATA_DIR = os.path.join(BASE_DIR, "data")
SCHEMA_PATH = os.path.join(DB_DIR, "schema.sql")
DEFAULT_DB_PATH = os.path.join(DB_DIR, "app.db")

_ACTIVE_DB_PATH = None


def set_active_db_path(path):
    """Sets a global database path override (e.g. for testing)."""
    global _ACTIVE_DB_PATH
    _ACTIVE_DB_PATH = path


def get_default_db_path():
    """Returns the default production database path."""
    return DEFAULT_DB_PATH


def get_db_path(override_path=None):
    """
    Returns the effective database path with clean dependency injection hierarchy:
    1. Explicit function override argument
    2. Flask application config (current_app.config['DATABASE_PATH']) if inside request context
    3. Global active override (set_active_db_path)
    4. Environment variable 'DATABASE_PATH'
    5. Default production path (backend/database/app.db)
    """
    if override_path:
        return override_path

    # Check Flask request/app context
    try:
        from flask import current_app, has_app_context
        if has_app_context() and current_app and current_app.config.get("DATABASE_PATH"):
            return current_app.config["DATABASE_PATH"]
    except ImportError:
        pass

    # Check module-level active override
    if _ACTIVE_DB_PATH:
        return _ACTIVE_DB_PATH

    # Check environment variable
    env_path = os.environ.get("DATABASE_PATH")
    if env_path:
        return env_path

    return DEFAULT_DB_PATH


def get_db_connection(db_path=None):
    """
    Creates and returns a connection to the SQLite database.
    Enables foreign keys and uses Row factory for dict-like access.
    """
    path = get_db_path(db_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


@contextmanager
def db_session(db_path=None):
    """
    Context manager that yields an active SQLite connection and guarantees
    that transactions are committed and the connection is closed properly.
    """
    conn = get_db_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()



def row_to_hospital(row):
    """
    Converts a database row into a standardized hospital dictionary.
    Exposes both canonical schema fields and backward-compatible aliases
    so that the recommendation engine, analytics, and frontend operate seamlessly.
    """
    if row is None:
        return None
    d = dict(row)

    h_id = d.get("id") or d.get("hospital_id")
    h_name = d.get("name") or d.get("hospital_name")
    h_dist = (d.get("district") or "").strip()
    h_loc = (d.get("location") or "").strip()
    if not h_dist and h_loc:
        parts = [p.strip() for p in h_loc.split(",")]
        h_dist = parts[1] if len(parts) >= 2 else h_loc
    if not h_loc and h_dist:
        h_loc = h_dist
    if not h_dist:
        h_dist = "Central District"
        h_loc = "Central District"

    icu_tot = d.get("icu_total") if d.get("icu_total") is not None else d.get("icu_beds", 20)
    icu_avail = d.get("icu_available") if d.get("icu_available") is not None else d.get("available_icu_beds", 5)
    cost = d.get("standard_cost") if d.get("standard_cost") is not None else d.get("estimated_cost", 35000)
    wait = d.get("average_wait_minutes") if d.get("average_wait_minutes") is not None else d.get("average_wait_time", 30)
    phone = d.get("phone") or d.get("contact", "+91-80-2800-0000")
    queue_len = d.get("queue_length", 10)

    return {
        # Canonical schema fields
        "id": h_id,
        "name": h_name,
        "specialty": d.get("specialty"),
        "district": h_dist,
        "location": h_loc,
        "latitude": float(d.get("latitude", 12.9716)),
        "longitude": float(d.get("longitude", 77.5946)),
        "rating": float(d.get("rating", 4.5)),
        "total_beds": int(d.get("total_beds", 100)),
        "available_beds": int(d.get("available_beds", 20)),
        "icu_total": int(icu_tot),
        "icu_available": int(icu_avail),
        "emergency_services": d.get("emergency_services", "Yes"),
        "emergency_load": d.get("emergency_load", "Medium"),
        "standard_cost": int(cost),
        "average_wait_minutes": int(wait),
        "queue_length": int(queue_len),
        "phone": phone,
        "address": d.get("address") or h_loc or f"Medical Corridor, {h_dist}",
        "status": d.get("status", "Active"),

        # Backward compatibility aliases
        "hospital_id": h_id,
        "hospital_name": h_name,
        "icu_beds": int(icu_tot),
        "available_icu_beds": int(icu_avail),
        "estimated_cost": int(cost),
        "average_wait_time": int(wait),
        "contact": phone,
        "ventilators_total": int(d.get("ventilators_total") or max(5, int(icu_tot * 0.8))),
        "ventilators_available": int(d.get("ventilators_available") or max(2, int(icu_avail * 0.3)))
    }


def row_to_surgery(row):
    """Converts a database row into a standardized surgery dictionary."""
    if row is None:
        return None
    d = dict(row)
    s_id = d.get("id") or d.get("surgery_id")
    cost = d.get("estimated_cost", 100000)
    wait_mins = d.get("average_wait_minutes", 30)
    wait_days = d.get("waiting_days", 3)
    slots = d.get("available_slots", 4)
    loc = d.get("location") or d.get("district", "Kerala")

    return {
        "id": s_id,
        "surgery_id": s_id,
        "hospital_id": d.get("hospital_id"),
        "surgery_name": d.get("surgery_name"),
        "specialty": d.get("specialty"),
        "estimated_cost": int(cost),
        "average_wait_minutes": int(wait_mins),
        "waiting_days": int(wait_days),
        "available_slots": int(slots),
        "location": loc,
        "district": loc,
        "availability": d.get("availability", "Available"),
        "status": d.get("status", "Active")
    }


def row_to_blood_bank(row):
    """Converts a database row into a standardized blood bank dictionary."""
    if row is None:
        return None
    d = dict(row)
    b_id = d.get("id") or d.get("blood_bank_id")
    units = d.get("units_available") if d.get("units_available") is not None else d.get("available_units", 0)
    dist = d.get("district") or d.get("location", "Central District")

    return {
        "id": b_id,
        "blood_bank_id": b_id,
        "hospital_id": d.get("hospital_id"),
        "name": d.get("name", "District Blood Bank"),
        "blood_group": d.get("blood_group"),
        "units_available": int(units),
        "available_units": int(units),
        "district": dist,
        "location": dist,
        "contact": d.get("contact", "+91-80-22114400"),
        "last_updated": d.get("last_updated", "2026-10-01 18:30"),
        "status": d.get("status", "Active")
    }


def row_to_resource(row):
    """Converts a database row into a standardized hospital resource dictionary."""
    if row is None:
        return None
    d = dict(row)
    return {
        "id": d.get("id"),
        "hospital_id": d.get("hospital_id"),
        "resource_name": d.get("resource_name"),
        "resource_type": d.get("resource_type"),
        "total_quantity": int(d.get("total_quantity", 0)),
        "available_quantity": int(d.get("available_quantity", 0)),
        "status": d.get("status", "Available"),
        "last_updated": d.get("last_updated", "2026-10-01 18:30")
    }


def init_db(db_path=None, force_reseed=False):
    """
    Initializes the SQLite database.
    - Creates tables from schema.sql if they do not exist.
    - Seeds baseline data from CSV datasets if the database is empty or force_reseed is True.
    - Preserves existing data without duplicate re-seeding if records already exist.
    """
    path = get_db_path(db_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with db_session(path) as conn:
        cursor = conn.cursor()

        # 1. Execute schema definition
        if os.path.exists(SCHEMA_PATH):
            with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            cursor.executescript(schema_sql)

        # 2. Check if hospitals already seeded
        cursor.execute("SELECT COUNT(*) FROM hospitals;")
        count = cursor.fetchone()[0]

        if count > 0 and not force_reseed:
            conn.commit()
            return  # Already populated, do not duplicate

        # 3. Seed Hospitals from hospitals.csv
        hospitals_csv = os.path.join(DATA_DIR, "hospitals.csv")
        if os.path.exists(hospitals_csv):
            with open(hospitals_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    h_id = row.get("hospital_id", "").strip()
                    if not h_id:
                        continue
                    loc_val = row.get("location", "").strip()
                    dist_val = row.get("district", "").strip()
                    if not dist_val and loc_val:
                        parts = [p.strip() for p in loc_val.split(",")]
                        dist_val = parts[1] if len(parts) >= 2 else loc_val
                    if not loc_val and dist_val:
                        loc_val = dist_val

                    cursor.execute("""
                        INSERT OR REPLACE INTO hospitals (
                            id, name, specialty, district, location, latitude, longitude,
                            rating, total_beds, available_beds, icu_total, icu_available,
                            emergency_services, emergency_load, standard_cost,
                            average_wait_minutes, queue_length, phone, address, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        h_id,
                        row.get("hospital_name", "").strip(),
                        row.get("specialty", "General Medicine").strip(),
                        dist_val,
                        loc_val,
                        float(row.get("latitude", 12.9716) or 12.9716),
                        float(row.get("longitude", 77.5946) or 77.5946),
                        float(row.get("rating", 4.5) or 4.5),
                        int(row.get("total_beds", 100) or 100),
                        int(row.get("available_beds", 20) or 20),
                        int(row.get("icu_beds", 20) or 20),
                        int(row.get("available_icu_beds", 5) or 5),
                        row.get("emergency_services", "Yes").strip(),
                        row.get("emergency_load", "Medium").strip(),
                        int(float(row.get("estimated_cost", 35000) or 35000)),
                        int(row.get("average_wait_time", 30) or 30),
                        int(row.get("queue_length", 10) or 10),
                        row.get("contact", "").strip() if row.get("contact") else f"+91-484-280{abs(hash(h_id)) % 9000 + 1000}",
                        loc_val if loc_val else (row.get("address", "").strip() or f"Medical Corridor, {dist_val}"),
                        "Active"
                    ))

        # 4. Seed Surgeries from surgeries.csv
        surgeries_csv = os.path.join(DATA_DIR, "surgeries.csv")
        if os.path.exists(surgeries_csv):
            with open(surgeries_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    s_id = row.get("surgery_id", "").strip()
                    h_id = row.get("hospital_id", "").strip()
                    if not s_id or not h_id:
                        continue
                    cursor.execute("""
                        INSERT OR REPLACE INTO surgeries (
                            id, hospital_id, surgery_name, specialty, estimated_cost,
                            average_wait_minutes, waiting_days, available_slots, location,
                            availability, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        s_id,
                        h_id,
                        row.get("surgery_name", "").strip(),
                        row.get("specialty", "General").strip(),
                        int(float(row.get("estimated_cost", 100000) or 100000)),
                        30,
                        int(row.get("waiting_days", 3) or 3),
                        int(row.get("available_slots", 4) or 4),
                        row.get("location", "").strip(),
                        "Available",
                        "Active"
                    ))

        # 5. Seed Blood Banks from blood_banks.csv
        blood_csv = os.path.join(DATA_DIR, "blood_banks.csv")
        if os.path.exists(blood_csv):
            with open(blood_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    b_id = row.get("blood_bank_id", "").strip()
                    if not b_id:
                        continue
                    loc_val = row.get("location", "").strip()
                    dist_val = row.get("district", "").strip()
                    if not dist_val and loc_val:
                        parts = [p.strip() for p in loc_val.split(",")]
                        dist_val = parts[1] if len(parts) >= 2 else loc_val
                    cursor.execute("""
                        INSERT OR REPLACE INTO blood_banks (
                            id, hospital_id, name, blood_group, units_available,
                            district, contact, last_updated, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        b_id,
                        None,
                        row.get("name", "District Blood Center").strip(),
                        row.get("blood_group", "O+").strip(),
                        int(row.get("available_units", 20) or 20),
                        dist_val,
                        row.get("contact", "+91-80-22114400").strip(),
                        row.get("last_updated", "2026-10-02 23:00").strip(),
                        "Active"
                    ))

        # 6. Seed Hospital Resources from resources.csv if present, else fallback generator
        resources_csv = os.path.join(DATA_DIR, "resources.csv")
        if os.path.exists(resources_csv):
            with open(resources_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    r_id = row.get("id", "").strip()
                    h_id = row.get("hospital_id", "").strip()
                    if not r_id or not h_id:
                        continue
                    cursor.execute("""
                        INSERT OR REPLACE INTO resources (
                            id, hospital_id, resource_name, resource_type,
                            total_quantity, available_quantity, status, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        r_id,
                        h_id,
                        row.get("resource_name", "").strip(),
                        row.get("resource_type", "").strip(),
                        int(row.get("total_quantity", 0) or 0),
                        int(row.get("available_quantity", 0) or 0),
                        row.get("status", "Available").strip() or "Available",
                        row.get("last_updated", "2026-10-02 23:00").strip()
                    ))
        else:
            cursor.execute("SELECT id, total_beds, icu_total FROM hospitals;")
            hosp_list = cursor.fetchall()
            for h in hosp_list:
                hid = h["id"]
                tot_beds = h["total_beds"]
                tot_icu = h["icu_total"]

                # Ventilators
                cursor.execute("""
                    INSERT OR REPLACE INTO resources (id, hospital_id, resource_name, resource_type, total_quantity, available_quantity, status, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (f"RES-VENT-{hid}", hid, "Mechanical Ventilator", "Critical Care", max(5, int(tot_icu * 0.8)), max(2, int(tot_icu * 0.3)), "Available", "2026-10-01 18:30"))

                # Oxygen Beds
                cursor.execute("""
                    INSERT OR REPLACE INTO resources (id, hospital_id, resource_name, resource_type, total_quantity, available_quantity, status, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (f"RES-OXY-{hid}", hid, "High-Flow Oxygen Bed", "Inpatient Care", max(10, int(tot_beds * 0.4)), max(5, int(tot_beds * 0.15)), "Available", "2026-10-01 18:30"))

                # Dialysis Units
                cursor.execute("""
                    INSERT OR REPLACE INTO resources (id, hospital_id, resource_name, resource_type, total_quantity, available_quantity, status, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (f"RES-DIAL-{hid}", hid, "Hemodialysis Station", "Specialized Care", 8, 3, "Available", "2026-10-01 18:30"))

        conn.commit()


# ==============================================================================
# DATA ACCESS LAYER (DAL) FUNCTIONS
# ==============================================================================

def get_all_hospitals(specialty=None, district=None, emergency_only=False, db_path=None):
    """
    Retrieves all registered hospitals from SQLite.
    Applies optional SQL-level filters using parameterized queries.
    """
    init_db(db_path)
    path = get_db_path(db_path)

    query = "SELECT * FROM hospitals WHERE status = 'Active'"
    params = []

    if specialty and specialty.strip() and specialty.strip().lower() != "all":
        query += " AND LOWER(specialty) = LOWER(?)"
        params.append(specialty.strip())

    if district and district.strip() and district.strip().lower() != "all":
        query += " AND (LOWER(district) = LOWER(?) OR LOWER(location) LIKE ?)"
        params.append(district.strip())
        params.append(f"%{district.strip()}%")

    if emergency_only:
        query += " AND LOWER(emergency_services) = 'yes' AND LOWER(emergency_load) != 'critical'"

    query += " ORDER BY id ASC;"

    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [row_to_hospital(r) for r in rows]


def get_hospital_by_id(hospital_id, db_path=None):
    """
    Retrieves a single hospital by its ID from SQLite using a parameterized query.
    Supports exact match, case-insensitivity, and flexible numeric formatting (e.g. H101 -> H0101).
    Enriches with ventilator metrics from resources table if present.
    """
    if not hospital_id:
        return None
    init_db(db_path)
    path = get_db_path(db_path)
    raw_id = str(hospital_id).strip()

    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hospitals WHERE UPPER(id) = UPPER(?) LIMIT 1;", (raw_id,))
        row = cursor.fetchone()

        if not row and raw_id.upper().startswith("H") and raw_id[1:].isdigit():
            num = int(raw_id[1:])
            alt_id = f"H{num:04d}" if len(raw_id) != 5 else f"H{num}"
            cursor.execute("SELECT * FROM hospitals WHERE UPPER(id) = UPPER(?) LIMIT 1;", (alt_id,))
            row = cursor.fetchone()

        if not row:
            return None

        h = row_to_hospital(row)
        actual_id = h.get("id") or raw_id
        cursor.execute(
            "SELECT total_quantity, available_quantity FROM resources WHERE UPPER(hospital_id) = UPPER(?) AND LOWER(resource_name) LIKE '%ventilator%' LIMIT 1;",
            (actual_id,)
        )
        v_row = cursor.fetchone()
        if v_row:
            h["ventilators_total"] = int(v_row["total_quantity"])
            h["ventilators_available"] = int(v_row["available_quantity"])

        return h


def create_hospital(data, db_path=None):
    """
    Creates a new hospital record in SQLite using parameterized queries.
    Validates required fields and numerical constraints.
    """
    if not data or not isinstance(data, dict):
        raise ValueError("Invalid hospital data provided.")

    name = str(data.get("name") or data.get("hospital_name") or "").strip()
    specialty = str(data.get("specialty") or "").strip()
    if not name or not specialty:
        raise ValueError("Hospital name and medical specialty are required fields.")

    init_db(db_path)
    path = get_db_path(db_path)

    with db_session(path) as conn:
        cursor = conn.cursor()

        # Generate unique ID if not explicitly provided
        new_id = data.get("id") or data.get("hospital_id")
        if not new_id:
            cursor.execute("SELECT id FROM hospitals ORDER BY LENGTH(id) DESC, id DESC LIMIT 1;")
            last_row = cursor.fetchone()
            if last_row and last_row["id"].startswith("H"):
                try:
                    num = int(last_row["id"][1:]) + 1
                    new_id = f"H{num:04d}" if len(last_row["id"]) == 5 else f"H{num}"
                except ValueError:
                    new_id = f"H{last_row['id']}_1"
            else:
                new_id = "H0001"

        # Check for duplicate ID
        cursor.execute("SELECT 1 FROM hospitals WHERE UPPER(id) = UPPER(?);", (new_id,))
        if cursor.fetchone():
            raise ValueError(f"Hospital with ID '{new_id}' already exists.")

        loc_val = str(data.get("location") or "").strip()
        dist_val = str(data.get("district") or "").strip()
        if not dist_val and loc_val:
            parts = [p.strip() for p in loc_val.split(",")]
            dist_val = parts[1] if len(parts) >= 2 else loc_val
        if not loc_val and dist_val:
            loc_val = dist_val
        if not dist_val:
            dist_val = "Central District"
            loc_val = "Central District"

        lat = float(data.get("latitude", 12.9716) or 12.9716)
        lng = float(data.get("longitude", 77.5946) or 77.5946)
        rating = max(1.0, min(5.0, float(data.get("rating", 4.5) or 4.5)))

        total_beds = max(1, int(data.get("total_beds", 100) or 100))
        avail_beds = max(0, min(total_beds, int(data.get("available_beds", 20) or 20)))

        icu_total = max(0, int(data.get("icu_total") if data.get("icu_total") is not None else data.get("icu_beds", 20) or 20))
        icu_avail = max(0, min(icu_total, int(data.get("icu_available") if data.get("icu_available") is not None else data.get("available_icu_beds", 5) or 5)))

        em_services = str(data.get("emergency_services", "Yes")).strip()
        em_load = str(data.get("emergency_load", "Medium")).strip()
        if em_load not in ["Low", "Medium", "High", "Critical"]:
            em_load = "Medium"

        cost = max(0, int(data.get("standard_cost") if data.get("standard_cost") is not None else data.get("estimated_cost", 35000) or 35000))
        wait = max(5, int(data.get("average_wait_minutes") if data.get("average_wait_minutes") is not None else data.get("average_wait_time", 30) or 30))
        queue_len = max(0, int(data.get("queue_length", 10) or 10))

        phone = str(data.get("phone") or data.get("contact") or "+91-80-2800-0000").strip()
        address = str(data.get("address") or loc_val or f"Medical Corridor, {dist_val}").strip()
        status = str(data.get("status", "Active")).strip()

        cursor.execute("""
            INSERT INTO hospitals (
                id, name, specialty, district, location, latitude, longitude, rating,
                total_beds, available_beds, icu_total, icu_available,
                emergency_services, emergency_load, standard_cost,
                average_wait_minutes, queue_length, phone, address, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (
            new_id, name, specialty, dist_val, loc_val, lat, lng, rating,
            total_beds, avail_beds, icu_total, icu_avail,
            em_services, em_load, cost, wait, queue_len, phone, address, status
        ))
        conn.commit()

        # Fetch and return the newly created record
        cursor.execute("SELECT * FROM hospitals WHERE id = ?;", (new_id,))
        return row_to_hospital(cursor.fetchone())


def update_hospital(hospital_id, data, db_path=None):
    """
    Updates an existing hospital record in SQLite using parameterized queries.
    Supports partial updates while enforcing data integrity constraints.
    """
    if not hospital_id or not data or not isinstance(data, dict):
        return None

    init_db(db_path)
    path = get_db_path(db_path)

    raw_id = str(hospital_id).strip()
    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hospitals WHERE UPPER(id) = UPPER(?);", (raw_id,))
        existing_row = cursor.fetchone()
        if not existing_row and raw_id.upper().startswith("H") and raw_id[1:].isdigit():
            num = int(raw_id[1:])
            alt_id = f"H{num:04d}" if len(raw_id) != 5 else f"H{num}"
            cursor.execute("SELECT * FROM hospitals WHERE UPPER(id) = UPPER(?);", (alt_id,))
            existing_row = cursor.fetchone()

        if not existing_row:
            return None

        current = dict(existing_row)
        updates = []
        params = []

        field_mapping = {
            "name": ("name", str),
            "hospital_name": ("name", str),
            "specialty": ("specialty", str),
            "district": ("district", str),
            "location": ("location", str),
            "latitude": ("latitude", float),
            "longitude": ("longitude", float),
            "rating": ("rating", float),
            "total_beds": ("total_beds", int),
            "available_beds": ("available_beds", int),
            "icu_total": ("icu_total", int),
            "icu_beds": ("icu_total", int),
            "icu_available": ("icu_available", int),
            "available_icu_beds": ("icu_available", int),
            "emergency_services": ("emergency_services", str),
            "emergency_load": ("emergency_load", str),
            "standard_cost": ("standard_cost", int),
            "estimated_cost": ("standard_cost", int),
            "average_wait_minutes": ("average_wait_minutes", int),
            "average_wait_time": ("average_wait_minutes", int),
            "queue_length": ("queue_length", int),
            "phone": ("phone", str),
            "contact": ("phone", str),
            "address": ("address", str),
            "status": ("status", str)
        }

        applied_cols = set()
        for key, val in data.items():
            if key in field_mapping and val is not None:
                col_name, cast_fn = field_mapping[key]
                if col_name in applied_cols:
                    continue
                try:
                    cast_val = cast_fn(val)
                    updates.append(f"{col_name} = ?")
                    params.append(cast_val)
                    applied_cols.add(col_name)
                except (ValueError, TypeError):
                    continue

        if not updates:
            return row_to_hospital(existing_row)

        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(current["id"])
        sql = f"UPDATE hospitals SET {', '.join(updates)} WHERE id = ?;"

        cursor.execute(sql, params)
        conn.commit()

        cursor.execute("SELECT * FROM hospitals WHERE id = ?;", (current["id"],))
        return row_to_hospital(cursor.fetchone())


def delete_hospital(hospital_id, db_path=None):
    """
    Deletes a hospital and its associated resources/surgeries (via CASCADE)
    using a parameterized query. Returns True if deleted, False if not found.
    """
    if not hospital_id:
        return False

    init_db(db_path)
    path = get_db_path(db_path)

    raw_id = str(hospital_id).strip()
    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM hospitals WHERE UPPER(id) = UPPER(?);", (raw_id,))
        row = cursor.fetchone()
        if not row and raw_id.upper().startswith("H") and raw_id[1:].isdigit():
            num = int(raw_id[1:])
            alt_id = f"H{num:04d}" if len(raw_id) != 5 else f"H{num}"
            cursor.execute("SELECT id FROM hospitals WHERE UPPER(id) = UPPER(?);", (alt_id,))
            row = cursor.fetchone()

        if not row:
            return False

        target_id = row["id"]
        cursor.execute("DELETE FROM hospitals WHERE id = ?;", (target_id,))
        conn.commit()
        return True


def get_surgeries(hospital_id=None, specialty=None, location=None, db_path=None):
    """Retrieves surgery records from SQLite with optional filtering."""
    init_db(db_path)
    path = get_db_path(db_path)

    query = "SELECT * FROM surgeries WHERE status = 'Active'"
    params = []

    if hospital_id:
        query += " AND UPPER(hospital_id) = UPPER(?)"
        params.append(hospital_id.strip())
    if specialty and specialty.strip() and specialty.strip().lower() != "all":
        query += " AND LOWER(specialty) = LOWER(?)"
        params.append(specialty.strip())
    if location and location.strip() and location.strip().lower() != "all":
        query += " AND (LOWER(location) = LOWER(?) OR LOWER(location) LIKE ?)"
        params.append(location.strip())
        params.append(f"%{location.strip()}%")

    query += " ORDER BY id ASC;"

    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [row_to_surgery(r) for r in cursor.fetchall()]


def get_blood_banks(blood_group=None, district=None, db_path=None):
    """Retrieves blood bank records from SQLite with optional filtering."""
    init_db(db_path)
    path = get_db_path(db_path)

    query = "SELECT * FROM blood_banks WHERE status = 'Active'"
    params = []

    if blood_group and blood_group.strip() and blood_group.strip().lower() != "all":
        query += " AND UPPER(blood_group) = UPPER(?)"
        params.append(blood_group.strip())
    if district and district.strip() and district.strip().lower() != "all":
        query += " AND (LOWER(district) = LOWER(?) OR LOWER(district) LIKE ?)"
        params.append(district.strip())
        params.append(f"%{district.strip()}%")

    query += " ORDER BY id ASC;"

    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [row_to_blood_bank(r) for r in cursor.fetchall()]


def get_resources(hospital_id=None, status=None, db_path=None):
    """Retrieves resource inventory records from SQLite with optional filtering."""
    init_db(db_path)
    path = get_db_path(db_path)

    query = "SELECT * FROM resources WHERE 1=1"
    params = []

    if status and status.strip().lower() != "all":
        query += " AND LOWER(status) = LOWER(?)"
        params.append(status.strip())

    if hospital_id:
        query += " AND UPPER(hospital_id) = UPPER(?)"
        params.append(hospital_id.strip())

    query += " ORDER BY id ASC;"

    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return [row_to_resource(r) for r in cursor.fetchall()]


def get_system_resource_summary(db_path=None):
    """
    Computes system-wide aggregate bed and ICU metrics directly from SQLite.
    """
    init_db(db_path)
    path = get_db_path(db_path)

    with db_session(path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                SUM(total_beds) AS total_registered_beds,
                SUM(available_beds) AS available_beds,
                SUM(icu_total) AS total_icu_beds,
                SUM(icu_available) AS available_icu_beds
            FROM hospitals
            WHERE status = 'Active';
        """)
        row = cursor.fetchone()
        tot_beds = int(row["total_registered_beds"] or 0)
        avail_beds = int(row["available_beds"] or 0)
        tot_icu = int(row["total_icu_beds"] or 0)
        avail_icu = int(row["available_icu_beds"] or 0)

        return {
            "total_registered_beds": tot_beds,
            "available_beds": avail_beds,
            "occupied_beds": tot_beds - avail_beds,
            "total_icu_beds": tot_icu,
            "available_icu_beds": avail_icu
        }
