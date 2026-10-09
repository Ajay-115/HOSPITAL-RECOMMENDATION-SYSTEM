"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Backend Service: Database-Backed Data Access Layer (Phase 5 SQLite Integration)
"""

import os
from database.database import (
    init_db,
    get_all_hospitals as db_get_all_hospitals,
    get_hospital_by_id as db_get_hospital_by_id,
    create_hospital as db_create_hospital,
    update_hospital as db_update_hospital,
    delete_hospital as db_delete_hospital,
    get_surgeries as db_get_surgeries,
    get_blood_banks as db_get_blood_banks,
    get_resources as db_get_resources,
    get_system_resource_summary as db_get_system_resource_summary
)

# Auto-initialize database schema and seeds on startup
try:
    init_db()
except Exception as e:
    print(f"[WARN] Database initialization warning in data_loader: {e}")


def get_hospitals(reload=False):
    """
    Returns all active hospitals from SQLite persistent database.
    Always reads the latest state so admin modifications take immediate effect.
    """
    return db_get_all_hospitals()


def get_hospital_by_id(hospital_id):
    """Returns a single hospital by ID from SQLite."""
    return db_get_hospital_by_id(hospital_id)


def get_surgeries(reload=False):
    """Returns all active surgical procedures from SQLite."""
    return db_get_surgeries()


def get_blood_banks(reload=False):
    """Returns all active blood bank inventories from SQLite."""
    return db_get_blood_banks()


def get_resources(hospital_id=None):
    """Returns all available hospital resources from SQLite."""
    return db_get_resources(hospital_id=hospital_id)


def get_resource_summary():
    """Returns aggregated bed, ICU, and ventilator capacity from SQLite."""
    return db_get_system_resource_summary()


def save_hospitals(records):
    """
    Backward compatibility helper for any legacy caller.
    Synchronizes records to SQLite database.
    """
    for rec in records:
        hid = rec.get("id") or rec.get("hospital_id")
        if not hid:
            continue
        existing = db_get_hospital_by_id(hid)
        if existing:
            db_update_hospital(hid, rec)
        else:
            db_create_hospital(rec)
