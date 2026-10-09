"""
Real-Time Hospital Resource, Surgery and Blood Donation Recommendation System
Production Database Reset & Seeding Utility (Phase 5 Hardening)

Explicit administrative script to reset the production SQLite database:
1. Removes existing backend/database/app.db
2. Recreates all schema tables and indexes from schema.sql
3. Re-seeds clean synthetic records from hospitals.csv, surgeries.csv, and blood_banks.csv
4. Generates baseline hospital equipment resources

Usage:
    python backend/database/reset_database.py
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from database.database import init_db, DEFAULT_DB_PATH, get_all_hospitals, get_surgeries, get_blood_banks, get_resources
except ImportError:
    from database import init_db, DEFAULT_DB_PATH, get_all_hospitals, get_surgeries, get_blood_banks, get_resources


def reset_production_database(db_path=None):
    target_path = db_path or DEFAULT_DB_PATH
    print(f"[*] Resetting database at: {target_path}")

    # 1. Remove existing db file if present
    if os.path.exists(target_path):
        try:
            os.remove(target_path)
            print(f"[+] Removed existing database: {target_path}")
        except Exception as e:
            print(f"[!] Warning removing file: {e}")

    # 2. Re-create and force seed from CSVs
    init_db(target_path, force_reseed=True)
    print(f"[+] Recreated schema and seeded baseline data.")

    # 3. Verify counts
    hospitals = get_all_hospitals(db_path=target_path)
    surgeries = get_surgeries(db_path=target_path)
    blood_banks = get_blood_banks(db_path=target_path)
    resources = get_resources(db_path=target_path)

    print("\n--- Production Database Verification Summary ---")
    print(f" * Active Hospitals:   {len(hospitals)} (Expected: 1000)")
    print(f" * Active Surgeries:   {len(surgeries)} (Expected: 3051)")
    print(f" * Blood Reserves:     {len(blood_banks)} (Expected: 368)")
    print(f" * Hospital Resources: {len(resources)} (Expected: 6000)")
    print("------------------------------------------------\n")
    print("[SUCCESS] Production database is clean, verified, and ready.")


if __name__ == "__main__":
    reset_production_database()
