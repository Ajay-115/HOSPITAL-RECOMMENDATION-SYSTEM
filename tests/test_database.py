"""
Automated Test Suite: SQLite Database Persistence & Admin CRUD Integration (Phase 5 Hardening)
Verifies database creation, isolation, CRUD operations, persistence across connections,
and real-time impact on the recommendation engine without touching the production database.
"""

import sys
import os
import unittest
import tempfile
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend")))

from database.database import (
    init_db,
    get_all_hospitals,
    get_hospital_by_id,
    create_hospital,
    update_hospital,
    delete_hospital,
    get_surgeries,
    get_blood_banks,
    get_resources,
    get_system_resource_summary,
    get_default_db_path,
    set_active_db_path
)
from services.recommendation import rank_hospitals, compute_hospital_score
from app import app


class TestDatabasePersistence(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # 1. Snapshot production database state before tests run
        cls.prod_db_path = get_default_db_path()
        cls.prod_hosp_count_before = len(get_all_hospitals(db_path=cls.prod_db_path))

        # 2. Create an isolated temporary SQLite database specifically for unit tests
        cls.test_db_fd, cls.test_db_path = tempfile.mkstemp(suffix="_test_app.db")
        os.close(cls.test_db_fd)
        init_db(cls.test_db_path, force_reseed=True)

        # 3. Configure Flask and global DAL to point exclusively to the isolated test database
        app.config["TESTING"] = True
        app.config["DATABASE_PATH"] = cls.test_db_path
        set_active_db_path(cls.test_db_path)

    @classmethod
    def tearDownClass(cls):
        # 1. Reset active database path back to production
        set_active_db_path(None)
        app.config["DATABASE_PATH"] = cls.prod_db_path

        # 2. Clean up temporary test database file
        if os.path.exists(cls.test_db_path):
            try:
                os.remove(cls.test_db_path)
            except OSError:
                pass

        # 3. Verify production database was NEVER touched by automated tests
        prod_hosp_count_after = len(get_all_hospitals(db_path=cls.prod_db_path))
        assert prod_hosp_count_after == cls.prod_hosp_count_before, (
            f"Isolation failure: Production database was modified! "
            f"Before: {cls.prod_hosp_count_before}, After: {prod_hosp_count_after}"
        )

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_01_database_creation(self):
        """1. Verify tables and indexes are created successfully in test database"""
        hospitals = get_all_hospitals(db_path=self.test_db_path)
        surgeries = get_surgeries(db_path=self.test_db_path)
        blood_banks = get_blood_banks(db_path=self.test_db_path)
        resources = get_resources(db_path=self.test_db_path)

        self.assertGreaterEqual(len(hospitals), 20, "Hospitals table must contain seeded records.")
        self.assertGreaterEqual(len(surgeries), 15, "Surgeries table must contain seeded records.")
        self.assertGreaterEqual(len(blood_banks), 20, "Blood banks table must contain seeded records.")
        self.assertGreaterEqual(len(resources), 20, "Resources table must contain seeded records.")

    def test_02_hospital_insertion(self):
        """2. Verify inserting a new hospital into SQLite test database"""
        data = {
            "name": "St. Teresa Heart Institute",
            "specialty": "Cardiology",
            "district": "North Zone",
            "latitude": 13.045,
            "longitude": 77.585,
            "total_beds": 180,
            "available_beds": 55,
            "icu_total": 35,
            "icu_available": 12,
            "standard_cost": 48000,
            "average_wait_minutes": 25,
            "emergency_services": "Yes",
            "emergency_load": "Low",
            "rating": 4.9
        }
        created = create_hospital(data, db_path=self.test_db_path)
        self.assertIsNotNone(created)
        self.assertEqual(created["hospital_name"], "St. Teresa Heart Institute")
        self.assertEqual(created["available_beds"], 55)
        self.assertTrue(created["id"].startswith("H"))

    def test_03_hospital_retrieval(self):
        """3. Verify retrieving hospitals by ID and by filters from test database"""
        h = get_hospital_by_id("H0001", db_path=self.test_db_path)
        self.assertIsNotNone(h)
        self.assertEqual(h["hospital_id"], "H0001")
        self.assertEqual(h["specialty"], "General Medicine")

        gen_med = get_all_hospitals(specialty="General Medicine", db_path=self.test_db_path)
        self.assertGreater(len(gen_med), 0)
        for gm in gen_med:
            self.assertEqual(gm["specialty"], "General Medicine")

    def test_04_hospital_update(self):
        """4. Verify updating hospital capacity and details in test database"""
        updated = update_hospital("H101", {
            "available_beds": 5,
            "icu_available": 1,
            "emergency_load": "Critical",
            "standard_cost": 85000
        }, db_path=self.test_db_path)

        self.assertIsNotNone(updated)
        self.assertEqual(updated["available_beds"], 5)
        self.assertEqual(updated["available_icu_beds"], 1)
        self.assertEqual(updated["emergency_load"], "Critical")
        self.assertEqual(updated["estimated_cost"], 85000)

        # Confirm persisted on re-fetch from test DB
        fetched = get_hospital_by_id("H101", db_path=self.test_db_path)
        self.assertEqual(fetched["available_beds"], 5)
        self.assertEqual(fetched["emergency_load"], "Critical")

    def test_05_hospital_deletion(self):
        """5. Verify hospital deletion and cascade consistency in test database"""
        temp = create_hospital({
            "name": "Deletable Clinic",
            "specialty": "General Medicine",
            "district": "South Zone"
        }, db_path=self.test_db_path)
        temp_id = temp["id"]

        deleted = delete_hospital(temp_id, db_path=self.test_db_path)
        self.assertTrue(deleted)
        self.assertIsNone(get_hospital_by_id(temp_id, db_path=self.test_db_path))

    def test_06_persistence_across_connections(self):
        """6. Verify data persists after reopening database connection"""
        data = {
            "id": "H999",
            "name": "Persistent Test Infirmary",
            "specialty": "Pediatrics",
            "district": "East Metro",
            "available_beds": 18
        }
        create_hospital(data, db_path=self.test_db_path)

        reloaded = get_hospital_by_id("H999", db_path=self.test_db_path)
        self.assertIsNotNone(reloaded)
        self.assertEqual(reloaded["name"], "Persistent Test Infirmary")
        self.assertEqual(reloaded["available_beds"], 18)

    def test_07_recommendation_reading_updated_database_values(self):
        """7. Verify recommendation engine scores reflect updated database metrics immediately"""
        h_before = get_hospital_by_id("H102", db_path=self.test_db_path)
        score_before = compute_hospital_score(h_before, {"specialty": h_before["specialty"]})["total_score"]

        # Drop available beds from 42 to 1 and cost to 95000 in test database
        update_hospital("H102", {"available_beds": 1, "standard_cost": 95000}, db_path=self.test_db_path)
        h_after = get_hospital_by_id("H102", db_path=self.test_db_path)
        score_after = compute_hospital_score(h_after, {"specialty": h_after["specialty"]})["total_score"]

        self.assertLess(score_after, score_before, "Recommendation score must decrease when resources drop and cost rises.")

    def test_08_admin_created_hospital_in_get_api(self):
        """8. Admin-created hospital via POST /api/hospitals operates on test database and appears in GET /api/hospitals"""
        payload = {
            "hospital_name": "API Test Healthcare",
            "specialty": "Neurology",
            "location": "Central District",
            "total_beds": 200,
            "available_beds": 40,
            "icu_beds": 25,
            "available_icu_beds": 8,
            "estimated_cost": 50000,
            "emergency_load": "Low"
        }
        post_res = self.app.post("/api/hospitals", data=json.dumps(payload), content_type="application/json")
        self.assertEqual(post_res.status_code, 201)
        created_id = post_res.get_json()["hospital"]["hospital_id"]

        get_res = self.app.get("/api/hospitals")
        self.assertEqual(get_res.status_code, 200)
        hospitals = get_res.get_json()["data"]
        matching = [h for h in hospitals if h["hospital_id"] == created_id]
        self.assertEqual(len(matching), 1, "Admin created hospital must appear in GET /api/hospitals.")

    def test_09_admin_updated_hospital_affects_recommendations(self):
        """9. Admin PUT /api/hospitals/<id> immediately changes live recommendation results in test database"""
        hid = "H0001"
        self.app.put(f"/api/hospitals/{hid}", data=json.dumps({
            "available_beds": 200,
            "estimated_cost": 15000,
            "average_wait_time": 15,
            "emergency_load": "Low"
        }), content_type="application/json")

        rec_res = self.app.post("/api/recommend", data=json.dumps({
            "specialty": "General Medicine"
        }), content_type="application/json")
        self.assertEqual(rec_res.status_code, 200)
        recs = rec_res.get_json()["recommendations"]
        h_rec = next((r for r in recs if r["hospital_id"] == hid), None)
        self.assertIsNotNone(h_rec)
        self.assertEqual(h_rec["factor_breakdown"]["emergency_load"], 100)
        self.assertGreaterEqual(h_rec["recommendation_score"], 70)

    def test_10_deleted_hospital_excluded_from_recommendations(self):
        """10. Deleted hospital via DELETE /api/hospitals/<id> is excluded from recommendations"""
        payload = {
            "hospital_name": "Ephemeral Medical Center",
            "specialty": "Dermatology",
            "location": "Central District"
        }
        post_res = self.app.post("/api/hospitals", data=json.dumps(payload), content_type="application/json")
        del_id = post_res.get_json()["hospital"]["hospital_id"]

        # Confirm it is found
        rec_res1 = self.app.post("/api/recommend", data=json.dumps({"specialty": "Dermatology"}), content_type="application/json")
        self.assertTrue(any(r["hospital_id"] == del_id for r in rec_res1.get_json()["recommendations"]))

        # Delete it
        del_res = self.app.delete(f"/api/hospitals/{del_id}")
        self.assertEqual(del_res.status_code, 200)

        # Confirm it no longer appears in recommendations
        rec_res2 = self.app.post("/api/recommend", data=json.dumps({"specialty": "Dermatology"}), content_type="application/json")
        self.assertFalse(any(r["hospital_id"] == del_id for r in rec_res2.get_json()["recommendations"]))

    def test_11_admin_frontend_api_contract(self):
        """11. Verify GET /api/hospitals adheres to { success: True, count: N, data: [...] } contract"""
        res = self.app.get("/api/hospitals")
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertIn("success", body)
        self.assertTrue(body["success"])
        self.assertIn("count", body)
        self.assertIsInstance(body["count"], int)
        self.assertIn("data", body)
        self.assertIsInstance(body["data"], list)
        self.assertEqual(len(body["data"]), body["count"])

    def test_12_production_database_isolation_check(self):
        """12. Explicitly verify production app.db was not modified by any test in this suite"""
        prod_hospitals = get_all_hospitals(db_path=self.prod_db_path)
        self.assertEqual(
            len(prod_hospitals),
            self.prod_hosp_count_before,
            f"Production app.db hospital count changed! Expected {self.prod_hosp_count_before}, found {len(prod_hospitals)}"
        )
        # Ensure none of the test hospital names were written to production app.db
        test_names = ["API Test Healthcare", "Persistent Test Infirmary", "Ephemeral Medical Center", "St. Teresa Heart Institute"]
        for h in prod_hospitals:
            self.assertNotIn(h["name"], test_names, f"Test record '{h['name']}' leaked into production database!")


if __name__ == "__main__":
    unittest.main()
