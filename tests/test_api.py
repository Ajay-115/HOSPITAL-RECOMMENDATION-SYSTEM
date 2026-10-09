"""
Automated Test Suite: Flask REST API Endpoints (Phase 3 & Phase 4)
"""

import sys
import os
import unittest
import json

import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend")))

from database.database import init_db, get_default_db_path, set_active_db_path, get_all_hospitals
from app import app


class TestRestAPI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.prod_db_path = get_default_db_path()
        cls.prod_hosp_count_before = len(get_all_hospitals(db_path=cls.prod_db_path))

        cls.test_db_fd, cls.test_db_path = tempfile.mkstemp(suffix="_test_api.db")
        os.close(cls.test_db_fd)
        init_db(cls.test_db_path, force_reseed=True)

        app.config["TESTING"] = True
        app.config["DATABASE_PATH"] = cls.test_db_path
        set_active_db_path(cls.test_db_path)

    @classmethod
    def tearDownClass(cls):
        set_active_db_path(None)
        app.config["DATABASE_PATH"] = cls.prod_db_path
        if os.path.exists(cls.test_db_path):
            try:
                os.remove(cls.test_db_path)
            except OSError:
                pass
        prod_hosp_count_after = len(get_all_hospitals(db_path=cls.prod_db_path))
        assert prod_hosp_count_after == cls.prod_hosp_count_before

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_health_endpoint(self):
        """Test GET /api/health"""
        res = self.app.get("/api/health")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data["status"], "healthy")

    def test_hospitals_get_endpoint(self):
        """Test GET /api/hospitals"""
        res = self.app.get("/api/hospitals?specialty=Neurology")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["success"])
        self.assertTrue(data["count"] > 0)
        for h in data["data"]:
            self.assertEqual(h["specialty"], "Neurology")

    def test_hospital_detail_endpoint(self):
        """Test GET /api/hospitals/<id>"""
        res = self.app.get("/api/hospitals/H101")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["success"])
        self.assertEqual(data["data"]["hospital"]["hospital_id"], "H101")
        self.assertIn("baseline_recommendation", data["data"])

    def test_recommend_endpoint_valid(self):
        """Test POST /api/recommend with valid criteria"""
        payload = {
            "location": "Central District",
            "specialty": "Cardiology",
            "max_cost": 70000,
            "max_wait": 60,
            "emergency_only": False,
            "sort_by": "score_desc"
        }
        res = self.app.post("/api/recommend", data=json.dumps(payload), content_type="application/json")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["success"])
        self.assertGreater(data["count"], 0)
        top = data["recommendations"][0]
        self.assertEqual(top["specialty"], "Cardiology")
        self.assertIn("factor_breakdown", top)
        self.assertIn("recommendation_explanation", top)

    def test_recommend_endpoint_invalid_input(self):
        """Test POST /api/recommend with negative cost returns 400"""
        payload = {"max_cost": -100}
        res = self.app.post("/api/recommend", data=json.dumps(payload), content_type="application/json")
        self.assertEqual(res.status_code, 400)
        data = res.get_json()
        self.assertFalse(data["success"])
        self.assertIn("error", data)

    def test_surgeries_endpoint(self):
        """Test GET /api/surgeries and POST /api/surgery/recommend"""
        res = self.app.get("/api/surgeries")
        self.assertEqual(res.status_code, 200)
        self.assertGreater(res.get_json()["count"], 0)

        payload = {"specialty": "Orthopedics", "max_budget": 200000}
        res2 = self.app.post("/api/surgery/recommend", data=json.dumps(payload), content_type="application/json")
        self.assertEqual(res2.status_code, 200)
        self.assertGreater(res2.get_json()["count"], 0)

    def test_blood_endpoints(self):
        """Test GET /api/blood and POST /api/blood/search"""
        res = self.app.get("/api/blood?group=O+")
        self.assertEqual(res.status_code, 200)
        self.assertGreater(res.get_json()["count"], 0)

        payload = {"blood_group": "B+", "location": "North Zone", "units_required": 5}
        res2 = self.app.post("/api/blood/search", data=json.dumps(payload), content_type="application/json")
        self.assertEqual(res2.status_code, 200)
        self.assertTrue(res2.get_json()["success"])

    def test_resources_and_analytics_endpoints(self):
        """Test GET /api/resources and GET /api/analytics"""
        res1 = self.app.get("/api/resources")
        self.assertEqual(res1.status_code, 200)
        self.assertIn("summary", res1.get_json())

        res2 = self.app.get("/api/analytics")
        self.assertEqual(res2.status_code, 200)
        self.assertIn("analytics", res2.get_json())


    def test_waiting_time_analytics_endpoint(self):
        """Test GET /api/waiting-time/analytics"""
        res = self.app.get("/api/waiting-time/analytics")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["success"])
        self.assertIn("average_wait_minutes", data["data"])
        self.assertIn("records", data["data"])
        self.assertGreater(len(data["data"]["records"]), 0)

    def test_admin_hospital_crud_lifecycle(self):
        """Test POST, PUT, and DELETE /api/hospitals endpoints"""
        # 1. POST new hospital
        new_hosp = {
            "hospital_name": "Test General Hospital",
            "specialty": "Cardiology",
            "location": "North Zone",
            "total_beds": 150,
            "available_beds": 45,
            "icu_beds": 30,
            "available_icu_beds": 10,
            "estimated_cost": 42000,
            "average_wait_time": 25,
            "emergency_load": "Low"
        }
        post_res = self.app.post("/api/hospitals", data=json.dumps(new_hosp), content_type="application/json")
        self.assertEqual(post_res.status_code, 201)
        post_data = post_res.get_json()
        self.assertTrue(post_data["success"])
        created_id = post_data["hospital"]["hospital_id"]

        # 2. PUT update hospital
        put_res = self.app.put(f"/api/hospitals/{created_id}", data=json.dumps({"available_beds": 12}), content_type="application/json")
        self.assertEqual(put_res.status_code, 200)
        put_data = put_res.get_json()
        self.assertTrue(put_data["success"])
        self.assertEqual(put_data["hospital"]["available_beds"], 12)

        # 3. DELETE hospital
        del_res = self.app.delete(f"/api/hospitals/{created_id}")
        self.assertEqual(del_res.status_code, 200)
        del_data = del_res.get_json()
        self.assertTrue(del_data["success"])

    def test_recommend_endpoint_zero_results(self):
        """Test POST /api/recommend with impossible filters returns clean zero-result and suggestions"""
        payload = {
            "specialty": "Cardiology",
            "max_cost": 10, # Impossibly low
            "min_beds": 99999
        }
        res = self.app.post("/api/recommend", data=json.dumps(payload), content_type="application/json")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertTrue(data["success"])
        self.assertEqual(data["count"], 0)
        self.assertIn("message", data)
        self.assertIn("suggestions", data)


if __name__ == "__main__":
    unittest.main()

