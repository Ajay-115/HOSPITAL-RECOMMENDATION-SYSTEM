"""
Automated Test Suite: Explainable Recommendation Engine (Phase 3)
"""

import sys
import os
import unittest

# Ensure backend package can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend")))

from services.recommendation import (
    RECOMMENDATION_WEIGHTS,
    validate_criteria,
    normalize_features,
    compute_hospital_score,
    rank_hospitals,
    calculate_distance
)
from services.data_loader import get_hospitals


class TestRecommendationEngine(unittest.TestCase):

    def setUp(self):
        self.hospitals = get_hospitals()
        self.assertTrue(len(self.hospitals) > 0, "Hospital dataset must not be empty.")

    def test_weights_sum_to_one(self):
        """Verify weights strictly sum to 1.0 (100%)"""
        total_weight = sum(RECOMMENDATION_WEIGHTS.values())
        self.assertAlmostEqual(total_weight, 1.0, places=5, msg="Recommendation weights must sum to 1.0")

    def test_feature_normalization_bounds(self):
        """Verify all 6 normalized factor scores are strictly between 0 and 100"""
        for hospital in self.hospitals:
            criteria = {"specialty": "Cardiology", "location": "Central District"}
            dist = calculate_distance(criteria["location"], hospital)
            norm = normalize_features(hospital, criteria, dist)

            for factor_key in ["specialty_match", "distance", "resource_availability", "waiting_time", "cost", "emergency_load"]:
                val = norm[factor_key]
                self.assertGreaterEqual(val, 0, f"Factor {factor_key} was below 0: {val}")
                self.assertLessEqual(val, 100, f"Factor {factor_key} was above 100: {val}")

    def test_strict_specialty_eligibility_filter(self):
        """
        Verify that requesting a specific specialty strictly excludes hospitals
        that do not provide that specialty, regardless of their other scores.
        """
        criteria = {"specialty": "Cardiology", "location": "Central District"}
        result = rank_hospitals(self.hospitals, criteria)

        self.assertTrue(result["success"])
        self.assertGreater(result["count"], 0)

        for rec in result["recommendations"]:
            self.assertEqual(
                rec["specialty"].lower(),
                "cardiology",
                f"Hospital {rec['hospital_name']} has specialty {rec['specialty']}, expected Cardiology only."
            )

    def test_strict_emergency_eligibility_filter(self):
        """Verify emergency filter strictly excludes facilities without emergency services or critical load"""
        criteria = {"specialty": "All", "emergency_only": True}
        result = rank_hospitals(self.hospitals, criteria)

        self.assertTrue(result["success"])
        for rec in result["recommendations"]:
            self.assertEqual(str(rec.get("emergency_services")).lower(), "yes")
            self.assertNotEqual(str(rec.get("emergency_load")).lower(), "critical")

    def test_resource_minimum_constraints(self):
        """Verify min_available_beds filter enforces threshold"""
        criteria = {"min_available_beds": 60}
        result = rank_hospitals(self.hospitals, criteria)

        self.assertTrue(result["success"])
        for rec in result["recommendations"]:
            self.assertGreaterEqual(int(rec["available_beds"]), 60)

    def test_score_explainability_and_breakdown(self):
        """Verify each recommendation contains a human explanation and 6-factor mathematical breakdown"""
        criteria = {"specialty": "Neurology", "location": "North Zone"}
        result = rank_hospitals(self.hospitals, criteria)

        self.assertTrue(result["success"])
        self.assertGreater(result["count"], 0)

        top_pick = result["recommendations"][0]
        self.assertIn("recommendation_score", top_pick)
        self.assertIn("factor_breakdown", top_pick)
        self.assertIn("recommendation_explanation", top_pick)
        self.assertTrue(len(top_pick["recommendation_explanation"]) > 10)

        bd = top_pick["factor_breakdown"]
        self.assertEqual(len(bd), 6)
        expected_keys = {"specialty_match", "distance", "resource_availability", "waiting_time", "cost", "emergency_load"}
        self.assertEqual(set(bd.keys()), expected_keys)

    def test_zero_results_and_suggestions(self):
        """Verify impossible constraints return 0 results with actionable suggestions"""
        criteria = {"specialty": "Cardiology", "max_cost": 500}  # No cardiology under INR 500
        result = rank_hospitals(self.hospitals, criteria)

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 0)
        self.assertEqual(len(result["recommendations"]), 0)
        self.assertTrue(len(result["suggestions"]) > 0, "Zero-result response must provide relaxation suggestions")

    def test_input_validation(self):
        """Verify criteria validator flags invalid types and values"""
        # Negative max cost
        valid, _, errs = validate_criteria({"max_cost": -500})
        self.assertFalse(valid)
        self.assertTrue(any("negative" in e for e in errs))

        # Invalid type for criteria
        valid, _, errs = validate_criteria("not a dict")
        self.assertFalse(valid)


if __name__ == "__main__":
    unittest.main()
