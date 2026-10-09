"""
Automated Test Suite: Waiting Time Estimation Engine (Phase 3)
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "backend")))

from services.waiting_time import estimate_waiting_time, get_hospital_wait_analytics
from services.data_loader import get_hospitals


class TestWaitingTimeEngine(unittest.TestCase):

    def test_deterministic_calculation(self):
        """
        Verify formula: ((Q * S) / C) * Gamma
        With Q=16, S=8, C=4, Gamma(Low)=1.0:
        (16 * 8 / 4) * 1.0 = 32 mins
        """
        wait = estimate_waiting_time(queue_length=16, avg_service_time=8, emergency_load="low", channels=4)
        self.assertEqual(wait, 32)

    def test_emergency_load_multipliers(self):
        """Verify higher emergency load increases waiting time monotonically"""
        low = estimate_waiting_time(10, 8, "low")
        med = estimate_waiting_time(10, 8, "medium")
        high = estimate_waiting_time(10, 8, "high")
        crit = estimate_waiting_time(10, 8, "critical")

        self.assertLess(low, med)
        self.assertLess(med, high)
        self.assertLess(high, crit)

    def test_historical_blending(self):
        """Verify dynamic wait blends with historical baseline using alpha weight"""
        # Dynamic: (10 * 8 / 4) * 1.25 = 25
        # Historical: 45
        # Alpha: 0.5 -> (0.5 * 25) + (0.5 * 45) = 35
        blended = estimate_waiting_time(10, 8, "medium", channels=4, historical_avg_wait=45, alpha=0.5)
        self.assertEqual(blended, 35)

    def test_extreme_inputs(self):
        """Verify zero queue length or None returns sensible minimum without exception"""
        self.assertGreaterEqual(estimate_waiting_time(0, 8, "low"), 5)
        self.assertGreaterEqual(estimate_waiting_time(None, None, None), 5)

    def test_wait_analytics(self):
        """Verify system-wide waiting time analytics calculation"""
        hospitals = get_hospitals()
        analytics = get_hospital_wait_analytics(hospitals)

        self.assertIn("average_wait_minutes", analytics)
        self.assertGreater(analytics["average_wait_minutes"], 0)
        self.assertEqual(analytics["hospitals_analyzed"], len(hospitals))
        self.assertIn("Deterministic M/M/c", analytics["methodology"]["model_type"])


if __name__ == "__main__":
    unittest.main()
