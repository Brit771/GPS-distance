import unittest
from unittest.mock import patch, MagicMock
from client import fetch_and_process_data, process_gps_data, calculate_distance, parse_data

class TestClient(unittest.TestCase):
    def test_calculate_distance(self):
        """Test the Haversine distance calculation function."""
        lat1, lng1 = 52.2296756, 21.0122287
        lat2, lng2 = 41.8919300, 12.5113300
        expected_distance = 1315.51  # Approximate distance in kilometers
        result = calculate_distance(lat1, lng1, lat2, lng2)
        print(f"Calculated: {result}, Expected: {expected_distance}")
        self.assertAlmostEqual(result, expected_distance, places=2)

    def test_process_gps_data_valid(self):
        previous = {"lat": 52.2296756, "lng": 21.0122287}
        current = {"lat": "41.8919300", "lng": "12.5113300"}
        distance, new_point = process_gps_data(previous, current)
        self.assertAlmostEqual(distance, 1315.51, places=2)
        self.assertEqual(new_point, {"lat": 41.89193, "lng": 12.51133})

    def test_process_gps_data_invalid(self):
        """Test processing GPS data with invalid inputs."""
        previous = {"lat": 52.2296756, "lng": 21.0122287}
        current = {"lat": "invalid", "lng": "12.5113300"}
        distance, new_point = process_gps_data(previous, current)
        self.assertEqual(distance, 0)
        self.assertEqual(new_point, previous)

    def test_parse_data_valid(self):
        """Test parsing valid JSON data."""
        line = '{"gps": {"lat": "52.2296756", "lng": "21.0122287"}}'
        parsed = parse_data(line)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["gps"]["lat"], "52.2296756")

    def test_parse_data_invalid(self):
        """Test parsing invalid JSON data."""
        line = '{"gps": {"lat": "52.2296756", "lng": "21.0122287"'
        parsed = parse_data(line)
        self.assertIsNone(parsed)

if __name__ == "__main__":
    unittest.main()
