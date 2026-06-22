import unittest

from scrapers.geocode_listings import _matches_city


class GeocodeListingsCityMatchTests(unittest.TestCase):
    def test_matches_nagpur_city_token(self):
        result = {
            "display_name": "Wardha Road, Nagpur, Maharashtra, India",
            "address": {"city": "Nagpur", "state": "Maharashtra"},
        }
        self.assertTrue(_matches_city(result, "Nagpur"))

    def test_rejects_wrong_city_for_nagpur(self):
        result = {
            "display_name": "Civil Lines, Amravati, Maharashtra, India",
            "address": {"city": "Amravati", "state": "Maharashtra"},
        }
        self.assertFalse(_matches_city(result, "Nagpur"))

    def test_matches_phase_2_city_aliases(self):
        nashik = {
            "display_name": "Gangapur Road, Nasik, Maharashtra, India",
            "address": {"city": "Nasik", "state": "Maharashtra"},
        }
        aurangabad = {
            "display_name": "CIDCO, Chhatrapati Sambhajinagar, Maharashtra, India",
            "address": {
                "city": "Chhatrapati Sambhajinagar",
                "state": "Maharashtra",
            },
        }
        self.assertTrue(_matches_city(nashik, "Nashik"))
        self.assertTrue(_matches_city(aurangabad, "Aurangabad"))


if __name__ == "__main__":
    unittest.main()
