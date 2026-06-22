import unittest
from unittest.mock import patch

import main
from scrapers.scraper_igr import ScraperIGR


class CitySupportTest(unittest.TestCase):
    def test_canonical_city_names_include_phase_2_cities(self):
        self.assertEqual(main._canonical_city_name(" nashik "), "Nashik")
        self.assertEqual(main._canonical_city_name("aurangabad"), "Aurangabad")
        self.assertIn("Nashik", main.SUPPORTED_CITIES)
        self.assertIn("Aurangabad", main.SUPPORTED_CITIES)

    def test_supported_city_can_be_created_for_first_run(self):
        def fake_select_rows(table, filters=None, limit=100):
            self.assertEqual(table, "cities")
            return []

        with patch("db.connection.select_rows", side_effect=fake_select_rows), \
                patch("db.connection.insert_row", return_value={"id": 42}) as insert_row:
            city_id = main._resolve_city_id("aurangabad", create=True)

        self.assertEqual(city_id, 42)
        insert_row.assert_called_once_with(
            "cities",
            {"name": "Aurangabad", "state": "Maharashtra"},
        )

    def test_unknown_city_still_refuses_to_default(self):
        with patch("db.connection.select_rows", return_value=[]):
            with self.assertRaises(ValueError):
                main._resolve_city_id("Not A City", create=True)

    def test_igr_refuses_unconfigured_phase_2_city(self):
        with self.assertRaisesRegex(ValueError, "Aurangabad"):
            ScraperIGR(district="Aurangabad")


if __name__ == "__main__":
    unittest.main()
