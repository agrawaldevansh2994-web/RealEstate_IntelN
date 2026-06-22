import unittest

from scrapers.scraper_99acres import Scraper99Acres


class Scraper99AcresNormalizationTests(unittest.TestCase):
    def test_buy_url_uses_resale_route(self):
        scraper = Scraper99Acres(city="Nagpur")
        self.assertEqual(
            scraper.search_urls["buy"],
            "https://www.99acres.com/resale-property-in-nagpur-ffid",
        )

    def test_parse_area_line_sqft(self):
        raw_value, raw_unit, area_sqft = Scraper99Acres._parse_area_line("130 sqft")
        self.assertEqual(raw_value, 130.0)
        self.assertEqual(raw_unit, "sqft")
        self.assertEqual(area_sqft, 130.0)

    def test_parse_area_line_sq_yard(self):
        raw_value, raw_unit, area_sqft = Scraper99Acres._parse_area_line("14,520 sq yard")
        self.assertEqual(raw_value, 14520.0)
        self.assertEqual(raw_unit, "sq yard")
        self.assertEqual(area_sqft, 130680.0)

    def test_parse_area_from_url(self):
        raw_value, raw_unit, area_sqft = Scraper99Acres._parse_area_from_url(
            "https://www.99acres.com/residential-land-plot-for-sale-in-kaulkhed-akola-14520-sq-yard-spid-A1"
        )
        self.assertEqual(raw_value, 14520.0)
        self.assertEqual(raw_unit, "sq yard")
        self.assertEqual(area_sqft, 130680.0)

    def test_extract_price_per_sqft_converts_sq_yard_rate(self):
        price_per_sqft, raw_unit = Scraper99Acres._extract_price_per_sqft(
            ["₹783/sq yard"],
            listed_price=None,
            area_sqft=None,
        )
        self.assertEqual(raw_unit, "sq yard")
        self.assertEqual(price_per_sqft, 87.0)

    def test_extract_price_per_sqft_derives_from_total_price(self):
        price_per_sqft, raw_unit = Scraper99Acres._extract_price_per_sqft(
            [],
            listed_price=10500000,
            area_sqft=121000,
        )
        self.assertEqual(raw_unit, "derived")
        self.assertAlmostEqual(price_per_sqft, 86.78, places=2)

    def test_canonicalize_locality_strips_city_suffix(self):
        locality = Scraper99Acres._canonicalize_locality("Geeta Nagar, Akola", "Akola")
        self.assertEqual(locality, "Geeta Nagar")

    def test_canonicalize_locality_merges_known_alias(self):
        locality = Scraper99Acres._canonicalize_locality("Khadki Bk, Akola", "Akola")
        self.assertEqual(locality, "Khadki")

    def test_extract_locality_from_title_prefers_real_area(self):
        locality = Scraper99Acres._extract_locality_from_title(
            "1 BHK Builder Floor in Khadki, Akola",
            "Akola",
        )
        self.assertEqual(locality, "Khadki, Akola")

    def test_extract_locality_from_title_ignores_city_only(self):
        locality = Scraper99Acres._extract_locality_from_title(
            "2 BHK Flat in Akola",
            "Akola",
        )
        self.assertEqual(locality, "")

    def test_resolve_locality_prefers_title_over_project_name(self):
        locality, locality_source_raw = Scraper99Acres._resolve_locality(
            "dipak residency",
            "1 BHK Builder Floor in Khadki, Akola",
            "Akola",
        )
        self.assertEqual(locality, "Khadki")
        self.assertEqual(locality_source_raw, "Khadki, Akola")

    def test_resolve_locality_uses_title_for_apartment_named_card(self):
        locality, locality_source_raw = Scraper99Acres._resolve_locality(
            "gajanan plaza apartment",
            "2 BHK Flat in Dwaraka Nagri, Akola",
            "Akola",
        )
        self.assertEqual(locality, "Dwaraka Nagri")
        self.assertEqual(locality_source_raw, "Dwaraka Nagri, Akola")

    def test_resolve_locality_falls_back_to_card_when_title_is_generic(self):
        locality, locality_source_raw = Scraper99Acres._resolve_locality(
            "Geeta Nagar, Akola",
            "2 BHK Flat in Akola",
            "Akola",
        )
        self.assertEqual(locality, "Geeta Nagar")
        self.assertEqual(locality_source_raw, "Geeta Nagar, Akola")


if __name__ == "__main__":
    unittest.main()
