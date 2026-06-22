import unittest
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import call, patch

from models.anomaly_detector import AnomalyDetector
from models.confidence_scorer import ConfidenceScorer
from models.dbscan_anomaly_tester import DBSCANAnomalyTester
from models.pattern_detector import PatternDetector
from models.trend_detector import TrendDetector


class RegressionRuleTests(unittest.TestCase):
    def setUp(self):
        patcher_flags = patch.object(
            AnomalyDetector, "_load_existing_flags", autospec=True, return_value=None
        )
        patcher_patterns = patch.object(
            PatternDetector, "_load_existing_patterns", autospec=True, return_value=None
        )
        patcher_dbscan = patch.object(
            DBSCANAnomalyTester, "_load_existing_flags", autospec=True, return_value=None
        )
        self.addCleanup(patcher_flags.stop)
        self.addCleanup(patcher_patterns.stop)
        self.addCleanup(patcher_dbscan.stop)
        patcher_flags.start()
        patcher_patterns.start()
        patcher_dbscan.start()

    def test_escrow_deficit_skips_missing_escrow_balance(self):
        detector = AnomalyDetector(city_id=7)
        projects = [{
            "id": "p1",
            "project_name": "Sunrise Residency",
            "amount_collected": 10000000,
            "escrow_balance": None,
        }]

        with patch("models.anomaly_detector.select_rows", return_value=projects), patch.object(
            detector, "_flag_project", return_value=True
        ) as mock_flag:
            created = detector.check_rera_escrow_deficit()

        self.assertEqual(created, 0)
        mock_flag.assert_not_called()

    def test_escrow_deficit_flags_real_shortfall(self):
        detector = AnomalyDetector(city_id=7)
        projects = [{
            "id": "p2",
            "project_name": "Lakeview Heights",
            "rera_registration": "P123",
            "amount_collected": 10000000,
            "escrow_balance": 2000000,
        }]

        with patch("models.anomaly_detector.select_rows", return_value=projects), patch.object(
            detector, "_flag_project", return_value=True
        ) as mock_flag:
            created = detector.check_rera_escrow_deficit()

        self.assertEqual(created, 1)
        self.assertEqual(mock_flag.call_args.kwargs["flag_type"], "rera_escrow_deficit")
        self.assertAlmostEqual(
            mock_flag.call_args.kwargs["evidence"]["escrow_ratio"], 0.2
        )

    def test_listing_outlier_merges_marketplace_sources_for_comparables(self):
        detector = AnomalyDetector(city_id=9)
        listings = [
            {
                "id": "l1",
                "source": "99acres",
                "listing_status": "active",
                "locality": "Murtizapur Road, Akola",
                "property_type": "apartment",
                "listing_type": "sale",
                "price_per_sqft": 1000,
            },
            {
                "id": "l2",
                "source": "99acres",
                "listing_status": "active",
                "locality": "Murtizapur Road",
                "property_type": "apartment",
                "listing_type": "sale",
                "price_per_sqft": 1020,
            },
            {
                "id": "l3",
                "source": "99acres",
                "listing_status": "active",
                "locality": "Murtizapur Road",
                "property_type": "apartment",
                "listing_type": "sale",
                "price_per_sqft": 980,
            },
            {
                "id": "l4",
                "source": "magicbricks",
                "listing_status": "active",
                "locality": "Murtizapur Road",
                "property_type": "apartment",
                "listing_type": "sale",
                "price_per_sqft": 1010,
            },
            {
                "id": "l5",
                "source": "magicbricks",
                "listing_status": "active",
                "locality": "Murtizapur Road",
                "property_type": "apartment",
                "listing_type": "sale",
                "price_per_sqft": 2100,
            },
            {
                "id": "l6",
                "source": "magicbricks",
                "listing_status": "active",
                "locality": "Murtizapur Road",
                "property_type": "apartment",
                "listing_type": "sale",
                "price_per_sqft": 995,
            },
        ]
        detector.MIN_CITY_LISTINGS_FOR_OUTLIER = len(listings)

        with patch(
            "models.anomaly_detector.select_rows",
            side_effect=[listings[:3], listings[3:]],
        ) as mock_select, patch.object(
            detector, "_flag_listing", return_value=True
        ) as mock_flag:
            created = detector.check_listing_price_outliers()

        self.assertEqual(created, 1)
        mock_select.assert_has_calls([
            call(
                "listings",
                filters={
                    "city_id": 9,
                    "source": "99acres",
                    "listing_status": "active",
                },
                limit=2000,
            ),
            call(
                "listings",
                filters={
                    "city_id": 9,
                    "source": "magicbricks",
                    "listing_status": "active",
                },
                limit=2000,
            ),
        ])
        evidence = mock_flag.call_args.kwargs["evidence"]
        self.assertEqual(evidence["normalized_locality"], "murtizapur road")
        self.assertEqual(evidence["group_size"], 6)
        self.assertEqual(evidence["comparable_count"], 6)

    def test_listing_outlier_ignores_groups_below_minimum_size(self):
        detector = AnomalyDetector(city_id=9)
        listings = [
            {
                "id": f"l{i}",
                "source": "99acres",
                "listing_status": "active",
                "locality": "Kaulkhed",
                "property_type": "plot",
                "listing_type": "sale",
                "price_per_sqft": price,
            }
            for i, price in enumerate([1000, 1020, 980, 1010, 2100], start=1)
        ]
        detector.MIN_CITY_LISTINGS_FOR_OUTLIER = len(listings)

        with patch(
            "models.anomaly_detector.select_rows",
            side_effect=[listings, []],
        ), patch.object(
            detector, "_flag_listing", return_value=True
        ) as mock_flag:
            created = detector.check_listing_price_outliers()

        self.assertEqual(created, 0)
        mock_flag.assert_not_called()

    def test_anomaly_flags_are_inserted_open(self):
        detector = AnomalyDetector(city_id=9)

        with patch("models.anomaly_detector.insert_row", return_value={}) as mock_insert:
            created = detector._create_flag_record(
                flag_type="listing_price_outlier",
                severity="medium",
                title="Test flag",
                reason="test reason",
                listing_id="listing-1",
                evidence={"ratio": 2.0},
            )

        self.assertTrue(created)
        payload = mock_insert.call_args.args[1]
        self.assertEqual(payload["status"], "open")

    def test_promoter_cluster_ignores_same_entity_punctuation_variants(self):
        detector = PatternDetector(city_id=5)
        projects = [
            {
                "id": "p1",
                "promoter_name": "Shree Developers Pvt. Ltd.",
                "rera_registration": "A1",
            },
            {
                "id": "p2",
                "promoter_name": "Shree Developers Pvt Ltd",
                "rera_registration": "A2",
            },
        ]

        with patch("models.pattern_detector.select_rows", return_value=projects), patch.object(
            detector, "_write_pattern", return_value=True
        ) as mock_write:
            created = detector.detect_promoter_name_clusters()

        self.assertEqual(created, 0)
        mock_write.assert_not_called()

    def test_repeat_offender_uses_only_trusted_parent_flags(self):
        detector = PatternDetector(city_id=11)
        recent_date = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat()
        projects = [
            {
                "id": "p-old",
                "promoter_name": "ABC Builders LLP",
                "project_name": "Legacy Towers",
                "registration_date": "2025-01-01T00:00:00+00:00",
                "rera_registration": "OLD1",
            },
            {
                "id": "p-new",
                "promoter_name": "ABC Builders LLP",
                "project_name": "Fresh Launch",
                "registration_date": recent_date,
                "rera_registration": "NEW1",
            },
        ]
        trusted_flags = [{
            "flag_type": "repeated_complaints",
            "rera_project_id": "p-old",
            "evidence": {"promoter_name": "ABC Builders LLP"},
        }]
        noisy_flags = [{
            "flag_type": "price_trend_spike",
            "rera_project_id": "p-old",
            "evidence": {"promoter_name": "ABC Builders LLP"},
        }]

        with patch("models.pattern_detector.select_rows", side_effect=[trusted_flags, projects]), patch.object(
            detector, "_write_pattern", return_value=True
        ) as mock_write:
            created = detector.detect_repeat_offender_new_project()

        self.assertEqual(created, 1)
        self.assertEqual(
            mock_write.call_args.kwargs["pattern_type"], "repeat_offender_new_project"
        )

        detector._existing_pattern_keys.clear()
        with patch("models.pattern_detector.select_rows", side_effect=[noisy_flags, projects]), patch.object(
            detector, "_write_pattern", return_value=True
        ) as mock_write:
            created = detector.detect_repeat_offender_new_project()

        self.assertEqual(created, 0)
        mock_write.assert_not_called()

    def test_pattern_write_skips_existing_open_duplicate_title(self):
        detector = PatternDetector(city_id=13)
        detector._existing_pattern_keys.clear()
        detector._existing_pattern_titles.clear()

        existing_rows = [{
            "flag_type": "complaint_velocity",
            "status": "open",
            "title": "Systemic complaints: Pankaj Kothari (2 projects affected)",
            "evidence": {"promoter_name": "Pankaj Kothari"},
        }]

        with patch("models.pattern_detector.select_rows", return_value=existing_rows), patch(
            "models.pattern_detector.insert_row"
        ) as mock_insert:
            created = detector._write_pattern(
                pattern_type="complaint_velocity",
                severity="high",
                title="Systemic complaints: Pankaj Kothari (2 projects affected)",
                description="duplicate check",
                evidence={"promoter_name": "Pankaj Kothari"},
            )

        self.assertFalse(created)
        mock_insert.assert_not_called()

    def test_trend_detector_preloads_open_flag_keys(self):
        existing_flags = [{
            "status": "open",
            "evidence": {
                "locality": "Aadarsh Colony",
                "property_type": "flat",
                "window_days": 7,
            },
        }]

        with patch("models.trend_detector.select_rows", return_value=existing_flags):
            detector = TrendDetector(city_id=1, city="Akola")

        self.assertIn(
            ("aadarsh colony", "flat", 7),
            detector._open_trend_flag_keys,
        )

    def test_confidence_repeated_complaints_ranks_multi_project_spread_higher(self):
        scorer = ConfidenceScorer(city_id=1)
        spread_score, spread_note = scorer._score_flag({
            "id": "f-spread",
            "flag_type": "repeated_complaints",
            "evidence": {
                "total_complaints": 6,
                "project_count": 3,
            },
        })
        concentrated_score, concentrated_note = scorer._score_flag({
            "id": "f-concentrated",
            "flag_type": "repeated_complaints",
            "evidence": {
                "total_complaints": 6,
                "project_count": 1,
            },
        })

        self.assertGreaterEqual(spread_score - concentrated_score, 10)
        self.assertIn("spread across 3 projects", spread_note)
        self.assertIn("concentrated in one project", concentrated_note)

    def test_confidence_listing_outlier_drops_for_plot_rural_recent_questionable(self):
        scorer = ConfidenceScorer(city_id=1)
        scorer._listings["l1"] = {
            "id": "l1",
            "source": "99acres",
            "listing_status": "active",
            "listed_price": 100000,
            "price_per_sqft": 250,
            "property_type": "plot",
            "listing_type": "sale",
            "locality": "Mouza Shivar Village",
            "source_listing_id": "SPID1",
            "area_sqft": 50000,
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
        }

        score, note = scorer._score_flag({
            "id": "f-listing",
            "flag_type": "listing_price_outlier",
            "listing_id": "l1",
            "evidence": {
                "ratio": 2.2,
                "comparable_count": 5,
                "property_type": "plot",
                "locality": "Mouza Shivar Village",
            },
        })

        self.assertLess(score, 30)
        self.assertIn("too few comparables", note)
        self.assertIn("plot/land pricing", note)
        self.assertIn("recent questionable scrape", note)

    def test_confidence_accepts_magicbricks_as_marketplace_source(self):
        scorer = ConfidenceScorer(city_id=1)
        scorer._listings["mb1"] = {
            "id": "mb1",
            "source": "magicbricks",
            "listing_status": "active",
            "price_per_sqft": 6000,
            "property_type": "flat",
            "locality": "Baner",
        }

        _, note = scorer._score_flag({
            "id": "f-mb",
            "flag_type": "listing_price_outlier",
            "listing_id": "mb1",
            "evidence": {
                "ratio": 2.2,
                "comparable_count": 10,
                "property_type": "flat",
                "locality": "Baner",
            },
        })

        self.assertNotIn("unexpected listing source", note)

    def test_dbscan_feature_row_uses_implied_total_when_missing(self):
        features = DBSCANAnomalyTester._feature_row({
            "price_per_sqft": 2000,
            "area_sqft": 750,
            "listed_price": None,
        })

        self.assertIsNotNone(features)
        self.assertEqual(len(features), 3)

    def test_dbscan_write_flag_uses_experimental_flag_type(self):
        detector = DBSCANAnomalyTester(city_id=9, max_flags=5)
        finding = {
            "listing": {
                "id": "l-dbscan",
                "locality": "Civil Lines",
                "price_per_sqft": 5200,
                "area_sqft": 900,
                "listed_price": 4680000,
                "flag_reasons": [],
            },
            "distance": 3.8,
            "property_type": "apartment",
            "listing_type": "sale",
            "group_size": 60,
            "group_median_price_per_sqft": 2600,
            "locality_median_price_per_sqft": 2550,
            "ratio_to_group_median": 2.0,
            "ratio_to_locality_median": 2.039,
        }

        with patch("models.dbscan_anomaly_tester.insert_row", return_value={}) as mock_insert, patch(
            "models.dbscan_anomaly_tester.update_rows", return_value=[]
        ) as mock_update:
            created = detector._write_flag(finding)

        self.assertTrue(created)
        payload = mock_insert.call_args.args[1]
        self.assertEqual(payload["flag_type"], "dbscan_listing_anomaly")
        self.assertEqual(payload["listing_id"], "l-dbscan")
        self.assertEqual(payload["evidence"]["model"], "DBSCAN")
        self.assertEqual(payload["evidence"]["cluster_label"], -1)
        mock_update.assert_called_once()

    def test_confidence_dbscan_listing_anomaly_scores_experimental_signal(self):
        scorer = ConfidenceScorer(city_id=1)
        score, note = scorer._score_flag({
            "id": "f-dbscan",
            "flag_type": "dbscan_listing_anomaly",
            "evidence": {
                "distance_from_scaled_origin": 4.2,
                "group_size": 80,
                "ratio_to_group_median": 2.1,
                "property_type": "apartment",
                "locality": "Civil Lines",
            },
        })

        self.assertGreaterEqual(score, 70)
        self.assertIn("DBSCAN", note)

    def test_confidence_locality_spike_rewards_stable_sample_and_penalizes_single_driver(self):
        stable = ConfidenceScorer(city_id=1)
        stable._listings = {
            f"s{i}": {
                "id": f"s{i}",
                "source": "99acres",
                "listing_status": "active",
                "locality": "Civil Lines",
                "price_per_sqft": price,
            }
            for i, price in enumerate([2400, 2450, 2500, 2525, 2550, 2575, 2600, 2625, 2650, 2675, 2700, 2750])
        }
        stable_score, stable_note = stable._score_flag({
            "id": "f-stable",
            "flag_type": "locality_price_spike",
            "evidence": {
                "locality": "Civil Lines",
                "city_median": 1000,
                "locality_median": 2600,
                "spike_ratio": 1.6,
                "listing_count": 12,
            },
        })

        driven = ConfidenceScorer(city_id=1)
        driven._listings = {
            f"d{i}": {
                "id": f"d{i}",
                "source": "99acres",
                "listing_status": "active",
                "locality": "Thin Locality",
                "price_per_sqft": price,
            }
            for i, price in enumerate([1000, 1000, 3000, 3100])
        }
        driven_score, driven_note = driven._score_flag({
            "id": "f-driven",
            "flag_type": "locality_price_spike",
            "evidence": {
                "locality": "Thin Locality",
                "city_median": 1000,
                "locality_median": 3000,
                "spike_ratio": 2.0,
                "listing_count": 4,
            },
        })

        self.assertGreaterEqual(stable_score, 70)
        self.assertLess(driven_score, stable_score)
        self.assertIn("tight spread", stable_note)
        self.assertIn("one high listing drives spike", driven_note)

    def test_confidence_locality_spike_reads_json_evidence_and_derives_ratio(self):
        scorer = ConfidenceScorer(city_id=1)
        score, note = scorer._score_flag({
            "id": "f-json-spike",
            "flag_type": "locality_price_spike",
            "evidence": json.dumps({
                "locality": "Ramdaspeth",
                "city_median": 1000,
                "locality_median": 2200,
                "listing_count": 9,
                "min_price": 1900,
                "max_price": 2300,
            }),
        })

        self.assertGreaterEqual(score, 60)
        self.assertIn("spike=120%", note)
        self.assertIn("good sample n=9", note)

    @unittest.skip(
        "No live litigation detector in the current codebase; keep this placeholder until that rule returns."
    )
    def test_litigation_regression_placeholder(self):
        self.fail("Placeholder for future litigation regression coverage.")


class PatternDetectorHistoricalDedupeTests(unittest.TestCase):
    def test_closed_locality_price_spike_blocks_recreation(self):
        existing_rows = [{
            "flag_type": "locality_price_spike",
            "status": "closed",
            "title": "Price spike in Aadarsh Colony, Akola: 257% above city median",
            "evidence": {"locality": "Aadarsh Colony, Akola"},
        }]

        with patch("models.pattern_detector.select_rows", return_value=existing_rows):
            detector = PatternDetector(city_id=1)

        self.assertIn("locality_spike_aadarsh_colony", detector._existing_pattern_keys)

        with patch("models.pattern_detector.insert_row") as mock_insert:
            created = detector._write_pattern(
                pattern_type="locality_price_spike",
                severity="medium",
                title="Price spike in Aadarsh Colony: 200% above city median",
                description="duplicate locality spike",
                evidence={"locality": "Aadarsh Colony", "spike_ratio": 2.0},
            )

        self.assertFalse(created)
        mock_insert.assert_not_called()


if __name__ == "__main__":
    unittest.main()
