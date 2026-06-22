"""
main.py - Run scrapers and detection pipeline
Usage:
    python main.py --scraper 99acres --city Akola
    python main.py --scraper rera --city Akola
    python main.py --scraper enrich --city Akola
    python main.py --scraper rera --city Amravati
    python main.py --scraper 99acres --city Amravati
    python main.py --scraper enrich --city Amravati
    python main.py --scraper 99acres --city Nagpur
    python main.py --scraper rera --city Nagpur
    python main.py --scraper enrich --city Nagpur
    python main.py --geocode-listings --city Nagpur
    python main.py --scraper all --city Akola
    python main.py --scraper all --city Amravati
    python main.py --scraper all --city Nagpur
    python main.py --geocode-listings --city Akola
    python main.py --geocode-listings --city Amravati
    python main.py --snapshot --city Akola
    python main.py --snapshot-report --city Akola
    python main.py --detect --city Akola
    python main.py --patterns --city Akola
    python main.py --trends --city Akola
    python main.py --trend-report --city Akola
    python main.py --detect --patterns --city Akola
    python main.py --snapshot --city Amravati
    python main.py --snapshot-report --city Amravati
    python main.py --detect --city Amravati
    python main.py --patterns --city Amravati
    python main.py --trends --city Amravati
    python main.py --trend-report --city Amravati
    python main.py --detect --patterns --city Amravati
    python main.py --snapshot --city Nagpur
    python main.py --snapshot-report --city Nagpur
    python main.py --detect --city Nagpur
    python main.py --patterns --city Nagpur
    python main.py --trends --city Nagpur
    python main.py --trend-report --city Nagpur
    python main.py --detect --patterns --city Nagpur
    python main.py --dbscan-anomaly --city Nagpur
    python main.py --score --city Nagpur
    python main.py --score-report --city Nagpur
    python main.py --score --city Akola
    python main.py --score-report --city Akola
    python main.py --score --city Amravati
    python main.py --score-report --city Amravati
    python main.py --explain-flags --city Akola --explain-limit 5
    python main.py --explain-flags --city Amravati --explain-limit 5
    python main.py --explain-flags --city Nagpur --explain-limit 5
    python main.py --scraper igr --city Akola
    python main.py --scraper igr --city Amravati 
    python main.py --scraper igr --city Nagpur
    python main.py --scraper 99acres --city Pune
    python main.py --scraper rera --city Pune
    python main.py --scraper enrich --city Pune
    python main.py --scraper all --city Pune
    python main.py --geocode-listings --city Pune
    python main.py --snapshot --city Pune
    python main.py --detect --city Pune
    python main.py --patterns --city Pune
    python main.py --trends --city Pune
    python main.py --score --city Pune
    python main.py --explain-flags --city Pune --explain-limit 5
    python main.py --scraper 99acres --city Nashik
    python main.py --scraper rera --city Nashik
    python main.py --scraper enrich --city Nashik
    python main.py --scraper all --city Nashik
    python main.py --snapshot --city Nashik
    python main.py --detect --city Nashik
    python main.py --patterns --city Nashik
    python main.py --trends --city Nashik
    python main.py --score --city Nashik
    python main.py --scraper magicbricks --city Nashik
    python main.py --scraper magicbricks --city Aurangabad
    python main.py --scraper magicbricks --city Pune
    python main.py --scraper magicbricks --city Nagpur
    python main.py --scraper magicbricks --city Akola
    python main.py --scraper magicbricks --city Amravati
    python main.py --scraper easr --city Nagpur
    python main.py --scraper easr --city Pune
    python main.py --scraper easr --city Nashik
    python main.py --scraper easr --city Aurangabad
    python main.py --scraper 99acres --city Aurangabad
    python main.py --scraper rera --city Aurangabad
    python main.py --scraper enrich --city Aurangabad
    python main.py --scraper all --city Aurangabad
    python main.py --snapshot --city Aurangabad
    python main.py --detect --city Aurangabad
    python main.py --patterns --city Aurangabad
    python main.py --trends --city Aurangabad
    python main.py --score --city Aurangabad
"""

import argparse
import logging
import sys

from dotenv import load_dotenv

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("main")

SUPPORTED_CITIES = (
    "Akola",
    "Amravati",
    "Nagpur",
    "Pune",
    "Nashik",
    "Aurangabad",
)
_CITY_LOOKUP = {city.lower(): city for city in SUPPORTED_CITIES}


def _canonical_city_name(city: str) -> str:
    cleaned = " ".join(str(city or "").strip().split())
    return _CITY_LOOKUP.get(cleaned.lower(), cleaned)


def _resolve_city_id(city: str, *, create: bool = False) -> int:
    """Look up city_id from the cities table, optionally creating supported cities."""
    from db.connection import insert_row, select_rows

    city_name = _canonical_city_name(city)
    if not city_name:
        raise ValueError("City name cannot be empty")

    rows = select_rows("cities", filters={"name": city_name}, limit=5)
    if rows:
        return rows[0]["id"]

    for row in select_rows("cities", limit=100):
        if str(row.get("name") or "").strip().lower() == city_name.lower():
            return row["id"]

    if create and city_name in SUPPORTED_CITIES:
        row = insert_row(
            "cities",
            {"name": city_name, "state": "Maharashtra"},
        )
        logger.info(f"Created city row for {city_name} (id={row['id']})")
        return row["id"]

    logger.error(
        f"City '{city_name}' not found in cities table - refusing to default. "
        f"Check spelling or add the city to the cities table.")
    raise ValueError(f"Unknown city: '{city_name}'")


def run_scraper(scraper_name: str, city: str):
    city = _canonical_city_name(city)
    _resolve_city_id(city, create=True)

    if scraper_name in ("99acres", "all"):
        logger.info("Starting 99acres scraper (Playwright)...")
        from scrapers.scraper_99acres import Scraper99Acres
        scraper = Scraper99Acres(city=city, listing_types=["buy"])
        scraper.run()

    if scraper_name in ("rera", "all"):
        logger.info("Starting MahaRERA scraper (Playwright)...")
        from scrapers.scraper_rera import ScraperMahaRERA
        scraper = ScraperMahaRERA(district=city, max_pages=80)
        scraper.run()

    if scraper_name in ("enrich", "all"):
        logger.info("Starting MahaRERA detail enrichment...")
        from scrapers.scraper_rera_detail import RERADetailScraper
        from models.price_tracker import PriceTracker
        scraper = RERADetailScraper()
        scraper.run(city)

    if scraper_name == "igr":
        logger.info("Starting IGR Maharashtra scraper (Playwright)...")
        from scrapers.scraper_igr import ScraperIGR
        scraper = ScraperIGR(district=city, years=[2024, 2025])
        scraper.run()

    if scraper_name in ("magicbricks", "all"):
        logger.info("Starting MagicBricks scraper (Playwright)...")
        from scrapers.scraper_magicbricks import ScraperMagicBricks
        scraper = ScraperMagicBricks(city=city)
        scraper.run()

    if scraper_name == "easr":
        logger.info("Starting eASR Ready Reckoner scraper (Playwright)...")
        from scrapers.scraper_easr import ScraperEASR
        scraper = ScraperEASR(city=city)
        scraper.run()


def run_detection(city: str):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(f"Running anomaly detection for {city} (city_id={city_id})...")
    from models.anomaly_detector import AnomalyDetector
    detector = AnomalyDetector(city_id=city_id)
    total = detector.run_all()
    logger.info(f"Detection complete — {total} flags raised")


def run_patterns(city: str):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(
        f"Running cross-table pattern detection for {city} (city_id={city_id})...")
    from models.pattern_detector import PatternDetector
    detector = PatternDetector(city_id=city_id)
    total = detector.run_all()
    logger.info(
        f"Pattern detection complete — {total} new patterns written to suspicious_flags")


def run_snapshot(city: str):
    city = _canonical_city_name(city)
    _resolve_city_id(city, create=True)
    logger.info(f"Running price snapshot for {city}...")
    from models.price_tracker import PriceTracker
    written = PriceTracker().snapshot(city=city)
    logger.info(f"Price snapshot complete — {written} locality rows written")


def run_trends(city: str):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(
        f"Running price trend detection for {city} (city_id={city_id})...")
    from models.trend_detector import TrendDetector
    spikes = TrendDetector(city_id=city_id, city=city).run_all()
    logger.info(f"Trend analysis complete — {spikes} spike(s) detected")


def run_dbscan_anomaly(city: str, plot_path: str | None = None):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(
        f"Running DBSCAN anomaly dry run for {city} (city_id={city_id})...")
    from models.dbscan_anomaly_tester import DBSCANAnomalyTester
    flags = DBSCANAnomalyTester(city_id=city_id, plot_path=plot_path).run()
    logger.info(
        f"DBSCAN anomaly dry run complete - {flags} candidate(s) found")


def run_snapshot_report(city: str):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(f"Generating price snapshot report for {city}...")
    from reports.price_snapshot_report import write_report
    write_report(city=city, city_id=city_id,
                 path=f"logs/price_snapshot_{city.lower()}.html")


def run_trend_report(city: str):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(f"Generating price trend report for {city}...")
    from reports.trend_report import write_report
    write_report(city=city, city_id=city_id,
                 path=f"logs/trend_{city.lower()}.html")


def run_score_report(city: str):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(f"Generating confidence score report for {city}...")
    from reports.confidence_report import write_report
    write_report(city=city, city_id=city_id,
                 path=f"logs/confidence_{city.lower()}.html")


def run_ai_explanations(city: str, limit: int, overwrite: bool):
    city = _canonical_city_name(city)
    city_id = _resolve_city_id(city, create=True)
    logger.info(
        f"Generating AI flag explanations for {city} "
        f"(city_id={city_id}, limit={limit}, overwrite={overwrite})..."
    )
    from models.ai_explainer import AIFlagExplainer
    updated = AIFlagExplainer(
        city_id=city_id,
        limit=limit,
        overwrite=overwrite,
    ).run()
    logger.info(f"AI flag explanation complete - {updated} flag(s) updated")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real Estate Intel Platform")
    parser.add_argument(
        "--scraper", choices=["99acres", "rera", "enrich", "igr", "magicbricks", "easr", "all"])
    parser.add_argument(
        "--city",
        default="Akola",
        help=f"City to run. Active cities: {', '.join(SUPPORTED_CITIES)}",
    )
    parser.add_argument('--geocode-listings', action='store_true',
                        help='Geocode listings by locality')
    parser.add_argument("--detect",   action="store_true")
    parser.add_argument("--patterns", action="store_true",
                        help="Run cross-table pattern detection")
    parser.add_argument("--snapshot", action="store_true",
                        help="Snapshot current listings into price_history")
    parser.add_argument("--snapshot-report", action="store_true",
                        help="Write HTML price snapshot report from price_history")
    parser.add_argument("--trends",   action="store_true",
                        help="Detect price trend spikes from price_history")
    parser.add_argument("--trend-report", action="store_true",
                        help="Write HTML trend report from price_spikes + price_history")
    parser.add_argument('--dbscan-anomaly', action='store_true',
                        help='Dry run experimental DBSCAN listing anomaly detection without writing flags')
    parser.add_argument('--dbscan-plot',
                        help='Write a local HTML scatter plot for DBSCAN clusters')
    parser.add_argument('--score', action='store_true',
                        help='Score confidence on all flags')
    parser.add_argument('--score-report', action='store_true',
                        help='Write HTML confidence score report from suspicious_flags')
    parser.add_argument('--explain-flags', action='store_true',
                        help='Generate Azure OpenAI plain-English explanations for open flags')
    parser.add_argument('--explain-limit', type=int, default=10,
                        help='Maximum flags to explain in one run')
    parser.add_argument('--explain-overwrite', action='store_true',
                        help='Regenerate explanations even when evidence.ai_explanation exists')
    args = parser.parse_args()
    args.city = _canonical_city_name(args.city)

    # ── Per-run log file ───────────────────────────────────────────────────────
    # Name: logs/{scraper}_{city}.log  or  logs/models_{city}.log
    # Each scraper+city combo gets its own file — parallel runs don't collide.
    import os
    os.makedirs("logs", exist_ok=True)
    _log_prefix = args.scraper if args.scraper else "models"
    _log_path = f"logs/{_log_prefix}_{args.city.lower()}.log"
    _fh = logging.FileHandler(_log_path, encoding="utf-8")
    _fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    logging.getLogger().addHandler(_fh)
    logger.info(f"Logging to {_log_path}")

    if args.scraper:
        run_scraper(args.scraper, args.city)

    if args.geocode_listings:
        _resolve_city_id(args.city, create=True)
        from scrapers.geocode_listings import geocode_listings
        geocode_listings(city=args.city)

    if args.snapshot:
        run_snapshot(args.city)

    if args.snapshot_report:
        run_snapshot_report(args.city)

    if args.detect:
        run_detection(args.city)

    if args.patterns:
        run_patterns(args.city)

    if args.score:
        from models.confidence_scorer import ConfidenceScorer
        ConfidenceScorer(city_id=_resolve_city_id(
            args.city, create=True)).run()

    if args.score_report:
        run_score_report(args.city)

    if args.trends:
        run_trends(args.city)

    if args.trend_report:
        run_trend_report(args.city)

    if args.dbscan_anomaly:
        run_dbscan_anomaly(args.city, args.dbscan_plot)

    if args.explain_flags:
        run_ai_explanations(args.city, args.explain_limit,
                            args.explain_overwrite)

    if not any([args.scraper, args.detect, args.patterns, args.snapshot, args.snapshot_report, args.score, args.score_report, args.geocode_listings, args.trends, args.trend_report, args.dbscan_anomaly, args.explain_flags]):
        parser.print_help()
