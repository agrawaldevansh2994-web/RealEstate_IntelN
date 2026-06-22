"""
scrapers/fix_null_localities.py
One-shot utility: recovers locality strings for listings that have
lat/lon coordinates but a NULL locality field.

Uses Nominatim reverse-geocode (zoom=14 = suburb level) then pipes the
result through scraper_99acres._canonicalize_locality so the recovered
string is cleaned to the same standard as scraped localities.

Usage:
    python scrapers/fix_null_localities.py --city Nashik
    python scrapers/fix_null_localities.py --city Aurangabad
    python scrapers/fix_null_localities.py            # runs all cities

Safe to re-run: skips rows that already have a locality.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db.connection import select_rows, update_rows
from scrapers.scraper_99acres import Scraper99Acres

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

NOMINATIM_REVERSE_URL = "https://nominatim.openstreetmap.org/reverse"
NOMINATIM_HEADERS     = {"User-Agent": "HomesageIntelPlatform/1.0"}
REQUEST_DELAY_SEC     = 1.1   # OSM fair-use: max 1 req/sec

# Address fields tried in priority order when extracting locality from
# Nominatim's reverse-geocode response.
_ADDRESS_FIELDS = [
    "suburb",
    "neighbourhood",
    "quarter",
    "city_district",
    "village",
    "town",
    "county",
]


def _reverse_geocode(lat: float, lon: float) -> str | None:
    """
    Call Nominatim reverse geocode and return the best locality string,
    or None if no usable address component is found.
    """
    try:
        resp = requests.get(
            NOMINATIM_REVERSE_URL,
            params={
                "lat":    lat,
                "lon":    lon,
                "format": "json",
                "zoom":   14,        # suburb-level detail
            },
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        data = resp.json()
        address = data.get("address") or {}

        for field in _ADDRESS_FIELDS:
            value = (address.get(field) or "").strip()
            if value:
                logger.debug(f"  Nominatim [{field}] → '{value}'")
                return value

        logger.debug(f"  Nominatim: no usable address field. Full: {address}")
        return None

    except Exception as exc:
        logger.warning(f"  Nominatim error for ({lat},{lon}): {exc}")
        return None


def fix_city(city_name: str) -> int:
    """
    Recover locality for all null-locality listings in city_name.
    Returns the number of listings updated.
    """
    logger.info(f"fix_null_localities: processing {city_name}")

    cities = select_rows("cities", filters={"name": city_name}, limit=5)
    if not cities:
        logger.error(f"City not found: {city_name}")
        return 0
    city_id = cities[0]["id"]

    listings = select_rows(
        "listings",
        filters={"city_id": city_id, "source": "99acres"},
        limit=2000,
    )

    targets = [
        l for l in listings
        if not (l.get("locality") or "").strip()
        and l.get("latitude") is not None
        and l.get("longitude") is not None
    ]

    if not targets:
        logger.info(f"  No null-locality listings with coords found for {city_name}")
        return 0

    logger.info(f"  Found {len(targets)} listings to fix")
    updated = 0
    last_request = 0.0

    for listing in targets:
        lat = float(listing["latitude"])
        lon = float(listing["longitude"])

        elapsed = time.time() - last_request
        if elapsed < REQUEST_DELAY_SEC:
            time.sleep(REQUEST_DELAY_SEC - elapsed)

        raw_locality = _reverse_geocode(lat, lon)
        last_request = time.time()

        if not raw_locality:
            logger.info(f"  [{listing['id']}] No locality recovered from ({lat:.4f},{lon:.4f})")
            continue

        # Push through the same canonicalization as the scraper
        canonical = Scraper99Acres._canonicalize_locality(raw_locality, city_name)

        if not canonical:
            logger.info(
                f"  [{listing['id']}] Nominatim returned '{raw_locality}' "
                f"→ rejected by _canonicalize_locality"
            )
            continue

        try:
            update_rows(
                "listings",
                filters={"id": listing["id"]},
                updates={"locality": canonical},
            )
            updated += 1
            logger.info(
                f"  [{listing['id']}] ({lat:.4f},{lon:.4f}) "
                f"→ '{raw_locality}' → '{canonical}'"
            )
        except Exception as exc:
            logger.warning(f"  [{listing['id']}] Update failed: {exc}")

    logger.info(f"  {city_name}: {updated}/{len(targets)} listings updated")
    return updated


def main():
    parser = argparse.ArgumentParser(
        description="Recover locality from coords for null-locality listings"
    )
    parser.add_argument(
        "--city",
        type=str,
        default=None,
        help="City name (e.g. Nashik). Omit to run all cities.",
    )
    args = parser.parse_args()

    city_map = {
        "Akola": 1, "Nagpur": 2, "Pune": 3,
        "Nashik": 5, "Amravati": 9, "Aurangabad": 10,
    }

    if args.city:
        if args.city not in city_map:
            logger.error(f"Unknown city '{args.city}'. Valid: {list(city_map)}")
            return
        fix_city(args.city)
    else:
        total = 0
        for city in city_map:
            total += fix_city(city)
        logger.info(f"Done — {total} total listings updated")


if __name__ == "__main__":
    main()
