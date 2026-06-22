"""
scrapers/geocode_listings.py
Backfills listings.latitude + listings.longitude using listing locality text.

Behavior:
  - Works on shared `listings` table filtered by city_id
  - Skips rows that already have both coordinates
  - Groups rows by locality so repeated localities are geocoded once
  - Is safe to rerun after interruption
"""

import logging
import time
from collections import defaultdict

import requests

from db.connection import select_rows, update_rows

logger = logging.getLogger(__name__)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "MahaRERA-IntelPlatform/1.0 (research)"}
CITY_MATCH_TOKENS = {
    "Akola": {"akola"},
    "Amravati": {"amravati"},
    "Nagpur": {"nagpur"},
    "Pune": {"pune"},
    "Nashik": {"nashik", "nasik"},
    "Aurangabad": {"aurangabad", "chhatrapati sambhajinagar", "sambhajinagar"},
}


def _has_coords(row: dict) -> bool:
    return row.get("latitude") is not None and row.get("longitude") is not None


def _build_queries(locality: str, city_name: str, pin_code: str = "") -> list[str]:
    """Build progressively broader Nominatim queries for one locality string."""
    parts = [p.strip() for p in str(locality or "").split(",") if p and p.strip()]
    if not parts:
        return []

    queries: list[str] = []

    def add(query: str) -> None:
        if query and query not in queries:
            queries.append(query)

    full = ", ".join(parts)
    add(f"{full}, {city_name}, Maharashtra, India")
    if pin_code:
        add(f"{full}, {pin_code}, {city_name}, Maharashtra, India")
    add(f"{full}, Maharashtra, India")

    for i in range(1, len(parts)):
        suffix = ", ".join(parts[i:])
        add(f"{suffix}, {city_name}, Maharashtra, India")
        if pin_code:
            add(f"{suffix}, {pin_code}, {city_name}, Maharashtra, India")
        add(f"{suffix}, Maharashtra, India")

    bare = parts[0]
    add(f"{bare}, {city_name}, Maharashtra, India")
    if pin_code:
        add(f"{pin_code}, {city_name}, Maharashtra, India")
        add(f"{bare}, {pin_code}, Maharashtra, India")
    add(f"{bare}, Maharashtra, India")

    return queries


def _result_text(result: dict) -> str:
    address = result.get("address") or {}
    parts = [str(result.get("display_name") or "")]
    if isinstance(address, dict):
        parts.extend(str(v) for v in address.values() if v)
    return " | ".join(parts).lower()


def _matches_city(result: dict, city_name: str) -> bool:
    tokens = CITY_MATCH_TOKENS.get(city_name, {city_name.lower()})
    haystack = _result_text(result)
    return any(token in haystack for token in tokens)


def _geocode_locality(locality: str, city_name: str, pin_code: str = "") -> tuple[float, float] | None:
    """
    Try progressively broader locality queries until Nominatim returns a result.
    Returns (lat, lon) or None.
    """
    for query in _build_queries(locality, city_name, pin_code):
        try:
            resp = requests.get(
                NOMINATIM_URL,
                params={
                    "q": query,
                    "format": "json",
                    "limit": 1,
                    "countrycodes": "in",
                    "addressdetails": 1,
                },
                headers=NOMINATIM_HEADERS,
                timeout=10,
            )
            resp.raise_for_status()
            results = resp.json()
            if results:
                result = results[0]
                if _matches_city(result, city_name):
                    return float(result["lat"]), float(result["lon"])
                logger.debug(
                    "Rejected wrong-city geocode for '%s' via '%s': %s",
                    locality,
                    query,
                    result.get("display_name", ""),
                )
        except Exception as exc:
            logger.debug(f"Nominatim failed for '{query}': {exc}")

        # Respect Nominatim's public rate limit between attempts.
        time.sleep(1.1)

    return None


def geocode_listings(city: str = "Akola") -> int:
    """Backfill coordinates for listings in one city. Returns rows updated."""
    logger.info(f"geocode_listings: starting for {city}")

    city_rows = select_rows("cities", filters={"name": city}, limit=5)
    if not city_rows:
        logger.error(f"City '{city}' not found in DB")
        return 0

    city_id = city_rows[0]["id"]
    city_name = city_rows[0]["name"]

    listings = select_rows("listings", filters={"city_id": city_id}, limit=2000)
    if not listings:
        logger.info("No listings found.")
        return 0

    locality_to_rows: dict[str, list[dict]] = defaultdict(list)
    already_geocoded = 0

    for row in listings:
        if _has_coords(row):
            already_geocoded += 1
            continue

        locality = str(row.get("locality") or "").strip()
        if not locality:
            continue
        locality_to_rows[locality].append(row)

    unique_localities = len(locality_to_rows)
    total_pending = sum(len(rows) for rows in locality_to_rows.values())
    logger.info(
        f"  {already_geocoded} already geocoded, "
        f"{total_pending} pending across {unique_localities} unique localities"
    )

    if not locality_to_rows:
        logger.info("Nothing to geocode.")
        return 0

    updated = 0
    failed_localities = 0

    for locality, rows in locality_to_rows.items():
        ids = [str(row["id"]) for row in rows]
        pin_codes = sorted(
            {
                str(row.get("pin_code") or "").strip()
                for row in rows
                if str(row.get("pin_code") or "").strip()
            }
        )
        pin_code = pin_codes[0] if pin_codes else ""

        logger.info(f"  Geocoding '{locality}' ({len(ids)} listings)...")
        coords = _geocode_locality(locality, city_name, pin_code)

        if not coords:
            logger.warning(f"  [no] No result for '{locality}'")
            failed_localities += 1
            continue

        lat, lon = coords
        logger.info(f"  [ok] {lat:.5f}, {lon:.5f}")

        for listing_id in ids:
            try:
                update_rows(
                    "listings",
                    filters={"id": listing_id},
                    updates={"latitude": lat, "longitude": lon},
                )
                updated += 1
            except Exception as exc:
                logger.warning(f"  Could not update listing {listing_id}: {exc}")

    logger.info(
        f"geocode_listings complete - updated={updated} "
        f"failed_localities={failed_localities} already_had_coords={already_geocoded}"
    )
    return updated


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    import sys

    city_arg = sys.argv[1] if len(sys.argv) > 1 else "Akola"
    geocode_listings(city=city_arg)
