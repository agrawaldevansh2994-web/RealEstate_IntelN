"""
scrapers/scraper_magicbricks.py
Extracts listings from MagicBricks via window.SERVER_PRELOADED_STATE_.
No DOM traversal needed — all listing data is in the SSR JS state blob.

Geocoding strategy:
  - MB listings include ltcoordGeo ("lat,lng" string) on most listings.
    Use this first — it's faster and avoids Nominatim rate limits.
  - For listings where ltcoordGeo is missing or has zero coords,
    fall back to the same Nominatim geocoder as scraper_99acres.py.
  - Locality-level coord cache shared across both sources.

Locality canonicalization:
  - Reuses LOCALITY_ALIASES, JUNK_LOCALITIES, KNOWN_OTHER_CITIES and
    Scraper99Acres._canonicalize_locality() — no duplication.

Source dedup:
  - source_listing_id = MB's `id` field (numeric string, e.g. "73717707").
  - Cross-source content-hash dedup: same (city_id, locality, property_type,
    listed_price, area_sqft, bedrooms) as an existing active listing
    from ANY source → skip insert, keep original alive.
"""

import json
import logging
import re
import time
from datetime import datetime
from typing import Generator

import requests
from playwright.sync_api import sync_playwright

from scrapers.base import BaseScraper
from scrapers.scraper_99acres import (
    JUNK_LOCALITIES,
    KNOWN_OTHER_CITIES,
    LOCALITY_ALIASES,
    Scraper99Acres,
    _content_match,
)

logger = logging.getLogger(__name__)

# ── City slug map ────────────────────────────────────────────────────────────
# MB uses these exact strings in the cityName URL param.
# "Aurangabad" still works on MB despite the official rename to Sambhaji Nagar.
_MB_CITY_SLUGS: dict[str, str] = {
    "akola":       "Akola",
    "amravati":    "Amravati",
    "nagpur":      "Nagpur",
    "pune":        "Pune",
    "nashik":      "Nashik",
    "aurangabad":  "Aurangabad",
}

# Residential property types we care about (same scope as 99acres scraper)
_MB_PROPTYPE_PARAM = (
    "Multistorey-Apartment,Builder-Floor-Apartment,"
    "Penthouse,Studio-Apartment,1-RK-Studio-Apartment"
)

# MB propTypeD string → our DB enum
_PROP_TYPE_MAP: dict[str, str] = {
    "apartment":          "flat",
    "flat":               "flat",
    "penthouse":          "flat",
    "studio apartment":   "flat",
    "1 rk":               "flat",
    "independent house":  "house_villa",
    "villa":              "house_villa",
    "independent floor":  "house_villa",
    "builder floor":      "house_villa",
    "plot":               "plot",
    "land":               "plot",
    "residential land":   "plot",
}

# MB userType → our DB enum
_LISTED_BY_MAP: dict[str, str] = {
    "owner":   "owner",
    "agent":   "broker",
    "builder": "builder",
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "MahaRERA-IntelPlatform/1.0 (research)"}

# Max area for a residential flat/house — same ceiling as 99acres scraper.
MAX_URBAN_AREA_SQFT = 20_000


class ScraperMagicBricks(BaseScraper):
    name = "magicbricks"
    city = "Nashik"

    # MB is less aggressive about bot-detection than 99acres;
    # shorter delays are safe but we keep them conservative.
    delay_min = 3.0
    delay_max = 7.0

    def __init__(self, city: str = "Nashik", max_pages: int | None = None):
        super().__init__()
        self.city = city
        city_lower = city.lower()

        # MB Nashik has 1,223 listings (41 pages); Aurangabad has ~415 (14 pages).
        # Default: scrape up to 20 pages for large cities, all pages for smaller.
        if max_pages is not None:
            self.max_pages = max_pages
        elif city_lower in {"pune", "nagpur"}:
            self.max_pages = 25   # high-volume guard (same logic as 99acres)
        elif city_lower == "nashik":
            self.max_pages = 25   # ~600 listings — doubles current Nashik density
        else:
            self.max_pages = 40   # Aurangabad (14 pages) and smaller cities

        mb_slug = _MB_CITY_SLUGS.get(city_lower, city)
        self.search_url = (
            "https://www.magicbricks.com/property-for-sale/residential-real-estate"
            f"?proptype={_MB_PROPTYPE_PARAM}&cityName={mb_slug}"
        )

        # Seen-ID set for _retire_unseen_listings (sale only — MB URL is sale-only)
        self._seen_listing_ids: set[str] = set()

        self._geocode_cache: dict[str, tuple[float, float] | None] = {}
        self._last_geocode_time = 0.0

    # ── URL builder ─────────────────────────────────────────────────────────

    def _page_url(self, page_num: int) -> str:
        if page_num == 1:
            return self.search_url
        return f"{self.search_url}&page={page_num}"

    # ── Locality helpers (delegate to 99acres) ───────────────────────────────

    def _canonicalize(self, raw: str) -> str:
        """Thin wrapper — reuses the exact same logic as scraper_99acres."""
        return Scraper99Acres._canonicalize_locality(raw, self.city)

    # ── Geocoding ────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_ltcoord(ltcoord_geo: str | None) -> tuple[float, float] | None:
        """
        Parse MB's ltcoordGeo string ("19.9701198,73.8293695").
        Returns None if string is absent, malformed, or both coords are 0.
        """
        if not ltcoord_geo:
            return None
        try:
            parts = str(ltcoord_geo).split(",")
            if len(parts) != 2:
                return None
            lat, lng = float(parts[0].strip()), float(parts[1].strip())
            if lat == 0.0 and lng == 0.0:
                return None
            return lat, lng
        except (ValueError, TypeError):
            return None

    def _geocode_nominatim(self, locality: str) -> tuple[float, float] | None:
        """
        Nominatim fallback — identical to scraper_99acres._geocode().
        Only called when ltcoordGeo is unavailable.
        """
        if not locality:
            return None
        if locality in self._geocode_cache:
            return self._geocode_cache[locality]

        parts = [p.strip() for p in locality.split(",") if p.strip()]
        if not any(p.lower() == self.city.lower() for p in parts):
            parts.append(self.city)

        queries = []
        for i in range(len(parts)):
            q = f"{', '.join(parts[i:])}, Maharashtra, India"
            if q not in queries:
                queries.append(q)

        result = None
        for query in queries:
            elapsed = time.time() - self._last_geocode_time
            if elapsed < 1.1:
                time.sleep(1.1 - elapsed)
            try:
                resp = requests.get(
                    NOMINATIM_URL,
                    params={"q": query, "format": "json",
                            "limit": 1, "countrycodes": "in"},
                    headers=NOMINATIM_HEADERS,
                    timeout=10,
                )
                self._last_geocode_time = time.time()
                data = resp.json()
                if data:
                    result = (float(data[0]["lat"]), float(data[0]["lon"]))
                    logger.debug(
                        f"Nominatim: '{locality}' via '{query}' → {result}")
                    break
            except Exception as exc:
                logger.debug(f"Nominatim error for '{query}': {exc}")

        self._geocode_cache[locality] = result
        return result

    def _resolve_coords(
        self,
        ltcoord_geo: str | None,
        locality: str,
    ) -> tuple[float | None, float | None]:
        """
        Priority:
        1. ltcoordGeo (present on ~70% of MB listings, always a point-level coord)
        2. Locality-level geocode cache hit (Nominatim)
        3. Nominatim geocode (rate-limited to 1 rps)
        """
        coords = self._parse_ltcoord(ltcoord_geo)
        if coords:
            return coords

        if locality in self._geocode_cache:
            cached = self._geocode_cache[locality]
            return cached if cached else (None, None)

        coords = self._geocode_nominatim(locality)
        return coords if coords else (None, None)

    # ── Property-type normalisation ──────────────────────────────────────────

    @staticmethod
    def _normalise_prop_type(mb_prop_type: str | None) -> str:
        key = str(mb_prop_type or "").strip().lower()
        return _PROP_TYPE_MAP.get(key, "flat")

    # ── Core extraction ──────────────────────────────────────────────────────

    def _navigate_and_extract(self, page, url: str, page_num: int) -> list[dict]:
        """
        Load a MB search results page and extract all listings from
        window.SERVER_PRELOADED_STATE_.searchResult.
        Returns an empty list on failure.
        """
        for attempt in range(1, 3):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
            except Exception as exc:
                self.logger.warning(f"Load warning p{page_num}: {exc}")
                if page.is_closed():
                    return []

            # Wait until the SSR state is hydrated
            try:
                page.wait_for_function(
                    "() => !!(window.SERVER_PRELOADED_STATE_ "
                    "&& window.SERVER_PRELOADED_STATE_.searchResult "
                    "&& Object.keys(window.SERVER_PRELOADED_STATE_.searchResult).length > 0)",
                    timeout=15000,
                )
            except Exception:
                pass  # state may already be present; proceed to extraction

            # Brief settle — avoids extracting before lazy-hydration completes
            page.wait_for_timeout(2500 if page_num == 1 else 1500)
            if page.is_closed():
                return []

            listings = self._extract_from_state(page)
            if listings:
                return listings

            if attempt == 1:
                self.logger.info(
                    f"Retrying page {page_num} ({self.city}) after empty extraction"
                )
                page.wait_for_timeout(4000)

        return []

    def _extract_from_state(self, page) -> list[dict]:
        """
        Read window.SERVER_PRELOADED_STATE_ and normalise each listing
        into the same schema dict that save() expects.
        """
        try:
            raw = page.evaluate("""
                () => {
                    const s = window.SERVER_PRELOADED_STATE_;
                    if (!s || !s.searchResult) return null;
                    return {
                        listings: Object.values(s.searchResult),
                        pageCount: s.searchAdditionalDataBean
                                    ? parseInt(s.searchAdditionalDataBean.pageCount || '0', 10)
                                    : 0
                    };
                }
            """)
        except Exception as exc:
            self.logger.error(f"State extraction error: {exc}")
            return []

        if not raw or not raw.get("listings"):
            return []

        # Store total-page count on first successful extraction
        if not hasattr(self, "_total_pages"):
            self._total_pages = raw.get("pageCount", 0)
            self.logger.info(
                f"{self.city}: MB reports {self._total_pages} pages total"
            )

        normalized = []
        for item in raw["listings"]:
            try:
                result = self._normalise_listing(item)
                if result:
                    normalized.append(result)
            except Exception as exc:
                self.logger.debug(
                    f"Listing normalise error id={item.get('id')}: {exc}"
                )

        return normalized

    def _normalise_listing(self, item: dict) -> dict | None:
        """
        Convert one raw MB listing dict to our internal schema dict.
        Returns None if the listing should be discarded (wrong city, junk locality, etc.).
        """
        listing_id = str(item.get("id", "")).strip()
        if not listing_id:
            return None

        # ── Locality ────────────────────────────────────────────────────────
        locality_raw = str(item.get("lmtDName")
                           or item.get("locSeoName") or "").strip()
        locality = self._canonicalize(locality_raw)

        # Reject wrong-city artefacts
        if locality.lower() in KNOWN_OTHER_CITIES:
            logger.debug(
                f"Wrong-city listing: locality='{locality}' id={listing_id}")
            return None

        # Reject junk locality strings (still store the raw for raw_data)
        if not locality:
            locality = locality_raw   # keep raw if canonicalization produced nothing

        # ── Price ────────────────────────────────────────────────────────────
        price_raw = item.get("price")
        try:
            listed_price = int(price_raw) if price_raw is not None else None
        except (ValueError, TypeError):
            listed_price = None

        # ── Area ─────────────────────────────────────────────────────────────
        area_raw = item.get("caSqFt") or item.get("coveredArea")
        try:
            area_sqft = float(area_raw) if area_raw is not None else None
        except (ValueError, TypeError):
            area_sqft = None

        # MB area is always sqft (confirmed: coverAreaUnitD is always "Sq-ft")
        prop_type = self._normalise_prop_type(item.get("propTypeD"))

        # Null impossibly large areas (same ceiling as 99acres)
        if area_sqft is not None and area_sqft > MAX_URBAN_AREA_SQFT and prop_type in ("flat", "house_villa"):
            logger.debug(
                f"Nulling huge {prop_type} area {area_sqft:.0f}sqft id={listing_id}")
            area_sqft = None

        # Null impossibly small areas
        if area_sqft is not None and area_sqft < 50 and prop_type in ("flat", "house_villa"):
            logger.debug(
                f"Nulling tiny {prop_type} area {area_sqft:.0f}sqft id={listing_id}")
            area_sqft = None

        # ── Price per sqft ───────────────────────────────────────────────────
        # Prefer MB's own sqFtPrice (already computed) if it looks reasonable;
        # derive from price/area otherwise (same as 99acres fallback).
        mb_psqft = item.get("sqFtPrice")
        price_per_sqft = None
        if mb_psqft:
            try:
                candidate = round(float(mb_psqft), 2)
                if 100 < candidate < 500_000:    # sanity bounds
                    price_per_sqft = candidate
            except (ValueError, TypeError):
                pass
        if price_per_sqft is None and listed_price and area_sqft and area_sqft > 0:
            price_per_sqft = round(listed_price / area_sqft, 2)

        # ── Bedrooms ─────────────────────────────────────────────────────────
        bedrooms_raw = item.get("bedroomD") or item.get("bd")
        try:
            # bedroomD is a string like "3"; bd can be "11702" (BHK code)
            # Only accept small integers (1–9) — bd codes are 5-digit ints
            bedrooms_val = int(str(bedrooms_raw).strip().split()[0])
            bedrooms = bedrooms_val if 1 <= bedrooms_val <= 9 else None
        except (ValueError, TypeError, AttributeError):
            bedrooms = None

        # ── Listed by ────────────────────────────────────────────────────────
        user_type = str(item.get("userType") or "").strip().lower()
        listed_by = _LISTED_BY_MAP.get(user_type, "owner")

        # ── Coordinates ──────────────────────────────────────────────────────
        ltcoord_geo = item.get("ltcoordGeo")
        lat, lon = self._resolve_coords(ltcoord_geo, locality)

        # ── Source URL ───────────────────────────────────────────────────────
        seo_url = item.get("seoURL") or item.get("url") or ""
        if seo_url and not seo_url.startswith("http"):
            seo_url = f"https://www.magicbricks.com/propertyDetails-{seo_url}"

        # ── Address raw ──────────────────────────────────────────────────────
        address_raw = (
            item.get("propertyTitle")
            or item.get("auto_desc")
            or f"{bedrooms}BHK {prop_type} in {locality_raw}, {self.city}"
        )

        return {
            "source":             "magicbricks",
            "source_listing_id":  listing_id,
            "source_url":         seo_url,
            "city":               self.city,
            "listing_type":       "sale",      # MB /property-for-sale/ only
            "listing_status":     "active",
            "property_type":      prop_type,
            "listed_price":       listed_price,
            "price_per_sqft":     price_per_sqft,
            "area_sqft":          area_sqft,
            "bedrooms":           bedrooms,
            "locality":           locality,
            "address_raw":        str(address_raw)[:500],
            "listed_by":          listed_by,
            "latitude":           lat,
            "longitude":          lon,
            "raw_data":           json.dumps({
                "id":          listing_id,
                "lmtDName":    locality_raw,
                "price":       price_raw,
                "caSqFt":      area_raw,
                "bedroomD":    bedrooms_raw,
                "propTypeD":   item.get("propTypeD"),
                "userType":    item.get("userType"),
                "ltcoordGeo":  ltcoord_geo,
                "reraId":      item.get("reraId"),
                "devName":     item.get("devName"),
                "prjname":     item.get("prjname"),
                "ppd":         item.get("ppd"),
            }),
            "scraped_at": datetime.utcnow().isoformat(),
        }

    # ── Main scrape loop ─────────────────────────────────────────────────────

    def scrape(self) -> Generator[dict, None, None]:
        import random

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-IN",
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = context.new_page()

            self.logger.info(
                f"Scraping MagicBricks sale listings for {self.city}")

            page_num = 1
            while page_num <= self.max_pages:
                url = self._page_url(page_num)
                self.logger.info(f"Loading page {page_num}: {url}")

                listings = self._navigate_and_extract(page, url, page_num)

                if not listings:
                    self.logger.info(
                        f"No listings on page {page_num}, stopping."
                    )
                    break

                self.logger.info(
                    f"Page {page_num} — {len(listings)} listings extracted"
                )
                for listing in listings:
                    yield listing

                # Stop early if we've already seen all available pages
                if hasattr(self, "_total_pages") and self._total_pages > 0:
                    if page_num >= self._total_pages:
                        self.logger.info(
                            f"Reached last available page ({self._total_pages}), stopping."
                        )
                        break

                page_num += 1
                delay = random.uniform(self.delay_min, self.delay_max)
                self.logger.debug(f"Inter-page delay: {delay:.1f}s")
                time.sleep(delay)

            browser.close()

    # ── Persistence ──────────────────────────────────────────────────────────

    def save(self, record: dict) -> str:
        from db.connection import insert_row, select_rows, update_rows

        table = "listings"
        self._seen_listing_ids.add(record["source_listing_id"])

        cities = select_rows("cities", {"name": record["city"]})
        city_id = cities[0]["id"] if cities else None

        zone_id = None
        if record.get("locality") and city_id:
            zones = select_rows("zones", {"city_id": city_id})
            for zone in zones:
                if record["locality"].lower() in zone["name"].lower():
                    zone_id = zone["id"]
                    break

        # ── 1. Exact match: same source + source_listing_id ──────────────────
        existing = select_rows(
            table,
            {
                "source":            "magicbricks",
                "source_listing_id": record["source_listing_id"],
            },
        )
        if existing:
            updates = {
                "listed_price":    record["listed_price"],
                "price_per_sqft":  record["price_per_sqft"],
                "area_sqft":       record["area_sqft"],
                "listing_status":  record["listing_status"],
                "last_seen_at":    record["scraped_at"],
                "property_type":   record["property_type"],
                "bedrooms":        record["bedrooms"],
                "locality":        record["locality"],
                "address_raw":     record["address_raw"],
                "listed_by":       record["listed_by"],
                "raw_data":        record["raw_data"],
            }
            if zone_id is not None:
                updates["zone_id"] = zone_id
            update_rows(table, filters={
                        "id": existing[0]["id"]}, updates=updates)
            return "updated"

        # ── 2. Content-hash dedup (cross-source) ─────────────────────────────
        # Guards against the same physical listing appearing on both MB and 99acres
        # with different source IDs. Checks ALL sources for the same content fingerprint.
        locality = (record.get("locality") or "").strip()
        if locality and record.get("listed_price"):
            locality_candidates = select_rows(
                table,
                {
                    "city_id":        city_id,
                    "locality":       record["locality"],
                    "property_type":  record["property_type"],
                    "listing_status": "active",
                },
                limit=30,
            )
            for match in locality_candidates:
                if (
                    _content_match(match.get("listed_price"),
                                   record.get("listed_price"))
                    and _content_match(match.get("area_sqft"),    record.get("area_sqft"))
                    and _content_match(match.get("bedrooms"),     record.get("bedrooms"))
                ):
                    # Keep the original listing alive in the seen-set
                    # (only matters if it's also a magicbricks listing)
                    if match.get("source") == "magicbricks":
                        self._seen_listing_ids.add(
                            match.get("source_listing_id"))
                    logger.debug(
                        f"Content-hash dedup: skipping MB {record['source_listing_id']}"
                        f" — matches listing {match['id']} "
                        f"(source={match.get('source')} locality={locality})"
                    )
                    return "duplicate"

        # ── 3. Insert new listing ─────────────────────────────────────────────
        lat = record.get("latitude")
        lon = record.get("longitude")

        # If MB didn't provide coords, try Nominatim now
        if (lat is None or lon is None) and locality:
            coords = self._geocode_nominatim(locality)
            if coords:
                lat, lon = coords

        row = {
            "city_id":           city_id,
            "zone_id":           zone_id,
            "source":            "magicbricks",
            "source_listing_id": record["source_listing_id"],
            "source_url":        record["source_url"],
            "listing_type":      record["listing_type"],
            "listing_status":    record["listing_status"],
            "last_seen_at":      record["scraped_at"],
            "property_type":     record["property_type"],
            "listed_price":      record["listed_price"],
            "price_per_sqft":    record["price_per_sqft"],
            "area_sqft":         record["area_sqft"],
            "bedrooms":          record["bedrooms"],
            "locality":          record["locality"],
            "address_raw":       record["address_raw"],
            "listed_by":         record["listed_by"],
            "raw_data":          record["raw_data"],
            "latitude":          lat,
            "longitude":         lon,
        }
        row = {k: v for k, v in row.items() if v is not None}
        insert_row(table, row)
        return "inserted"

    # ── Stale listing retirement ─────────────────────────────────────────────

    def _retire_unseen_listings(self) -> None:
        from db.connection import select_rows, update_rows

        cities = select_rows("cities", {"name": self.city}, limit=5)
        if not cities:
            self.logger.warning(
                f"Could not resolve city_id for {self.city}; skipping stale retirement"
            )
            return

        city_id = cities[0]["id"]
        active_rows = select_rows(
            "listings",
            filters={
                "city_id":        city_id,
                "source":         "magicbricks",
                "listing_type":   "sale",
                "listing_status": "active",
            },
            limit=5000,
        )

        active_count = len(active_rows)
        seen_count = len(self._seen_listing_ids)

        if active_count == 0 or seen_count == 0:
            self.logger.info(
                f"Skipping stale cleanup for {self.city}: "
                f"active={active_count}, seen={seen_count}"
            )
            return

        coverage = seen_count / active_count
        if coverage < 0.75:
            self.logger.warning(
                f"Skipping stale cleanup for {self.city}: "
                f"coverage too low ({seen_count}/{active_count} = {coverage:.0%})"
            )
            return

        retired = 0
        for row in active_rows:
            if row.get("source_listing_id") in self._seen_listing_ids:
                continue
            update_rows(
                "listings",
                filters={"id": row["id"]},
                updates={"listing_status": "inactive"},
            )
            retired += 1

        self.logger.info(
            f"Retired {retired} stale MB sale listings for {self.city}"
        )

    # ── Quality report ───────────────────────────────────────────────────────

    def _quality_report(self) -> None:
        from db.connection import select_rows
        try:
            cities = select_rows("cities", {"name": self.city}, limit=5)
            if not cities:
                return
            city_id = cities[0]["id"]

            rows = select_rows(
                "listings",
                filters={
                    "city_id":        city_id,
                    "source":         "magicbricks",
                    "listing_status": "active",
                },
                limit=5000,
            )
            if not rows:
                return

            low_psqft = sum(1 for r in rows if r.get(
                "price_per_sqft") and float(r["price_per_sqft"]) < 500)
            null_area = sum(1 for r in rows if r.get("area_sqft") is None)
            huge_area = sum(1 for r in rows if r.get("area_sqft")
                            and float(r["area_sqft"]) > 20_000)
            null_psqft = sum(1 for r in rows if r.get(
                "price_per_sqft") is None)
            with_coords = sum(1 for r in rows if r.get("latitude") is not None)
            spids = [r["source_listing_id"]
                     for r in rows if r.get("source_listing_id")]
            dupes = len(spids) - len(set(spids))

            self.logger.info(
                f"── MB Quality report [{self.city}] ──────────────────\n"
                f"  Active listings      : {len(rows)}\n"
                f"  Geocoded             : {with_coords} ({with_coords/len(rows):.0%})\n"
                f"  Null area_sqft       : {null_area}\n"
                f"  Null price_per_sqft  : {null_psqft}\n"
                f"  Low psqft (<500)     : {low_psqft}  ← check if agricultural\n"
                f"  Huge area (>20k sqft): {huge_area}  ← agricultural/rural plots\n"
                f"  Duplicate spids      : {dupes}      ← should be 0\n"
                f"──────────────────────────────────────────────────────"
            )
        except Exception as exc:
            self.logger.warning(f"Quality report failed: {exc}")

    # ── Entry point ──────────────────────────────────────────────────────────

    def run(self):
        self.start_run()
        status = "success"
        try:
            for record in self.scrape():
                self.stats["fetched"] += 1
                try:
                    result = self.save(record)
                    if result == "inserted":
                        self.stats["inserted"] += 1
                    elif result == "updated":
                        self.stats["updated"] += 1
                except Exception as exc:
                    self.logger.error(
                        f"Save error: {exc} | record={str(record)[:200]}"
                    )
                    self.stats["errors"].append(str(exc))

            if not self.stats["errors"]:
                self._retire_unseen_listings()
        except Exception as exc:
            self.logger.exception(f"Fatal scrape error: {exc}")
            self.stats["errors"].append(f"FATAL: {exc}")
            status = "failed"
        finally:
            self._quality_report()
            self.finish_run(status)
