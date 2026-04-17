"""
scrapers/scraper_99acres.py
Extracts listings directly from the SSR HTML page.
No API interception needed - data is in the DOM.

Geocoding:
  - New listings are geocoded by locality at insert time
  - Locality-level cache means same area is only geocoded once per run
  - Uses OSM Nominatim (free, no key) with progressive fallback queries
  - Updates are skipped for geocoding (coordinates do not change)
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

logger = logging.getLogger(__name__)

LISTING_TYPE_MAP = {"buy": "sale", "rent": "rent"}
AREA_UNIT_TO_SQFT = {
    "sqft": 1.0,
    "sq ft": 1.0,
    "sq feet": 1.0,
    "square feet": 1.0,
    "sqyd": 9.0,
    "sq yd": 9.0,
    "sq yard": 9.0,
    "sq yards": 9.0,
    "square yard": 9.0,
    "square yards": 9.0,
    "sqm": 10.7639,
    "sq m": 10.7639,
    "sq meter": 10.7639,
    "sq meters": 10.7639,
    "sq metre": 10.7639,
    "sq metres": 10.7639,
}
LOCALITY_ALIASES = {
    "khadki bk": "khadki",
    "khadki budruk": "khadki",
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "MahaRERA-IntelPlatform/1.0 (research)"}


class Scraper99Acres(BaseScraper):
    name = "99acres"
    city = "Akola"
    delay_min = 1.0
    delay_max = 2.0

    def __init__(self, city="Akola", listing_types=None, max_pages=6):
        super().__init__()
        self.city = city
        self.listing_types = listing_types or ["buy", "rent"]
        self.max_pages = max_pages
        self._seen_listing_ids: dict[str, set[str]] = {
            LISTING_TYPE_MAP.get(listing_type, listing_type): set()
            for listing_type in self.listing_types
        }

        city_slug = city.lower().replace(" ", "-")
        self.search_urls = {
            "buy": f"https://www.99acres.com/property-in-{city_slug}-ffid",
            "rent": f"https://www.99acres.com/rent-property-in-{city_slug}-ffid",
        }

        self._geocode_cache: dict[str, tuple[float, float] | None] = {}
        self._last_geocode_time = 0.0

    @staticmethod
    def _normalize_area_unit(unit: str) -> str:
        cleaned = re.sub(r"[^a-z\s]", " ", str(unit or "").lower())
        cleaned = cleaned.replace("square", "sq")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    @classmethod
    def _parse_area_line(cls, line: str) -> tuple[float | None, str | None, float | None]:
        match = re.search(
            r"([\d,]+(?:\.\d+)?)\s*(sq(?:\s+|\.?)(?:ft|feet|yd|yard|yards|m|meter|meters|metre|metres)|sqft|sqyd|sqm)\b",
            str(line or ""),
            flags=re.I,
        )
        if not match:
            return None, None, None

        raw_value = float(match.group(1).replace(",", ""))
        raw_unit = cls._normalize_area_unit(match.group(2))
        factor = AREA_UNIT_TO_SQFT.get(raw_unit)
        if factor is None:
            return raw_value, raw_unit, None
        return raw_value, raw_unit, round(raw_value * factor, 2)

    @classmethod
    def _parse_area_from_url(cls, url: str) -> tuple[float | None, str | None, float | None]:
        match = re.search(
            r"-(\d[\d,]*(?:\.\d+)?)-sq-(ft|feet|yard|yards|yd|m|meter|meters|metre|metres)\b",
            str(url or "").lower(),
        )
        if not match:
            return None, None, None

        raw_value = float(match.group(1).replace(",", ""))
        raw_unit = cls._normalize_area_unit(f"sq {match.group(2)}")
        factor = AREA_UNIT_TO_SQFT.get(raw_unit)
        if factor is None:
            return raw_value, raw_unit, None
        return raw_value, raw_unit, round(raw_value * factor, 2)

    @classmethod
    def _extract_area_sqft(
        cls,
        lines: list[str],
        url: str,
    ) -> tuple[float | None, str | None, float | None]:
        for line in lines or []:
            raw_value, raw_unit, area_sqft = cls._parse_area_line(line)
            if raw_value is not None:
                return raw_value, raw_unit, area_sqft
        return cls._parse_area_from_url(url)

    @classmethod
    def _extract_price_per_sqft(
        cls,
        lines: list[str],
        listed_price: float | None,
        area_sqft: float | None,
    ) -> tuple[float | None, str | None]:
        for line in lines or []:
            match = re.search(
                r"₹\s*([\d,]+(?:\.\d+)?)\s*/\s*(sq(?:\s+|\.?)(?:ft|feet|yd|yard|yards|m|meter|meters|metre|metres)|sqft|sqyd|sqm)\b",
                str(line or ""),
                flags=re.I,
            )
            if not match:
                continue

            raw_rate = float(match.group(1).replace(",", ""))
            raw_unit = cls._normalize_area_unit(match.group(2))
            factor = AREA_UNIT_TO_SQFT.get(raw_unit)
            if factor is None:
                return None, raw_unit
            return round(raw_rate / factor, 2), raw_unit

        if listed_price and area_sqft and area_sqft > 0:
            return round(float(listed_price) / float(area_sqft), 2), "derived"
        return None, None

    @classmethod
    def _canonicalize_locality(cls, locality: str, city: str) -> str:
        value = str(locality or "").strip()
        city_name = str(city or "").strip()
        if not value:
            return ""

        value = re.sub(
            rf",\s*{re.escape(city_name)}\s*$",
            "",
            value,
            flags=re.I,
        )
        value = value.replace("/", " ")
        value = re.sub(r"[()]", " ", value)
        value = re.sub(r"\s+", " ", value).strip(" ,.-")

        normalized = value.lower()
        normalized = re.sub(r"\b(bk|bk\.)\b$", "", normalized).strip(" ,.-")
        normalized = LOCALITY_ALIASES.get(normalized, normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        if not normalized:
            return value
        return normalized.title()

    @staticmethod
    def _looks_like_project_name(locality: str) -> bool:
        value = str(locality or "").strip().lower()
        if not value:
            return False

        project_markers = (
            "apartment",
            "appartment",
            "residency",
            "residence",
            "plaza",
            "park",
            "heights",
            "height",
            "enclave",
            "villa",
            "villas",
            "duplex",
            "tower",
            "towers",
            "county",
            "palace",
        )
        return any(marker in value for marker in project_markers)

    @staticmethod
    def _clean_title_locality(candidate: str) -> str:
        value = str(candidate or "").strip(" ,-")
        value = re.sub(r"\s+", " ", value)
        return value.strip(" ,-")

    @classmethod
    def _extract_locality_from_title(cls, title: str, city: str) -> str:
        text = str(title or "").strip()
        city_name = str(city or "").strip()
        if not text:
            return ""

        match = re.search(r"\bin\s+(.+)$", text, flags=re.I)
        if not match:
            return ""

        candidate = cls._clean_title_locality(match.group(1))
        if not candidate:
            return ""

        if city_name and re.fullmatch(re.escape(city_name), candidate, flags=re.I):
            return ""

        candidate_parts = [part.strip()
                           for part in candidate.split(",") if part.strip()]
        if len(candidate_parts) == 1 and city_name and candidate_parts[0].lower() == city_name.lower():
            return ""

        return candidate

    @classmethod
    def _resolve_locality(cls, locality_raw: str, title: str, city: str) -> tuple[str, str]:
        title_locality_raw = cls._extract_locality_from_title(title, city)
        title_locality = cls._canonicalize_locality(title_locality_raw, city)
        raw_locality = cls._canonicalize_locality(locality_raw, city)

        if title_locality:
            return title_locality, title_locality_raw

        if raw_locality:
            return raw_locality, locality_raw

        if cls._looks_like_project_name(locality_raw) and title_locality_raw:
            return title_locality, title_locality_raw

        return "", locality_raw

    def _geocode(self, locality: str) -> tuple[float, float] | None:
        if not locality:
            return None

        if locality in self._geocode_cache:
            return self._geocode_cache[locality]

        parts = [p.strip() for p in locality.split(",") if p and p.strip()]
        if not any(p.lower() == self.city.lower() for p in parts):
            parts.append(self.city)

        queries = []
        for i in range(len(parts)):
            suffix = ", ".join(parts[i:])
            query = f"{suffix}, Maharashtra, India"
            if query not in queries:
                queries.append(query)

        result = None
        for query in queries:
            elapsed = time.time() - self._last_geocode_time
            if elapsed < 1.1:
                time.sleep(1.1 - elapsed)

            try:
                resp = requests.get(
                    NOMINATIM_URL,
                    params={
                        "q": query,
                        "format": "json",
                        "limit": 1,
                        "countrycodes": "in",
                    },
                    headers=NOMINATIM_HEADERS,
                    timeout=10,
                )
                self._last_geocode_time = time.time()
                data = resp.json()
                if data:
                    result = (float(data[0]["lat"]), float(data[0]["lon"]))
                    logger.debug(
                        f"Geocoded '{locality}' via '{query}' -> {result}")
                    break
            except Exception as exc:
                logger.debug(f"Nominatim error for '{query}': {exc}")

        if result is None:
            logger.debug(
                f"Could not geocode '{locality}' - all queries exhausted")

        self._geocode_cache[locality] = result
        return result

    def scrape(self) -> Generator[dict, None, None]:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-IN",
            )
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = context.new_page()

            for listing_type in self.listing_types:
                self.logger.info(
                    f"Scraping {listing_type} listings for {self.city}")
                url = self.search_urls.get(
                    listing_type, self.search_urls["buy"])
                page_num = 1

                while page_num <= self.max_pages:
                    nav_url = url if page_num == 1 else f"{url}?page={page_num}"
                    self.logger.info(f"Loading page {page_num}: {nav_url}")

                    try:
                        page.goto(
                            nav_url, wait_until="domcontentloaded", timeout=30000)
                        page.wait_for_timeout(3000)
                    except Exception as exc:
                        self.logger.warning(f"Load warning: {exc}")

                    listings = self._extract_from_dom(page, listing_type)

                    if not listings:
                        self.logger.info(
                            f"No listings on page {page_num}, stopping.")
                        break

                    self.logger.info(
                        f"Page {page_num} - {len(listings)} listings extracted"
                    )
                    for listing in listings:
                        yield listing

                    page_num += 1

            browser.close()

    def _extract_from_dom(self, page, listing_type) -> list:
        try:
            listings = page.evaluate("""
                () => {
                    const results = [];
                    const cards = document.querySelectorAll('a[href*="spid-"]');

                    cards.forEach(link => {
                        try {
                            let card = link;
                            for (let i = 0; i < 10; i++) {
                                card = card.parentElement;
                                if (!card) break;
                                const text = card.innerText || '';
                                if (text.includes('/sqft') && text.includes('sqft')) break;
                            }
                            if (!card) return;

                            const cardText = card.innerText || '';
                            const lines = cardText.split('\\n').map(l => l.trim()).filter(l => l);
                            const url = link.href || '';
                            const spidMatch = url.match(/spid-([A-Z0-9]+)/i);
                            const spid = spidMatch ? spidMatch[1] : '';
                            const title = link.innerText.trim();

                            let price = null;
                            for (const line of lines) {
                                if (line.startsWith('₹') && !line.includes('/sqft')) {
                                    const p = line.replace('₹', '').replace(',', '').trim();
                                    if (p.includes('Cr')) price = parseFloat(p) * 10000000;
                                    else if (p.includes('Lac')) price = parseFloat(p) * 100000;
                                    else price = parseFloat(p) || null;
                                }
                            }

                            let bedrooms = null;
                            for (const line of lines) {
                                const m = line.match(/(\\d+)\\s*BHK/i);
                                if (m) { bedrooms = parseInt(m[1]); break; }
                                if (line.includes('1 RK')) { bedrooms = 1; break; }
                            }

                            const locality = lines[0] || '';

                            let propertyType = 'flat';
                            const tl = title.toLowerCase();
                            if (tl.includes('plot') || tl.includes('land')) propertyType = 'plot';
                            else if (tl.includes('house') || tl.includes('villa')) propertyType = 'house_villa';

                            let listedBy = 'owner';
                            if (cardText.includes('Dealer')) listedBy = 'broker';
                            if (cardText.includes('Builder')) listedBy = 'builder';

                            results.push({
                                spid, title, url, price, bedrooms,
                                locality, propertyType, listedBy, lines,
                            });
                        } catch (e) {}
                    });
                    return results;
                }
            """)

            normalized = []
            for record in listings or []:
                if not record.get("spid"):
                    continue

                lines = record.get("lines") or []
                locality_raw = (record.get("locality") or "").strip()
                locality, locality_source_raw = self._resolve_locality(
                    locality_raw,
                    record.get("title", ""),
                    self.city,
                )
                area_value_raw, area_unit_raw, area_sqft = self._extract_area_sqft(
                    lines,
                    record.get("url", ""),
                )
                price_per_sqft, price_unit_raw = self._extract_price_per_sqft(
                    lines,
                    record.get("price"),
                    area_sqft,
                )

                record["localityRaw"] = locality_raw
                record["localitySourceRaw"] = locality_source_raw
                record["localityNormalized"] = locality
                record["areaValueRaw"] = area_value_raw
                record["areaUnitRaw"] = area_unit_raw
                record["areaSqftNormalized"] = area_sqft
                record["pricePerSqftNormalized"] = price_per_sqft
                record["priceUnitRaw"] = price_unit_raw

                normalized.append({
                    "source": "99acres",
                    "source_listing_id": record["spid"],
                    "source_url": record.get("url", ""),
                    "city": self.city,
                    "listing_type": LISTING_TYPE_MAP.get(listing_type, listing_type),
                    "listing_status": "active",
                    "property_type": record.get("propertyType", "flat"),
                    "listed_price": record.get("price"),
                    "price_per_sqft": price_per_sqft,
                    "area_sqft": area_sqft,
                    "bedrooms": record.get("bedrooms"),
                    "locality": locality or locality_raw,
                    "address_raw": record.get("title", ""),
                    "listed_by": record.get("listedBy", "owner"),
                    "raw_data": json.dumps(record),
                    "scraped_at": datetime.utcnow().isoformat(),
                })
            return normalized

        except Exception as exc:
            self.logger.error(f"DOM extraction error: {exc}")
            return []

    def _retire_unseen_listings(self) -> None:
        from db.connection import select_rows, update_rows

        cities = select_rows("cities", {"name": self.city}, limit=5)
        if not cities:
            self.logger.warning(
                f"Could not resolve city_id for {self.city}; skipping stale listing retirement"
            )
            return

        city_id = cities[0]["id"]

        for listing_type, seen_ids in self._seen_listing_ids.items():
            active_rows = select_rows(
                "listings",
                filters={
                    "city_id": city_id,
                    "source": "99acres",
                    "listing_type": listing_type,
                    "listing_status": "active",
                },
                limit=5000,
            )

            active_count = len(active_rows)
            seen_count = len(seen_ids)

            if active_count == 0 or seen_count == 0:
                self.logger.info(
                    f"Skipping stale cleanup for {self.city}/{listing_type}: "
                    f"active={active_count}, seen={seen_count}"
                )
                continue

            coverage = seen_count / active_count
            if coverage < 0.75:
                self.logger.warning(
                    f"Skipping stale cleanup for {self.city}/{listing_type}: "
                    f"coverage too low ({seen_count}/{active_count} = {coverage:.0%})"
                )
                continue

            retired = 0
            for row in active_rows:
                if row.get("source_listing_id") in seen_ids:
                    continue
                update_rows(
                    "listings",
                    filters={"id": row["id"]},
                    updates={"listing_status": "inactive"},
                )
                retired += 1

            self.logger.info(
                f"Retired {retired} stale {listing_type} listings for {self.city}"
            )

    def save(self, record) -> str:
        from db.connection import insert_row, select_rows, update_rows

        table = "listings"
        self._seen_listing_ids.setdefault(record["listing_type"], set()).add(
            record["source_listing_id"]
        )

        cities = select_rows("cities", {"name": record["city"]})
        city_id = cities[0]["id"] if cities else None

        zone_id = None
        if record.get("locality") and city_id:
            zones = select_rows("zones", {"city_id": city_id})
            for zone in zones:
                if record["locality"].lower() in zone["name"].lower():
                    zone_id = zone["id"]
                    break

        existing = select_rows(
            table,
            {
                "source": "99acres",
                "source_listing_id": record["source_listing_id"],
            },
        )

        if existing:
            updates = {
                "listed_price": record["listed_price"],
                "price_per_sqft": record["price_per_sqft"],
                "area_sqft": record["area_sqft"],
                "listing_status": record["listing_status"],
                "last_seen_at": record["scraped_at"],
                "property_type": record["property_type"],
                "bedrooms": record["bedrooms"],
                "locality": record["locality"],
                "address_raw": record["address_raw"],
                "listed_by": record["listed_by"],
                "raw_data": record["raw_data"],
            }
            if zone_id is not None:
                updates["zone_id"] = zone_id

            update_rows(
                table,
                filters={"id": existing[0]["id"]},
                updates=updates,
            )
            return "updated"

        lat, lon = None, None
        locality = (record.get("locality") or "").strip()
        if locality:
            coords = self._geocode(locality)
            if coords:
                lat, lon = coords

        row = {
            "city_id": city_id,
            "zone_id": zone_id,
            "source": "99acres",
            "source_listing_id": record["source_listing_id"],
            "source_url": record["source_url"],
            "listing_type": record["listing_type"],
            "listing_status": record["listing_status"],
            "last_seen_at": record["scraped_at"],
            "property_type": record["property_type"],
            "listed_price": record["listed_price"],
            "price_per_sqft": record["price_per_sqft"],
            "area_sqft": record["area_sqft"],
            "bedrooms": record["bedrooms"],
            "locality": record["locality"],
            "address_raw": record["address_raw"],
            "listed_by": record["listed_by"],
            "raw_data": record["raw_data"],
            "latitude": lat,
            "longitude": lon,
        }
        row = {key: value for key, value in row.items() if value is not None}
        insert_row(table, row)
        return "inserted"

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
                        f"Save error: {exc} | record={str(record)[:200]}")
                    self.stats["errors"].append(str(exc))

            if not self.stats["errors"]:
                self._retire_unseen_listings()
        except Exception as exc:
            self.logger.exception(f"Fatal scrape error: {exc}")
            self.stats["errors"].append(f"FATAL: {exc}")
            status = "failed"
        finally:
            self.finish_run(status)
