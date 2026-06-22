"""
models/price_tracker.py
Reads listings table → computes locality-level aggregates →
inserts a daily snapshot into price_history.

Uses actual column names from the DB:
  period_date, city_id, zone_id, locality, property_type,
  total_listings, avg_price_sqft, median_price_sqft, min_price_sqft,
  max_price_sqft, listing_count, avg_price_total, source, snapshot_date

Run standalone:  python main.py --snapshot
"""

import logging
import re
from collections import defaultdict
from datetime import date

from db.connection import select_rows, upsert_row
from models.listing_sources import (
    COMBINED_MARKETPLACE_SOURCE,
    MARKETPLACE_SOURCES,
)

logger = logging.getLogger(__name__)

# Akola city_id in the cities table
DEFAULT_CITY_ID = None   # auto-resolved below

# Per-city upper ceiling for price_per_sqft (₹/sqft).
# Listings above these are bad data — area in wrong units (sq yards stored
# as sqft), data-entry errors, etc. Ceilings are set well above any
# realistic premium property in each market to avoid false exclusions.
_PSQFT_CEILING: dict[str, float] = {
    "pune":       30_000,
    "nagpur":     20_000,
    "nashik":     20_000,
    "aurangabad": 20_000,
    "amravati":   15_000,
    "akola":      15_000,
}
_PSQFT_DEFAULT_CEILING = 20_000   # fallback for any future city

# Strings that are not real localities — administrative units, landmarks,
# gut/plot/survey number strings, etc.
_JUNK_LOCALITY_PATTERNS = re.compile(
    r"^(gut\s*no|plot\s*no|survey\s*no|s\.?\s*no|cts\s*no|gat\s*no"
    r"|sector\s*\d|phase\s*\d|village|taluka|tehsil|mandal|ward\s*no"
    r"|mouza|khasra|revenue|municipal|nagar\s*parishad|gram\s*panchayat"
    r"|panchayat\s*samiti|unknown|n/?a|nil|none|not\s*available)",
    re.IGNORECASE,
)


class PriceTracker:

    @staticmethod
    def _clean_locality(locality: str, city: str) -> str | None:
        """
        Normalise a raw locality string.

        - Strips leading/trailing whitespace
        - Removes ', CityName' suffix (case-insensitive)
        - Rejects strings that are gut/plot/survey numbers, admin units,
          or other known junk patterns
        - Rejects strings shorter than 3 chars after cleaning
        - Returns None if the locality is not usable
        """
        if not locality:
            return None

        cleaned = locality.strip()

        # Remove trailing city name suffix e.g. "Dwarka, Nashik" → "Dwarka"
        suffix_pattern = re.compile(
            r",?\s*" + re.escape(city) + r"\s*$", re.IGNORECASE
        )
        cleaned = suffix_pattern.sub("", cleaned).strip().rstrip(",").strip()

        if len(cleaned) < 3:
            return None

        if _JUNK_LOCALITY_PATTERNS.match(cleaned):
            return None

        return cleaned

    def snapshot(self, city: str = "Akola") -> int:
        logger.info(f"PriceTracker: snapshotting listings for {city}")

        # Resolve city_id
        cities = select_rows("cities", limit=50)
        city_id = next(
            (c["id"]
             for c in cities if (c.get("name") or "").lower() == city.lower()),
            None
        )

        listings = []
        for source in MARKETPLACE_SOURCES:
            listings.extend(select_rows(
                "listings",
                filters={
                    "city_id": city_id,
                    "source": source,
                    "listing_status": "active",
                },
                limit=2000,
            ))
        if not listings:
            logger.warning("PriceTracker: no listings found")
            return 0

        # Group by (locality, property_type)
        groups: dict[tuple, list] = defaultdict(list)
        group_totals: dict[tuple, list] = defaultdict(list)
        group_sources: dict[tuple, set[str]] = defaultdict(set)

        for l in listings:
            ppsf = l.get("price_per_sqft")
            if not ppsf:
                continue
            try:
                ppsf = float(ppsf)
            except (ValueError, TypeError):
                continue

            ceiling = _PSQFT_CEILING.get(city.lower(), _PSQFT_DEFAULT_CEILING)
            if ppsf > ceiling:
                logger.debug(
                    f"Skipping outlier psqft ₹{ppsf:,.0f} "
                    f"(>{ceiling:,}) for listing {l.get('id')}"
                )
                continue

            locality = self._clean_locality(
                (l.get("locality") or "").strip(), city
            )
            if not locality:
                continue

            property_type = (l.get("property_type")
                             or "unknown").strip().lower()
            listing_type = (l.get("listing_type") or "sale").strip().lower()

            key = (locality, property_type, listing_type)
            groups[key].append(ppsf)
            group_sources[key].add((l.get("source") or "").lower())

            total = l.get("listed_price")
            if total:
                try:
                    group_totals[key].append(float(total))
                except (ValueError, TypeError):
                    pass

        today = date.today().isoformat()
        written = 0

        for (locality, property_type, listing_type), prices in groups.items():
            if len(prices) < 2:
                continue

            prices_sorted = sorted(prices)
            n = len(prices_sorted)
            median = prices_sorted[n // 2]
            avg = sum(prices_sorted) / n
            totals = group_totals.get(
                (locality, property_type, listing_type), [])
            sources = group_sources[(locality, property_type, listing_type)]
            snapshot_source = (
                next(iter(sources))
                if len(sources) == 1
                else COMBINED_MARKETPLACE_SOURCE
            )

            row = {
                "period_date":        today,
                "snapshot_date":      today,
                "city":               city,
                "city_id":            city_id,
                "locality":           locality,
                "property_type":      property_type,
                "listing_type":       listing_type,
                "total_listings":     n,
                "listing_count":      n,
                "avg_price_sqft":     round(avg, 2),
                "min_price_sqft":     round(prices_sorted[0], 2),
                "max_price_sqft":     round(prices_sorted[-1], 2),
                "median_price_sqft":  round(median, 2),
                "avg_price_total":    round(sum(totals) / len(totals), 2) if totals else None,
                "source":             snapshot_source,
            }

            try:
                upsert_row("price_history", row)
                written += 1
                logger.debug(
                    f"Snapshot: {locality}/{property_type} → ₹{avg:,.0f}/sqft ({n} listings)")
            except Exception as e:
                logger.warning(
                    f"Could not write snapshot for {locality}/{property_type}: {e}")

        logger.info(
            f"PriceTracker: {written} marketplace locality snapshots written for {today}")

        # ── RERA actual transaction prices ────────────────────────────────────
        # Derives avg actual price per unit from amount_collected / units_sold.
        # Stored with source="rera" so dashboard and detectors can distinguish
        # marketplace asking price from actual transacted price (RERA).
        #
        # IMPORTANT: avg_price_sqft and median_price_sqft are intentionally
        # left NULL for RERA rows. amount_collected / units_sold gives a
        # per-unit total price (e.g. ₹50L per flat), NOT a per-sqft price —
        # MahaRERA does not expose per-unit area in the API. Writing this value
        # into avg_price_sqft would corrupt trend detection and spike detection
        # (both read that column expecting ₹/sqft). The per-unit price is
        # preserved in avg_txn_sqft and txn_total_value for reference.
        rera_written = 0
        rera_skipped = 0
        try:
            rera_projects = select_rows(
                "rera_projects", filters={"city_id": city_id}, limit=1000
            )
        except Exception as e:
            logger.warning(f"PriceTracker: could not load RERA projects: {e}")
            rera_projects = []

        for p in rera_projects:
            collected = float(p.get("amount_collected") or 0)
            sold = int(p.get("units_sold") or 0)
            if collected <= 0 or sold <= 0:
                continue

            avg_actual = round(collected / sold, 2)

            # Clean locality from address_raw — reject gut/plot/survey strings
            # and raw MahaRERA address blobs; fall back to district then city.
            raw_address = (p.get("address_raw") or "").strip()
            locality = self._clean_locality(raw_address, city)
            if not locality:
                district = (p.get("district") or "").strip()
                locality = self._clean_locality(district, city)
            if not locality:
                # Skip entirely — writing with city name as locality would
                # create a meaningless catch-all row that poisons aggregates.
                rera_skipped += 1
                logger.debug(
                    f"RERA snapshot skipped (no clean locality): "
                    f"{p.get('project_name', 'unknown')} — address_raw={raw_address!r}"
                )
                continue

            property_type = (p.get("project_type") or "residential").lower()

            rera_row = {
                "period_date":       today,
                "snapshot_date":     today,
                "city":              city,
                "city_id":           city_id,
                "locality":          locality,
                "property_type":     property_type,
                "listing_type":      "sale",
                "source":            "rera",
                "txn_count":         sold,
                "txn_total_value":   round(collected, 2),
                # avg_txn_sqft stores per-unit price for reference only.
                # Do NOT use for psqft comparisons — it is not ₹/sqft.
                "avg_txn_sqft":      avg_actual,
                "total_listings":    sold,
                "listing_count":     sold,
                # avg_price_sqft and median_price_sqft intentionally omitted
                # (left NULL) — per-unit price cannot be converted to ₹/sqft
                # without unit area, which MahaRERA API does not provide.
                # TrendDetector and PatternDetector both skip source='rera'
                # rows when computing psqft-based signals.
            }

            try:
                upsert_row("price_history", rera_row)
                rera_written += 1
                logger.debug(
                    f"RERA snapshot: {locality}/{property_type} "
                    f"→ ₹{avg_actual:,.0f}/unit ({sold} sold)"
                )
            except Exception as e:
                logger.warning(
                    f"Could not write RERA snapshot for {locality}: {e}"
                )

        if rera_written or rera_skipped:
            logger.info(
                f"PriceTracker: {rera_written} RERA snapshots written, "
                f"{rera_skipped} skipped (no clean locality) for {today}"
            )

        total_written = written + rera_written
        logger.info(
            f"PriceTracker: {total_written} total snapshots written for {today} "
            f"({written} marketplace + {rera_written} RERA)"
        )
        return total_written
