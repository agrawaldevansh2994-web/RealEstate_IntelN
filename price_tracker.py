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
from collections import defaultdict
from datetime import date

from db.connection import select_rows, upsert_row

logger = logging.getLogger(__name__)

# Akola city_id in the cities table
DEFAULT_CITY_ID = None   # auto-resolved below


class PriceTracker:

    def snapshot(self, city: str = "Akola") -> int:
        logger.info(f"PriceTracker: snapshotting listings for {city}")

        # Resolve city_id
        cities = select_rows("cities", limit=50)
        city_id = next(
            (c["id"]
             for c in cities if (c.get("name") or "").lower() == city.lower()),
            None
        )

        listings = select_rows("listings", filters={
                               "city_id": city_id}, limit=2000)
        if not listings:
            logger.warning("PriceTracker: no listings found")
            return 0

        # Group by (locality, property_type)
        groups: dict[tuple, list] = defaultdict(list)
        group_totals: dict[tuple, list] = defaultdict(list)

        for l in listings:
            ppsf = l.get("price_per_sqft")
            if not ppsf:
                continue
            try:
                ppsf = float(ppsf)
            except (ValueError, TypeError):
                continue

            locality = (l.get("locality") or "Unknown").strip()
            property_type = (l.get("property_type")
                             or "unknown").strip().lower()
            listing_type = (l.get("listing_type") or "sale").strip().lower()

            key = (locality, property_type, listing_type)
            groups[key].append(ppsf)

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
                "source":             "99acres",
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
            f"PriceTracker: {written} locality snapshots written for {today}")
        return written
