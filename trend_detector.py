"""
models/trend_detector.py
Analyses price_history table for locality-level price spikes.

Compares recent prices against rolling windows (7d, 14d, 30d).
Writes confirmed spikes to price_spikes table AND suspicious_flags.

Run after price_tracker.py:
    python main.py --trends --city Akola
"""

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from db.connection import insert_row, select_rows, upsert_row

logger = logging.getLogger(__name__)


class TrendDetector:

    WINDOWS = [
        {"days": 7,  "medium": 0.08, "high": 0.15, "critical": 0.25},
        {"days": 14, "medium": 0.12, "high": 0.20, "critical": 0.35},
        {"days": 30, "medium": 0.20, "high": 0.35, "critical": 0.50},
    ]

    MIN_SNAPSHOTS = 3

    def __init__(self, city_id: int = 1, city: str = "Akola"):
        self.city_id = city_id
        self.city = city
        self._open_trend_flag_keys: set[tuple[str, str, int]] = set()
        self._load_existing_trend_flags()

    def run_all(self) -> int:
        logger.info(
            f"TrendDetector: starting analysis for city_id={self.city_id} ({self.city})")

        history = select_rows("price_history", filters={
                              "city_id": self.city_id}, limit=5000)

        if not history:
            logger.info(
                "TrendDetector: no price_history data yet — run --snapshot for a few days first")
            return 0

        # Group by (locality, property_type)
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for row in history:
            key = (
                row.get("locality", ""),
                row.get("property_type", ""),
            )
            groups[key].append(row)

        # Sort each group by period_date ascending
        for key in groups:
            groups[key].sort(key=lambda r: r.get("period_date")
                             or r.get("snapshot_date") or "")

        total_spikes = 0
        for (locality, property_type), snapshots in groups.items():
            if len(snapshots) < self.MIN_SNAPSHOTS:
                continue
            spikes = self._analyse_group(locality, property_type, snapshots)
            total_spikes += spikes

        logger.info(
            f"TrendDetector: {total_spikes} spike(s) detected for {self.city}")
        return total_spikes

    def _analyse_group(self, locality: str, property_type: str, snapshots: list[dict]) -> int:
        latest = snapshots[-1]
        latest_price = float(latest.get("median_price_sqft")
                             or latest.get("avg_price_sqft") or 0)
        latest_date = latest.get("period_date") or latest.get(
            "snapshot_date") or ""

        if latest_price <= 0 or not latest_date:
            return 0

        count = 0
        for window in self.WINDOWS:
            days = window["days"]

            target_date = _date_minus_days(str(latest_date), days)
            baseline = _find_nearest_snapshot(snapshots[:-1], target_date)

            if not baseline:
                continue

            baseline_price = float(
                baseline.get("median_price_sqft") or baseline.get(
                    "avg_price_sqft") or 0
            )
            if baseline_price <= 0:
                continue

            change_pct = (latest_price - baseline_price) / baseline_price
            abs_change = abs(change_pct)
            direction = "increase" if change_pct > 0 else "decrease"

            if abs_change < window["medium"]:
                continue

            severity = (
                "critical" if abs_change >= window["critical"] else
                "high" if abs_change >= window["high"] else
                "medium"
            )

            # Deduplicate — skip if already logged today
            existing = select_rows("price_spikes", filters={
                                   "city": self.city}, limit=2000)
            today = date.today().isoformat()
            already = any(
                s.get("locality") == locality
                and s.get("property_type") == property_type
                and s.get("window_days") == days
                and str(s.get("detected_date", ""))[:10] == today
                for s in existing
            )
            if already:
                continue

            # Write to price_spikes
            spike_row = {
                "detected_date": today,
                "city":          self.city,
                "locality":      locality,
                "property_type": property_type,
                "window_days":   days,
                "price_start":   round(baseline_price, 2),
                "price_end":     round(latest_price, 2),
                "change_pct":    round(change_pct * 100, 2),
                "severity":      severity,
                "status":        "open",
            }
            try:
                upsert_row("price_spikes", spike_row)
            except Exception as e:
                logger.warning(f"Could not write price spike: {e}")
                continue

            trend_flag_key = self._trend_flag_key(locality, property_type, days)
            if trend_flag_key in self._open_trend_flag_keys:
                continue

            # Write to suspicious_flags so Make alert picks it up
            flag_title = (
                f"Price {direction} {abs_change:.0%} in {days}d — "
                f"{locality}, {self.city} ({property_type})"
            )
            flag_desc = (
                f"Median price/sqft in '{locality}' ({property_type}) moved from "
                f"₹{baseline_price:,.0f} to ₹{latest_price:,.0f} "
                f"— a {change_pct:+.1%} {direction} over {days} days. "
                f"Based on {latest.get('total_listings') or latest.get('listing_count') or '?'} listings. "
                + (
                    "Possible insider activity, upcoming infra announcement, or manipulation."
                    if change_pct > 0 else
                    "Sharp drop may indicate project issues or market correction."
                )
            )
            try:
                insert_row("suspicious_flags", {
                    "flag_type":   "price_trend_spike",
                    "severity":    severity,
                    "title":       flag_title,
                    "description": flag_desc,
                    "status":      "open",
                    "city_id":     self.city_id,
                    "evidence": {
                        "city":          self.city,
                        "locality":      locality,
                        "property_type": property_type,
                        "window_days":   days,
                        "price_start":   baseline_price,
                        "price_end":     latest_price,
                        "change_pct":    round(change_pct * 100, 2),
                        "listing_count": latest.get("total_listings") or latest.get("listing_count"),
                        "baseline_date": str(baseline.get("period_date") or baseline.get("snapshot_date")),
                        "latest_date":   str(latest_date),
                    }
                })
                self._open_trend_flag_keys.add(trend_flag_key)
                logger.info(
                    f"Spike: {locality}/{property_type} {change_pct:+.1%} in {days}d → {severity}")
                count += 1
            except Exception as e:
                logger.warning(
                    f"Could not write spike to suspicious_flags: {e}")

        return count


# ── Helpers ──────────────────────────────────────────────────────────────────

    def _load_existing_trend_flags(self) -> None:
        try:
            rows = select_rows(
                "suspicious_flags",
                filters={"city_id": self.city_id, "flag_type": "price_trend_spike"},
                limit=5000,
            )
        except Exception as exc:
            logger.warning(f"Could not preload existing trend flags: {exc}")
            return

        for row in rows:
            status = str(row.get("status") or "").strip().lower()
            if status not in ("", "open"):
                continue

            evidence = row.get("evidence") or {}
            if isinstance(evidence, str):
                continue

            locality = evidence.get("locality")
            property_type = evidence.get("property_type")
            window_days = evidence.get("window_days")
            try:
                if locality and property_type and window_days is not None:
                    self._open_trend_flag_keys.add(
                        self._trend_flag_key(locality, property_type, int(window_days))
                    )
            except (TypeError, ValueError):
                continue

    @staticmethod
    def _trend_flag_key(locality: str, property_type: str, window_days: int) -> tuple[str, str, int]:
        normalized_locality = " ".join(str(locality or "").strip().lower().split())
        normalized_property_type = str(property_type or "").strip().lower()
        return (normalized_locality, normalized_property_type, int(window_days))


def _date_minus_days(date_str: str, days: int) -> str:
    try:
        d = date.fromisoformat(date_str[:10])
        return (d - timedelta(days=days)).isoformat()
    except Exception:
        return ""


def _find_nearest_snapshot(snapshots: list[dict], target_date: str) -> dict | None:
    if not target_date:
        return None
    candidates = [
        s for s in snapshots
        if str(s.get("period_date") or s.get("snapshot_date") or "")[:10] <= target_date
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda s: str(s.get("period_date") or s.get("snapshot_date") or ""))
