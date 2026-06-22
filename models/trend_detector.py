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
from models.listing_sources import PRICE_HISTORY_MARKETPLACE_SOURCES

logger = logging.getLogger(__name__)


class TrendDetector:

    WINDOWS = [
        {"days": 7,  "medium": 0.08, "high": 0.15, "critical": 0.25, "min_snapshots": 3},
        {"days": 14, "medium": 0.12, "high": 0.20, "critical": 0.35, "min_snapshots": 5},
        {"days": 30, "medium": 0.20, "high": 0.35, "critical": 0.50, "min_snapshots": 14},
    ]

    MIN_SNAPSHOTS = 3

    # Dampening: suppress re-flag if same locality was flagged within this
    # many days, UNLESS the new spike magnitude exceeds the previous one by
    # at least MAGNITUDE_ESCALATION_PCT percentage points (absolute).
    DAMPEN_DAYS = 7
    MAGNITUDE_ESCALATION_PCT = 5.0

    def __init__(self, city_id: int = 1, city: str = "Akola"):
        self.city_id = city_id
        self.city = city
        # Maps (locality, property_type, window_days) →
        #   {"flagged_at": "YYYY-MM-DD", "change_pct": float (abs, pct units)}
        self._open_trend_flag_keys: dict[tuple[str, str, int], dict] = {}
        self._load_existing_trend_flags()

    def run_all(self) -> int:
        logger.info(
            f"TrendDetector: starting analysis for city_id={self.city_id} ({self.city})")

        # Explicitly filter marketplace sources — RERA rows store per-unit total
        # price in avg_price_sqft (not ₹/sqft) and must not enter spike
        # detection. PatternDetector already applies this filter; mirroring it
        # here keeps both detectors consistent.
        all_history = select_rows("price_history", filters={
                                  "city_id": self.city_id}, limit=5000)

        history = [
            row for row in (all_history or [])
            if (row.get("source") or "").lower() in PRICE_HISTORY_MARKETPLACE_SOURCES
        ]

        if not history:
            logger.info(
                "TrendDetector: no marketplace price_history data yet — "
                "run --snapshot for a few days first")
            return 0

        logger.debug(
            f"TrendDetector: {len(all_history)} total rows loaded, "
            f"{len(history)} after filtering to marketplace sources"
        )

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

            if len(snapshots) < window["min_snapshots"]:
                logger.debug(
                    f"Skipping {days}d window for {locality}/{property_type} "
                    f"— only {len(snapshots)} snapshots, need {window['min_snapshots']}"
                )
                continue

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

            trend_flag_key = self._trend_flag_key(locality, property_type, days, self.city)
            existing = self._open_trend_flag_keys.get(trend_flag_key)
            if existing:
                days_since = (date.today() - date.fromisoformat(existing["flagged_at"])).days
                prev_abs_pct = existing.get("change_pct", 0.0)
                new_abs_pct  = abs_change * 100
                if days_since < self.DAMPEN_DAYS and new_abs_pct < prev_abs_pct + self.MAGNITUDE_ESCALATION_PCT:
                    logger.debug(
                        f"Dampened: {locality}/{property_type} {days}d "
                        f"(flagged {days_since}d ago, {new_abs_pct:.1f}% vs prev {prev_abs_pct:.1f}%)"
                    )
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
                self._open_trend_flag_keys[trend_flag_key] = {
                    "flagged_at": date.today().isoformat(),
                    "change_pct": round(abs_change * 100, 2),
                    "severity":   severity,
                }
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
                    flagged_at = str(row.get("created_at") or "")[:10] or date.today().isoformat()
                    change_pct = abs(float(evidence.get("change_pct") or 0))
                    key = self._trend_flag_key(locality, property_type, int(window_days), self.city)
                    # Keep the most recent entry if the same key appears multiple times
                    existing = self._open_trend_flag_keys.get(key)
                    if not existing or flagged_at > existing["flagged_at"]:
                        self._open_trend_flag_keys[key] = {
                            "flagged_at": flagged_at,
                            "change_pct": change_pct,
                            "severity":   str(row.get("severity") or "medium"),
                        }
            except (TypeError, ValueError):
                continue

    @staticmethod
    def _trend_flag_key(locality: str, property_type: str, window_days: int, city: str = "") -> tuple[str, str, int]:
        loc = str(locality or "").strip().lower()
        if city:
            # Strip ", CityName" suffix so "Geeta Nagar, Akola" and
            # "Geeta Nagar" resolve to the same key.
            suffix = f", {city.strip().lower()}"
            if loc.endswith(suffix):
                loc = loc[: -len(suffix)].strip()
        normalized_locality = " ".join(loc.split())
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
