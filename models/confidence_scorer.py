"""
models/confidence_scorer.py
Scores every flag in suspicious_flags with a confidence value (0–100).

Score = base score for flag type
      + boosters from corroborating evidence
      - penalties for missing data or weak signals

Run after --detect and --patterns:
    python main.py --score --city Akola

Add to main.py:
    parser.add_argument('--score', action='store_true',
                        help='Score confidence on all unscored flags')

    if args.score:
        from models.confidence_scorer import ConfidenceScorer
        city_id = _resolve_city_id(args.city)
        ConfidenceScorer(city_id=city_id).run()

Add --score to run_scrapers.bat after --patterns:
    python main.py --detect  --city Akola
    python main.py --patterns --city Akola
    python main.py --score    --city Akola
    python main.py --trends   --city Akola
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from db.connection import select_rows, update_rows
from models.listing_sources import is_marketplace_source

logger = logging.getLogger(__name__)

# Strips trailing city name from locality strings e.g. "Baner, Pune" → "Baner".
# Updated whenever a new city is added to the platform.
_CITY_SUFFIX_RE = re.compile(
    r",?\s*(akola|amravati|nagpur|pune|nashik|aurangabad)\s*$",
    re.IGNORECASE,
)


# ── Base scores by flag type ──────────────────────────────────────────────────
# These are starting points before boosters/penalties are applied.
BASE_SCORES: dict[str, int] = {
    "cross_source_promoter_risk":   80,   # two independent sources agree = strong
    "rera_escrow_deficit":          75,   # financial data from RERA = verifiable
    "complaint_velocity":           70,   # ratio-based = harder to game
    "repeated_complaints":          65,   # direct RERA data
    "repeat_offender_new_project":  60,   # pattern across time
    "stalled_projects":             55,   # status data reliable, but common
    "promoter_name_cluster":        50,   # fuzzy match = some false positives
    "locality_price_spike":         52,   # locality data can be sparse; min entry threshold is 40% spike
    "price_trend_spike":            50,   # needs enough history to be reliable
    "dbscan_listing_anomaly":        45,   # unsupervised signal; useful but experimental
    "listing_price_outlier":        35,   # marketplace data quality varies
    "ghost_promoter":               30,   # absence of evidence ≠ evidence
    "name_reuse_serial":            50,
    "locality_convergence":         55,
}

DEFAULT_BASE = 40


class ConfidenceScorer:

    def __init__(self, city_id: int = 1):
        self.city_id = city_id
        self._projects: dict[str, dict] = {}   # id → project row
        self._listings: dict[str, dict] = {}   # id → listing row
        self._flags_by_project: dict[str, list] = {}  # project_id → [flags]

    def run(self) -> int:
        """Score all open flags for this city."""
        logger.info(
            f"ConfidenceScorer: loading data for city_id={self.city_id}")
        self._load_context()

        all_flags = select_rows(
            "suspicious_flags",
            filters={"city_id": self.city_id},
            limit=5000,
        )
        flags = [flag for flag in all_flags if self._is_open_flag(flag)]
        if not flags:
            logger.info("No flags found.")
            return 0

        updated = 0
        for flag in flags:
            score, note = self._score_flag(flag)
            current = flag.get("confidence") or 0

            # Only update if score changed meaningfully
            if abs(score - current) >= 2:
                try:
                    update_rows(
                        "suspicious_flags",
                        {"id": str(flag["id"])},
                        {"confidence": score, "confidence_note": note},
                    )
                    updated += 1
                except Exception as e:
                    logger.warning(f"Could not update flag {flag['id']}: {e}")

        logger.info(f"ConfidenceScorer: scored {updated} flags")
        return updated

    def _load_context(self):
        """Pre-load projects and listings for fast lookups."""
        projects = select_rows(
            "rera_projects",
            filters={"city_id": self.city_id},
            limit=1000,
        )
        for p in projects:
            self._projects[str(p["id"])] = p

        listings = select_rows(
            "listings",
            filters={"city_id": self.city_id},
            limit=2000,
        )
        for l in listings:
            self._listings[str(l["id"])] = l

        # Group all flags by project for corroboration check
        all_flags = select_rows(
            "suspicious_flags",
            filters={"city_id": self.city_id},
            limit=5000,
        )
        for f in all_flags:
            if not self._is_open_flag(f):
                continue
            pid = f.get("rera_project_id")
            if pid:
                self._flags_by_project.setdefault(str(pid), []).append(f)

    def _score_flag(self, flag: dict) -> tuple[int, str]:
        """
        Returns (score, note) for a single flag.
        score is clamped to [5, 98] — we never say 0% or 100% confident.
        """
        flag_type = flag.get("flag_type") or "unknown"
        project_id = flag.get("rera_project_id")
        listing_id = flag.get("listing_id")
        evidence = self._coerce_evidence(flag.get("evidence"))
        project = self._projects.get(str(project_id)) if project_id else None
        listing = self._listings.get(str(listing_id)) if listing_id else None

        score = BASE_SCORES.get(flag_type, DEFAULT_BASE)
        notes = [f"base={score}"]
        boosts = 0
        penals = 0
        other_flags: list[dict[str, Any]] = []

        # ── Universal boosters ────────────────────────────────────────────────

        # Corroboration: project has OTHER flags too
        if project_id:
            other_flags = [
                f for f in self._flags_by_project.get(str(project_id), [])
                if str(f.get("id")) != str(flag.get("id"))
            ]
            if len(other_flags) >= 3:
                boosts += 12
                notes.append("3+ corroborating flags +12")
            elif len(other_flags) >= 1:
                boosts += 6
                notes.append(f"{len(other_flags)} corroborating flag(s) +6")

        # Project data quality — more filled fields = more reliable signal
        if project:
            filled = sum(1 for f in [
                project.get("promoter_pan"),
                project.get("complaint_count"),
                project.get("rera_status"),
                project.get("proposed_completion"),
                project.get("total_units"),
                project.get("address_raw"),
            ] if f)
            if filled >= 5:
                boosts += 8
                notes.append("rich project data +8")
            elif filled >= 3:
                boosts += 4
                notes.append("partial project data +4")
            else:
                penals += 10
                notes.append("sparse project data -10")

        if listing:
            filled = sum(1 for f in [
                listing.get("listed_price"),
                listing.get("price_per_sqft"),
                listing.get("property_type"),
                listing.get("listing_type"),
                listing.get("locality") or listing.get("address_raw"),
                listing.get("source_listing_id"),
            ] if f not in (None, "", []))
            if filled >= 5:
                boosts += 6
                notes.append("rich listing data +6")
            elif filled >= 3:
                boosts += 3
                notes.append("partial listing data +3")
            else:
                penals += 8
                notes.append("sparse listing data -8")

        # ── Flag-type specific scoring ────────────────────────────────────────

        if flag_type == "repeated_complaints":
            total = self._to_int(evidence.get("total_complaints") or
                                 (project.get("complaint_count") if project else 0) or 0)
            project_count = self._to_int(
                evidence.get("affected_projects") or evidence.get("project_count") or 1
            )
            if total >= 5:
                boosts += 15
                notes.append(f"complaints={total} +15")
            elif total >= 3:
                boosts += 8
                notes.append(f"complaints={total} +8")
            elif total <= 1:
                penals += 10
                notes.append("only 1 complaint -10")

            if project_count >= 4:
                boosts += 14
                notes.append(f"spread across {project_count} projects +14")
            elif project_count >= 2:
                boosts += 8
                notes.append(f"spread across {project_count} projects +8")
            elif total >= 3:
                penals += 6
                notes.append("complaints concentrated in one project -6")

            # PAN verified = promoter identity confirmed
            if project and project.get("promoter_pan"):
                boosts += 8
                notes.append("PAN verified +8")

        elif flag_type == "rera_escrow_deficit":
            ratio = float(evidence.get("escrow_ratio") or 0)
            if ratio < 0.20:
                boosts += 20
                notes.append(f"severe deficit {ratio:.0%} +20")
            elif ratio < 0.50:
                boosts += 10
                notes.append(f"escrow={ratio:.0%} +10")
            # Missing financial data = can't verify
            if not evidence.get("amount_collected"):
                penals += 15
                notes.append("no financial data -15")

        elif flag_type == "complaint_velocity":
            affected = int(evidence.get("affected_projects")
                           or evidence.get("total_projects") or 1)
            total_complaints = int(evidence.get("total_complaints") or 0)
            # ratio = avg complaints per affected project
            ratio = total_complaints / affected if affected > 0 else 0
            if ratio >= 3.0:
                boosts += 15
                notes.append(f"velocity={ratio:.1f} +15")
            elif ratio >= 2.0:
                boosts += 8
                notes.append(f"velocity={ratio:.1f} +8")
            if affected >= 3:
                boosts += 8
                notes.append(f"{affected} projects +8")

        elif flag_type == "stalled_projects":
            stalled_count = int(evidence.get("stalled_project_count") or 1)
            if stalled_count >= 4:
                boosts += 15
                notes.append(f"{stalled_count} stalled +15")
            elif stalled_count >= 2:
                boosts += 8
                notes.append(f"{stalled_count} stalled +8")
            elif stalled_count == 1:
                penals += 10
                notes.append("only 1 stalled -10")
            # Complaints on stalled project = stronger
            if project and (project.get("complaint_count") or 0) > 0:
                boosts += 8
                notes.append("has complaints +8")

        elif flag_type == "cross_source_promoter_risk":
            listing_count = int(evidence.get("listing_count") or 0)
            if listing_count >= 3:
                boosts += 12
                notes.append(f"{listing_count} listings +12")
            elif listing_count >= 1:
                boosts += 6
                notes.append(f"{listing_count} listings +6")

        elif flag_type == "repeat_offender_new_project":
            if project and (project.get("complaint_count") or 0) >= 2:
                boosts += 10
                notes.append("complaints on new project +10")

        elif flag_type == "listing_price_outlier":
            ratio = self._to_float(evidence.get("ratio"), 1.0)
            deviation = abs(ratio - 1)
            anchor_psqft = self._to_float(evidence.get("price_per_sqft"))
            is_below = ratio < 1.0  # anchor listing is priced BELOW locality median

            # ── Direction check ───────────────────────────────────────────
            # Below-median outliers with very low psqft (<500) are almost
            # certainly scraper data errors (price in lakhs not converted,
            # area parsed in wrong unit, etc.) — not fraud signals.
            # Above-median outliers are the real manipulation signal.
            if is_below and anchor_psqft > 0 and anchor_psqft < 500:
                penals += 30
                notes.append(f"below-median + anchor_psqft=₹{anchor_psqft:.0f} (likely data error) -30")
            elif is_below:
                # Below-median but psqft is plausible — weaker fraud signal
                penals += 12
                notes.append(f"below-median outlier (less suspicious) -12")

            # ── Deviation magnitude ───────────────────────────────────────
            if deviation >= 1.0:
                boosts += 15
                notes.append(f"deviation={deviation:.0%} +15")
            elif deviation >= 0.75:
                boosts += 8
                notes.append(f"deviation={deviation:.0%} +8")
            elif deviation < 0.55:
                penals += 10
                notes.append("weak deviation -10")
            # comparable_count or legacy group_size — only penalise when field is present
            raw_count = evidence.get(
                "comparable_count") or evidence.get("group_size")
            if raw_count is not None:
                n = self._to_int(raw_count)
                if n < 6:
                    penals += 18
                    notes.append(f"too few comparables ({n}) -18")
                elif n < 8:
                    penals += 8
                    notes.append(f"thin comparable group ({n}) -8")
                elif n >= 10:
                    boosts += 5
                    notes.append(f"strong comparables ({n}) +5")
                elif n >= 8:
                    boosts += 2
                    notes.append(f"usable comparables ({n}) +2")

            property_type = self._norm_text(
                evidence.get("property_type") or (listing or {}).get("property_type")
            )
            locality = self._norm_text(
                evidence.get("locality")
                or evidence.get("normalized_locality")
                or (listing or {}).get("locality")
                or (listing or {}).get("address_raw")
            )
            if self._is_plot_property(property_type):
                penals += 18
                notes.append("plot/land pricing is noisier -18")
            if self._is_rural_locality(locality):
                penals += 12
                notes.append("rural locality comparables -12")

            if listing:
                if (listing.get("listing_status") or "").lower() != "active":
                    penals += 12
                    notes.append("listing not active -12")
                if not is_marketplace_source(listing.get("source")):
                    penals += 8
                    notes.append("unexpected listing source -8")
                questionable_reasons = self._questionable_recent_listing(listing)
                if questionable_reasons:
                    penals += 18
                    notes.append(
                        "recent questionable scrape (" +
                        ", ".join(questionable_reasons[:3]) +
                        ") -18"
                    )

        elif flag_type == "dbscan_listing_anomaly":
            distance = self._to_float(
                evidence.get("distance_from_scaled_origin"))
            if distance >= 4.0:
                boosts += 14
                notes.append(f"strong DBSCAN distance={distance:.1f} +14")
            elif distance >= 3.0:
                boosts += 8
                notes.append(f"DBSCAN distance={distance:.1f} +8")
            elif distance and distance < 2.0:
                penals += 8
                notes.append(f"weak DBSCAN distance={distance:.1f} -8")

            group_size = self._to_int(evidence.get("group_size"))
            if group_size >= 80:
                boosts += 8
                notes.append(f"large cluster group n={group_size} +8")
            elif group_size >= 40:
                boosts += 4
                notes.append(f"usable cluster group n={group_size} +4")
            elif group_size and group_size < 25:
                penals += 10
                notes.append(f"thin cluster group n={group_size} -10")

            ratio = self._to_float(
                evidence.get("ratio_to_locality_median")
                or evidence.get("ratio_to_group_median"),
                1.0,
            )
            deviation = abs(ratio - 1)
            if deviation >= 1.0:
                boosts += 8
                notes.append(f"price ratio deviation={deviation:.0%} +8")
            elif deviation < 0.35:
                penals += 8
                notes.append("small price ratio deviation -8")

            property_type = self._norm_text(
                evidence.get("property_type") or (listing or {}).get("property_type")
            )
            locality = self._norm_text(
                evidence.get("locality")
                or (listing or {}).get("locality")
                or (listing or {}).get("address_raw")
            )
            if self._is_plot_property(property_type):
                penals += 12
                notes.append("plot/land DBSCAN noise -12")
            if self._is_rural_locality(locality):
                penals += 8
                notes.append("rural locality DBSCAN noise -8")

        elif flag_type == "locality_price_spike":
            # spike_ratio is stored as a fraction (e.g. 0.65 = 65% above city median).
            # Flags are only created when spike_ratio >= 0.40 (PatternDetector threshold),
            # so every flagged spike is already a material signal. Score accordingly.
            ratio = self._spike_ratio(evidence)
            n = self._to_int(
                evidence.get("listing_count")
                or evidence.get("total_listings")
                or evidence.get("sample_size")
            )
            spread = self._locality_spread(evidence)

            # ── Spike magnitude ───────────────────────────────────────────
            if ratio >= 2.0:
                boosts += 22
                notes.append(f"extreme spike={ratio:.0%} +22")
            elif ratio >= 1.0:
                boosts += 16
                notes.append(f"large spike={ratio:.0%} +16")
            elif ratio >= 0.65:
                boosts += 10
                notes.append(f"strong spike={ratio:.0%} +10")
            elif ratio >= 0.40:
                # minimum threshold — still a real signal, modest boost
                boosts += 5
                notes.append(f"moderate spike={ratio:.0%} +5")
            else:
                # below entry threshold — shouldn't happen but guard it
                penals += 10
                notes.append(f"weak spike={ratio:.0%} -10")

            # ── Sample size ───────────────────────────────────────────────
            if n >= 12:
                boosts += 14
                notes.append(f"strong sample n={n} +14")
            elif n >= 8:
                boosts += 9
                notes.append(f"good sample n={n} +9")
            elif n >= 5:
                boosts += 4
                notes.append(f"adequate sample n={n} +4")
            elif n >= 3:
                # 3–4 listings: reduce penalty since 40% spike on even 3 listings is notable
                penals += 8
                notes.append(f"thin sample n={n} -8")
            else:
                penals += 18
                notes.append(f"sparse n={n} -18")

            # ── Price spread within locality ──────────────────────────────
            if spread is not None:
                if spread >= 4.0:
                    penals += 14
                    notes.append(f"very wide spread {spread:.1f}x -14")
                elif spread >= 2.5:
                    penals += 7
                    notes.append(f"wide spread {spread:.1f}x -7")
                elif spread <= 1.6 and n >= 5:
                    boosts += 8
                    notes.append(f"tight spread {spread:.1f}x +8")

            # ── Single-listing driver check ───────────────────────────────
            driver_note = self._one_listing_driver_note(evidence)
            if driver_note:
                penals += 18
                notes.append(f"{driver_note} -18")

        elif flag_type == "price_trend_spike":
            window = int(evidence.get("window_days") or 30)
            change = abs(float(evidence.get("change_pct") or 0))
            if change >= 30 and window <= 14:
                boosts += 18
                notes.append(f"{change:.0f}% in {window}d +18")
            elif change >= 20:
                boosts += 10
                notes.append(f"{change:.0f}% change +10")
            elif change < 10:
                penals += 10
                notes.append("weak change -10")
            # Short windows with big moves = stronger signal
            if window == 7 and change >= 15:
                boosts += 8
                notes.append("rapid 7d spike +8")

        elif flag_type == "promoter_name_cluster":
            # Fuzzy match — penalise unless confirmed by other signals
            # cluster flags have no project_id so other_flags is not defined here
            _other_flags = other_flags if project_id else []
            if not _other_flags:
                penals += 12
                notes.append("no corroboration -12")
            # boost if cluster has many registrations (more entities = stronger signal)
            reg_count = int((evidence.get("registration_count")
                            if isinstance(evidence, dict) else 0) or 0)
            if reg_count >= 4:
                boosts += 10
                notes.append(f"{reg_count} registrations +10")
            elif reg_count >= 2:
                boosts += 5
                notes.append(f"{reg_count} registrations +5")

        elif flag_type == "ghost_promoter":
            # Absence of listings is weak — penalise heavily unless complaints exist
            if project and (project.get("complaint_count") or 0) == 0:
                penals += 15
                notes.append("no complaints, just absent -15")

        # ── Universal penalties ───────────────────────────────────────────────

        # Old flag on already-lapsed project with no new activity
        rera_status = (project.get("rera_status") or "") if project else ""
        if "completed" in rera_status:
            penals += 8
            notes.append("project completed -8")

        # ── Final score ───────────────────────────────────────────────────────
        final = score + boosts - penals
        # never 0 or 100 — always some uncertainty
        final = max(5, min(98, final))

        # Map to severity bucket for note
        if final >= 80:
            tier = "HIGH CONFIDENCE"
        elif final >= 60:
            tier = "MEDIUM CONFIDENCE"
        elif final >= 40:
            tier = "LOW CONFIDENCE"
        else:
            tier = "VERY LOW CONFIDENCE"

        note = f"{tier} ({final}) — " + " | ".join(notes)
        return final, note

    @staticmethod
    def _is_open_flag(flag: dict[str, Any]) -> bool:
        status = str(flag.get("status") or "").strip().lower()
        return status in ("", "open")

    @staticmethod
    def _coerce_evidence(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_int(value: Any, default: int = 0) -> int:
        try:
            if value in (None, ""):
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @classmethod
    def _normalize_locality(cls, value: str) -> str:
        """Lowercase + collapse whitespace + strip city suffix."""
        text = " ".join(str(value or "").strip().lower().split())
        text = _CITY_SUFFIX_RE.sub("", text).strip(" ,")
        return " ".join(text.split())

    @staticmethod
    def _norm_text(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @classmethod
    def _is_plot_property(cls, property_type: Any) -> bool:
        value = cls._norm_text(property_type)
        return any(token in value for token in ("plot", "land", "farm", "agri"))

    @classmethod
    def _is_rural_locality(cls, locality: Any) -> bool:
        value = cls._norm_text(locality)
        rural_tokens = (
            "village", "gaon", "gram", "mouza", "shivar", "taluka",
            "tehsil", "rural", "farm", "agri", "agricultural", "bk",
            "budruk", "khurd",
        )
        return any(f" {token} " in f" {value} " for token in rural_tokens)

    @classmethod
    def _is_recent_timestamp(cls, value: Any, days: int = 3) -> bool:
        if not value:
            return False
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).days
        return 0 <= age_days <= days

    def _questionable_recent_listing(self, listing: dict[str, Any]) -> list[str]:
        recent = any(
            self._is_recent_timestamp(listing.get(field))
            for field in ("last_seen_at", "first_seen_at", "created_at", "updated_at")
        )
        if not recent:
            return []

        reasons: list[str] = []
        ppsf = self._to_float(listing.get("price_per_sqft"))
        area = self._to_float(listing.get("area_sqft"))
        if 0 < ppsf < 500:
            reasons.append("low ppsf")
        if area > 20_000:
            reasons.append("large area")
        if not listing.get("area_sqft"):
            reasons.append("missing area")
        if self._is_plot_property(listing.get("property_type")):
            reasons.append("plot")
        if self._is_rural_locality(listing.get("locality") or listing.get("address_raw")):
            reasons.append("rural locality")
        return reasons

    def _locality_spread(self, evidence: dict[str, Any]) -> float | None:
        min_price = self._to_float(evidence.get("min_price"))
        max_price = self._to_float(evidence.get("max_price"))
        if min_price > 0 and max_price > 0:
            return max_price / min_price

        prices = self._locality_prices_for_evidence(evidence)
        if not prices:
            return None
        return max(prices) / min(prices) if min(prices) > 0 else None

    def _spike_ratio(self, evidence: dict[str, Any]) -> float:
        explicit = self._to_float(evidence.get("spike_ratio"))
        if explicit:
            return explicit

        locality_median = self._to_float(evidence.get("locality_median"))
        city_median = self._to_float(evidence.get("city_median"))
        if locality_median > 0 and city_median > 0:
            return (locality_median - city_median) / city_median

        return 0.0

    def _one_listing_driver_note(self, evidence: dict[str, Any]) -> str:
        prices = self._locality_prices_for_evidence(evidence)
        if len(prices) < 3:
            return ""

        city_median = self._to_float(evidence.get("city_median"))
        original_median = self._to_float(evidence.get("locality_median"))
        if city_median <= 0:
            return ""
        if original_median <= 0:
            original_median = self._median(prices)

        without_high = prices[:-1]
        trimmed_median = self._median(without_high)
        original_ratio = (original_median - city_median) / city_median
        trimmed_ratio = (trimmed_median - city_median) / city_median

        if original_ratio >= 0.40 and trimmed_ratio < 0.40:
            return "one high listing drives spike"
        if original_median > 0 and (original_median - trimmed_median) / original_median >= 0.30:
            return "median sensitive to one high listing"
        return ""

    def _locality_prices_for_evidence(self, evidence: dict[str, Any]) -> list[float]:
        locality = self._normalize_locality(evidence.get("locality") or "")
        if not locality:
            return []

        prices: list[float] = []
        for listing in self._listings.values():
            if self._normalize_locality(
                listing.get("locality") or listing.get("address_raw") or ""
            ) != locality:
                continue
            if not is_marketplace_source(listing.get("source")):
                continue
            if self._norm_text(listing.get("listing_status")) not in ("", "active"):
                continue
            ppsf = self._to_float(listing.get("price_per_sqft"))
            if ppsf > 0:
                prices.append(ppsf)
        return sorted(prices)

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2
