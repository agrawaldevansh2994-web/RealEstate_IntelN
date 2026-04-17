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

import logging
from typing import Any

from db.connection import select_rows, update_rows

logger = logging.getLogger(__name__)


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
    "locality_price_spike":         45,   # locality data can be sparse
    "price_trend_spike":            50,   # needs enough history to be reliable
    "listing_price_outlier":        35,   # 99acres data quality varies
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
        evidence = flag.get("evidence") or {}
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
            total = int(evidence.get("total_complaints") or
                        (project.get("complaint_count") if project else 0) or 0)
            if total >= 5:
                boosts += 15
                notes.append(f"complaints={total} +15")
            elif total >= 3:
                boosts += 8
                notes.append(f"complaints={total} +8")
            elif total <= 1:
                penals += 10
                notes.append("only 1 complaint -10")

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
            ratio = float(evidence.get("ratio") or 1)
            deviation = abs(ratio - 1)
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
                n = int(raw_count)
                if n < 5:
                    penals += 8
                    notes.append(f"few comparables ({n}) -8")
                elif n >= 10:
                    boosts += 5
                    notes.append(f"strong comparables ({n}) +5")
            if listing:
                if (listing.get("listing_status") or "").lower() != "active":
                    penals += 12
                    notes.append("listing not active -12")
                if (listing.get("source") or "").lower() != "99acres":
                    penals += 8
                    notes.append("unexpected listing source -8")

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
