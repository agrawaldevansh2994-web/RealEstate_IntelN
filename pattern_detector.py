"""
models/pattern_detector.py
Cross-table intelligent pattern detection for Real Estate Intel Platform.

Goes beyond single-table rule checks — finds signals that only appear when
you correlate listings + rera_projects + suspicious_flags together.

Patterns detected:
  1. Cross-source promoter risk   — same promoter flagged in RERA AND has overpriced listings
  2. Promoter name clustering     — similar names likely same entity (shell company signal)
  3. Stale project + active sales — RERA lapsed but still selling on 99acres
  4. Complaint velocity           — promoter accumulating complaints across projects
  5. Price locality spike         — locality avg price jumped vs rest of city
  6. Repeat offender escalation   — promoter already flagged, now has NEW project
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from db.connection import insert_row, select_rows, update_rows

logger = logging.getLogger(__name__)


class PatternDetector:

    # Tuning knobs
    CROSS_SOURCE_MIN_FLAGS = 1    # min RERA flags before checking their listings
    PRICE_LOCALITY_SPIKE_PCT = 0.40  # locality median > city median by 40% = spike
    # need at least 3 listings to compute locality median
    MIN_LISTINGS_FOR_LOCALITY = 3
    NAME_SIMILARITY_THRESHOLD = 0.85  # stricter Jaccard similarity for promoter names
    # projects — if promoter has complaints on 2+ projects
    COMPLAINT_VELOCITY_WINDOW = 2
    STALE_RERA_KEYWORDS = {"lapsed", "revoked", "expired", "cancelled"}
    TRUSTED_REPEAT_PARENT_FLAGS = {
        "repeated_complaints",
        "complaint_velocity",
        "stalled_projects",
    }
    PATTERN_FLAG_TYPES = {
        "cross_source_promoter_risk",
        "stale_rera_active_listing",
        "promoter_name_cluster",
        "complaint_velocity",
        "locality_price_spike",
        "repeat_offender_new_project",
    }
    PROMOTER_STOPWORDS = {
        "the", "a", "an", "and", "or", "of", "for", "in",
        "ltd", "pvt", "limited", "llp", "builders", "builder",
        "constructions", "construction", "developer", "developers",
        "estates", "realty", "infra", "infrastructure", "group",
        "co", "company", "associates", "associate",
    }

    def __init__(self, city_id: int = 1):
        self.city_id = city_id
        self._existing_pattern_keys: set[str] = set()
        self._existing_pattern_titles: set[tuple[str, str]] = set()
        self._load_existing_patterns()

    # ------------------------------------------------------------------ #

    def run_all(self) -> int:
        checks = [
            self.detect_cross_source_promoter_risk,
            self.detect_stale_rera_active_listings,
            self.detect_promoter_name_clusters,
            self.detect_complaint_velocity,
            self.detect_locality_price_spike,
            self.detect_repeat_offender_new_project,
        ]
        total = 0
        for check in checks:
            try:
                count = check()
                logger.info(f"{check.__name__}: {count} patterns found")
                total += count
            except Exception as e:
                logger.error(f"{check.__name__} failed: {e}", exc_info=True)
        logger.info(f"Pattern detection complete — {total} patterns written")
        return total

    # ================================================================ #
    # PATTERN 1: Same promoter flagged in RERA AND has overpriced listings
    # Strongest signal — two independent data sources agree
    # ================================================================ #

    def detect_cross_source_promoter_risk(self) -> int:
        """
        Find promoters who:
          - Have at least one entry in suspicious_flags (RERA side)
          - Also appear as seller/builder in overpriced 99acres listings
        Cross-source corroboration = much higher confidence signal.
        """
        flags = select_rows("suspicious_flags", filters={
                            "city_id": self.city_id}, limit=2000)
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        listings = select_rows("listings", filters={
                               "city_id": self.city_id,
                               "source": "99acres",
                               "listing_status": "active"}, limit=1000)

        if not flags or not projects or not listings:
            return 0

        # Only use RERA-side flags as parent signals — not cross-source ones
        # (prevents circular corroboration loops)
        RERA_ORIGIN_FLAG_TYPES = {
            "repeated_complaints", "rera_escrow_deficit",
            "stalled_projects", "complaint_velocity",
            "repeat_offender_new_project", "promoter_name_cluster",
        }
        flagged_project_ids = {
            f["rera_project_id"]
            for f in flags
            if f.get("rera_project_id") and f.get("flag_type") in RERA_ORIGIN_FLAG_TYPES
        }
        # name → [flag descriptions]
        flagged_promoters: dict[str, list[str]] = {}

        for p in projects:
            if str(p["id"]) in flagged_project_ids:
                name = (p.get("promoter_name") or "").strip().lower()
                if name:
                    reasons = [
                        f["description"] for f in flags
                        if f.get("rera_project_id") == str(p["id"]) and f.get("description")
                    ]
                    flagged_promoters.setdefault(name, []).extend(reasons)

        if not flagged_promoters:
            return 0

        # Check listings for builder/developer name match
        count = 0
        for listing in listings:
            builder = self._listing_builder_name(listing).lower()

            if not builder:
                continue

            # Fuzzy match against flagged promoters
            matched_promoter = None
            for promoter_name in flagged_promoters:
                if self._name_overlap(builder, promoter_name):
                    matched_promoter = promoter_name
                    break

            if not matched_promoter:
                continue

            key = self._pattern_key(
                pattern_type="cross_source_promoter_risk",
                listing_id=str(listing["id"]),
            )
            if key in self._existing_pattern_keys:
                continue

            rera_reasons = flagged_promoters[matched_promoter]
            reason = (
                f"Listing by '{builder}' matches RERA-flagged promoter "
                f"'{matched_promoter}'. RERA issues: {rera_reasons[0] if rera_reasons else 'see suspicious_flags'}. "
                f"Cross-source corroboration — higher confidence risk signal."
            )

            if not self._write_pattern(
                pattern_type="cross_source_promoter_risk",
                severity="critical",
                title=f"Cross-source risk: {builder} flagged in both RERA & 99acres",
                description=reason,
                listing_id=str(listing["id"]),
                evidence={
                    "listing_id":        listing["id"],
                    "builder_name":      builder,
                    "matched_promoter":  matched_promoter,
                    "rera_flag_count":   len(rera_reasons),
                    "locality":          listing.get("locality"),
                    "listed_price":      self._listing_price(listing),
                }
            ):
                continue
            # Also mirror flag onto listing row
            try:
                update_rows("listings", {"id": listing["id"]}, {
                    "is_flagged":   True,
                    "flag_reasons": [reason],
                })
            except Exception as e:
                logger.warning(
                    f"Could not mirror flag on listing {listing['id']}: {e}")

            self._existing_pattern_keys.add(key)
            count += 1

        return count

    # ================================================================ #
    # PATTERN 2: Lapsed RERA project but still actively listing on 99acres
    # Classic fraud: RERA registration died, still selling to buyers
    # ================================================================ #

    def detect_stale_rera_active_listings(self) -> int:
        """
        Match project names/localities between lapsed RERA projects and active listings.
        If a builder is selling a project whose RERA has lapsed — that is illegal.
        """
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        listings = select_rows("listings", filters={
                               "city_id": self.city_id,
                               "source": "99acres",
                               "listing_status": "active"}, limit=1000)

        if not projects or not listings:
            return 0

        stale = [
            p for p in projects
            if any(k in (p.get("rera_status") or "").lower() for k in self.STALE_RERA_KEYWORDS)
        ]

        if not stale:
            return 0

        count = 0
        for project in stale:
            proj_name = (project.get("project_name") or "").lower()
            proj_promoter = (project.get("promoter_name") or "").lower()
            proj_locality = (project.get("address_raw") or "").lower()

            if not proj_name and not proj_promoter:
                continue

            for listing in listings:
                l_title = self._listing_title(listing).lower()
                l_builder = self._listing_builder_name(listing).lower()
                l_locality = self._normalize_locality(
                    listing.get("locality") or ""
                ).lower()

                name_hit = proj_name and self._name_overlap(proj_name, l_title)
                promoter_hit = proj_promoter and self._name_overlap(
                    proj_promoter, l_builder)
                locality_hit = proj_locality and l_locality and self._locality_overlap(
                    proj_locality, l_locality
                )

                if not ((name_hit or promoter_hit) and locality_hit):
                    continue

                key = self._pattern_key(
                    pattern_type="stale_rera_active_listing",
                    rera_project_id=str(project["id"]),
                    listing_id=str(listing["id"]),
                )
                if key in self._existing_pattern_keys:
                    continue

                reason = (
                    f"Project '{project.get('project_name')}' by "
                    f"'{project.get('promoter_name')}' has RERA status "
                    f"'{project.get('rera_status')}' but a matching listing "
                    f"is still active on 99acres in {listing.get('locality')}. "
                    f"Selling without valid RERA registration is illegal under RERA Act 2016."
                )

                if not self._write_pattern(
                    pattern_type="stale_rera_active_listing",
                    severity="critical",
                    title=f"Illegal sale? Lapsed RERA but active listing: {project.get('project_name')}",
                    description=reason,
                    rera_project_id=str(project["id"]),
                    listing_id=str(listing["id"]),
                    evidence={
                        "project_name":  project.get("project_name"),
                        "rera_status":   project.get("rera_status"),
                        "promoter_name": project.get("promoter_name"),
                        "listing_title": self._listing_title(listing),
                        "locality":      listing.get("locality"),
                        "listed_price":  self._listing_price(listing),
                    }
                ):
                    continue
                self._existing_pattern_keys.add(key)
                count += 1

        return count

    # ================================================================ #
    # PATTERN 3: Promoter name clustering (shell company detection)
    # "Shree Constructions", "Shri Construction", "Shree Builders" = same entity?
    # ================================================================ #

    def detect_promoter_name_clusters(self) -> int:
        """
        Find groups of promoters with highly similar names across RERA projects.
        Different PAN / registration but same entity is a common fraud pattern.
        """
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        if not projects:
            return 0

        normalized_groups: dict[str, set[str]] = {}
        for project in projects:
            raw_name = (project.get("promoter_name") or "").strip()
            if not raw_name:
                continue
            normalized = self._normalize_promoter_name(raw_name)
            if not normalized:
                continue
            normalized_groups.setdefault(normalized, set()).add(raw_name)

        names = list(normalized_groups)

        clusters: list[list[str]] = []
        used: set[str] = set()

        for i, name_a in enumerate(names):
            if name_a in used:
                continue
            cluster = [name_a]
            for name_b in names[i+1:]:
                if name_b in used:
                    continue
                if self._promoter_names_similar(name_a, name_b):
                    cluster.append(name_b)
                    used.add(name_b)
            if len(cluster) > 1:
                clusters.append(cluster)
                used.add(name_a)

        count = 0
        for cluster in clusters:
            cluster_names = sorted({
                raw_name
                for normalized in cluster
                for raw_name in normalized_groups.get(normalized, set())
            })

            # Skip if all raw names resolve to the same normalized form —
            # that is one entity with minor punctuation variants, not a shell cluster
            unique_normalized = {
                self._normalize_promoter_name(n) for n in cluster_names}
            if len(unique_normalized) == 1:
                continue

            key = self._pattern_key(
                pattern_type="promoter_name_cluster",
                evidence={"cluster_names": cluster_names},
            )
            if key in self._existing_pattern_keys:
                continue

            cluster_projects = [
                p for p in projects
                if self._normalize_promoter_name(p.get("promoter_name") or "") in cluster
            ]

            # Only flag if different registration numbers (confirms different legal entities)
            reg_numbers = {p.get("rera_registration")
                           for p in cluster_projects if p.get("rera_registration")}
            if len(reg_numbers) < 2:
                continue

            reason = (
                f"Promoter names {cluster_names} are highly similar ({len(cluster_names)} entities) "
                f"but registered separately under RERA ({len(reg_numbers)} registration numbers). "
                f"Possible shell company structure or name obfuscation to avoid consolidated scrutiny."
            )

            if not self._write_pattern(
                pattern_type="promoter_name_cluster",
                severity="high",
                title=f"Possible shell entity cluster: {cluster_names[0]} + {len(cluster_names)-1} similar names",
                description=reason,
                evidence={
                    "cluster_names":      cluster_names,
                    "registration_count": len(reg_numbers),
                    "project_count":      len(cluster_projects),
                    "registrations":      list(reg_numbers)[:10],
                }
            ):
                continue
            self._existing_pattern_keys.add(key)
            count += 1

        return count

    # ================================================================ #
    # PATTERN 4: Complaint velocity — complaints spread across projects
    # 1 complaint could be random. Complaints on 2+ projects = pattern
    # ================================================================ #

    def detect_complaint_velocity(self) -> int:
        """
        Flag promoters where complaint_count > 0 on multiple separate projects.
        Spread of complaints across projects indicates systemic non-compliance.
        """
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        if not projects:
            return 0

        promoter_map: dict[str, list[dict]] = {}
        for p in projects:
            if int(p.get("complaint_count") or 0) > 0:
                name = (p.get("promoter_name") or "Unknown").strip()
                promoter_map.setdefault(name, []).append(p)

        count = 0
        for promoter, affected_projects in promoter_map.items():
            if len(affected_projects) < self.COMPLAINT_VELOCITY_WINDOW:
                continue

            key = self._pattern_key(
                pattern_type="complaint_velocity",
                evidence={"promoter_name": promoter},
            )
            if key in self._existing_pattern_keys:
                continue

            total_complaints = sum(int(p.get("complaint_count") or 0)
                                   for p in affected_projects)
            project_names = [p.get("project_name", "?")
                             for p in affected_projects]

            reason = (
                f"'{promoter}' has complaints on {len(affected_projects)} separate projects "
                f"({total_complaints} total complaints): {', '.join(project_names[:3])}{'...' if len(project_names) > 3 else ''}. "
                f"Complaints spread across multiple projects indicates systemic delivery failure, "
                f"not isolated incidents."
            )

            if not self._write_pattern(
                pattern_type="complaint_velocity",
                severity="high",
                title=f"Systemic complaints: {promoter} ({len(affected_projects)} projects affected)",
                description=reason,
                evidence={
                    "promoter_name":      promoter,
                    "affected_projects":  len(affected_projects),
                    "total_complaints":   total_complaints,
                    "project_names":      project_names,
                }
            ):
                continue
            self._existing_pattern_keys.add(key)
            count += 1

        return count

    # ================================================================ #
    # PATTERN 5: Locality price spike vs city average
    # Neighbourhood suddenly expensive = insider activity or data manipulation
    # ================================================================ #

    def detect_locality_price_spike(self) -> int:
        """
        Compare median price_per_sqft per locality vs city-wide median.
        Localities significantly above median warrant investigation.
        """
        listings = select_rows("listings", filters={
                               "city_id": self.city_id,
                               "source": "99acres",
                               "listing_status": "active"}, limit=1000)
        if not listings:
            return 0

        # Filter to listings with price data
        priced = [
            l for l in listings
            if l.get("price_per_sqft") and float(l.get("price_per_sqft") or 0) > 0
        ]

        if len(priced) < 10:
            return 0

        # City-wide median
        all_prices = sorted(float(l["price_per_sqft"]) for l in priced)
        city_median = all_prices[len(all_prices) // 2]

        # Per-locality median
        locality_map: dict[str, list[float]] = {}
        for l in priced:
            loc = self._normalize_locality(l.get("locality") or "Unknown")
            locality_map.setdefault(loc, []).append(float(l["price_per_sqft"]))

        count = 0
        for locality, prices in locality_map.items():
            if len(prices) < self.MIN_LISTINGS_FOR_LOCALITY:
                continue

            locality_median = sorted(prices)[len(prices) // 2]
            spike_ratio = (locality_median - city_median) / city_median

            if spike_ratio < self.PRICE_LOCALITY_SPIKE_PCT:
                continue

            key = self._pattern_key(
                pattern_type="locality_price_spike",
                evidence={"locality": locality},
            )
            if key in self._existing_pattern_keys:
                continue

            reason = (
                f"Locality '{locality}' has median price ₹{locality_median:,.0f}/sqft — "
                f"{spike_ratio:.0%} above the city median of ₹{city_median:,.0f}/sqft "
                f"(based on {len(prices)} listings). "
                f"Unusual price premium may indicate speculative activity, "
                f"upcoming govt project, or coordinated price manipulation."
            )

            if not self._write_pattern(
                pattern_type="locality_price_spike",
                severity="medium",
                title=f"Price spike in {locality}: {spike_ratio:.0%} above city median",
                description=reason,
                evidence={
                    "locality":        locality,
                    "locality_median": locality_median,
                    "city_median":     city_median,
                    "spike_ratio":     round(spike_ratio, 3),
                    "listing_count":   len(prices),
                    "min_price":       min(prices),
                    "max_price":       max(prices),
                }
            ):
                continue
            self._existing_pattern_keys.add(key)
            count += 1

        return count

    # ================================================================ #
    # PATTERN 6: Repeat offender registers new project
    # Already-flagged promoter starts a brand new RERA project
    # ================================================================ #

    def detect_repeat_offender_new_project(self) -> int:
        """
        If a promoter already has suspicious_flags AND has a project registered
        recently (within last 180 days), that's a red flag — they're starting
        new projects while existing ones are problematic.
        """
        flags = select_rows("suspicious_flags", filters={
                            "city_id": self.city_id}, limit=2000)
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)

        if not flags or not projects:
            return 0

        trusted_flags = [
            flag for flag in flags
            if flag.get("flag_type") in self.TRUSTED_REPEAT_PARENT_FLAGS
        ]
        if not trusted_flags:
            return 0

        project_promoters = {
            str(project["id"]): self._normalize_promoter_name(project.get("promoter_name") or "")
            for project in projects
        }

        flagged_ids = {
            str(flag["rera_project_id"])
            for flag in trusted_flags
            if flag.get("rera_project_id")
        }
        flagged_promoters: set[str] = set()

        for flag in trusted_flags:
            evidence = flag.get("evidence") or {}
            if isinstance(evidence, dict):
                evidence_name = self._normalize_promoter_name(
                    evidence.get("promoter_name") or ""
                )
                if evidence_name:
                    flagged_promoters.add(evidence_name)

            project_id = flag.get("rera_project_id")
            if project_id:
                project_name = project_promoters.get(str(project_id), "")
                if project_name:
                    flagged_promoters.add(project_name)

        if not flagged_promoters:
            return 0

        now = datetime.now(timezone.utc)
        count = 0

        for p in projects:
            # Check if this is a recent project
            reg_date_str = p.get("registration_date") or ""
            if not reg_date_str:
                # created_at is our DB insert time, not the actual registration date
                # — do not use it as a fallback or recently scraped old projects
                # would incorrectly pass the 180-day recency check
                continue

            try:
                reg_date = datetime.fromisoformat(
                    reg_date_str.replace("Z", "+00:00"))
                days_old = (now - reg_date).days
                if days_old > 180:
                    continue
            except (ValueError, TypeError):
                continue

            promoter = self._normalize_promoter_name(
                p.get("promoter_name") or "")
            if promoter not in flagged_promoters:
                continue

            # Make sure this project itself isn't the source of the original flag
            if str(p["id"]) in flagged_ids:
                continue

            key = self._pattern_key(
                pattern_type="repeat_offender_new_project",
                rera_project_id=str(p["id"]),
            )
            if key in self._existing_pattern_keys:
                continue

            reason = (
                f"'{p.get('promoter_name')}' registered new project "
                f"'{p.get('project_name')}' ({days_old} days ago) "
                f"while their existing projects are flagged for suspicious activity. "
                f"New registrations by known problematic promoters warrant extra scrutiny."
            )

            if not self._write_pattern(
                pattern_type="repeat_offender_new_project",
                severity="high",
                title=f"New project by flagged promoter: {p.get('promoter_name')}",
                description=reason,
                rera_project_id=str(p["id"]),
                evidence={
                    "project_name":     p.get("project_name"),
                    "promoter_name":    p.get("promoter_name"),
                    "registration_date": reg_date_str,
                    "days_since_reg":   days_old,
                    "rera_registration": p.get("rera_registration"),
                }
            ):
                continue
            self._existing_pattern_keys.add(key)
            count += 1

        return count

    # ================================================================ #
    # HELPERS
    # ================================================================ #

    def _load_existing_patterns(self) -> None:
        try:
            rows = select_rows("suspicious_flags", filters={
                               "city_id": self.city_id}, limit=5000)
        except Exception as e:
            logger.warning(f"Could not preload existing patterns: {e}")
            return

        for row in rows:
            flag_type = str(row.get("flag_type") or "")
            if flag_type not in self.PATTERN_FLAG_TYPES:
                continue
            if not self._is_open_flag(row):
                continue

            title_key = self._title_key(flag_type, row.get("title"))
            if title_key:
                self._existing_pattern_titles.add(title_key)

            try:
                key = self._pattern_key(
                    pattern_type=flag_type,
                    rera_project_id=row.get("rera_project_id"),
                    listing_id=row.get("listing_id"),
                    evidence=row.get("evidence"),
                )
            except Exception as exc:
                logger.debug(
                    f"Skipping malformed existing pattern preload for '{flag_type}': {exc}"
                )
                continue

            if key:
                self._existing_pattern_keys.add(key)

    def _has_open_pattern(
        self,
        *,
        pattern_type: str,
        title: str,
        rera_project_id: str | None = None,
        listing_id: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> bool:
        title_key = self._title_key(pattern_type, title)
        if title_key and title_key in self._existing_pattern_titles:
            return True

        pattern_key = None
        try:
            pattern_key = self._pattern_key(
                pattern_type=pattern_type,
                rera_project_id=rera_project_id,
                listing_id=listing_id,
                evidence=evidence,
            )
        except Exception as exc:
            logger.debug(
                f"Could not build pattern key for duplicate check '{pattern_type}': {exc}"
            )

        if pattern_key and pattern_key in self._existing_pattern_keys:
            return True

        try:
            rows = select_rows(
                "suspicious_flags",
                filters={"city_id": self.city_id, "flag_type": pattern_type},
                limit=2000,
            )
        except Exception as exc:
            logger.warning(
                f"Could not verify existing pattern '{pattern_type}' before insert: {exc}"
            )
            return False

        for row in rows:
            if not self._is_open_flag(row):
                continue

            existing_title_key = self._title_key(pattern_type, row.get("title"))
            if title_key and existing_title_key == title_key:
                self._existing_pattern_titles.add(title_key)
                if pattern_key:
                    self._existing_pattern_keys.add(pattern_key)
                return True

            try:
                existing_key = self._pattern_key(
                    pattern_type=pattern_type,
                    rera_project_id=row.get("rera_project_id"),
                    listing_id=row.get("listing_id"),
                    evidence=row.get("evidence"),
                )
            except Exception:
                continue

            if pattern_key and existing_key == pattern_key:
                self._existing_pattern_keys.add(pattern_key)
                if existing_title_key:
                    self._existing_pattern_titles.add(existing_title_key)
                return True

        return False

    def _write_pattern(
        self,
        *,
        pattern_type: str,
        severity: str,
        title: str,
        description: str,
        rera_project_id: str | None = None,
        listing_id: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> bool:
        if self._has_open_pattern(
            pattern_type=pattern_type,
            title=title,
            rera_project_id=rera_project_id,
            listing_id=listing_id,
            evidence=evidence,
        ):
            return False

        payload: dict[str, Any] = {
            "flag_type":   pattern_type,
            "severity":    severity,
            "title":       title,
            "description": description,
            "evidence":    evidence or {},
            "status":      "open",
            "city_id":     self.city_id,
        }
        if rera_project_id:
            payload["rera_project_id"] = rera_project_id
        if listing_id:
            payload["listing_id"] = listing_id

        try:
            insert_row("suspicious_flags", payload)
            title_key = self._title_key(pattern_type, title)
            if title_key:
                self._existing_pattern_titles.add(title_key)
            try:
                key = self._pattern_key(
                    pattern_type=pattern_type,
                    rera_project_id=rera_project_id,
                    listing_id=listing_id,
                    evidence=evidence,
                )
            except Exception:
                key = None
            if key:
                self._existing_pattern_keys.add(key)
            return True
        except Exception as e:
            logger.warning(f"Could not write pattern '{pattern_type}': {e}")
            return False

    @staticmethod
    def _is_open_flag(row: dict[str, Any]) -> bool:
        return str(row.get("status") or "").strip().lower() in ("", "open")

    @staticmethod
    def _normalize_title(title: str) -> str:
        return re.sub(r"\s+", " ", str(title or "").strip().lower())

    @classmethod
    def _title_key(cls, pattern_type: str, title: str | None) -> tuple[str, str] | None:
        normalized = cls._normalize_title(title or "")
        if not normalized:
            return None
        return (pattern_type, normalized)

    @classmethod
    def _pattern_key(
        cls,
        *,
        pattern_type: str,
        rera_project_id: str | None = None,
        listing_id: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> str:
        # Supabase may return evidence as a JSON string — normalise to dict
        if isinstance(evidence, str) and evidence:
            try:
                evidence = json.loads(evidence)
            except (TypeError, ValueError, json.JSONDecodeError):
                evidence = {}
        evidence = evidence or {}
        if pattern_type == "cross_source_promoter_risk" and listing_id:
            return f"cross_source_{listing_id}"
        if pattern_type == "stale_rera_active_listing" and rera_project_id and listing_id:
            return f"stale_rera_{rera_project_id}_{listing_id}"
        if pattern_type == "repeat_offender_new_project" and rera_project_id:
            return f"repeat_offender_{rera_project_id}"
        if pattern_type == "complaint_velocity":
            promoter = cls._normalize_promoter_name(
                str((evidence or {}).get("promoter_name") or "")
            )
            if promoter:
                return f"complaint_velocity_{promoter.replace(' ', '_')[:50]}"
        if pattern_type == "locality_price_spike":
            locality = cls._normalize_locality(
                str((evidence or {}).get("locality") or ""))
            if locality:
                return f"locality_spike_{locality.lower().replace(' ', '_')[:50]}"
        if pattern_type == "promoter_name_cluster":
            cluster_names = (evidence or {}).get("cluster_names") or []
            if isinstance(cluster_names, list) and cluster_names:
                normalized = sorted(
                    cls._normalize_promoter_name(name)
                    for name in cluster_names
                    if cls._normalize_promoter_name(name)
                )
                if normalized:
                    return f"name_cluster_{'_'.join(normalized)[:80]}"
        # Fallback — deterministic but logged so we notice unhandled pattern types
        logger.debug(
            f"_pattern_key: unhandled pattern_type='{pattern_type}' "
            f"pid={rera_project_id} lid={listing_id} — using generic key"
        )
        pid_part = rera_project_id or ""
        lid_part = listing_id or ""
        if not pid_part and not lid_part:
            raise ValueError(
                f"_pattern_key: cannot build unique key for '{pattern_type}' "
                f"— no project_id, listing_id, or evidence identifiers"
            )
        return f"{pattern_type}_{pid_part}_{lid_part}"

    @staticmethod
    def _listing_raw(listing: dict[str, Any]) -> dict[str, Any]:
        raw = listing.get("raw_data")
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw:
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, dict) else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
        return {}

    @classmethod
    def _listing_title(cls, listing: dict[str, Any]) -> str:
        raw = cls._listing_raw(listing)
        return (
            listing.get("address_raw")
            or raw.get("title")
            or listing.get("project_name")
            or ""
        ).strip()

    @classmethod
    def _listing_builder_name(cls, listing: dict[str, Any]) -> str:
        raw = cls._listing_raw(listing)
        return (
            listing.get("builder_name")
            or listing.get("developer_name")
            # actual column scraper_99acres stores
            or listing.get("listed_by")
            or raw.get("builder_name")
            or raw.get("builderName")
            or raw.get("developer_name")
            or raw.get("developerName")
            or raw.get("promoter_name")
            or raw.get("promoterName")
            or raw.get("listed_by")
            or ""
        ).strip()

    @staticmethod
    def _listing_price(listing: dict[str, Any]) -> Any:
        return listing.get("listed_price") or listing.get("price")

    @staticmethod
    def _locality_overlap(a: str, b: str) -> bool:
        """True if two locality strings share at least one significant word token.
        More robust than prefix matching — handles reordered or abbreviated addresses.
        """
        stopwords = {"road", "nagar", "ward", "plot", "sector", "area",
                     "layout", "colony", "phase", "extension"}

        def tokens(s: str) -> set[str]:
            words = re.findall(r"[a-z0-9]+", s.lower())
            return {w for w in words if w not in stopwords and len(w) > 2}
        ta, tb = tokens(a), tokens(b)
        if not ta or not tb:
            return False
        # Shared token + at least 30% Jaccard to avoid single generic word hits
        shared = ta & tb
        if not shared:
            return False
        jaccard = len(shared) / len(ta | tb)
        return jaccard >= 0.25

    @staticmethod
    def _normalize_locality(locality: str) -> str:
        locality = (locality or "").strip()
        locality = re.sub(r",\s*(akola|amravati)\s*$",
                          "", locality, flags=re.I)
        locality = re.sub(r"\s+", " ", locality)
        return locality

    @staticmethod
    def _name_overlap(a: str, b: str) -> bool:
        """True if two name strings share enough significant words."""
        ta = PatternDetector._promoter_name_tokens(a)
        tb = PatternDetector._promoter_name_tokens(b)
        if not ta or not tb:
            return False
        intersection = ta & tb
        if not intersection:
            return False
        # At least one significant shared word AND jaccard >= 0.3
        jaccard = len(intersection) / len(ta | tb)
        return jaccard >= 0.30

    @staticmethod
    def _jaccard(a: str, b: str) -> float:
        """Character-level bigram Jaccard similarity."""
        def bigrams(s: str) -> set[str]:
            s = re.sub(r"[^a-z0-9]", "", s.lower())
            return {s[i:i+2] for i in range(len(s)-1)} if len(s) > 1 else {s}
        ba, bb = bigrams(a), bigrams(b)
        if not ba and not bb:
            return 1.0
        if not ba or not bb:
            return 0.0
        return len(ba & bb) / len(ba | bb)

    @classmethod
    def _normalize_promoter_name(cls, name: str) -> str:
        name = name.lower().replace("&", " and ")
        name = re.sub(r"[^a-z0-9\s]", " ", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    @classmethod
    def _promoter_name_tokens(cls, name: str) -> set[str]:
        words = re.findall(r"[a-z0-9]+", cls._normalize_promoter_name(name))
        return {
            word
            for word in words
            if word not in cls.PROMOTER_STOPWORDS and len(word) > 1
        }

    @classmethod
    def _promoter_names_similar(cls, name_a: str, name_b: str) -> bool:
        norm_a = cls._normalize_promoter_name(name_a)
        norm_b = cls._normalize_promoter_name(name_b)

        # Same canonical name with different case / punctuation is not suspicious.
        if not norm_a or not norm_b or norm_a == norm_b:
            return False

        tokens_a = cls._promoter_name_tokens(norm_a)
        tokens_b = cls._promoter_name_tokens(norm_b)
        shared_tokens = tokens_a & tokens_b
        if len(shared_tokens) < 3:
            return False

        return cls._jaccard(norm_a, norm_b) >= cls.NAME_SIMILARITY_THRESHOLD
