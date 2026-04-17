"""
models/anomaly_detector.py
Detects suspicious patterns in RERA projects and listings.

Uses the REST helpers in db.connection and stores detailed results in the
suspicious_flags table. It also mirrors a simple flag state onto the source
rows through is_flagged / flag_reasons for easy filtering.
"""

import logging
import re
from typing import Any

from db.connection import insert_row, select_rows, update_rows

logger = logging.getLogger(__name__)


class AnomalyDetector:

    # Thresholds
    MIN_RERA_COMPLAINTS = 2        # flag builder if >= 2 complaints
    ESCROW_MIN_RATIO = 0.70        # RERA mandates 70% in escrow
    PRICE_DEVIATION_PCT = 0.75     # listing price >75% above/below median
    MIN_OUTLIER_GROUP_SIZE = 4     # need enough comparable listings in a locality

    def __init__(self, city_id: int = 1):
        self.city_id = city_id
        self._project_flag_keys: set[tuple[str, str]] = set()
        self._listing_flag_keys: set[tuple[str, str]] = set()
        self._load_existing_flags()

    # ------------------------------------------------------------------ #

    def run_all(self):
        checks = [
            self.check_repeated_complaints,
            self.check_rera_escrow_deficit,
            self.check_stalled_projects,
            self.check_listing_price_outliers,
        ]
        total_flags = 0
        for check in checks:
            try:
                count = check()
                logger.info(f"{check.__name__}: {count} flags created")
                total_flags += count
            except Exception as exc:
                logger.error(f"{check.__name__} failed: {exc}", exc_info=True)
        logger.info(f"Anomaly detection complete - {total_flags} total flags")
        return total_flags

    # ================================================================ #
    # CHECK 1: Builders with repeated RERA complaints
    # ================================================================ #

    def check_repeated_complaints(self) -> int:
        """Flag builders whose projects have >= MIN_RERA_COMPLAINTS total."""
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        if not projects:
            logger.info(
                "check_repeated_complaints: no rera_projects rows found")
            return 0

        promoter_complaints: dict[str, int] = {}
        promoter_projects: dict[str, list[dict[str, Any]]] = {}
        for project in projects:
            name = project.get("promoter_name") or "Unknown"
            complaints = int(project.get("complaint_count") or 0)
            promoter_complaints[name] = promoter_complaints.get(
                name, 0) + complaints
            promoter_projects.setdefault(name, []).append(project)

        count = 0
        for promoter, total in promoter_complaints.items():
            if total < self.MIN_RERA_COMPLAINTS:
                continue

            related_projects = promoter_projects[promoter]
            for project in related_projects:
                reason = (
                    f"Promoter '{promoter}' has {total} RERA complaints "
                    f"across {len(related_projects)} project(s). Exercise caution."
                )
                title = f"Repeated RERA complaints for {promoter}"
                if self._flag_project(
                    project=project,
                    flag_type="repeated_complaints",
                    severity="high",
                    title=title,
                    reason=reason,
                    evidence={
                        "promoter_name": promoter,
                        "total_complaints": total,
                        "project_count": len(related_projects),
                        "project_name": project.get("project_name"),
                        "rera_registration": project.get("rera_registration"),
                    },
                ):
                    count += 1

        return count

    # ================================================================ #
    # CHECK 2: Escrow deficit
    # ================================================================ #

    def check_rera_escrow_deficit(self) -> int:
        """Flag RERA projects where escrow balance < 70% of amount collected."""
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        if not projects:
            return 0

        count = 0
        for project in projects:
            collected = float(project.get("amount_collected") or 0)
            escrow_raw = project.get("escrow_balance")

            if collected <= 0:
                continue

            # MahaRERA often leaves escrow fields absent in the public payload.
            # Treat missing data as "unknown", not as a real zero balance.
            if escrow_raw in (None, ""):
                continue

            try:
                escrow = float(escrow_raw)
            except (TypeError, ValueError):
                continue

            ratio = escrow / collected
            if ratio >= self.ESCROW_MIN_RATIO:
                continue

            severity = "critical" if ratio < 0.30 else "high"
            reason = (
                f"Escrow deficit: only {ratio:.0%} of collected funds "
                f"(Rs {collected/1e7:.1f} Cr collected, Rs {escrow/1e7:.1f} Cr in escrow). "
                f"RERA mandates minimum 70%. Risk of project default."
            )
            title = f"Escrow deficit in {project.get('project_name') or 'RERA project'}"
            if self._flag_project(
                project=project,
                flag_type="rera_escrow_deficit",
                severity=severity,
                title=title,
                reason=reason,
                evidence={
                    "project_name": project.get("project_name"),
                    "rera_registration": project.get("rera_registration"),
                    "amount_collected": collected,
                    "escrow_balance": escrow,
                    "escrow_ratio": ratio,
                },
            ):
                count += 1

        return count

    # ================================================================ #
    # CHECK 3: Stalled / lapsed projects by same promoter
    # ================================================================ #

    def check_stalled_projects(self) -> int:
        """Flag promoters with multiple projects in lapsed/stalled status."""
        projects = select_rows("rera_projects", filters={
                               "city_id": self.city_id}, limit=1000)
        if not projects:
            return 0

        stalled_statuses = {"lapsed", "revoked", "expired", "stalled"}
        stalled_by_promoter: dict[str, list[dict[str, Any]]] = {}

        for project in projects:
            status = (project.get("rera_status") or "").lower()
            if any(token in status for token in stalled_statuses):
                name = project.get("promoter_name") or "Unknown"
                stalled_by_promoter.setdefault(name, []).append(project)

        count = 0
        for promoter, stalled_projects in stalled_by_promoter.items():
            if len(stalled_projects) < 2:
                continue

            for project in stalled_projects:
                reason = (
                    f"Promoter '{promoter}' has {len(stalled_projects)} projects "
                    f"with lapsed/revoked RERA status. Pattern may indicate "
                    f"abandonment or repeated non-compliance."
                )
                title = f"Multiple stalled projects for {promoter}"
                if self._flag_project(
                    project=project,
                    flag_type="stalled_projects",
                    severity="high",
                    title=title,
                    reason=reason,
                    evidence={
                        "promoter_name": promoter,
                        "stalled_project_count": len(stalled_projects),
                        "project_name": project.get("project_name"),
                        "rera_registration": project.get("rera_registration"),
                        "rera_status": project.get("rera_status"),
                    },
                ):
                    count += 1

        return count

    # ================================================================ #
    # CHECK 4: Listing price outliers (99acres data)
    # ================================================================ #

    def check_listing_price_outliers(self) -> int:
        """
        Flag listings where price_per_sqft deviates materially from the
        locality median for the same property_type + listing_type.
        """
        listings = select_rows(
            "listings",
            filters={
                "city_id": self.city_id,
                "source": "99acres",
                "listing_status": "active",
            },
            limit=2000,
        )
        if not listings:
            logger.info("check_listing_price_outliers: no listings rows found")
            return 0

        groups: dict[str, list[tuple[float, dict[str, Any]]]] = {}
        for listing in listings:
            ppsf = listing.get("price_per_sqft")
            if not ppsf:
                continue

            locality = self._normalize_locality(
                listing.get("locality") or listing.get("address_raw") or ""
            )
            if not locality:
                continue

            property_type = listing.get("property_type") or "unknown"
            listing_type = listing.get("listing_type") or "unknown"
            key = f"{locality}__{property_type}__{listing_type}"
            groups.setdefault(key, []).append((float(ppsf), listing))

        count = 0
        for key, items in groups.items():
            if len(items) < self.MIN_OUTLIER_GROUP_SIZE:
                continue

            prices = sorted(value for value, _ in items)
            median = self._median(prices)
            if median <= 0:
                continue

            locality_key, property_type, listing_type = key.split("__", 2)
            for price, listing in items:
                ratio = price / median
                if 1 - self.PRICE_DEVIATION_PCT <= ratio <= 1 + self.PRICE_DEVIATION_PCT:
                    continue

                direction = "above" if ratio > 1 else "below"
                reason = (
                    f"Price Rs {price:,.0f}/sqft is {abs(ratio - 1):.0%} {direction} "
                    f"the locality median of Rs {median:,.0f}/sqft for "
                    f"{property_type.replace('_', ' ')} {listing_type} listings "
                    f"in {locality_key.title()}. "
                    f"Possible data error or price manipulation."
                )
                title = (
                    f"Listing price outlier in "
                    f"{listing.get('locality') or listing.get('address_raw') or 'unknown locality'}"
                )
                if self._flag_listing(
                    listing=listing,
                    flag_type="listing_price_outlier",
                    severity="medium",
                    title=title,
                    reason=reason,
                    evidence={
                        "listing_type": listing.get("listing_type"),
                        "property_type": listing.get("property_type"),
                        "locality": listing.get("locality"),
                        "normalized_locality": locality_key,
                        "price_per_sqft": price,
                        "median_price_per_sqft": median,
                        "ratio": ratio,
                        "group_size": len(items),
                        "comparable_count": len(items),
                    },
                ):
                    count += 1

        return count

    # ================================================================ #
    # HELPERS
    # ================================================================ #

    def _load_existing_flags(self) -> None:
        try:
            existing_flags = select_rows(
                "suspicious_flags",
                filters={"city_id": self.city_id},
                limit=5000,
            )
        except Exception as exc:
            logger.warning(f"Could not preload suspicious_flags: {exc}")
            return

        for flag in existing_flags:
            if not self._is_open_flag(flag):
                continue
            flag_type = flag.get("flag_type")
            project_id = flag.get("rera_project_id")
            listing_id = flag.get("listing_id")
            if flag_type and project_id:
                self._project_flag_keys.add((str(project_id), str(flag_type)))
            if flag_type and listing_id:
                self._listing_flag_keys.add((str(listing_id), str(flag_type)))

    @staticmethod
    def _merge_reasons(existing: Any, reason: str) -> list[str]:
        reasons = list(existing or [])
        if reason not in reasons:
            reasons.append(reason)
        return reasons

    def _create_flag_record(
        self,
        *,
        flag_type: str,
        severity: str,
        title: str,
        reason: str,
        rera_project_id: str | None = None,
        listing_id: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> bool:
        payload: dict[str, Any] = {
            "flag_type": flag_type,
            "severity": severity,
            "title": title,
            "description": reason,
            "evidence": evidence or {},
            "city_id": self.city_id,
        }
        if rera_project_id:
            payload["rera_project_id"] = rera_project_id
        if listing_id:
            payload["listing_id"] = listing_id

        try:
            insert_row("suspicious_flags", payload)
            return True
        except Exception as exc:
            target = rera_project_id or listing_id or "unknown"
            logger.warning(
                f"Could not create suspicious flag '{flag_type}' for {target}: {exc}"
            )
            return False

    def _flag_project(
        self,
        *,
        project: dict[str, Any],
        flag_type: str,
        severity: str,
        title: str,
        reason: str,
        evidence: dict[str, Any] | None = None,
    ) -> bool:
        project_id = str(project["id"])
        flag_key = (project_id, flag_type)
        if flag_key in self._project_flag_keys:
            return False

        if not self._create_flag_record(
            flag_type=flag_type,
            severity=severity,
            title=title,
            reason=reason,
            rera_project_id=project_id,
            evidence=evidence,
        ):
            return False

        self._project_flag_keys.add(flag_key)

        reasons = self._merge_reasons(project.get("flag_reasons"), reason)
        try:
            update_rows(
                "rera_projects",
                {"id": project_id},
                {
                    "is_flagged": True,
                    "flag_reasons": reasons,
                },
            )
            project["is_flagged"] = True
            project["flag_reasons"] = reasons
        except Exception as exc:
            logger.warning(
                f"Could not mirror project flag on {project_id}: {exc}")

        return True

    def _flag_listing(
        self,
        *,
        listing: dict[str, Any],
        flag_type: str,
        severity: str,
        title: str,
        reason: str,
        evidence: dict[str, Any] | None = None,
    ) -> bool:
        listing_id = str(listing["id"])
        flag_key = (listing_id, flag_type)
        if flag_key in self._listing_flag_keys:
            return False

        if not self._create_flag_record(
            flag_type=flag_type,
            severity=severity,
            title=title,
            reason=reason,
            listing_id=listing_id,
            evidence=evidence,
        ):
            return False

        self._listing_flag_keys.add(flag_key)

        reasons = self._merge_reasons(listing.get("flag_reasons"), reason)
        try:
            update_rows(
                "listings",
                {"id": listing_id},
                {
                    "is_flagged": True,
                    "flag_reasons": reasons,
                },
            )
            listing["is_flagged"] = True
            listing["flag_reasons"] = reasons
        except Exception as exc:
            logger.warning(
                f"Could not mirror listing flag on {listing_id}: {exc}")

        return True

    @staticmethod
    def _normalize_locality(value: str) -> str:
        locality = (value or "").strip().lower()
        locality = re.sub(r",\s*(akola|amravati)\s*$", "", locality)
        locality = re.sub(r"\s+", " ", locality)
        return locality.strip(" ,.-")

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        mid = len(values) // 2
        if len(values) % 2:
            return values[mid]
        return (values[mid - 1] + values[mid]) / 2

    @staticmethod
    def _is_open_flag(flag: dict[str, Any]) -> bool:
        status = str(flag.get("status") or "").strip().lower()
        return status in ("", "open")
