"""
models/dbscan_anomaly_tester.py
Experimental DBSCAN-based listing anomaly detector.

This is intentionally separate from anomaly_detector.py. The rule-based
detector remains production logic; this model tests whether unsupervised
clustering can find unusual marketplace listings worth review.

Dry run/report:
    python main.py --dbscan-anomaly --city Nagpur

Visual graph:
    python main.py --dbscan-anomaly --city Nagpur --dbscan-plot logs/dbscan_nagpur.html
"""

import html
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

from db.connection import insert_row, select_rows, update_rows
from models.listing_sources import MARKETPLACE_SOURCES

logger = logging.getLogger(__name__)


class DBSCANAnomalyTester:
    FLAG_TYPE = "dbscan_listing_anomaly"

    MIN_CITY_LISTINGS = 120
    MIN_GROUP_SIZE = 20
    DEFAULT_EPS = 1.35
    DEFAULT_MIN_SAMPLES = 5
    DEFAULT_MAX_FLAGS = 30

    def __init__(
        self,
        city_id: int = 1,
        *,
        eps: float = DEFAULT_EPS,
        min_samples: int = DEFAULT_MIN_SAMPLES,
        max_flags: int = DEFAULT_MAX_FLAGS,
        write_to_flags: bool = False,
        plot_path: str | None = None,
    ):
        self.city_id = city_id
        self.eps = eps
        self.min_samples = min_samples
        self.max_flags = max_flags
        self.write_to_flags = write_to_flags
        self.plot_path = plot_path
        self._plot_points: list[dict[str, Any]] = []
        self._open_listing_flags: set[str] = set()
        if self.write_to_flags:
            self._load_existing_flags()

    def run(self) -> int:
        listings = []
        for source in MARKETPLACE_SOURCES:
            listings.extend(select_rows(
                "listings",
                filters={
                    "city_id": self.city_id,
                    "source": source,
                    "listing_status": "active",
                },
                limit=5000,
            ))
        candidates = [row for row in listings if self._feature_row(row)]
        if len(candidates) < self.MIN_CITY_LISTINGS:
            logger.info(
                "DBSCANAnomalyTester: skipping city_id=%s - only %s usable listings",
                self.city_id,
                len(candidates),
            )
            return 0

        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for listing in candidates:
            key = (
                self._norm(listing.get("property_type") or "unknown"),
                self._norm(listing.get("listing_type") or "sale"),
            )
            groups[key].append(listing)

        findings: list[dict[str, Any]] = []
        for (property_type, listing_type), group in groups.items():
            if len(group) < self.MIN_GROUP_SIZE:
                continue
            group_findings = self._cluster_group(
                group,
                property_type=property_type,
                listing_type=listing_type,
            )
            findings.extend(group_findings)

        if self.plot_path:
            self._write_plot(self.plot_path)

        findings.sort(key=lambda item: item["distance"], reverse=True)
        selected = findings[: self.max_flags]

        if not self.write_to_flags:
            self._log_findings(selected)
            logger.info(
                "DBSCANAnomalyTester: dry run found %s candidate(s) for city_id=%s",
                len(selected),
                self.city_id,
            )
            return len(selected)

        written = 0
        for finding in selected:
            if self._write_flag(finding):
                written += 1

        logger.info(
            "DBSCANAnomalyTester: wrote %s DBSCAN anomaly flag(s) for city_id=%s",
            written,
            self.city_id,
        )
        return written

    def _write_plot(self, path: str) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        points = self._plot_points
        if not points:
            output.write_text(
                "<!doctype html><meta charset='utf-8'><title>DBSCAN clusters</title>"
                "<p>No DBSCAN cluster points were available.</p>",
                encoding="utf-8",
            )
            logger.info("DBSCANAnomalyTester: wrote empty cluster plot to %s", output)
            return

        xs = [point["x"] for point in points]
        ys = [point["y"] for point in points]
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        width, height = 1100, 720
        pad = 64

        def scale(value: float, low: float, high: float, out_low: int, out_high: int) -> float:
            if high == low:
                return (out_low + out_high) / 2
            return out_low + ((value - low) / (high - low)) * (out_high - out_low)

        colors = [
            "#2563eb", "#16a34a", "#d97706", "#7c3aed", "#0891b2",
            "#db2777", "#65a30d", "#9333ea", "#0f766e", "#ea580c",
        ]
        circles = []
        anomaly_rows = []
        for point in points:
            cx = scale(point["x"], min_x, max_x, pad, width - pad)
            cy = scale(point["y"], min_y, max_y, height - pad, pad)
            label = int(point["cluster_label"])
            color = "#dc2626" if label == -1 else colors[label % len(colors)]
            radius = 6 if label == -1 else 4
            opacity = "0.95" if label == -1 else "0.62"
            title = html.escape(
                f"id={point['listing_id']} | cluster={label} | "
                f"{point['property_type']}/{point['listing_type']} | "
                f"{point['locality']} | Rs {point['price_per_sqft']}/sqft | "
                f"area {point['area_sqft']} sqft"
            )
            circles.append(
                f"<circle cx='{cx:.1f}' cy='{cy:.1f}' r='{radius}' "
                f"fill='{color}' opacity='{opacity}'><title>{title}</title></circle>"
            )
            if label == -1:
                anomaly_rows.append(
                    "<tr>"
                    f"<td>{html.escape(str(point['listing_id']))}</td>"
                    f"<td>{html.escape(str(point['locality']))}</td>"
                    f"<td>{html.escape(str(point['property_type']))}</td>"
                    f"<td>{html.escape(str(point['listing_type']))}</td>"
                    f"<td>{point['price_per_sqft']:,.0f}</td>"
                    f"<td>{point['area_sqft']:,.0f}</td>"
                    f"<td>{point['listed_price']:,.0f}</td>"
                    f"<td>{label}</td>"
                    "</tr>"
                )

        noise_count = sum(1 for point in points if point["cluster_label"] == -1)
        group_count = len({
            (point["property_type"], point["listing_type"]) for point in points
        })
        markup = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>DBSCAN Listing Clusters</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #172033; }}
    .meta {{ color: #596275; margin-bottom: 16px; }}
    svg {{ border: 1px solid #d8dee9; background: #fbfcfe; max-width: 100%; height: auto; }}
    .axis {{ stroke: #8b95a7; stroke-width: 1; }}
    .label {{ fill: #465064; font-size: 13px; }}
    .legend {{ display: flex; gap: 20px; margin-top: 14px; align-items: center; }}
    .dot {{ display: inline-block; width: 11px; height: 11px; border-radius: 50%; margin-right: 6px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 24px; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #d8dee9; padding: 8px 10px; text-align: left; }}
    th {{ background: #eef2f7; color: #263244; position: sticky; top: 0; }}
    td:nth-child(5), td:nth-child(6), td:nth-child(7), td:nth-child(8) {{ text-align: right; }}
  </style>
</head>
<body>
  <h1>DBSCAN Listing Clusters</h1>
  <div class="meta">
    Points: {len(points)} | Noise/anomaly candidates: {noise_count} |
    Comparable groups: {group_count} | eps={self.eps} | min_samples={self.min_samples}
  </div>
  <svg viewBox="0 0 {width} {height}" role="img" aria-label="DBSCAN cluster scatter plot">
    <line class="axis" x1="{pad}" y1="{height - pad}" x2="{width - pad}" y2="{height - pad}"/>
    <line class="axis" x1="{pad}" y1="{pad}" x2="{pad}" y2="{height - pad}"/>
    <text class="label" x="{width / 2 - 100:.0f}" y="{height - 20}">log(price_per_sqft)</text>
    <text class="label" x="18" y="{height / 2 + 80:.0f}" transform="rotate(-90 18 {height / 2 + 80:.0f})">log(area_sqft)</text>
    {''.join(circles)}
  </svg>
  <div class="legend">
    <span><span class="dot" style="background:#dc2626"></span>Noise / anomaly candidate</span>
    <span><span class="dot" style="background:#2563eb"></span>Clustered listing</span>
  </div>
  <h2>Anomaly Candidates</h2>
  <table>
    <thead>
      <tr>
        <th>Listing ID</th>
        <th>Locality</th>
        <th>Property Type</th>
        <th>Listing Type</th>
        <th>Price / Sqft</th>
        <th>Area Sqft</th>
        <th>Listed Price</th>
        <th>Cluster</th>
      </tr>
    </thead>
    <tbody>
      {''.join(anomaly_rows) if anomaly_rows else '<tr><td colspan="8">No anomaly candidates found.</td></tr>'}
    </tbody>
  </table>
</body>
</html>
"""
        output.write_text(markup, encoding="utf-8")
        logger.info("DBSCANAnomalyTester: wrote cluster plot to %s", output)

    def _log_findings(self, findings: list[dict[str, Any]]) -> None:
        if not findings:
            logger.info("DBSCANAnomalyTester: no DBSCAN candidates found")
            return

        for idx, finding in enumerate(findings, start=1):
            listing = finding["listing"]
            logger.info(
                "DBSCAN candidate %s: listing_id=%s locality=%s property_type=%s "
                "ppsf=%s group_median=%s distance=%s group_size=%s",
                idx,
                listing.get("id"),
                listing.get("locality") or listing.get("address_raw"),
                finding["property_type"],
                listing.get("price_per_sqft"),
                finding["group_median_price_per_sqft"],
                finding["distance"],
                finding["group_size"],
            )

    def _cluster_group(
        self,
        listings: list[dict[str, Any]],
        *,
        property_type: str,
        listing_type: str,
    ) -> list[dict[str, Any]]:
        try:
            from sklearn.cluster import DBSCAN
            from sklearn.preprocessing import StandardScaler
        except ImportError as exc:
            raise RuntimeError(
                "DBSCAN anomaly testing requires scikit-learn. "
                "Install dependencies with: pip install -r requirements.txt"
            ) from exc

        matrix = [self._feature_row(row) for row in listings]
        valid_pairs = [
            (row, features)
            for row, features in zip(listings, matrix, strict=False)
            if features is not None
        ]
        if len(valid_pairs) < self.MIN_GROUP_SIZE:
            return []

        valid_listings = [row for row, _ in valid_pairs]
        features = [values for _, values in valid_pairs]
        scaled = StandardScaler().fit_transform(features)
        labels = DBSCAN(eps=self.eps, min_samples=self.min_samples).fit_predict(scaled)

        for row, label, row_features in zip(valid_listings, labels, features, strict=False):
            self._plot_points.append({
                "listing_id": row.get("id"),
                "locality": row.get("locality") or row.get("address_raw") or "",
                "property_type": property_type,
                "listing_type": listing_type,
                "price_per_sqft": self._to_float(row.get("price_per_sqft")),
                "area_sqft": self._to_float(row.get("area_sqft")),
                "listed_price": self._to_float(row.get("listed_price") or row.get("price")),
                "cluster_label": int(label),
                "x": row_features[0],
                "y": row_features[1],
            })

        group_ppsf = [
            self._to_float(row.get("price_per_sqft"))
            for row in valid_listings
            if self._to_float(row.get("price_per_sqft")) > 0
        ]
        group_median = self._median(group_ppsf)
        locality_medians = self._locality_medians(valid_listings)

        findings: list[dict[str, Any]] = []
        for row, label, scaled_features in zip(valid_listings, labels, scaled, strict=False):
            if int(label) != -1:
                continue
            listing_id = str(row.get("id"))
            if not listing_id or listing_id in self._open_listing_flags:
                continue

            distance = self._euclidean(scaled_features)
            locality = self._normalize_locality(
                row.get("locality") or row.get("address_raw") or ""
            )
            locality_median = locality_medians.get(locality, 0.0)
            ppsf = float(row.get("price_per_sqft") or 0)
            ratio_to_group = ppsf / group_median if group_median > 0 else 0
            ratio_to_locality = (
                ppsf / locality_median if locality_median > 0 else 0
            )

            findings.append({
                "listing": row,
                "distance": round(distance, 3),
                "property_type": property_type,
                "listing_type": listing_type,
                "group_size": len(valid_listings),
                "group_median_price_per_sqft": round(group_median, 2),
                "locality_median_price_per_sqft": round(locality_median, 2)
                if locality_median else None,
                "ratio_to_group_median": round(ratio_to_group, 3)
                if ratio_to_group else None,
                "ratio_to_locality_median": round(ratio_to_locality, 3)
                if ratio_to_locality else None,
            })

        return findings

    def _write_flag(self, finding: dict[str, Any]) -> bool:
        listing = finding["listing"]
        listing_id = str(listing["id"])
        locality = listing.get("locality") or listing.get("address_raw") or "unknown locality"
        ppsf = float(listing.get("price_per_sqft") or 0)
        distance = finding["distance"]
        severity = "high" if distance >= 3.5 else "medium"

        title = f"DBSCAN listing anomaly in {locality}"
        reason = (
            f"Listing is outside DBSCAN clusters for "
            f"{finding['property_type'].replace('_', ' ')} "
            f"{finding['listing_type']} listings. "
            f"Price is Rs {ppsf:,.0f}/sqft; group median is "
            f"Rs {finding['group_median_price_per_sqft']:,.0f}/sqft. "
            f"Treat as statistical anomaly, not confirmed fraud."
        )
        payload = {
            "flag_type": self.FLAG_TYPE,
            "severity": severity,
            "title": title,
            "description": reason,
            "status": "open",
            "city_id": self.city_id,
            "listing_id": listing_id,
            "evidence": {
                "model": "DBSCAN",
                "eps": self.eps,
                "min_samples": self.min_samples,
                "cluster_label": -1,
                "distance_from_scaled_origin": distance,
                "group_size": finding["group_size"],
                "property_type": finding["property_type"],
                "listing_type": finding["listing_type"],
                "locality": listing.get("locality"),
                "price_per_sqft": ppsf,
                "area_sqft": self._to_float(listing.get("area_sqft")),
                "listed_price": self._to_float(listing.get("listed_price")),
                "group_median_price_per_sqft": finding["group_median_price_per_sqft"],
                "locality_median_price_per_sqft": finding[
                    "locality_median_price_per_sqft"
                ],
                "ratio_to_group_median": finding["ratio_to_group_median"],
                "ratio_to_locality_median": finding["ratio_to_locality_median"],
            },
        }

        try:
            insert_row("suspicious_flags", payload)
            self._open_listing_flags.add(listing_id)
            self._mirror_listing_flag(listing, reason)
            return True
        except Exception as exc:
            logger.warning(
                "Could not write DBSCAN anomaly for listing %s: %s",
                listing_id,
                exc,
            )
            return False

    def _mirror_listing_flag(self, listing: dict[str, Any], reason: str) -> None:
        listing_id = str(listing["id"])
        reasons = list(listing.get("flag_reasons") or [])
        if reason not in reasons:
            reasons.append(reason)
        try:
            update_rows(
                "listings",
                {"id": listing_id},
                {"is_flagged": True, "flag_reasons": reasons},
            )
        except Exception as exc:
            logger.warning(
                "Could not mirror DBSCAN flag on listing %s: %s",
                listing_id,
                exc,
            )

    def _load_existing_flags(self) -> None:
        try:
            rows = select_rows(
                "suspicious_flags",
                filters={"city_id": self.city_id, "flag_type": self.FLAG_TYPE},
                limit=5000,
            )
        except Exception as exc:
            logger.warning("Could not preload DBSCAN flags: %s", exc)
            return

        for row in rows:
            if not self._is_open_flag(row):
                continue
            listing_id = row.get("listing_id")
            if listing_id:
                self._open_listing_flags.add(str(listing_id))

    @classmethod
    def _feature_row(cls, listing: dict[str, Any]) -> list[float] | None:
        ppsf = cls._to_float(listing.get("price_per_sqft"))
        area = cls._to_float(listing.get("area_sqft"))
        total = cls._to_float(listing.get("listed_price") or listing.get("price"))
        if total <= 0 and ppsf > 0 and area > 0:
            total = ppsf * area
        if ppsf <= 0 or area <= 0 or total <= 0:
            return None
        return [
            math.log1p(ppsf),
            math.log1p(area),
            math.log1p(total),
        ]

    @classmethod
    def _locality_medians(cls, listings: list[dict[str, Any]]) -> dict[str, float]:
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in listings:
            locality = cls._normalize_locality(
                row.get("locality") or row.get("address_raw") or ""
            )
            ppsf = cls._to_float(row.get("price_per_sqft"))
            if locality and ppsf > 0:
                grouped[locality].append(ppsf)
        return {
            locality: cls._median(prices)
            for locality, prices in grouped.items()
            if len(prices) >= 3
        }

    @staticmethod
    def _normalize_locality(value: str) -> str:
        return " ".join(str(value or "").strip().lower().split())

    @staticmethod
    def _norm(value: Any) -> str:
        return " ".join(str(value or "").strip().lower().split()) or "unknown"

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        mid = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2

    @staticmethod
    def _euclidean(values: Any) -> float:
        return math.sqrt(sum(float(value) ** 2 for value in values))

    @staticmethod
    def _is_open_flag(row: dict[str, Any]) -> bool:
        status = str(row.get("status") or "").strip().lower()
        return status in ("", "open")
