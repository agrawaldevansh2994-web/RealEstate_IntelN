"""
reports/trend_report.py
Reads price_spikes and price_history for a city and writes a self-contained HTML report.

Intentionally decoupled from TrendDetector — reads from the DB directly so it
can be run independently at any time after --trends has populated the tables.

Run via main.py:
    python main.py --trend-report --city Nagpur
    python main.py --trend-report --city Akola
    python main.py --trend-report --city Amravati
"""

import html as html_mod
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from db.connection import select_rows

logger = logging.getLogger(__name__)

WINDOWS = [
    {"days": 7,  "medium": 0.08, "high": 0.15, "critical": 0.25},
    {"days": 14, "medium": 0.12, "high": 0.20, "critical": 0.35},
    {"days": 30, "medium": 0.20, "high": 0.35, "critical": 0.50},
]

LINE_COLORS = [
    "#2563eb", "#16a34a", "#d97706", "#7c3aed",
    "#0891b2", "#db2777", "#65a30d", "#ea580c",
]

SEV_COLORS = {"critical": "#dc2626", "high": "#d97706", "medium": "#2563eb"}


def write_report(city: str, city_id: int, path: str) -> None:

    # ── Load spike records ────────────────────────────────────────────────────
    spike_rows = select_rows("price_spikes", filters={"city": city}, limit=2000)
    spike_rows = [r for r in spike_rows if (r.get("status") or "") != "closed"]

    # ── Load full price_history to draw lines ─────────────────────────────────
    history = select_rows("price_history", filters={"city_id": city_id}, limit=5000)

    # Group history by (locality, property_type)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in history:
        key = (
            str(row.get("locality") or ""),
            str(row.get("property_type") or ""),
        )
        groups[key].append(row)
    for key in groups:
        groups[key].sort(
            key=lambda r: str(r.get("period_date") or r.get("snapshot_date") or "")
        )

    if not spike_rows and not history:
        logger.warning(
            f"trend_report: no data for {city}. Run --trends first."
        )
        return

    # ── Pick spike localities for the line chart (up to 8) ───────────────────
    seen: list[tuple[str, str]] = []
    for rec in spike_rows:
        key = (str(rec.get("locality") or ""), str(rec.get("property_type") or ""))
        if key not in seen:
            seen.append(key)
        if len(seen) >= 8:
            break

    # Spike dates per locality key for red marker dots
    spike_dates_by_key: dict[tuple, list[str]] = {}
    for rec in spike_rows:
        k = (str(rec.get("locality") or ""), str(rec.get("property_type") or ""))
        d = str(rec.get("detected_date") or "")[:10]
        if d:
            spike_dates_by_key.setdefault(k, []).append(d)

    # ── Build line chart SVG ──────────────────────────────────────────────────
    width, height = 1100, 440
    pad_l, pad_r, pad_t, pad_b = 70, 30, 30, 50
    chart_w = width - pad_l - pad_r
    chart_h = height - pad_t - pad_b

    svg_elements: list[str] = []
    legend_items: list[str] = []

    # Collect all dates across selected localities
    all_dates: list[str] = []
    series_data: list[tuple[str, str, list[dict], str]] = []
    for idx, (locality, property_type) in enumerate(seen):
        snaps = groups.get((locality, property_type), [])
        color = LINE_COLORS[idx % len(LINE_COLORS)]
        series_data.append((locality, property_type, snaps, color))
        for s in snaps:
            d = str(s.get("period_date") or s.get("snapshot_date") or "")[:10]
            if d and d not in all_dates:
                all_dates.append(d)

    all_dates.sort()

    if all_dates and series_data:
        all_prices: list[float] = []
        for _, _, snaps, _ in series_data:
            for s in snaps:
                p = float(s.get("median_price_sqft") or s.get("avg_price_sqft") or 0)
                if p > 0:
                    all_prices.append(p)

        min_p = min(all_prices, default=0) * 0.95
        max_p = max(all_prices, default=1) * 1.05
        if max_p == min_p:
            max_p = min_p + 1

        def sx(date_str: str) -> float:
            i = all_dates.index(date_str) if date_str in all_dates else 0
            return pad_l + (i / max(len(all_dates) - 1, 1)) * chart_w

        def sy(price: float) -> float:
            return pad_t + chart_h - ((price - min_p) / (max_p - min_p)) * chart_h

        # Y-axis ticks + grid lines
        for step in range(5):
            tick_val = min_p + (max_p - min_p) * step / 4
            tick_y   = sy(tick_val)
            svg_elements.append(
                f"<line x1='{pad_l - 4}' y1='{tick_y:.1f}' x2='{pad_l + chart_w}' "
                f"y2='{tick_y:.1f}' stroke='#e2e8f0' stroke-width='1'/>"
                f"<line x1='{pad_l - 4}' y1='{tick_y:.1f}' x2='{pad_l}' y2='{tick_y:.1f}' "
                f"stroke='#8b95a7' stroke-width='1'/>"
                f"<text x='{pad_l - 6}' y='{tick_y + 4:.1f}' font-size='10' "
                f"text-anchor='end' fill='#596275'>₹{int(tick_val):,}</text>"
            )

        # X-axis date labels
        step_every = max(1, len(all_dates) // 12)
        for i, d in enumerate(all_dates):
            if i % step_every == 0:
                x = sx(d)
                svg_elements.append(
                    f"<text x='{x:.1f}' y='{pad_t + chart_h + 18}' font-size='10' "
                    f"text-anchor='end' fill='#596275' "
                    f"transform='rotate(-35 {x:.1f} {pad_t + chart_h + 18})'>{d[5:]}</text>"
                )

        # Axes
        svg_elements.append(
            f"<line stroke='#8b95a7' stroke-width='1' x1='{pad_l}' y1='{pad_t}' "
            f"x2='{pad_l}' y2='{pad_t + chart_h}'/>"
            f"<line stroke='#8b95a7' stroke-width='1' x1='{pad_l}' y1='{pad_t + chart_h}' "
            f"x2='{pad_l + chart_w}' y2='{pad_t + chart_h}'/>"
        )

        for locality, property_type, snaps, color in series_data:
            date_to_price: dict[str, float] = {}
            for s in snaps:
                d = str(s.get("period_date") or s.get("snapshot_date") or "")[:10]
                p = float(s.get("median_price_sqft") or s.get("avg_price_sqft") or 0)
                if d and p > 0:
                    date_to_price[d] = p

            points = [(d, date_to_price[d]) for d in all_dates if d in date_to_price]
            if len(points) < 2:
                continue

            polyline = " ".join(f"{sx(d):.1f},{sy(p):.1f}" for d, p in points)
            svg_elements.append(
                f"<polyline points='{polyline}' fill='none' stroke='{color}' "
                f"stroke-width='2' opacity='0.85'/>"
            )

            # Data point circles
            for d, p in points:
                svg_elements.append(
                    f"<circle cx='{sx(d):.1f}' cy='{sy(p):.1f}' r='3' fill='{color}' opacity='0.9'>"
                    f"<title>{html_mod.escape(locality)} | {d} | ₹{p:,.0f}/sqft</title></circle>"
                )

            # Red spike markers
            for spike_date in spike_dates_by_key.get((locality, property_type), []):
                if spike_date in date_to_price:
                    sp = date_to_price[spike_date]
                    svg_elements.append(
                        f"<circle cx='{sx(spike_date):.1f}' cy='{sy(sp):.1f}' r='6' "
                        f"fill='#dc2626' opacity='0.9'>"
                        f"<title>SPIKE: {html_mod.escape(locality)} on {spike_date} "
                        f"— ₹{sp:,.0f}/sqft</title></circle>"
                    )

            legend_items.append(
                f"<span style='display:inline-flex;align-items:center;gap:5px;font-size:13px'>"
                f"<span style='display:inline-block;width:24px;height:3px;background:{color};"
                f"border-radius:2px'></span>"
                f"{html_mod.escape(locality)} ({html_mod.escape(property_type)})</span>"
            )

    chart_svg = (
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'aria-label="Price trend line chart for {html_mod.escape(city)} spike localities">'
        f"{''.join(svg_elements)}</svg>"
        if svg_elements else
        f'<p style="color:#596275">No history data to chart yet — '
        f'run --snapshot for several days first.</p>'
    )

    # ── Spike table ───────────────────────────────────────────────────────────
    spike_table_rows: list[str] = []
    for rec in sorted(spike_rows, key=lambda r: abs(float(r.get("change_pct") or 0)), reverse=True):
        sev      = str(rec.get("severity") or "medium")
        color    = SEV_COLORS.get(sev, "#596275")
        chg      = float(rec.get("change_pct") or 0)
        arrow    = "▲" if chg > 0 else "▼"
        p_start  = float(rec.get("price_start") or 0)
        p_end    = float(rec.get("price_end") or 0)
        det_date = str(rec.get("detected_date") or "")
        spike_table_rows.append(
            "<tr>"
            f"<td>{html_mod.escape(str(rec.get('locality') or ''))}</td>"
            f"<td>{html_mod.escape(str(rec.get('property_type') or ''))}</td>"
            f"<td style='text-align:center'>{rec.get('window_days') or '—'}d</td>"
            f"<td style='text-align:right'>₹{p_start:,.0f}</td>"
            f"<td style='text-align:right'>₹{p_end:,.0f}</td>"
            f"<td style='text-align:right;color:{color};font-weight:500'>{arrow} {chg:+.1f}%</td>"
            f"<td style='color:{color}'>{sev}</td>"
            f"<td>{det_date[5:] if det_date else '—'}</td>"
            f"<td>{rec.get('status') or 'open'}</td>"
            "</tr>"
        )

    no_spikes = (
        ""
        if spike_table_rows else
        "<tr><td colspan='9' style='color:#596275;padding:16px'>"
        "No open spikes found — run --trends first.</td></tr>"
    )

    markup = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Price Trend Report — {html_mod.escape(city)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #172033; }}
    h1   {{ margin-bottom: 4px; }}
    .meta {{ color: #596275; font-size: 13px; margin-bottom: 20px; }}
    svg  {{ border: 1px solid #d8dee9; background: #fbfcfe; max-width: 100%;
             height: auto; display: block; margin-bottom: 12px; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 24px; }}
    .legend-spike {{ display: inline-flex; align-items: center; gap: 6px; font-size: 13px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #d8dee9; padding: 7px 10px; text-align: left; }}
    th {{ background: #eef2f7; color: #263244; position: sticky; top: 0; }}
    h2 {{ margin: 24px 0 10px; font-size: 16px; color: #263244; }}
  </style>
</head>
<body>
  <h1>Price Trend Report — {html_mod.escape(city)}</h1>
  <div class="meta">
    Report date: {date.today().isoformat()} &nbsp;|&nbsp;
    Open spikes: {len(spike_rows)} &nbsp;|&nbsp;
    Windows: {", ".join(str(w["days"]) + "d" for w in WINDOWS)}
  </div>

  <h2>Price history — spike localities (up to 8 shown)</h2>
  {chart_svg}
  <div class="legend">
    {''.join(legend_items)}
    <span class="legend-spike">
      <span style="display:inline-block;width:12px;height:12px;border-radius:50%;
                   background:#dc2626"></span>Spike date
    </span>
  </div>

  <h2>Open price spikes</h2>
  <table>
    <thead>
      <tr>
        <th>Locality</th><th>Property type</th><th>Window</th>
        <th>Price start</th><th>Price end</th><th>Change</th>
        <th>Severity</th><th>Detected</th><th>Status</th>
      </tr>
    </thead>
    <tbody>
      {''.join(spike_table_rows)}
      {no_spikes}
    </tbody>
  </table>
</body>
</html>"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markup, encoding="utf-8")
    logger.info("trend_report: report written → %s", output.resolve())
