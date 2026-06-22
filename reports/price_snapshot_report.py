"""
reports/price_snapshot_report.py
Reads today's price_history rows for a city and writes a self-contained HTML report.

Intentionally decoupled from PriceTracker — reads from the DB directly so it
can be run independently at any time after --snapshot has populated the table.

Run via main.py:
    python main.py --snapshot-report --city Nagpur
    python main.py --snapshot-report --city Akola
    python main.py --snapshot-report --city Amravati
"""

import html as html_mod
import logging
from datetime import date
from pathlib import Path

from db.connection import select_rows

logger = logging.getLogger(__name__)


def write_report(city: str, city_id: int, path: str) -> None:
    today = date.today().isoformat()

    rows = select_rows("price_history", filters={"city_id": city_id}, limit=3000)
    # Filter to today's snapshot only
    rows = [
        r for r in rows
        if str(r.get("period_date") or r.get("snapshot_date") or "")[:10] == today
    ]

    if not rows:
        logger.warning(
            f"price_snapshot_report: no price_history rows for {city} on {today}. "
            f"Run --snapshot first."
        )
        return

    acres_rows = [r for r in rows if (r.get("source") or "") == "99acres"]
    rera_rows  = [r for r in rows if (r.get("source") or "") == "rera"]

    # ── Bar chart — top 25 99acres localities by median price/sqft ───────────
    chart_rows = sorted(
        acres_rows, key=lambda r: r.get("median_price_sqft") or 0, reverse=True
    )[:25]

    width, height = 1100, 420
    pad_l, pad_r, pad_t, pad_b = 70, 30, 30, 90
    chart_w = width - pad_l - pad_r
    chart_h = height - pad_t - pad_b
    max_val  = max((r.get("median_price_sqft") or 0 for r in chart_rows), default=1)
    slot_w   = chart_w // max(len(chart_rows), 1)
    bar_w    = max(6, slot_w - 6)

    bars: list[str] = []
    for i, r in enumerate(chart_rows):
        val   = r.get("median_price_sqft") or 0
        bh    = (val / max_val) * chart_h
        x     = pad_l + i * slot_w
        y     = pad_t + chart_h - bh
        prop  = r.get("property_type") or ""
        color = "#2563eb" if "residential" in prop else "#d97706"
        loc   = r.get("locality") or ""
        tip   = html_mod.escape(
            f"{loc} | {prop} | ₹{val:,.0f}/sqft | {r.get('listing_count') or r.get('total_listings') or 0} listings"
        )
        bars.append(
            f"<rect x='{x}' y='{y:.1f}' width='{bar_w}' height='{bh:.1f}' "
            f"fill='{color}' opacity='0.78'><title>{tip}</title></rect>"
        )
        bars.append(
            f"<text x='{x + bar_w // 2}' y='{pad_t + chart_h + 12}' font-size='10' "
            f"text-anchor='end' fill='#465064' "
            f"transform='rotate(-45 {x + bar_w // 2} {pad_t + chart_h + 12})'>"
            f"{html_mod.escape(loc[:22])}</text>"
        )

    ticks: list[str] = []
    for step in range(5):
        tick_val = int(max_val * step / 4)
        tick_y   = pad_t + chart_h - (tick_val / max_val) * chart_h if max_val else pad_t
        ticks.append(
            f"<line x1='{pad_l - 4}' y1='{tick_y:.1f}' x2='{pad_l}' y2='{tick_y:.1f}' "
            f"stroke='#8b95a7' stroke-width='1'/>"
            f"<text x='{pad_l - 6}' y='{tick_y + 4:.1f}' font-size='10' "
            f"text-anchor='end' fill='#596275'>₹{tick_val:,}</text>"
        )

    # ── Table rows helper ─────────────────────────────────────────────────────
    def _table_rows(source_rows: list[dict]) -> str:
        out = []
        for r in sorted(source_rows, key=lambda x: x.get("median_price_sqft") or 0, reverse=True):
            out.append(
                "<tr>"
                f"<td>{html_mod.escape(str(r.get('locality') or ''))}</td>"
                f"<td>{html_mod.escape(str(r.get('property_type') or ''))}</td>"
                f"<td>{html_mod.escape(str(r.get('listing_type') or ''))}</td>"
                f"<td>₹{(r.get('median_price_sqft') or 0):,.0f}</td>"
                f"<td>₹{(r.get('avg_price_sqft') or 0):,.0f}</td>"
                f"<td>₹{(r.get('min_price_sqft') or 0):,.0f}</td>"
                f"<td>₹{(r.get('max_price_sqft') or 0):,.0f}</td>"
                f"<td>{r.get('listing_count') or r.get('total_listings') or 0}</td>"
                "</tr>"
            )
        return "".join(out) or "<tr><td colspan='8' style='color:#596275'>No rows.</td></tr>"

    thead = (
        "<tr><th>Locality</th><th>Property type</th><th>Listing type</th>"
        "<th>Median ₹/sqft</th><th>Avg ₹/sqft</th><th>Min</th><th>Max</th><th>Listings</th></tr>"
    )

    markup = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Price Snapshot — {html_mod.escape(city)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #172033; }}
    h1   {{ margin-bottom: 4px; }}
    .meta {{ color: #596275; font-size: 13px; margin-bottom: 20px; }}
    svg  {{ border: 1px solid #d8dee9; background: #fbfcfe; max-width: 100%;
             height: auto; display: block; margin-bottom: 12px; }}
    .legend {{ display: flex; gap: 20px; margin-bottom: 24px; font-size: 13px; }}
    .dot {{ display: inline-block; width: 11px; height: 11px; border-radius: 2px;
             margin-right: 5px; vertical-align: middle; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 32px; }}
    th, td {{ border-bottom: 1px solid #d8dee9; padding: 7px 10px; text-align: left; }}
    td:nth-child(n+4) {{ text-align: right; }}
    th {{ background: #eef2f7; color: #263244; position: sticky; top: 0; }}
    h2 {{ margin: 24px 0 10px; font-size: 16px; color: #263244; }}
  </style>
</head>
<body>
  <h1>Price Snapshot — {html_mod.escape(city)}</h1>
  <div class="meta">
    Date: {today} &nbsp;|&nbsp;
    99acres rows: {len(acres_rows)} &nbsp;|&nbsp;
    RERA rows: {len(rera_rows)}
  </div>

  <h2>Median ₹/sqft by locality — 99acres (top {len(chart_rows)})</h2>
  <svg viewBox="0 0 {width} {height}" role="img"
       aria-label="Bar chart of median price per sqft by locality for {html_mod.escape(city)}">
    <line stroke="#8b95a7" stroke-width="1"
          x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + chart_h}"/>
    <line stroke="#8b95a7" stroke-width="1"
          x1="{pad_l}" y1="{pad_t + chart_h}" x2="{pad_l + chart_w}" y2="{pad_t + chart_h}"/>
    {''.join(ticks)}
    {''.join(bars)}
  </svg>
  <div class="legend">
    <span><span class="dot" style="background:#2563eb"></span>Residential</span>
    <span><span class="dot" style="background:#d97706"></span>Commercial / other</span>
  </div>

  <h2>99acres locality breakdown</h2>
  <table><thead>{thead}</thead><tbody>{_table_rows(acres_rows)}</tbody></table>

  <h2>RERA actual transaction prices</h2>
  <table><thead>{thead}</thead><tbody>{_table_rows(rera_rows)}</tbody></table>
</body>
</html>"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markup, encoding="utf-8")
    logger.info("price_snapshot_report: report written → %s", output.resolve())
