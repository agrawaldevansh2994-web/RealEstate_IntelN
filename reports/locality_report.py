"""
reports/locality_report.py
Generates a locality × flag risk matrix HTML report for all 6 cities.

Usage:
    python reports/locality_report.py
    python reports/locality_report.py --cities Pune Nashik
    python reports/locality_report.py --out reports/output/locality_report.html

Output: Single self-contained HTML file (no external dependencies).
"""

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

from db.connection import select_rows

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

ALL_CITIES = [
    {"id": 1,  "name": "Akola"},
    {"id": 2,  "name": "Nagpur"},
    {"id": 3,  "name": "Pune"},
    {"id": 5,  "name": "Nashik"},
    {"id": 9,  "name": "Amravati"},
    {"id": 10, "name": "Aurangabad"},
]

# Flag types that store locality in evidence
LOCALITY_FLAG_TYPES = {"locality_price_spike", "price_trend_spike", "listing_price_outlier"}

SEVERITY_WEIGHT  = {"critical": 4, "high": 3, "medium": 2, "low": 1}
SEVERITY_COLOUR  = {"critical": "#C0392B", "high": "#E67E22", "medium": "#F1C40F", "low": "#95A5A6"}
SEVERITY_BG      = {"critical": "#FDEDEC", "high": "#FEF5E7", "medium": "#FEFDE7", "low": "#F2F3F4"}

FLAG_LABEL = {
    "locality_price_spike":       "Price spike",
    "price_trend_spike":          "Trend spike",
    "listing_price_outlier":      "Price outlier",
    "repeated_complaints":        "Complaints",
    "complaint_velocity":         "Complaint velocity",
    "stalled_projects":           "Stalled project",
    "repeat_offender_new_project":"Repeat offender",
    "promoter_name_cluster":      "Name cluster",
}

DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "output", "locality_report.html")


# ── Data loading ──────────────────────────────────────────────────────────────

def _safe_evidence(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _extract_locality(flag: dict) -> str | None:
    """Pull locality string from flag evidence; normalise to Title Case."""
    ev = _safe_evidence(flag.get("evidence"))
    raw = ev.get("locality") or ev.get("normalized_locality") or ""
    if not raw:
        return None
    return raw.strip().title()


def load_city_data(city_id: int) -> dict:
    """
    Returns:
      {
        "flags":          list[dict],       # all open suspicious_flags
        "listings_map":   {locality: count},
        "psqft_map":      {locality: float}, # latest snapshot avg psqft
        "snapshot_count": int,               # number of distinct snapshot dates
      }
    """
    flags = select_rows("suspicious_flags",
                        filters={"city_id": city_id, "status": "open"},
                        limit=2000) or []

    listings_raw = select_rows("listings",
                               filters={"city_id": city_id, "listing_status": "active"},
                               limit=5000) or []

    ph_raw = select_rows("price_history",
                         filters={"city_id": city_id, "source": "99acres"},
                         limit=5000) or []

    # Listings per locality
    listings_map: dict[str, int] = defaultdict(int)
    for l in listings_raw:
        loc = (l.get("locality") or "").strip().title()
        if loc:
            listings_map[loc] += 1

    # Latest psqft per locality (max snapshot_date wins)
    latest_snap: dict[str, str] = {}
    psqft_map: dict[str, float] = {}
    for row in ph_raw:
        loc  = (row.get("locality") or "").strip().title()
        snap = str(row.get("snapshot_date") or row.get("period_date") or "")
        psqft = row.get("avg_price_sqft")
        if not loc or not snap or not psqft:
            continue
        if loc not in latest_snap or snap > latest_snap[loc]:
            latest_snap[loc] = snap
            psqft_map[loc] = float(psqft)

    snapshot_count = len({
        str(r.get("snapshot_date") or r.get("period_date") or "")
        for r in ph_raw
        if r.get("snapshot_date") or r.get("period_date")
    })

    return {
        "flags":          flags,
        "listings_map":   dict(listings_map),
        "psqft_map":      psqft_map,
        "snapshot_count": snapshot_count,
    }


# ── Aggregation ───────────────────────────────────────────────────────────────

def _risk_score(flags: list[dict]) -> float:
    """Weighted confidence × severity sum, normalised to 0–100."""
    if not flags:
        return 0.0
    raw = sum(
        float(f.get("confidence") or 0) * SEVERITY_WEIGHT.get(
            (f.get("severity") or "low").lower(), 1
        )
        for f in flags
    )
    # Max possible per flag = 100 conf × 4 (critical) = 400
    normalised = min(100.0, raw / (len(flags) * 4))
    return round(normalised, 1)


def build_locality_table(city_id: int, data: dict) -> list[dict]:
    """
    Returns list of locality rows sorted by risk_score desc.
    Each row:
      locality, listings, psqft, flag_count, flag_types (set),
      max_severity, avg_confidence, risk_score, flags (list)
    """
    locality_flags: dict[str, list[dict]] = defaultdict(list)

    for flag in data["flags"]:
        if flag.get("flag_type") in LOCALITY_FLAG_TYPES:
            loc = _extract_locality(flag)
            if loc:
                locality_flags[loc].append(flag)

    rows = []
    for loc, flags in locality_flags.items():
        severities = [f.get("severity", "low").lower() for f in flags]
        max_sev = max(severities, key=lambda s: SEVERITY_WEIGHT.get(s, 1))
        avg_conf = round(
            sum(float(f.get("confidence") or 0) for f in flags) / len(flags), 1
        )
        flag_types = sorted({FLAG_LABEL.get(f.get("flag_type", ""), f.get("flag_type", "")) for f in flags})

        rows.append({
            "locality":     loc,
            "listings":     data["listings_map"].get(loc, 0),
            "psqft":        data["psqft_map"].get(loc),
            "flag_count":   len(flags),
            "flag_types":   flag_types,
            "max_severity": max_sev,
            "avg_conf":     avg_conf,
            "risk_score":   _risk_score(flags),
            "flags":        flags,
        })

    rows.sort(key=lambda r: (-r["risk_score"], -r["flag_count"]))
    return rows


def city_summary(data: dict, rows: list[dict]) -> dict:
    """Top-level city metrics for summary cards."""
    all_open   = data["flags"]
    total_conf = [float(f.get("confidence") or 0) for f in all_open]
    avg_psqft_vals = list(data["psqft_map"].values())
    return {
        "total_flags":       len(all_open),
        "flagged_localities": len(rows),
        "avg_confidence":    round(sum(total_conf) / len(total_conf), 1) if total_conf else 0,
        "top_locality":      rows[0]["locality"] if rows else "—",
        "top_risk_score":    rows[0]["risk_score"] if rows else 0,
        "snapshot_days":     data["snapshot_count"],
        "city_avg_psqft":    round(sum(avg_psqft_vals) / len(avg_psqft_vals)) if avg_psqft_vals else None,
    }


# ── HTML rendering ────────────────────────────────────────────────────────────

_CSS = """
:root {
  --navy:   #1B2A4A;
  --navy2:  #243556;
  --accent: #2E6FE6;
  --red:    #C0392B;
  --orange: #E67E22;
  --amber:  #D4AC0D;
  --green:  #1D9E75;
  --gray:   #6B7280;
  --bg:     #F7F8FA;
  --card:   #FFFFFF;
  --border: #E5E7EB;
  --text:   #111827;
  --sub:    #6B7280;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: var(--bg); color: var(--text); font-size: 13px; }
header { background: var(--navy); color: #fff; padding: 20px 32px; }
header h1 { font-size: 18px; font-weight: 600; letter-spacing: .02em; }
header p  { font-size: 12px; color: #9FB3D8; margin-top: 4px; }
.container { max-width: 1200px; margin: 0 auto; padding: 24px 24px 48px; }

/* City tabs */
.tabs { display: flex; gap: 4px; border-bottom: 2px solid var(--border);
        margin-bottom: 24px; overflow-x: auto; }
.tab { padding: 8px 16px; cursor: pointer; font-size: 12px; font-weight: 500;
       color: var(--sub); border-bottom: 2px solid transparent;
       margin-bottom: -2px; white-space: nowrap; transition: color .15s; }
.tab:hover  { color: var(--accent); }
.tab.active { color: var(--accent); border-bottom-color: var(--accent); }
.city-panel { display: none; }
.city-panel.active { display: block; }

/* Summary cards */
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
         gap: 12px; margin-bottom: 24px; }
.card { background: var(--card); border: 1px solid var(--border);
        border-radius: 8px; padding: 14px 16px; }
.card .label { font-size: 11px; color: var(--sub); text-transform: uppercase;
               letter-spacing: .04em; margin-bottom: 6px; }
.card .value { font-size: 22px; font-weight: 600; line-height: 1; }
.card .sub   { font-size: 11px; color: var(--sub); margin-top: 4px; }

/* Table */
.table-wrap { background: var(--card); border: 1px solid var(--border);
              border-radius: 8px; overflow: hidden; }
table { width: 100%; border-collapse: collapse; }
thead th { background: var(--navy); color: #fff; padding: 10px 12px;
           font-size: 11px; font-weight: 500; text-align: left;
           cursor: pointer; user-select: none; white-space: nowrap; }
thead th:hover { background: var(--navy2); }
thead th .sort-arrow { opacity: .4; margin-left: 4px; }
thead th.asc  .sort-arrow::after { content: '▲'; opacity: 1; }
thead th.desc .sort-arrow::after { content: '▼'; opacity: 1; }
thead th .sort-arrow::after      { content: '⬍'; }
tbody tr { border-bottom: 1px solid var(--border); }
tbody tr:last-child { border-bottom: none; }
tbody tr:hover { background: #F0F4FF; }
td { padding: 9px 12px; vertical-align: middle; }
td.rank { color: var(--sub); font-weight: 600; font-size: 12px; width: 40px; }

/* Badges */
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px;
         font-size: 11px; font-weight: 600; white-space: nowrap; }
.b-critical { background: #FDEDEC; color: #C0392B; }
.b-high     { background: #FEF5E7; color: #D35400; }
.b-medium   { background: #FEFDE7; color: #9A7D0A; }
.b-low      { background: #F2F3F4; color: #717D7E; }
.flag-pill  { display: inline-block; background: #EBF3FD; color: #1A5FB4;
              font-size: 10px; padding: 1px 7px; border-radius: 10px;
              margin: 1px 2px; white-space: nowrap; }

/* Risk bar */
.risk-bar { display: flex; align-items: center; gap: 8px; }
.bar-outer { width: 60px; height: 6px; background: var(--border); border-radius: 3px; }
.bar-inner { height: 100%; border-radius: 3px; }
.risk-val  { font-size: 12px; font-weight: 600; min-width: 32px; }

.no-data { padding: 40px; text-align: center; color: var(--sub); }
.footer  { text-align: center; color: var(--sub); font-size: 11px; margin-top: 32px; }
"""

_JS = """
// Tab switching
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.city-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(tab.dataset.city).classList.add('active');
  });
});

// Table sorting
document.querySelectorAll('table').forEach(table => {
  const getCellVal = (tr, idx) => tr.children[idx]?.dataset.val ?? tr.children[idx]?.innerText ?? '';
  const comparer = (idx, asc) => (a, b) => {
    const va = getCellVal(a, idx), vb = getCellVal(b, idx);
    const na = parseFloat(va), nb = parseFloat(vb);
    if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
    return asc ? va.localeCompare(vb) : vb.localeCompare(va);
  };
  table.querySelectorAll('thead th').forEach((th, i) => {
    let asc = false;
    th.addEventListener('click', () => {
      table.querySelectorAll('thead th').forEach(t => t.classList.remove('asc','desc'));
      asc = !asc;
      th.classList.add(asc ? 'asc' : 'desc');
      const tbody = table.querySelector('tbody');
      [...tbody.rows].sort(comparer(i, asc)).forEach(r => tbody.appendChild(r));
      // Re-number rank column
      [...tbody.rows].forEach((r, idx) => {
        if (r.cells[0]?.classList.contains('rank')) r.cells[0].innerText = idx + 1;
      });
    });
  });
});
"""


def _sev_badge(sev: str) -> str:
    cls = f"b-{sev.lower()}" if sev.lower() in SEVERITY_WEIGHT else "b-low"
    return f'<span class="badge {cls}">{sev.upper()}</span>'


def _risk_bar(score: float) -> str:
    pct = min(100, score)
    if pct >= 75:   colour = "#C0392B"
    elif pct >= 50: colour = "#E67E22"
    elif pct >= 25: colour = "#D4AC0D"
    else:           colour = "#1D9E75"
    return (
        f'<div class="risk-bar">'
        f'<div class="bar-outer"><div class="bar-inner" style="width:{pct}%;background:{colour}"></div></div>'
        f'<span class="risk-val" style="color:{colour}">{score}</span>'
        f'</div>'
    )


def render_city_panel(city: dict, rows: list[dict], summary: dict) -> str:
    cid = f"city-{city['id']}"
    name = city["name"]

    # Summary cards
    cards_html = f"""
    <div class="cards">
      <div class="card">
        <div class="label">Total Flags</div>
        <div class="value">{summary['total_flags']}</div>
        <div class="sub">avg confidence {summary['avg_confidence']}%</div>
      </div>
      <div class="card">
        <div class="label">Flagged Localities</div>
        <div class="value">{summary['flagged_localities']}</div>
        <div class="sub">with price / trend signals</div>
      </div>
      <div class="card">
        <div class="label">Highest Risk</div>
        <div class="value" style="font-size:16px">{summary['top_locality']}</div>
        <div class="sub">risk score {summary['top_risk_score']}</div>
      </div>
      <div class="card">
        <div class="label">City Avg ₹/sqft</div>
        <div class="value">{"₹{:,}".format(int(summary['city_avg_psqft'])) if summary['city_avg_psqft'] else "—"}</div>
        <div class="sub">{summary['snapshot_days']} snapshot days</div>
      </div>
    </div>"""

    if not rows:
        table_html = '<div class="no-data">No locality-level flags found for this city.</div>'
    else:
        tbody_rows = []
        for i, row in enumerate(rows, 1):
            pills = " ".join(f'<span class="flag-pill">{t}</span>' for t in row["flag_types"])
            psqft = f"₹{int(row['psqft']):,}" if row["psqft"] else "—"
            tbody_rows.append(f"""
            <tr>
              <td class="rank" data-val="{i}">{i}</td>
              <td><strong>{row['locality']}</strong></td>
              <td data-val="{row['listings']}">{row['listings']}</td>
              <td data-val="{row['psqft'] or 0}">{psqft}</td>
              <td data-val="{row['flag_count']}">{row['flag_count']}</td>
              <td>{pills}</td>
              <td data-val="{list(SEVERITY_WEIGHT.keys()).index(row['max_severity']) if row['max_severity'] in SEVERITY_WEIGHT else 0}">{_sev_badge(row['max_severity'])}</td>
              <td data-val="{row['avg_conf']}">{row['avg_conf']}%</td>
              <td data-val="{row['risk_score']}">{_risk_bar(row['risk_score'])}</td>
            </tr>""")

        table_html = f"""
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>#<span class="sort-arrow"></span></th>
                <th>Locality<span class="sort-arrow"></span></th>
                <th>Listings<span class="sort-arrow"></span></th>
                <th>Avg ₹/sqft<span class="sort-arrow"></span></th>
                <th>Flags<span class="sort-arrow"></span></th>
                <th>Signal types</th>
                <th>Max severity<span class="sort-arrow"></span></th>
                <th>Avg conf<span class="sort-arrow"></span></th>
                <th>Risk score<span class="sort-arrow"></span></th>
              </tr>
            </thead>
            <tbody>
              {''.join(tbody_rows)}
            </tbody>
          </table>
        </div>"""

    return f'<div class="city-panel" id="{cid}">{cards_html}{table_html}</div>'


def render_html(city_panels: list[tuple[dict, str]], generated_at: str) -> str:
    tabs = ""
    panels = ""
    for i, (city, panel_html) in enumerate(city_panels):
        active = "active" if i == 0 else ""
        tabs   += f'<div class="tab {active}" data-city="city-{city["id"]}">{city["name"]}</div>'
        panels += panel_html.replace(
            f'class="city-panel" id="city-{city["id"]}"',
            f'class="city-panel {active}" id="city-{city["id"]}"'
        ) if i == 0 else panel_html

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Homesage — Locality Risk Report</title>
  <style>{_CSS}</style>
</head>
<body>
  <header>
    <h1>Homesage — Locality Risk Matrix</h1>
    <p>Maharashtra Real Estate Intelligence · Generated {generated_at}</p>
  </header>
  <div class="container">
    <div class="tabs">{tabs}</div>
    {panels}
    <div class="footer">Homesage Intelligence Platform · Data sourced from MahaRERA + 99acres · {generated_at}</div>
  </div>
  <script>{_JS}</script>
</body>
</html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate locality risk HTML report")
    parser.add_argument("--cities", nargs="+", default=None,
                        help="City names to include (default: all 6)")
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help=f"Output path (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    cities = ALL_CITIES
    if args.cities:
        names = {c.lower() for c in args.cities}
        cities = [c for c in ALL_CITIES if c["name"].lower() in names]
        if not cities:
            logger.error(f"No matching cities found for: {args.cities}")
            return

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    generated_at = datetime.now().strftime("%d %b %Y, %H:%M")
    city_panels  = []

    for city in cities:
        logger.info(f"Processing {city['name']} (id={city['id']})...")
        data    = load_city_data(city["id"])
        rows    = build_locality_table(city["id"], data)
        summary = city_summary(data, rows)
        panel   = render_city_panel(city, rows, summary)
        city_panels.append((city, panel))
        logger.info(f"  {city['name']}: {summary['flagged_localities']} flagged localities, "
                    f"{summary['total_flags']} total flags")

    html = render_html(city_panels, generated_at)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"Report written → {args.out}")


if __name__ == "__main__":
    main()
