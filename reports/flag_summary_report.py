"""
reports/flag_summary_report.py
Promoter-grouped flag risk brief — HTML report for all 6 cities.

For each promoter: complaint count, projects, flags by type,
severity distribution, avg confidence, risk score.

Usage:
    python reports/flag_summary_report.py
    python reports/flag_summary_report.py --cities Pune Nashik
    python reports/flag_summary_report.py --out reports/output/flag_summary.html

Output: Single self-contained HTML file.
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

SEVERITY_WEIGHT = {"critical": 4, "high": 3, "medium": 2, "low": 1}

FLAG_LABEL = {
    "repeated_complaints":         "Complaints",
    "complaint_velocity":          "Complaint velocity",
    "stalled_projects":            "Stalled project",
    "repeat_offender_new_project": "Repeat offender",
    "promoter_name_cluster":       "Name cluster",
    "locality_price_spike":        "Price spike",
    "price_trend_spike":           "Trend spike",
    "listing_price_outlier":       "Price outlier",
}

# Flag types that carry a promoter_name in evidence
PROMOTER_FLAG_TYPES = {
    "repeated_complaints",
    "complaint_velocity",
    "stalled_projects",
    "repeat_offender_new_project",
    "promoter_name_cluster",
}

DEFAULT_OUT = os.path.join(os.path.dirname(__file__), "output", "flag_summary.html")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ev(flag: dict) -> dict:
    raw = flag.get("evidence")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def _promoter(flag: dict) -> str | None:
    ev = _ev(flag)
    name = (
        ev.get("promoter_name")
        or ev.get("promoter")
        or (ev.get("cluster_names") or [None])[0]
    )
    return str(name).strip().title() if name else None


def _risk_score(flags: list[dict]) -> float:
    if not flags:
        return 0.0
    raw = sum(
        float(f.get("confidence") or 0) * SEVERITY_WEIGHT.get(
            (f.get("severity") or "low").lower(), 1
        )
        for f in flags
    )
    return round(min(100.0, raw / (len(flags) * 4)), 1)


# ── Data loading ──────────────────────────────────────────────────────────────

def load_city_data(city_id: int) -> dict:
    flags    = select_rows("suspicious_flags",
                           filters={"city_id": city_id, "status": "open"},
                           limit=2000) or []
    projects = select_rows("rera_projects",
                           filters={"city_id": city_id},
                           limit=2000) or []

    # Project lookup: promoter_name (title) → list of projects
    proj_by_promoter: dict[str, list[dict]] = defaultdict(list)
    for p in projects:
        name = (p.get("promoter_name") or "").strip().title()
        if name:
            proj_by_promoter[name].append(p)

    return {"flags": flags, "proj_by_promoter": dict(proj_by_promoter)}


# ── Aggregation ───────────────────────────────────────────────────────────────

def build_promoter_table(data: dict) -> list[dict]:
    """
    Groups flags by promoter. Returns list sorted by risk_score desc.
    Each row:
      promoter, total_complaints, projects (list), project_count,
      flags (list), flag_count, flag_types (list), max_severity,
      avg_conf, risk_score, active_projects, lapsed_projects
    """
    promoter_flags: dict[str, list[dict]] = defaultdict(list)

    for flag in data["flags"]:
        if flag.get("flag_type") in PROMOTER_FLAG_TYPES:
            p = _promoter(flag)
            if p:
                promoter_flags[p].append(flag)

    rows = []
    for promoter, flags in promoter_flags.items():
        projects  = data["proj_by_promoter"].get(promoter, [])

        # Complaint count — prefer evidence total_complaints, fall back to
        # summing rera_projects.complaint_count
        ev_complaints = max(
            (int(_ev(f).get("total_complaints") or 0) for f in flags),
            default=0
        )
        rera_complaints = sum(int(p.get("complaint_count") or 0) for p in projects)
        total_complaints = max(ev_complaints, rera_complaints)

        severities   = [f.get("severity", "low").lower() for f in flags]
        max_sev      = max(severities, key=lambda s: SEVERITY_WEIGHT.get(s, 1))
        avg_conf     = round(sum(float(f.get("confidence") or 0) for f in flags) / len(flags), 1)
        flag_types   = sorted({FLAG_LABEL.get(f.get("flag_type", ""), f.get("flag_type", "")) for f in flags})
        active_proj  = sum(1 for p in projects if (p.get("rera_status") or "").lower() == "active")
        lapsed_proj  = sum(1 for p in projects if (p.get("rera_status") or "").lower() in ("lapsed", "de-registered"))

        rows.append({
            "promoter":         promoter,
            "total_complaints": total_complaints,
            "projects":         projects,
            "project_count":    len(projects),
            "active_projects":  active_proj,
            "lapsed_projects":  lapsed_proj,
            "flags":            flags,
            "flag_count":       len(flags),
            "flag_types":       flag_types,
            "max_severity":     max_sev,
            "avg_conf":         avg_conf,
            "risk_score":       _risk_score(flags),
        })

    rows.sort(key=lambda r: (-r["risk_score"], -r["total_complaints"], -r["flag_count"]))
    return rows


def city_summary(data: dict, rows: list[dict]) -> dict:
    flags = data["flags"]
    confs = [float(f.get("confidence") or 0) for f in flags]
    return {
        "total_flags":     len(flags),
        "flagged_promoters": len(rows),
        "avg_confidence":  round(sum(confs) / len(confs), 1) if confs else 0,
        "top_promoter":    rows[0]["promoter"] if rows else "—",
        "top_risk":        rows[0]["risk_score"] if rows else 0,
        "top_complaints":  rows[0]["total_complaints"] if rows else 0,
    }


# ── HTML ──────────────────────────────────────────────────────────────────────

_CSS = """
:root {
  --navy:#1B2A4A; --navy2:#243556; --accent:#2E6FE6;
  --bg:#F7F8FA; --card:#FFFFFF; --border:#E5E7EB;
  --text:#111827; --sub:#6B7280;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
     background:var(--bg);color:var(--text);font-size:13px}
header{background:var(--navy);color:#fff;padding:20px 32px}
header h1{font-size:18px;font-weight:600;letter-spacing:.02em}
header p{font-size:12px;color:#9FB3D8;margin-top:4px}
.container{max-width:1200px;margin:0 auto;padding:24px 24px 48px}
.tabs{display:flex;gap:4px;border-bottom:2px solid var(--border);
      margin-bottom:24px;overflow-x:auto}
.tab{padding:8px 16px;cursor:pointer;font-size:12px;font-weight:500;
     color:var(--sub);border-bottom:2px solid transparent;
     margin-bottom:-2px;white-space:nowrap}
.tab:hover{color:var(--accent)}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.city-panel{display:none}
.city-panel.active{display:block}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
       gap:12px;margin-bottom:24px}
.card{background:var(--card);border:1px solid var(--border);
      border-radius:8px;padding:14px 16px}
.card .label{font-size:11px;color:var(--sub);text-transform:uppercase;
             letter-spacing:.04em;margin-bottom:6px}
.card .value{font-size:22px;font-weight:600;line-height:1}
.card .sub{font-size:11px;color:var(--sub);margin-top:4px}
.table-wrap{background:var(--card);border:1px solid var(--border);
            border-radius:8px;overflow:hidden}
table{width:100%;border-collapse:collapse}
thead th{background:var(--navy);color:#fff;padding:10px 12px;
         font-size:11px;font-weight:500;text-align:left;
         cursor:pointer;user-select:none;white-space:nowrap}
thead th:hover{background:var(--navy2)}
thead th .arr{opacity:.4;margin-left:4px}
thead th.asc .arr::after{content:'▲';opacity:1}
thead th.desc .arr::after{content:'▼';opacity:1}
thead th .arr::after{content:'⬍'}
tbody tr{border-bottom:1px solid var(--border)}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:#F0F4FF}
td{padding:9px 12px;vertical-align:middle}
td.rank{color:var(--sub);font-weight:600;font-size:12px;width:36px}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;
       font-size:11px;font-weight:600;white-space:nowrap}
.b-critical{background:#FDEDEC;color:#C0392B}
.b-high{background:#FEF5E7;color:#D35400}
.b-medium{background:#FEFDE7;color:#9A7D0A}
.b-low{background:#F2F3F4;color:#717D7E}
.pill{display:inline-block;background:#EBF3FD;color:#1A5FB4;
      font-size:10px;padding:1px 7px;border-radius:10px;margin:1px 2px;white-space:nowrap}
.cpill{display:inline-block;background:#FDEDEC;color:#C0392B;
       font-size:10px;padding:1px 7px;border-radius:10px;font-weight:600}
.risk-bar{display:flex;align-items:center;gap:8px}
.bar-outer{width:60px;height:6px;background:var(--border);border-radius:3px}
.bar-inner{height:100%;border-radius:3px}
.risk-val{font-size:12px;font-weight:600;min-width:32px}
.no-data{padding:40px;text-align:center;color:var(--sub)}
.footer{text-align:center;color:var(--sub);font-size:11px;margin-top:32px}
"""

_JS = """
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.city-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById(tab.dataset.city).classList.add('active');
  });
});
document.querySelectorAll('table').forEach(table => {
  const val = (tr, i) => tr.children[i]?.dataset.val ?? tr.children[i]?.innerText ?? '';
  const cmp = (i, asc) => (a, b) => {
    const va = val(a,i), vb = val(b,i);
    const na = parseFloat(va), nb = parseFloat(vb);
    if (!isNaN(na)&&!isNaN(nb)) return asc ? na-nb : nb-na;
    return asc ? va.localeCompare(vb) : vb.localeCompare(va);
  };
  table.querySelectorAll('thead th').forEach((th, i) => {
    let asc = false;
    th.addEventListener('click', () => {
      table.querySelectorAll('thead th').forEach(t => t.classList.remove('asc','desc'));
      asc = !asc;
      th.classList.add(asc ? 'asc' : 'desc');
      const tb = table.querySelector('tbody');
      [...tb.rows].sort(cmp(i,asc)).forEach(r => tb.appendChild(r));
      [...tb.rows].forEach((r,idx) => {
        if (r.cells[0]?.classList.contains('rank')) r.cells[0].innerText = idx+1;
      });
    });
  });
});
"""


def _sev_badge(sev: str) -> str:
    cls = f"b-{sev.lower()}" if sev.lower() in SEVERITY_WEIGHT else "b-low"
    return f'<span class="badge {cls}">{sev.upper()}</span>'


def _risk_bar_html(score: float) -> str:
    pct = min(100, score)
    colour = (
        "#C0392B" if pct >= 75 else
        "#E67E22" if pct >= 50 else
        "#D4AC0D" if pct >= 25 else
        "#1D9E75"
    )
    return (
        f'<div class="risk-bar">'
        f'<div class="bar-outer"><div class="bar-inner" '
        f'style="width:{pct}%;background:{colour}"></div></div>'
        f'<span class="risk-val" style="color:{colour}">{score}</span>'
        f'</div>'
    )


def render_city_panel(city: dict, rows: list[dict], summary: dict) -> str:
    cards = f"""
    <div class="cards">
      <div class="card">
        <div class="label">Total Flags</div>
        <div class="value">{summary['total_flags']}</div>
        <div class="sub">avg confidence {summary['avg_confidence']}%</div>
      </div>
      <div class="card">
        <div class="label">Flagged Promoters</div>
        <div class="value">{summary['flagged_promoters']}</div>
        <div class="sub">with open flags</div>
      </div>
      <div class="card">
        <div class="label">Highest Risk</div>
        <div class="value" style="font-size:15px">{summary['top_promoter']}</div>
        <div class="sub">score {summary['top_risk']} · {summary['top_complaints']} complaints</div>
      </div>
    </div>"""

    if not rows:
        table = '<div class="no-data">No promoter-level flags found for this city.</div>'
    else:
        tbody = []
        for i, r in enumerate(rows, 1):
            pills     = " ".join(f'<span class="pill">{t}</span>' for t in r["flag_types"])
            complaint = (
                f'<span class="cpill">{r["total_complaints"]}</span>'
                if r["total_complaints"] > 0 else "—"
            )
            proj_cell = (
                f'{r["active_projects"]} active'
                + (f', <span style="color:#C0392B">{r["lapsed_projects"]} lapsed</span>'
                   if r["lapsed_projects"] else "")
            ) if r["project_count"] else "—"

            tbody.append(f"""
            <tr>
              <td class="rank" data-val="{i}">{i}</td>
              <td><strong>{r['promoter']}</strong></td>
              <td data-val="{r['total_complaints']}">{complaint}</td>
              <td data-val="{r['project_count']}">{proj_cell}</td>
              <td data-val="{r['flag_count']}">{r['flag_count']}</td>
              <td>{pills}</td>
              <td data-val="{list(SEVERITY_WEIGHT.keys()).index(r['max_severity'])
                             if r['max_severity'] in SEVERITY_WEIGHT else 0}">{_sev_badge(r['max_severity'])}</td>
              <td data-val="{r['avg_conf']}">{r['avg_conf']}%</td>
              <td data-val="{r['risk_score']}">{_risk_bar_html(r['risk_score'])}</td>
            </tr>""")

        table = f"""
        <div class="table-wrap">
          <table>
            <thead><tr>
              <th>#<span class="arr"></span></th>
              <th>Promoter<span class="arr"></span></th>
              <th>Complaints<span class="arr"></span></th>
              <th>Projects<span class="arr"></span></th>
              <th>Flags<span class="arr"></span></th>
              <th>Flag types</th>
              <th>Max severity<span class="arr"></span></th>
              <th>Avg conf<span class="arr"></span></th>
              <th>Risk score<span class="arr"></span></th>
            </tr></thead>
            <tbody>{''.join(tbody)}</tbody>
          </table>
        </div>"""

    return f'<div class="city-panel" id="city-{city["id"]}">{cards}{table}</div>'


def render_html(city_panels: list[tuple[dict, str]], generated_at: str) -> str:
    tabs = panels = ""
    for i, (city, html) in enumerate(city_panels):
        active = "active" if i == 0 else ""
        tabs  += f'<div class="tab {active}" data-city="city-{city["id"]}">{city["name"]}</div>'
        panels += html.replace(
            f'class="city-panel" id="city-{city["id"]}"',
            f'class="city-panel {active}" id="city-{city["id"]}"'
        ) if i == 0 else html

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Homesage — Flag Summary Report</title>
  <style>{_CSS}</style>
</head>
<body>
  <header>
    <h1>Homesage — Promoter Flag Summary</h1>
    <p>Maharashtra Real Estate Intelligence · Generated {generated_at}</p>
  </header>
  <div class="container">
    <div class="tabs">{tabs}</div>
    {panels}
    <div class="footer">Homesage Intelligence Platform · MahaRERA + 99acres · {generated_at}</div>
  </div>
  <script>{_JS}</script>
</body>
</html>"""


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate promoter flag summary HTML report")
    parser.add_argument("--cities", nargs="+", default=None)
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    cities = ALL_CITIES
    if args.cities:
        names  = {c.lower() for c in args.cities}
        cities = [c for c in ALL_CITIES if c["name"].lower() in names]
        if not cities:
            logger.error(f"No matching cities: {args.cities}")
            return

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    generated_at = datetime.now().strftime("%d %b %Y, %H:%M")
    city_panels  = []

    for city in cities:
        logger.info(f"Processing {city['name']} (id={city['id']})...")
        data    = load_city_data(city["id"])
        rows    = build_promoter_table(data)
        summary = city_summary(data, rows)
        panel   = render_city_panel(city, rows, summary)
        city_panels.append((city, panel))
        logger.info(f"  {city['name']}: {summary['flagged_promoters']} flagged promoters, "
                    f"{summary['total_flags']} total flags")

    html = render_html(city_panels, generated_at)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"Report written → {args.out}")


if __name__ == "__main__":
    main()
