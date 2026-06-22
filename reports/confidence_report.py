"""
reports/confidence_report.py
Reads scored suspicious_flags for a city and writes a self-contained HTML report.

Shows confidence distribution histogram, per-flag-type breakdown, and full flag table.
Intentionally decoupled from ConfidenceScorer — reads from the DB directly.

Run via main.py:
    python main.py --score-report --city Nagpur
    python main.py --score-report --city Akola
    python main.py --score-report --city Amravati
"""

import html as html_mod
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from db.connection import select_rows

logger = logging.getLogger(__name__)

# Mirrors BASE_SCORES order for the breakdown table
FLAG_TYPE_ORDER = [
    "cross_source_promoter_risk",
    "rera_escrow_deficit",
    "complaint_velocity",
    "repeated_complaints",
    "repeat_offender_new_project",
    "stalled_projects",
    "locality_convergence",
    "promoter_name_cluster",
    "name_reuse_serial",
    "locality_price_spike",
    "price_trend_spike",
    "dbscan_listing_anomaly",
    "listing_price_outlier",
    "ghost_promoter",
]

TIER_COLORS = {
    "HIGH CONFIDENCE":     "#16a34a",
    "MEDIUM CONFIDENCE":   "#2563eb",
    "LOW CONFIDENCE":      "#d97706",
    "VERY LOW CONFIDENCE": "#dc2626",
}

BUCKET_COLORS = ["#dc2626", "#f97316", "#d97706", "#2563eb", "#16a34a"]
BUCKET_LABELS = ["0–20", "20–40", "40–60", "60–80", "80–100"]


def write_report(city: str, city_id: int, path: str) -> None:

    all_flags = select_rows(
        "suspicious_flags", filters={"city_id": city_id}, limit=5000
    )
    open_flags = [
        f for f in all_flags
        if str(f.get("status") or "").strip().lower() in ("", "open")
    ]

    if not open_flags:
        logger.warning(
            f"confidence_report: no open flags for {city}. "
            f"Run --detect --patterns --score first."
        )
        return

    scored = [f for f in open_flags if f.get("confidence") is not None]
    unscored_count = len(open_flags) - len(scored)

    # ── Confidence buckets for histogram ─────────────────────────────────────
    buckets = [0, 0, 0, 0, 0]   # 0-20, 20-40, 40-60, 60-80, 80-100
    for f in scored:
        c = int(f.get("confidence") or 0)
        idx = min(c // 20, 4)
        buckets[idx] += 1

    actionable = [f for f in scored if int(f.get("confidence") or 0) >= 60]
    avg_conf = round(sum(int(f.get("confidence") or 0) for f in scored) / max(len(scored), 1), 1)

    # ── Histogram SVG ─────────────────────────────────────────────────────────
    width, height = 700, 300
    pad_l, pad_r, pad_t, pad_b = 60, 30, 30, 50
    chart_w = width - pad_l - pad_r
    chart_h = height - pad_t - pad_b
    n_buckets = len(buckets)
    max_count = max(buckets) if any(buckets) else 1
    slot_w = chart_w // n_buckets
    bar_w  = slot_w - 10

    hist_bars: list[str] = []
    for i, (count, color, label) in enumerate(zip(buckets, BUCKET_COLORS, BUCKET_LABELS)):
        bh  = (count / max_count) * chart_h if max_count else 0
        x   = pad_l + i * slot_w + 5
        y   = pad_t + chart_h - bh
        tip = html_mod.escape(f"{label}: {count} flag(s)")
        hist_bars.append(
            f"<rect x='{x}' y='{y:.1f}' width='{bar_w}' height='{bh:.1f}' "
            f"fill='{color}' opacity='0.82'><title>{tip}</title></rect>"
        )
        # bucket label below bar
        hist_bars.append(
            f"<text x='{x + bar_w // 2}' y='{pad_t + chart_h + 18}' "
            f"font-size='12' text-anchor='middle' fill='#465064'>{label}</text>"
        )
        # count above bar
        if count:
            hist_bars.append(
                f"<text x='{x + bar_w // 2}' y='{y - 5:.1f}' "
                f"font-size='12' text-anchor='middle' fill='{color}' font-weight='500'>{count}</text>"
            )

    # Y-axis ticks
    hist_ticks: list[str] = []
    for step in range(5):
        tick_val = int(max_count * step / 4)
        tick_y   = pad_t + chart_h - (tick_val / max_count) * chart_h if max_count else pad_t + chart_h
        hist_ticks.append(
            f"<line x1='{pad_l - 4}' y1='{tick_y:.1f}' x2='{pad_l}' y2='{tick_y:.1f}' "
            f"stroke='#8b95a7' stroke-width='1'/>"
            f"<text x='{pad_l - 6}' y='{tick_y + 4:.1f}' font-size='10' "
            f"text-anchor='end' fill='#596275'>{tick_val}</text>"
        )

    histogram_svg = f"""<svg viewBox="0 0 {width} {height}" role="img"
     aria-label="Confidence score distribution histogram for {html_mod.escape(city)}">
  <line stroke="#8b95a7" stroke-width="1"
        x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + chart_h}"/>
  <line stroke="#8b95a7" stroke-width="1"
        x1="{pad_l}" y1="{pad_t + chart_h}" x2="{pad_l + chart_w}" y2="{pad_t + chart_h}"/>
  {''.join(hist_ticks)}
  {''.join(hist_bars)}
  <text x="{pad_l + chart_w // 2}" y="{pad_t + chart_h + 40}"
        font-size="12" text-anchor="middle" fill="#596275">Confidence score range</text>
  <text x="16" y="{pad_t + chart_h // 2}" font-size="11" text-anchor="middle"
        fill="#596275" transform="rotate(-90 16 {pad_t + chart_h // 2})">Flags</text>
</svg>"""

    # ── Per-flag-type breakdown table ─────────────────────────────────────────
    by_type: dict[str, list[int]] = defaultdict(list)
    for f in scored:
        ft = str(f.get("flag_type") or "unknown")
        by_type[ft].append(int(f.get("confidence") or 0))

    # Include any types not in the canonical order
    all_types = list(FLAG_TYPE_ORDER) + [
        t for t in by_type if t not in FLAG_TYPE_ORDER
    ]

    breakdown_rows: list[str] = []
    for ft in all_types:
        scores = by_type.get(ft)
        if not scores:
            continue
        avg  = round(sum(scores) / len(scores), 1)
        act  = sum(1 for s in scores if s >= 60)
        tier = (
            "HIGH CONFIDENCE"     if avg >= 80 else
            "MEDIUM CONFIDENCE"   if avg >= 60 else
            "LOW CONFIDENCE"      if avg >= 40 else
            "VERY LOW CONFIDENCE"
        )
        color = TIER_COLORS[tier]
        act_pct = round(act / len(scores) * 100)
        breakdown_rows.append(
            "<tr>"
            f"<td><code>{html_mod.escape(ft)}</code></td>"
            f"<td style='text-align:right'>{len(scores)}</td>"
            f"<td style='text-align:right;font-weight:500;color:{color}'>{avg}</td>"
            f"<td style='text-align:right'>{min(scores)}</td>"
            f"<td style='text-align:right'>{max(scores)}</td>"
            f"<td style='text-align:right'>{act} <span style='color:#596275;font-size:11px'>({act_pct}%)</span></td>"
            f"<td style='color:{color};font-size:12px'>{tier}</td>"
            "</tr>"
        )

    # ── Full flag table ───────────────────────────────────────────────────────
    sorted_flags = sorted(open_flags, key=lambda f: int(f.get("confidence") or 0), reverse=True)
    flag_rows: list[str] = []
    for f in sorted_flags:
        conf     = f.get("confidence")
        conf_str = str(int(conf)) if conf is not None else "—"
        sev      = str(f.get("severity") or "")
        sev_color = {"critical": "#dc2626", "high": "#d97706", "medium": "#2563eb"}.get(sev, "#596275")
        note     = str(f.get("confidence_note") or "")
        # Trim long notes — show tier + first factor only
        note_short = note[:120] + "…" if len(note) > 120 else note
        tier_color = TIER_COLORS.get(
            "HIGH CONFIDENCE"     if conf is not None and int(conf) >= 80 else
            "MEDIUM CONFIDENCE"   if conf is not None and int(conf) >= 60 else
            "LOW CONFIDENCE"      if conf is not None and int(conf) >= 40 else
            "VERY LOW CONFIDENCE",
            "#596275",
        )
        flag_rows.append(
            "<tr>"
            f"<td style='font-weight:500;color:{tier_color}'>{conf_str}</td>"
            f"<td><code style='font-size:11px'>{html_mod.escape(str(f.get('flag_type') or ''))}</code></td>"
            f"<td style='color:{sev_color}'>{html_mod.escape(sev)}</td>"
            f"<td style='max-width:260px'>{html_mod.escape(str(f.get('title') or ''))}</td>"
            f"<td style='color:#596275;font-size:11px;max-width:300px'>{html_mod.escape(note_short)}</td>"
            f"<td>{html_mod.escape(str(f.get('status') or 'open'))}</td>"
            "</tr>"
        )

    markup = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Confidence Report — {html_mod.escape(city)}</title>
  <style>
    body  {{ font-family: Arial, sans-serif; margin: 24px; color: #172033; }}
    h1    {{ margin-bottom: 4px; }}
    .meta {{ color: #596275; font-size: 13px; margin-bottom: 20px; }}
    .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 24px; }}
    .stat  {{ background: #f1f5f9; border-radius: 6px; padding: 12px 16px; }}
    .stat-val  {{ font-size: 26px; font-weight: 500; margin-bottom: 2px; }}
    .stat-label {{ font-size: 12px; color: #596275; }}
    svg   {{ border: 1px solid #d8dee9; background: #fbfcfe; max-width: 100%;
              height: auto; display: block; margin-bottom: 24px; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; margin-bottom: 32px; }}
    th, td {{ border-bottom: 1px solid #d8dee9; padding: 7px 10px; text-align: left; }}
    th    {{ background: #eef2f7; color: #263244; position: sticky; top: 0; }}
    h2    {{ margin: 24px 0 10px; font-size: 16px; color: #263244; }}
    code  {{ background: #f1f5f9; padding: 1px 4px; border-radius: 3px; font-size: 12px; }}
  </style>
</head>
<body>
  <h1>Confidence Report — {html_mod.escape(city)}</h1>
  <div class="meta">
    Report date: {date.today().isoformat()} &nbsp;|&nbsp;
    Open flags: {len(open_flags)} &nbsp;|&nbsp;
    Scored: {len(scored)} &nbsp;|&nbsp;
    Unscored: {unscored_count}
  </div>

  <div class="stats">
    <div class="stat">
      <div class="stat-val">{len(open_flags)}</div>
      <div class="stat-label">Open flags</div>
    </div>
    <div class="stat">
      <div class="stat-val">{avg_conf}</div>
      <div class="stat-label">Avg confidence</div>
    </div>
    <div class="stat">
      <div class="stat-val" style="color:#16a34a">{len(actionable)}</div>
      <div class="stat-label">Actionable (≥ 60)</div>
    </div>
    <div class="stat">
      <div class="stat-val" style="color:#dc2626">{buckets[0] + buckets[1]}</div>
      <div class="stat-label">Very low / low (< 40)</div>
    </div>
  </div>

  <h2>Confidence score distribution</h2>
  {histogram_svg}

  <h2>Breakdown by flag type</h2>
  <table>
    <thead>
      <tr>
        <th>Flag type</th><th>Count</th><th>Avg score</th>
        <th>Min</th><th>Max</th><th>Actionable ≥ 60</th><th>Tier</th>
      </tr>
    </thead>
    <tbody>
      {''.join(breakdown_rows) or '<tr><td colspan="7" style="color:#596275">No scored flags.</td></tr>'}
    </tbody>
  </table>

  <h2>All open flags — sorted by confidence</h2>
  <table>
    <thead>
      <tr>
        <th>Score</th><th>Flag type</th><th>Severity</th>
        <th>Title</th><th>Confidence note</th><th>Status</th>
      </tr>
    </thead>
    <tbody>
      {''.join(flag_rows) or '<tr><td colspan="6" style="color:#596275">No flags.</td></tr>'}
    </tbody>
  </table>
</body>
</html>"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(markup, encoding="utf-8")
    logger.info("confidence_report: report written → %s", output.resolve())
