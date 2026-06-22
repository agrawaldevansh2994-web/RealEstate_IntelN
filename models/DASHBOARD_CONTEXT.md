# Dashboard Context — RealEstate Intel (index.html)

## Project
- **Repo:** `C:\Users\agraw\Downloads\realestate_intel_v3\RealEstate_IntelN\index.html`
- **Supabase:** `https://bmiunaojlqwscybcayyk.supabase.co`
- **Stack:** Single HTML file, vanilla JS, Supabase REST API, Leaflet.js maps

---

## Cities (CITIES array in index.html)
| id | name | Notes |
|----|------|-------|
| 1 | Akola | Original city, 9+ price snapshots |
| 9 | Amravati | ~375 listings, geocoding gap ~24% |
| 2 | Nagpur | 1,000+ listings, 100% geocoded, best data quality |

Adding a new city = add `{ id: X, name: 'City' }` to the `CITIES` array. Everything else is city-agnostic via `city_id` filtering.

---

## Data Sources & Tables
| Table | Source | Notes |
|-------|--------|-------|
| `listings` | 99acres scraper | Shared table, filtered by `city_id` + `source='99acres'` + `listing_status='active'` |
| `rera_projects` | MahaRERA scraper | Shared table, filtered by `city_id` |
| `suspicious_flags` | anomaly_detector.py | 4 statuses: `open`, `monitoring`, `confirmed`, `dismissed` |
| `price_history` | price_tracker.py | Queried by `city` (name string, not city_id) |
| `cities` | — | id→name mapping |

### rera_status values (normalized — backend fixed)
- `active` — registered and currently active (was `registered` pre-normalization)
- `completed`
- `lapsed`
- `de-registered`

---

## Key Data Quality Rules (baked into index.html)

### Price filtering
```js
// Used in ALL price calculations (locality, market summary, BHK, stats avg)
v > 500 && v < 200000   // general filter — removes sq-yard conversion errors
v > 500 && v < 50000    // locality prices only — removes luxury/sq-yard outliers
```

### Locality price chart (renderLocalityPrices)
- Min **3 listings** per locality (`v.length >= 3`)
- Price range **₹500–₹50,000/sqft**
- Subtitle shows: `"Based on X listings · ≥3 per locality · ₹500–₹50k/sqft"`
- Empty state: `"No locality with 3+ listings in ₹500–₹50k/sqft range for {city}"`
- **Why:** 99acres Nagpur data is dominated by peripheral areas (Jamtha, Besa, Wardha Road). Core localities like Dharampeth only have 2 listings — correct to exclude until data accumulates.

### Listings query limit
```js
limit=2000  // was 500, raised because Nagpur has 1,000+ listings
```

---

## Flag Lifecycle
4 statuses with CHECK constraint in DB:

| Status | Meaning | Visible by default | Triggers alert |
|--------|---------|-------------------|----------------|
| `open` | New, needs review | ✅ | ❌ |
| `monitoring` | Real but watching | ✅ | ❌ |
| `confirmed` | Verified real issue | ✅ | ✅ |
| `dismissed` | False positive | ❌ (toggle) | ❌ |

**Dedup priority in JS:** `confirmed (4) > monitoring (3) > open (2) > dismissed (1)`
Groups by `(flag_type + title)`, keeps highest-status copy, shows `×N` badge.

### Flag types
- **Market-level flags** (no `rera_project_id`): price outliers, locality spikes — shown with "market-level" sublabel
- **Project-level flags** (have `rera_project_id`): complaint velocity, stalled projects, repeat offender, promoter cluster

---

## Map Modes (Leaflet.js)
| Filter | What renders |
|--------|-------------|
| All | RERA pins (circles) + 99acres squares |
| RERA only | RERA project pins |
| Listings only | 99acres listing squares |
| Flagged only | Flagged RERA pins only |
| 🌡 Price Heatmap | Locality circle markers, size=listing count, color=price tier |

### Heatmap
- Color scale: blue (#4ecba8 affordable) → gold (#c9a84c mid) → red (#e05252 premium)
- Sub-toggle: Sale / Rent / All
- Radius: `Math.max(5, Math.min(14, 5 + Math.sqrt(count) * 1.2))` — was too large, tightened
- Uses lat/lon centroid per locality, min 1 listing to render
- Nagpur most useful (265 localities, 100% geocoded)

---

## Analytics Panels

### Confidence × Severity Matrix
- 4 quadrants: HH (Act Now), HL (Investigate), LH (Monitor), LL (Noise)
- Clickable — filters flags table to that quadrant
- State: `activeMatrixFilter` — clicking same quadrant clears it
- **Bug history:** was broken because `renderFlags()` threw due to `isMonitoring` used before declaration. Fixed by moving `const isMonitoring` before `rowClass`.

### Flag Type Breakdown
- Horizontal bar chart per flag_type with count
- **Also broken** if `renderFlags` throws — they're called on the same line

### BHK Demand Distribution
- Sale listings only (`listing_type === 'sale'`)
- Excludes rent to avoid skewing BHK averages

### Price Trend Sparkline
- Queries `price_history` by city name string (not city_id)
- Drops snapshots with `< 10 total listings` (noise filter)
- Needs ≥2 usable snapshots to render

### Act Now Panel
- High/critical severity + confidence ≥70 + not dismissed
- One-click confirm/dismiss
- Shows "market-level" sublabel for unlinked flags

---

## Pagination
- `PAGE_SIZE = 50` for both RERA projects and 99acres listings tables
- State: `projectPage`, `listingPage` — reset on city switch, filter change, new search
- `renderPagination(current, total, handler)` — shared helper renders prev/next/numbered buttons
- `goProjectPage(p)` / `goListingPage(p)` — page navigation functions

---

## Project Detail Drawer
- Click any RERA project row → slide-in panel from right
- Shows: RERA reg, status, district, PIN, promoter name+PAN, type, completion dates, delay months, units sold/total with sale rate colour-coded, complaint count, flag status
- MahaRERA link opens in new tab
- `_drawerProjects[]` array — indexed by visible row position (respects search/filter)

---

## Project Search
- Input above RERA table: filters by project name, promoter name, or RERA registration
- Resets `projectPage = 1` on keystroke
- `filterProjects(term)` → `renderProjectRows(filtered)`

---

## Pipeline Health Dot (header)
- Replaces old "Live Data" dot
- Green = scraped <24h, Amber = <48h, Red = older
- Uses `allListings[0]?.last_seen_at` — zero extra queries
- Elements: `#healthDot`, `#healthLabel` inside `#pipelineHealth`

---

## Known Issues / Pending

### Data gaps
- **Nagpur RERA enrichment thin** — 640/800 projects missing `total_units`. Run: `python main.py --scraper enrich --city Nagpur`
- **Amravati geocoding gap** — ~91/375 listings (24%) have no lat/lon
- **Nagpur core localities underrepresented** — Dharampeth has only 2 listings. 99acres skews toward peripheral new-build stock. Fix: add second source (MagicBricks or Housing.com) or wait for data to accumulate.
- **Nagpur BHK missing on 44% of listings** — scraper extraction gap on commercial/plot listings

### Index improvements pending
- Locality name mismatch between `listings` and `price_history` — 196 Nagpur localities in listings not in price_history (different normalization paths). Price tracker needs same `_canonicalize_locality` logic as scraper.
- Price trend sparkline queries by city name string not city_id — brittle if city name ever changes

### Next phase
- Add **Pune** — `{ id: X, name: 'Pune' }` in CITIES array, run scrapers. Dramatically better data density than current cities.
- **Locality sparklines** — need 4+ weeks of data per locality with ≥3 listings each snapshot. Not yet meaningful.
- **Hottest Localities Ticker** — blocked on locality normalization fix in price_history. Currently shows fake 200%+ swings from name fragmentation artifacts.
- **Circle Rate Gauge** — needs Ready Reckoner data scraper first (not built yet).

---

## CSS Variables Reference
```css
--bg:#060810  --bg2:#0c0f1a  --bg3:#121628
--gold:#c9a84c  --red:#e05252  --green:#4ecba8
--blue:#5b8def  --purple:#a78bfa  --amber:#f59e0b
--muted:rgba(255,255,255,0.35)  --text:rgba(255,255,255,0.88)
--border:rgba(255,255,255,0.07)  --border2:rgba(255,255,255,0.12)
```

## External Dependencies
```html
<!-- Fonts -->
https://fonts.googleapis.com/css2?family=Syne&family=JetBrains+Mono

<!-- Map -->
https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css
https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js
```
