# Homesage — Dev Context Handoff

*Last updated: May 2026 — Phase 2 active*

\---

## CITIES (CURRENT STATE)

|City|city\_id|Status|Listings|RERA Projects|
|-|-|-|-|-|
|Akola|1|Active, Phase 1|\~149 active|114|
|Nagpur|2|Active, Phase 1|\~1000 active|800|
|Pune|3|**NEW — Phase 2, 2 runs complete**|745 active|800|
|Aurangabad|10|Active phase 2|470|375|
|Nashik|5|Active phase 2|450|376|
|Amravati|9|Active, Phase 1|\~375 active|370|

**Phase 2 next cities:** Nashik + Aurangabad-Sambhaji Nagar (both small enough to add simultaneously). Navi Mumbai deferred — administratively split across Thane/Raigad districts in MahaRERA, not a clean add.

\---

## TECH STACK (UNCHANGED)

* Python + Playwright scrapers → Supabase PostgreSQL
* Project root: `C:\\Users\\agraw\\Downloads\\realestate\_intel\_v3` with `venv`
* db/connection.py uses HTTP REST (`select\_rows`, `insert\_row`, `update\_rows`, `upsert\_row`) — NOT psycopg2
* Make.com scenario 4996492 — alerts on `suspicious\_flags` where `notified=false`, `confidence>=60`, `severity IN (high, critical)`
* Azure OpenAI for flag explanations (`--explain-flags`)
* Shared tables + `city\_id` (NOT city-specific tables)

\---

## SCRAPER CHANGES SINCE LAST HANDOFF

### scraper\_99acres.py

* `\_HIGH\_VOLUME\_CITIES = {"nagpur", "pune"}` — Pune added. Both get 15-page cap + 10–18s inter-page delays automatically.
* `main.py` no longer hardcodes `max\_pages` — removed `max\_pages=40` override, class defaults now control it per city.
* Nagpur dropped from 40 → 15 pages as a result (was scraping 1000+ listings, now \~375).
* `MIN\_CITY\_LISTINGS\_FOR\_OUTLIER = 400` in anomaly\_detector.py — **known tension**: Nagpur at 15 pages gets \~375 listings, just under threshold. Outlier detection may skip. Deferred fix.

### scraper\_rera.py

* `save()` now normalises `registered → active` before every DB write.
* MahaRERA list page returns `"rera\_status": "registered"` but DB/dashboard canonical is `"active"`. Fix applied in `\_extract\_cards` pipeline.
* SQL backfill already run on Pune: 663 active, 128 completed, 9 lapsed.
* Nagpur was normalised earlier in a previous session.

### scraper\_rera\_detail.py — Major Pune Fix

**Problem discovered:** Pune "View Details" URLs use CMS node IDs (4–4666) not the MahaRERA API's internal `projectId`. All API calls returned 500 for Pune.

**Fix implemented — two new methods:**

* `\_resolve\_api\_project\_id(url\_id, detail\_url)` — quick probe: tries URL ID directly against API. If 500, falls back to page interception. Caches result per run.
* `\_intercept\_project\_id\_from\_page(detail\_url)` — Playwright loads the detail page, listens for first XHR to `projectregistartion/` endpoint, extracts real `projectId` from POST payload.

**`should\_enrich()` filtering removed entirely.** Previously filtered by `created\_at < 36hrs` or `updated\_at > 12 days`. Now enriches ALL projects every run. Since enrich runs weekly via `run\_rera\_models.bat`, no time-based filtering is needed. `timedelta` import also removed.

**First Pune enrichment run:** \~1–2 hours (800 projects × \~8s Playwright interception each). One-time cost. Subsequent runs only process newly inserted projects (typically 10–20).

\---

## BAT FILE SCHEDULE (ALL 4 INCLUDE PUNE)

|File|When|What|
|-|-|-|
|`run\_models.bat`|Tue/Wed/Fri/Sat/Sun|snapshot + detect + patterns + trends + score + explain-flags|
|`run\_99\_models.bat`|Mon/Thu|99acres scrape all cities + all models|
|`run\_rera\_models.bat`|Weekly|RERA scrape + enrich all cities + all models|
|`run\_scrapers.bat`|Occasional|Full run (99acres + RERA + enrich + all models)|

All bat files include Pune. RERA scraping frequency dropped from alternate-day to weekly — data confirmed to change rarely (full table rewrite, 0 net changes between runs).

\---

## MODELS (ALL UNCHANGED IN LOGIC — DEFERRED UNTIL AFTER PHASE 2)

### AnomalyDetector

* `check\_repeated\_complaints` — promoter-level flag consolidation (one flag per promoter, not per project)
* `check\_rera\_escrow\_deficit` — skips null escrow (API doesn't return it for most projects)
* `check\_stalled\_projects` — promoter-level, same consolidation pattern
* `check\_listing\_price\_outliers` — skips cities with < 400 active listings (Akola, Amravati excluded; Nagpur borderline at 15 pages)
* `MIN\_CITY\_LISTINGS\_FOR\_OUTLIER = 400`

### PatternDetector

* 6 pattern types: `cross\_source\_promoter\_risk`, `stale\_rera\_active\_listing`, `promoter\_name\_cluster`, `complaint\_velocity`, `locality\_price\_spike`, `repeat\_offender\_new\_project`
* `PRICE\_LOCALITY\_SPIKE\_PCT = 0.40` — locality median must be 40%+ above city median
* `NAME\_SIMILARITY\_THRESHOLD = 0.85` Jaccard — tight, intentionally avoids false positives
* Known issue: Paranjape "Ltd" vs "Limited" + Marvel group variants producing separate flags instead of consolidating. Promoter name cluster threshold may be too tight. Deferred.

### ConfidenceScorer

* Direction-aware penalty for `listing\_price\_outlier`: below-median + anchor\_psqft < 500 → -30 (data error, not fraud)
* Base scores: `cross\_source\_promoter\_risk=80`, `rera\_escrow\_deficit=75`, `complaint\_velocity=70`, `repeated\_complaints=65`, `stalled\_projects=55`, `listing\_price\_outlier=35`
* Clamps to \[5, 98] — never 0% or 100%

### TrendDetector

* Windows: 7d/14d/30d with medium/high/critical thresholds
* Dedup: loads existing open `price\_trend\_spike` flags on init, skips if same (locality, property\_type, window\_days) already open
* Known issue: same locality re-flagged each run if flag is closed/dismissed. Accumulation deferred.

### PriceTracker (price\_tracker.py) — UPDATED

* Added `\_clean\_locality(locality, city)` static method:

  * Strips `, CityName` and ` CityName` suffix from end (catches city-suffix regressions)
  * Rejects junk strings: `unknown`, `n/a`, `tahsil`, `vyom`, `nil`, etc.
* Applied to both 99acres loop AND RERA `address\_raw` locality (RERA was previously completely unfiltered)
* **Migration ran**: 475 ghost price\_history rows deleted (64 stale localities across Akola/Nagpur/Amravati where locality strings diverged from scraper canonicalization after aliases were added)

\---

## DATA QUALITY — CURRENT STATE (POST PHASE 2)

### Pune (after 2 runs)

* 745 active listings, 0 inactive, 0 duplicates ✓
* 100% geocoded ✓ (best of all cities)
* 4 null price\_per\_sqft only
* Avg ₹9,674/sqft, median ₹9,244/sqft — realistic
* 800 RERA projects: 663 active, 128 completed, 9 lapsed (post-normalisation)
* 1,013 total complaints across 226 projects — highest of all cities
* 138 suspicious flags: 88 `repeated\_complaints` (avg conf 83), 26 `complaint\_velocity` (avg conf 84), 13 `locality\_price\_spike`, 6 `stalled\_projects`
* Top flagged: SSG Realty (93 complaints, conf 90), Marvel group (multiple entities, conf 98), Paranjape (conf 96–98)

### Nagpur

* 1000 active listings, \~375 at 15-page cap going forward
* 99.9% geocoded
* 800 RERA projects, 237 total complaints — strongest signal city
* 167 open flags, 79 alert-eligible

### Akola / Amravati

* Geocoding gaps remain: Akola \~35%, Amravati \~22% ungeocooded — deferred
* Below `MIN\_CITY\_LISTINGS\_FOR\_OUTLIER` threshold — outlier detection skipped for both

### price\_history (all cities)

* 475 ghost rows deleted (stale pre-alias locality strings)
* No duplicate snapshots ✓
* Pune: 2 snapshot days (May 22, May 26) — trend detection needs \~5–6 more days to be meaningful

\---

## KNOWN DEFERRED ISSUES (DO NOT FIX YET)

1. **Nagpur outlier threshold tension** — `MIN\_CITY\_LISTINGS\_FOR\_OUTLIER=400` vs \~375 listings at 15 pages. Fix after Nashik/Sambhaji Nagar runs.
2. **Geocoding gaps** — Akola 35%, Amravati 22%. Run `--geocode-listings` pass.
3. **price\_trend\_spike accumulation** — same locality re-flagged each run. Needs date-window dedup.
4. **Jaistambh Chowk** — locality slipped through junk filter in Amravati.
5. **Promoter name cluster threshold** — Paranjape Ltd/Limited and Marvel group variants not consolidating. Jaccard 0.85 may be too tight.
6. **Pune RERA status distribution** — 663 active, 128 completed, 9 lapsed out of 800. Only 38 were active before the normalisation SQL. Actual active project count for Pune may be much lower — the RERA list scraper may be returning a mix of all statuses including historical completed.
7. **city\_id=10 in price\_history** — unknown city has snapshot rows. Needs investigation.

\---

## DB PATTERNS TO REMEMBER

* `execute\_sql` for SELECT/schema inspection
* `apply\_migration` (with unique `name` param) for all DML — required, not optional
* Duplicate removal: SELECT first, then `apply\_migration` DELETE with specific IDs
* `rera\_status` canonical values in DB: `active`, `completed`, `lapsed`, `revoked` — NOT `registered`

\---

## PRODUCT DIRECTION (UNCHANGED)

* Paid due-diligence reports (buyer/lender)
* Broker/investor dashboard with city filters and risk matrix
* B2B/B2B2C model, not a listing portal
* ML deferred until 90+ days history, confirmed/dismissed labels, multi-city data

### Reports Built (in reports/ directory)

* `price\_snapshot\_report.py` — HTML price snapshot
* `trend\_report.py` — HTML trend report from price\_spikes + price\_history
* `confidence\_report.py` — HTML confidence score report

### Reports Planned (not yet built)

* `reports/flag\_summary\_report.py` — promoter-grouped investigator brief
* `reports/locality\_report.py` — locality × flag type risk matrix
* `reports/promoter\_report.py` — single promoter deep dive (paid product skeleton)

\---

## LOCALITY CANONICALIZATION (IMPORTANT FOR REPORTS)

The scraper applies a full pipeline: lowercase → city suffix strip → LOCALITY\_ALIASES lookup → JUNK\_LOCALITIES rejection → title-case. Key files:

* `LOCALITY\_ALIASES` dict in `scraper\_99acres.py` — canonical locality name map for Akola, Amravati, Nagpur sections
* `JUNK\_LOCALITIES` set in `scraper\_99acres.py` — administrative units, building names, landmark strings to reject
* **price\_tracker.py** now applies `\_clean\_locality()` as safety net before writing to `price\_history`
* Any report joining `listings` ↔ `price\_history` on `locality` string should be safe post-migration, but always filter `listing\_status = 'active'` and `source = '99acres'` on both sides

\---

*This file reflects state as of late May 2026. Next action: check phase 2 data quality nd improve logic.*

