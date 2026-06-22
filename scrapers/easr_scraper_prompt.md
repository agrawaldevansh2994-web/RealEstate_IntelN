# Task: Build eASR Circle Rate Scraper for Homesage

## What you're building

A Playwright scraper (`scrapers/scraper\_easr.py`) that pulls Maharashtra government Ready Reckoner (circle) rates from the IGR eASR portal for 6 cities, stores them in a new `circle\_rates` table, and enables market-vs-floor price comparison against existing `listings` data.

\---

## What the eASR portal is

URL: `https://igrmaharashtra.gov.in/eASR/eASRCommon.aspx`  
There are two versions:

* **eASR 1.9** — classic dropdown cascade (district → taluka → village → results grid). HTML table output. More scrapable.
* **eASR 2.0** — map-based UI, harder to automate.

The portal is a cascading form (likely ASP.NET WebForms with `\_\_VIEWSTATE`). Flow:

1. Select district from dropdown
2. Select taluka from dropdown (populated after district)
3. Select village/ward from dropdown (populated after taluka)
4. Rate table renders for that village — rows are: Location/Zone, Property Type, Rate (₹/sqm), Unit

**Critical: Before writing the scraper**, open DevTools → Network tab on the eASR portal for one of our cities (Nagpur recommended — it's the most data-complete). Check whether the dropdown selections trigger:

* JSON XHR/fetch calls → intercept those, much cleaner
* Full page POSTbacks with `\_\_VIEWSTATE` → use Playwright form interaction

The approach differs significantly. Inspect first, then build.

\---

## Data characteristics

* **Rates are in ₹ per sqm** (govt standard). Must store raw sqm AND derive sqft (divide by 10.764) for comparison with listings.
* **Property types in eASR:** Residential Flat/Apartment, Independent House/Bungalow, Open Plot/Land, Commercial (skip commercial for now).
* **Map to our enum:** flat → `flat`, house/bungalow → `house\_villa`, open plot → `plot`
* **Update frequency:** Once per year, April 1. Scraper runs annually, not weekly.
* **Coverage:** Urban wards/localities within city municipal limits. Rural areas (gramin) are less relevant — skip or store separately.

\---

## DB schema to create (via apply\_migration)

```sql
CREATE TABLE IF NOT EXISTS circle\_rates (
    id              SERIAL PRIMARY KEY,
    city\_id         INTEGER REFERENCES cities(id),
    district        VARCHAR,
    taluka          VARCHAR,
    village         VARCHAR,          -- govt's village/ward name (raw)
    locality        VARCHAR,          -- canonicalized, attempt match to listings.locality
    property\_type   VARCHAR,          -- flat / house\_villa / plot
    rate\_per\_sqm    NUMERIC,          -- raw from govt
    rate\_per\_sqft   NUMERIC,          -- derived: rate\_per\_sqm / 10.764
    effective\_year  INTEGER,          -- e.g. 2025
    source\_url      TEXT,
    raw\_data        JSONB,
    scraped\_at      TIMESTAMPTZ DEFAULT NOW(),
    created\_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS circle\_rates\_city\_locality\_idx
    ON circle\_rates(city\_id, locality, property\_type);

CREATE UNIQUE INDEX IF NOT EXISTS circle\_rates\_dedup\_idx
    ON circle\_rates(city\_id, village, property\_type, effective\_year);
```

Migration name: `create\_circle\_rates\_table`

\---

## Locality canonicalization

Government village names (`Dharampeth`, `Civil Lines Ward`, `Sadar Bazaar`) won't match our canonical locality strings exactly. Apply a best-effort normalization:

1. Lowercase, strip punctuation
2. Remove suffixes: ` ward`, ` layout`, ` colony`, ` nagar` only if the remainder still has 3+ chars
3. Try exact match against known locality aliases (import `LOCALITY\_ALIASES` from `scraper\_99acres.py`)
4. Store both raw government name (`village` column) and canonicalized name (`locality` column)
5. Log a warning when no alias match found — manual curation can fill gaps later

Don't block ingestion on failed locality matches. Store the rate regardless; the locality column just becomes the raw name.

\---

## Save logic

Upsert on `(city\_id, village, property\_type, effective\_year)` — the unique index handles dedup. On conflict, update `rate\_per\_sqm`, `rate\_per\_sqft`, `scraped\_at`. Use `apply\_migration` for DDL, standard `upsert\_row` for data writes.

\---

## City-to-district mapping

```python
\_CITY\_TO\_DISTRICT = {
    "akola":      "Akola",
    "amravati":   "Amravati",
    "nagpur":     "Nagpur",
    "pune":       "Pune",
    "nashik":     "Nashik",
    "aurangabad": "Aurangabad",   # officially Chhatrapati Sambhajinagar on portal
}
```

Note: Aurangabad may appear as "Aurangabad" or "Chhatrapati Sambhajinagar" on the portal — handle both.

\---

## What NOT to build yet

* No comparison report yet — that's a separate `reports/circle\_rate\_report.py` task
* No fraud flag integration yet — `stale\_rera\_active\_listing` and price outlier detectors will be updated separately once data exists
* No commercial property rates — residential only for now (flat, house\_villa, plot)
* No rural/gramin areas — urban municipal wards only

\---

## Deliverables

1. `scrapers/scraper\_easr.py` — full scraper class `ScraperEASR` with `run(city)` entry point
2. Migration SQL string (or as a standalone `.sql` file) for the `circle\_rates` table
3. Updated `main.py` snippet showing the `--scraper easr` addition
4. Log what you find about the portal's actual request structure before writing the scraper — the XHR vs WebForms finding changes the implementation significantly

Start by inspecting the portal network traffic for Nagpur. Then build.

