# Real Estate Intelligence Platform

Aggregate property prices, transactions, RERA projects, suspicious activity,
and government contracts for any Indian city — currently wired for Akola, Amravati, and Nagpur.

---

## Stack
- **PostgreSQL + PostGIS** — structured data + geo queries
- **Python scrapers** — 99acres, MahaRERA, (IGR next)
- **Anomaly detection** — rule-based flags on transactions
- **FastAPI** (next phase) — REST API layer

---

## Setup

### 1. PostgreSQL + PostGIS

```bash
# Ubuntu / Debian
sudo apt install postgresql postgresql-contrib postgis

# macOS
brew install postgresql postgis

# Create DB
psql -U postgres -c "CREATE DATABASE realestate_intel;"
psql -U postgres -d realestate_intel -c "CREATE EXTENSION postgis;"
psql -U postgres -d realestate_intel -c "CREATE EXTENSION \"uuid-ossp\";"
psql -U postgres -d realestate_intel -c "CREATE EXTENSION pg_trgm;"

# Load schema
psql -U postgres -d realestate_intel -f db/schema.sql
```

### 2. Python environment

```bash
python3 -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Config

```bash
cp .env.example .env
# Edit .env with your DB password
```

---

## Running Scrapers

```bash
# Scrape 99acres listings for Akola
python main.py --scraper 99acres --city Akola

# Scrape MahaRERA projects for Akola district
python main.py --scraper rera --city Akola

# Scrape both sources for Nagpur
python main.py --scraper all --city Nagpur

# Run the downstream city pipeline for Nagpur
python main.py --snapshot --city Nagpur
python main.py --detect --city Nagpur
python main.py --patterns --city Nagpur
python main.py --trends --city Nagpur
python main.py --score --city Nagpur

# Run all scrapers + anomaly detection
python main.py --scraper all --city Akola

# Run anomaly detection only (after scrapers have run)
python main.py --detect
```

---

## Data Sources Covered

| Source | Data | Status |
|--------|------|--------|
| 99acres | Buy/rent listings, prices | ✅ Scraper ready |
| MahaRERA | Projects, financials, complaints | ✅ Scraper ready |
| IGR Maharashtra | Registration deeds, transactions | 🔧 Next phase |
| MagicBricks | Listings backup | 🔧 Next phase |
| GeM Portal | Govt contracts | 🔧 Next phase |
| eCourts | Disputed properties | 🔧 Next phase |
| Gazette notifications | Infrastructure plans | 🔧 Next phase |

---

## Key Queries

```sql
-- Latest avg price per zone in Akola
SELECT * FROM v_zone_price_summary WHERE city = 'Akola' ORDER BY period_date DESC;

-- All open suspicious flags, high severity
SELECT * FROM suspicious_flags
WHERE status = 'open' AND severity IN ('high', 'critical')
ORDER BY created_at DESC;

-- Properties within 2km of a govt project
SELECT * FROM v_property_near_govt WHERE distance_m < 2000;

-- RERA projects with escrow deficit in Akola
SELECT project_name, promoter_name, escrow_balance, amount_collected,
       escrow_balance/amount_collected AS ratio
FROM rera_projects
WHERE city_id = 1 AND escrow_balance/amount_collected < 0.70;

-- Rapid resale properties
SELECT p.address_raw, COUNT(*) AS sales, MIN(t.registration_date), MAX(t.registration_date)
FROM transactions t
JOIN properties p ON p.id = t.property_id
WHERE t.registration_date > NOW() - INTERVAL '365 days'
GROUP BY p.id, p.address_raw
HAVING COUNT(*) >= 3;
```

---

## Next Phase: IGR Scraper

IGR Maharashtra (igrmaha.gov.in) has every property registration.
Strategy: Selenium + ChromeDriver to handle their JS forms, extract:
- Deed number, date, buyer/seller, transaction value, stamp duty
- Cross-reference with circle rates from Ready Reckoner

---

## Anomaly Flags Explained

| Flag Type | Meaning | Action |
|-----------|---------|--------|
| `price_vs_circle_rate` | Declared value >> or << govt rate | Check for hawala / stamp evasion |
| `rapid_resale` | Same property sold 3x/year | Possible price manipulation |
| `bulk_buyer` | Single buyer on 10+ transactions | Verify shell company |
| `rera_escrow_deficit` | Project escrow below 70% | Risk of default |
| `repeated_complaints` | Builder with 5+ RERA complaints | Avoid / flag to buyers |
| `price_spike` | Zone with 50%+ YoY price jump | Check for insider knowledge |
