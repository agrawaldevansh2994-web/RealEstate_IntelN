-- ============================================================
-- REAL ESTATE INTELLIGENCE PLATFORM — DATABASE SCHEMA
-- PostgreSQL + PostGIS
-- ============================================================

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- fuzzy text search

-- ============================================================
-- CORE LOOKUP TABLES
-- ============================================================

CREATE TABLE cities (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    state       VARCHAR(100) NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE zones (
    id          SERIAL PRIMARY KEY,
    city_id     INT REFERENCES cities(id),
    name        VARCHAR(200) NOT NULL,         -- e.g. "Ramdaspeth", "Civil Lines"
    pin_code    VARCHAR(10),
    zone_type   VARCHAR(50),                   -- residential / commercial / industrial / mixed
    boundary    GEOMETRY(POLYGON, 4326),       -- PostGIS polygon
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_zones_boundary ON zones USING GIST(boundary);
CREATE INDEX idx_zones_pin ON zones(pin_code);

-- ============================================================
-- PROPERTIES
-- ============================================================

CREATE TABLE properties (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id         INT REFERENCES cities(id),
    zone_id         INT REFERENCES zones(id),

    -- Identity
    survey_number   VARCHAR(100),              -- govt survey / gat number
    crts_number     VARCHAR(100),              -- registration index number
    address_raw     TEXT,
    pin_code        VARCHAR(10),
    location        GEOMETRY(POINT, 4326),     -- lat/lon PostGIS

    -- Classification
    property_type   VARCHAR(50),               -- flat / plot / bungalow / commercial / agricultural
    usage_type      VARCHAR(50),               -- residential / commercial / mixed
    total_area_sqft NUMERIC(12,2),
    built_up_sqft   NUMERIC(12,2),
    floor_number    INT,
    total_floors    INT,

    -- Govt data
    circle_rate     NUMERIC(14,2),             -- govt ready reckoner rate (per sqft)
    property_tax_id VARCHAR(100),
    municipal_corp  VARCHAR(100),

    -- Meta
    source          VARCHAR(50),               -- igr / rera / 99acres / manual
    source_id       VARCHAR(200),              -- original ID from source
    raw_data        JSONB,                     -- full raw payload
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_properties_location   ON properties USING GIST(location);
CREATE INDEX idx_properties_city       ON properties(city_id);
CREATE INDEX idx_properties_zone       ON properties(zone_id);
CREATE INDEX idx_properties_type       ON properties(property_type);
CREATE INDEX idx_properties_source_id  ON properties(source, source_id);
CREATE INDEX idx_properties_pin        ON properties(pin_code);
CREATE INDEX idx_properties_survey     ON properties(survey_number);

-- ============================================================
-- TRANSACTIONS (buy/sell registrations from IGR)
-- ============================================================

CREATE TABLE transactions (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    property_id         UUID REFERENCES properties(id),

    -- IGR / Registration data
    deed_number         VARCHAR(100),
    registration_date   DATE,
    doc_type            VARCHAR(100),          -- Sale Deed / Gift / Mortgage / Lease etc.

    -- Parties
    seller_name         TEXT,
    buyer_name          TEXT,
    seller_pan          VARCHAR(15),           -- anonymised in storage
    buyer_pan           VARCHAR(15),
    seller_entity_type  VARCHAR(30),           -- individual / company / trust / govt
    buyer_entity_type   VARCHAR(30),

    -- Financials
    transaction_value   NUMERIC(16,2),         -- declared sale consideration
    stamp_duty_value    NUMERIC(16,2),         -- govt assessed value (circle rate based)
    stamp_duty_paid     NUMERIC(14,2),
    registration_fee    NUMERIC(14,2),

    -- Derived
    value_per_sqft      NUMERIC(12,2),
    circle_rate_sqft    NUMERIC(12,2),
    premium_over_circle NUMERIC(8,4),          -- ratio: txn_value / stamp_duty_value

    -- Sub-registrar office
    sro_office          VARCHAR(200),
    district            VARCHAR(100),

    -- Flags
    is_suspicious       BOOLEAN DEFAULT FALSE,
    suspicious_reasons  TEXT[],

    -- Meta
    source              VARCHAR(50) DEFAULT 'igr',
    raw_data            JSONB,
    scraped_at          TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_txn_property      ON transactions(property_id);
CREATE INDEX idx_txn_date          ON transactions(registration_date);
CREATE INDEX idx_txn_deed          ON transactions(deed_number);
CREATE INDEX idx_txn_buyer         ON transactions(buyer_name);
CREATE INDEX idx_txn_seller        ON transactions(seller_name);
CREATE INDEX idx_txn_suspicious    ON transactions(is_suspicious) WHERE is_suspicious = TRUE;
CREATE INDEX idx_txn_value         ON transactions(transaction_value);

-- ============================================================
-- MARKET LISTINGS (from 99acres, MagicBricks, Housing.com)
-- ============================================================

CREATE TABLE listings (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    property_id     UUID REFERENCES properties(id),
    zone_id         INT REFERENCES zones(id),
    city_id         INT REFERENCES cities(id),

    -- Listing info
    listing_type    VARCHAR(20) NOT NULL,      -- sale / rent / pg
    listing_status  VARCHAR(20) DEFAULT 'active', -- active / sold / expired / removed
    listed_price    NUMERIC(16,2),
    price_per_sqft  NUMERIC(12,2),
    negotiable      BOOLEAN,

    -- Property snapshot
    property_type   VARCHAR(50),
    bedrooms        INT,
    bathrooms       INT,
    area_sqft       NUMERIC(12,2),
    furnishing      VARCHAR(30),               -- furnished / semi / unfurnished
    floor_number    INT,
    total_floors    INT,
    facing          VARCHAR(30),               -- North / East / Corner etc.
    age_years       INT,
    amenities       TEXT[],

    -- Location
    locality        VARCHAR(200),
    address_raw     TEXT,
    location        GEOMETRY(POINT, 4326),
    pin_code        VARCHAR(10),

    -- Contact / broker
    listed_by       VARCHAR(30),               -- owner / broker / builder
    broker_name     TEXT,
    broker_phone    VARCHAR(20),

    -- Source
    source          VARCHAR(50),               -- 99acres / magicbricks / housing / nobroker
    source_url      TEXT,
    source_listing_id VARCHAR(100),
    first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ DEFAULT NOW(),
    sold_at         TIMESTAMPTZ,

    -- Flags
    is_flagged      BOOLEAN DEFAULT FALSE,
    flag_reasons    TEXT[],

    raw_data        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_listings_property   ON listings(property_id);
CREATE INDEX idx_listings_zone       ON listings(zone_id);
CREATE INDEX idx_listings_location   ON listings USING GIST(location);
CREATE INDEX idx_listings_type       ON listings(listing_type, listing_status);
CREATE INDEX idx_listings_source     ON listings(source, source_listing_id);
CREATE INDEX idx_listings_price      ON listings(listed_price);
CREATE INDEX idx_listings_pin        ON listings(pin_code);
CREATE INDEX idx_listings_flagged    ON listings(is_flagged) WHERE is_flagged = TRUE;

-- ============================================================
-- PRICE HISTORY (time series per zone / property type)
-- ============================================================

CREATE TABLE price_history (
    id              SERIAL PRIMARY KEY,
    zone_id         INT REFERENCES zones(id),
    city_id         INT REFERENCES cities(id),
    property_type   VARCHAR(50),
    listing_type    VARCHAR(20),               -- sale / rent
    period_date     DATE NOT NULL,             -- monthly snapshot

    -- Aggregates
    avg_price_sqft  NUMERIC(12,2),
    median_price_sqft NUMERIC(12,2),
    min_price_sqft  NUMERIC(12,2),
    max_price_sqft  NUMERIC(12,2),
    total_listings  INT,
    new_listings    INT,
    closed_listings INT,
    avg_days_on_market INT,

    -- Transaction aggregates (from IGR)
    txn_count       INT,
    txn_total_value NUMERIC(18,2),
    avg_txn_sqft    NUMERIC(12,2),

    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(zone_id, property_type, listing_type, period_date)
);

CREATE INDEX idx_price_history_zone ON price_history(zone_id, period_date);
CREATE INDEX idx_price_history_city ON price_history(city_id, period_date);

-- ============================================================
-- RERA PROJECTS
-- ============================================================

CREATE TABLE rera_projects (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id             INT REFERENCES cities(id),
    zone_id             INT REFERENCES zones(id),

    -- RERA data
    rera_registration   VARCHAR(100) UNIQUE,   -- e.g. P51700012345
    project_name        TEXT NOT NULL,
    promoter_name       TEXT,
    promoter_pan        VARCHAR(15),
    promoter_type       VARCHAR(50),           -- individual / company / partnership

    -- Location
    address_raw         TEXT,
    district            VARCHAR(100),
    pin_code            VARCHAR(10),
    location            GEOMETRY(POINT, 4326),

    -- Project details
    project_type        VARCHAR(50),           -- residential / commercial / mixed
    total_units         INT,
    units_sold          INT,
    units_available     INT,
    total_area_sqm      NUMERIC(14,2),
    total_built_up_sqm  NUMERIC(14,2),
    land_area_sqm       NUMERIC(14,2),
    fsi_consumed        NUMERIC(8,4),

    -- Timeline
    application_date    DATE,
    approval_date       DATE,
    proposed_completion DATE,
    revised_completion  DATE,
    actual_completion   DATE,
    is_completed        BOOLEAN DEFAULT FALSE,
    delay_months        INT,                   -- computed: revised - proposed

    -- Financials
    project_cost        NUMERIC(18,2),
    amount_collected    NUMERIC(18,2),
    escrow_balance      NUMERIC(18,2),
    loan_amount         NUMERIC(18,2),

    -- Status & flags
    rera_status         VARCHAR(50),           -- registered / lapsed / revoked
    complaint_count     INT DEFAULT 0,
    is_flagged          BOOLEAN DEFAULT FALSE,
    flag_reasons        TEXT[],

    -- Meta
    source_url          TEXT,
    raw_data            JSONB,
    scraped_at          TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_rera_location    ON rera_projects USING GIST(location);
CREATE INDEX idx_rera_city        ON rera_projects(city_id);
CREATE INDEX idx_rera_promoter    ON rera_projects(promoter_name);
CREATE INDEX idx_rera_status      ON rera_projects(rera_status);
CREATE INDEX idx_rera_flagged     ON rera_projects(is_flagged) WHERE is_flagged = TRUE;

-- ============================================================
-- RERA COMPLAINTS
-- ============================================================

CREATE TABLE rera_complaints (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id      UUID REFERENCES rera_projects(id),
    complaint_no    VARCHAR(100),
    complainant     TEXT,
    respondent      TEXT,
    complaint_type  VARCHAR(100),
    filed_date      DATE,
    hearing_date    DATE,
    status          VARCHAR(50),
    order_summary   TEXT,
    raw_data        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_complaints_project ON rera_complaints(project_id);

-- ============================================================
-- GOVERNMENT CONTRACTS & INFRASTRUCTURE
-- ============================================================

CREATE TABLE govt_projects (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    city_id         INT REFERENCES cities(id),

    -- Source
    source          VARCHAR(50),               -- gem / eprocurement / gazette / manual
    tender_id       VARCHAR(200),

    -- Project
    title           TEXT NOT NULL,
    description     TEXT,
    dept            TEXT,                      -- department / ministry
    project_type    VARCHAR(100),              -- road / metro / hospital / school / sez / airport
    contractor      TEXT,
    contract_value  NUMERIC(18,2),

    -- Location
    locality        TEXT,
    pin_code        VARCHAR(10),
    location        GEOMETRY(POINT, 4326),
    impact_radius_m INT DEFAULT 2000,          -- how far it affects property prices
    impact_zone     GEOMETRY(POLYGON, 4326),   -- computed: buffer around location

    -- Timeline
    announced_date  DATE,
    tender_date     DATE,
    award_date      DATE,
    start_date      DATE,
    completion_date DATE,
    status          VARCHAR(50),

    -- Price signal
    price_impact_pct NUMERIC(6,2),            -- estimated % price bump in impact zone

    raw_data        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_govt_location    ON govt_projects USING GIST(location);
CREATE INDEX idx_govt_impact_zone ON govt_projects USING GIST(impact_zone);
CREATE INDEX idx_govt_city        ON govt_projects(city_id);
CREATE INDEX idx_govt_type        ON govt_projects(project_type);

-- ============================================================
-- SUSPICIOUS ACTIVITY FLAGS
-- ============================================================

CREATE TABLE suspicious_flags (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    flag_type       VARCHAR(100) NOT NULL,
    severity        VARCHAR(20) DEFAULT 'medium', -- low / medium / high / critical

    -- Linked entities (any combination)
    transaction_id  UUID REFERENCES transactions(id),
    property_id     UUID REFERENCES properties(id),
    rera_project_id UUID REFERENCES rera_projects(id),
    listing_id      UUID REFERENCES listings(id),

    -- Description
    title           TEXT NOT NULL,
    description     TEXT,
    evidence        JSONB,                     -- supporting data points

    -- Resolution
    status          VARCHAR(30) DEFAULT 'open', -- open / investigating / resolved / false_positive
    resolved_at     TIMESTAMPTZ,
    notes           TEXT,

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_flags_type       ON suspicious_flags(flag_type);
CREATE INDEX idx_flags_severity   ON suspicious_flags(severity);
CREATE INDEX idx_flags_txn        ON suspicious_flags(transaction_id);
CREATE INDEX idx_flags_property   ON suspicious_flags(property_id);
CREATE INDEX idx_flags_status     ON suspicious_flags(status);

-- ============================================================
-- SCRAPER RUN LOG
-- ============================================================

CREATE TABLE scraper_runs (
    id              SERIAL PRIMARY KEY,
    scraper_name    VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20) DEFAULT 'running', -- running / success / failed / partial
    records_fetched INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    errors          JSONB,
    config          JSONB
);

CREATE INDEX idx_scraper_runs_name   ON scraper_runs(scraper_name);
CREATE INDEX idx_scraper_runs_status ON scraper_runs(status);

-- ============================================================
-- VIEWS — CONVENIENCE QUERIES
-- ============================================================

-- Latest price per zone
CREATE VIEW v_zone_price_summary AS
SELECT
    z.name AS zone_name,
    z.pin_code,
    c.name AS city,
    ph.property_type,
    ph.avg_price_sqft,
    ph.median_price_sqft,
    ph.total_listings,
    ph.txn_count,
    ph.period_date,
    LAG(ph.avg_price_sqft) OVER (
        PARTITION BY ph.zone_id, ph.property_type
        ORDER BY ph.period_date
    ) AS prev_month_price,
    ROUND(
        (ph.avg_price_sqft - LAG(ph.avg_price_sqft) OVER (
            PARTITION BY ph.zone_id, ph.property_type ORDER BY ph.period_date
        )) / NULLIF(LAG(ph.avg_price_sqft) OVER (
            PARTITION BY ph.zone_id, ph.property_type ORDER BY ph.period_date
        ), 0) * 100, 2
    ) AS mom_change_pct
FROM price_history ph
JOIN zones z ON z.id = ph.zone_id
JOIN cities c ON c.id = ph.city_id;

-- Properties near govt projects
CREATE VIEW v_property_near_govt AS
SELECT
    p.id AS property_id,
    p.address_raw,
    g.title AS govt_project,
    g.project_type,
    g.contract_value,
    g.status AS project_status,
    ST_Distance(p.location::geography, g.location::geography) AS distance_m
FROM properties p
JOIN govt_projects g ON ST_DWithin(
    p.location::geography,
    g.location::geography,
    g.impact_radius_m
)
WHERE p.location IS NOT NULL AND g.location IS NOT NULL;

-- ============================================================
-- DATA API GRANTS
-- ============================================================
-- Supabase Data API access must be explicit for new public-schema
-- tables. Keep anonymous/dashboard access read-only; backend scrapers
-- and model jobs should use the service_role key for writes.

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

-- Static dashboard reads. Do not grant anon writes to base tables.
GRANT SELECT ON TABLE
    public.cities,
    public.zones,
    public.listings,
    public.rera_projects,
    public.suspicious_flags,
    public.price_history,
    public.v_zone_price_summary,
    public.v_property_near_govt
TO anon, authenticated;

-- Backend jobs using SUPABASE_SERVICE_KEY need full Data API access.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;

-- Dashboard review actions should go through SECURITY DEFINER RPCs such as
-- public.review_flag_secure, not direct table updates.
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated;

-- RLS: dashboard-facing tables are public-read only; internal tables have RLS
-- enabled with no anon/authenticated policies. service_role bypasses RLS for
-- backend jobs and scrapers.
ALTER TABLE public.cities ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.zones ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.price_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.rera_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.rera_complaints ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.govt_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.suspicious_flags ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.scraper_runs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public read cities"
    ON public.cities
    FOR SELECT
    TO anon, authenticated
    USING (true);

CREATE POLICY "public read zones"
    ON public.zones
    FOR SELECT
    TO anon, authenticated
    USING (true);

CREATE POLICY "public read active 99acres listings"
    ON public.listings
    FOR SELECT
    TO anon, authenticated
    USING (
        source = '99acres'
        AND COALESCE(listing_status, 'active') = 'active'
    );

CREATE POLICY "public read rera projects"
    ON public.rera_projects
    FOR SELECT
    TO anon, authenticated
    USING (true);

CREATE POLICY "public read suspicious flags"
    ON public.suspicious_flags
    FOR SELECT
    TO anon, authenticated
    USING (
        COALESCE(status, 'open') IN ('open', 'confirmed', 'dismissed', 'monitoring')
    );

CREATE POLICY "public read price history"
    ON public.price_history
    FOR SELECT
    TO anon, authenticated
    USING (true);

ALTER VIEW public.v_zone_price_summary SET (security_invoker = true);
ALTER VIEW public.v_property_near_govt SET (security_invoker = true);

-- Future tables/sequences/functions created by this migration role.
-- Add per-table anon/authenticated SELECT grants for any new dashboard-facing
-- table instead of making all future tables public by default.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO service_role;

-- ============================================================
-- SEED DATA — Cities
-- ============================================================

INSERT INTO cities (name, state) VALUES
    ('Akola', 'Maharashtra'),
    ('Nagpur', 'Maharashtra'),
    ('Pune', 'Maharashtra'),
    ('Mumbai', 'Maharashtra'),
    ('Nashik', 'Maharashtra'),
    ('Aurangabad', 'Maharashtra');

-- Akola zones (major localities)
INSERT INTO zones (city_id, name, pin_code, zone_type) VALUES
    (1, 'Ramdaspeth',      '444001', 'residential'),
    (1, 'Civil Lines',     '444001', 'mixed'),
    (1, 'Jatharpeth',      '444005', 'residential'),
    (1, 'Murtijapur Road', '444002', 'mixed'),
    (1, 'Akot Road',       '444004', 'mixed'),
    (1, 'MIDC Akola',      '444004', 'industrial'),
    (1, 'Murtizapur',      '444107', 'mixed'),
    (1, 'Borgaon Manju',   '444104', 'agricultural');
