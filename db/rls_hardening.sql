-- RLS hardening for Real Estate Intel public schema.
--
-- Run this after db/data_api_grants.sql. It addresses Supabase's
-- rls_disabled_in_public warning without giving anonymous users write access.
--
-- Expected behavior after this patch:
--   - Static dashboard can still read public/dashboard tables.
--   - Dashboard flag review still goes through review_flag_secure RPC.
--   - Scrapers/models using SUPABASE_SERVICE_KEY continue to read/write.
--   - Internal tables have RLS enabled and no anon/authenticated policies.

-- Remove any broad table privileges that may have existed before.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM anon;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM authenticated;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM anon;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM authenticated;

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

-- Dashboard read grants only.
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

-- Backend/service grants.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated;

-- Enable RLS on every current public table.
ALTER TABLE IF EXISTS public.cities ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.zones ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.properties ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.listings ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.price_history ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.rera_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.rera_complaints ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.govt_projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.suspicious_flags ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.scraper_runs ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF to_regclass('public.igr_transactions') IS NOT NULL THEN
        EXECUTE 'ALTER TABLE public.igr_transactions ENABLE ROW LEVEL SECURITY';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON public.igr_transactions TO service_role';
    END IF;
END $$;

-- Dashboard read policies.
DROP POLICY IF EXISTS "public read cities" ON public.cities;
CREATE POLICY "public read cities"
    ON public.cities
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS "public read zones" ON public.zones;
CREATE POLICY "public read zones"
    ON public.zones
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS "public read active 99acres listings" ON public.listings;
CREATE POLICY "public read active 99acres listings"
    ON public.listings
    FOR SELECT
    TO anon, authenticated
    USING (
        source = '99acres'
        AND COALESCE(listing_status, 'active') = 'active'
    );

DROP POLICY IF EXISTS "public read rera projects" ON public.rera_projects;
CREATE POLICY "public read rera projects"
    ON public.rera_projects
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS "public read suspicious flags" ON public.suspicious_flags;
CREATE POLICY "public read suspicious flags"
    ON public.suspicious_flags
    FOR SELECT
    TO anon, authenticated
    USING (
        COALESCE(status, 'open') IN ('open', 'confirmed', 'dismissed', 'monitoring')
    );

DROP POLICY IF EXISTS "public read price history" ON public.price_history;
CREATE POLICY "public read price history"
    ON public.price_history
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- Keep these views subject to invoker permissions/RLS where supported.
ALTER VIEW IF EXISTS public.v_zone_price_summary SET (security_invoker = true);
ALTER VIEW IF EXISTS public.v_property_near_govt SET (security_invoker = true);

-- Future objects created by this migration role: service access by default,
-- public dashboard grants/policies must be added table-by-table.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON TABLES FROM anon, authenticated;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE ALL ON SEQUENCES FROM anon, authenticated;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO service_role;
