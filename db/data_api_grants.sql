-- Supabase Data API grants for Real Estate Intel.
--
-- Run this once in Supabase SQL Editor for the current project, and keep the
-- same grant pattern in future table migrations. It is intentionally read-only
-- for anon/authenticated dashboard access; scrapers and backend model jobs
-- should continue using SUPABASE_SERVICE_KEY / service_role for writes.

GRANT USAGE ON SCHEMA public TO anon, authenticated, service_role;

-- Dashboard / static app reads.
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

-- Optional read exposure for future dashboard sections. Keep commented until
-- those screens exist, because these tables may contain more sensitive data.
-- GRANT SELECT ON TABLE public.rera_complaints TO anon, authenticated;
-- GRANT SELECT ON TABLE public.govt_projects TO anon, authenticated;

-- Backend REST access via service_role.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO service_role;

-- Existing/future RPCs used by the dashboard, e.g. review_flag_secure.
-- Keep RPC bodies SECURITY DEFINER where they perform controlled writes.
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO anon, authenticated;

-- Default privileges for objects created later by this migration owner.
-- Do not grant anon/authenticated on all future tables globally; add explicit
-- SELECT grants per dashboard-facing table when a new table is introduced.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT EXECUTE ON FUNCTIONS TO service_role;
