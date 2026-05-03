-- Fix refresh_sales_mvs() — drop CONCURRENTLY (NULL deduplication issue).
--
-- 0018 created REFRESH MATERIALIZED VIEW CONCURRENTLY but Postgres rejects it:
--   "cannot refresh materialized view ... concurrently"
-- Cause: unique index on mv_sales_monthly contains NULLable columns
-- (manager / delivery_city). NULL!=NULL means duplicate-NULL rows possible
-- → CONCURRENTLY fails because it can't dedup safely.
--
-- Fix: drop CONCURRENTLY. Plain REFRESH locks the MV for ~1-3 sec
-- (acceptable at 04:30 Almaty cron during low traffic). Selects during
-- refresh will block briefly, не блокирует write path к sales.
--
-- Apply once in Supabase SQL Editor (project WPAdil).

CREATE OR REPLACE FUNCTION refresh_sales_mvs()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW mv_sales_monthly;
  REFRESH MATERIALIZED VIEW mv_partner_aggregates;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Test:
SELECT refresh_sales_mvs();  -- should return void without error
SELECT count(*) FROM mv_sales_monthly;       -- > 0
SELECT count(*) FROM mv_partner_aggregates;  -- > 0
