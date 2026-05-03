-- ============================================================
-- Migration 0018: Materialized Views for Sales Analytics
--
-- Context: Sales-CRM endpoints load full tables into RAM for every
-- cold-cache request. With ~4300 sales × 12 445 sale_items on Railway
-- 512 MB, concurrent requests risk OOM-kill (sigkill bypasses Sentry).
-- This migration creates two materialized views that pre-aggregate the
-- heaviest data so future endpoints can query aggregates instead of
-- row-scanning full tables in JS.
--
-- Views created:
--   mv_sales_monthly      — per (month, shop_city, delivery_city, channel, manager)
--   mv_partner_aggregates — per partner_contact
--
-- Function created:
--   refresh_sales_mvs()   — CONCURRENTLY refreshes both MVs in one RPC call
--
-- Idempotent: CREATE MATERIALIZED VIEW IF NOT EXISTS, CREATE INDEX IF NOT EXISTS.
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
--
-- IMPORTANT: mv_partner_aggregates uses a UNIQUE index required for
-- CONCURRENTLY refresh. The initial refresh must be done synchronously
-- (REFRESH MATERIALIZED VIEW, not CONCURRENTLY) because CONCURRENTLY
-- requires at least one completed populate. Both MVs are populated by
-- this script via the non-concurrent refresh call at the bottom.
--
-- After running this migration the Bridge cron (mvRefresh.js) will keep
-- both MVs fresh daily at 04:30 Almaty. Manual refresh:
--   POST /admin/sales-crm/refresh-mvs  (x-api-key required)
-- ============================================================


-- ─── 1. mv_sales_monthly ────────────────────────────────────────────────────
--
-- Aggregates sales per (month, shop_city, delivery_city, channel, manager).
-- shop_city is derived from source_file prefix (same logic as salesCrm.js applyCity).
-- channel: 'b2b' when partner_id IS NOT NULL, else 'b2c'.
--
-- Future endpoints can JOIN this view instead of loading all sales rows
-- and bucketing in JS — eliminates the full-table-scan pattern.
--
-- NOTE: NULL manager values are kept as-is so aggregates are complete.
-- Callers that need "manager unknown" grouping should COALESCE(manager, 'Неизвестно').

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_monthly AS
SELECT
  date_trunc('month', sale_date)::date                                          AS month,
  CASE WHEN source_file LIKE 'Алматы%' THEN 'Алматы' ELSE 'Астана' END         AS shop_city,
  city                                                                           AS delivery_city,
  CASE WHEN partner_id IS NOT NULL THEN 'b2b' ELSE 'b2c' END                   AS channel,
  manager,
  COUNT(*)                                                                       AS orders_count,
  COUNT(DISTINCT customer_id)                                                    AS unique_customers,
  SUM(total_amount)                                                              AS total_revenue,
  AVG(total_amount)                                                              AS avg_check
FROM sales
WHERE sale_date IS NOT NULL
GROUP BY 1, 2, 3, 4, 5;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_monthly_pk
  ON mv_sales_monthly (month, shop_city, COALESCE(delivery_city, ''), COALESCE(channel, ''), COALESCE(manager, ''));

-- Support typical dashboard queries: city + month range filter
CREATE INDEX IF NOT EXISTS idx_mv_sales_monthly_shop
  ON mv_sales_monthly (shop_city, month);

CREATE INDEX IF NOT EXISTS idx_mv_sales_monthly_month
  ON mv_sales_monthly (month);


-- ─── 2. mv_partner_aggregates ───────────────────────────────────────────────
--
-- Aggregates per partner_contact. Replaces the scatter of JS-side grouping
-- in getSalesAnalytics top_partners section and getInsightsSummary.
-- shop_cities is an array of distinct shops the partner appeared in.
--
-- Note: LEFT JOIN so partners with 0 sales are included (useful for
-- "no orders yet" segment reporting). orders_count=0 means no linked sales.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_partner_aggregates AS
SELECT
  pc.id                                                                         AS partner_id,
  pc.canonical_name,
  pc.agency_id,
  COUNT(DISTINCT s.id)                                                           AS orders_count,
  COALESCE(SUM(s.total_amount), 0)                                              AS total_revenue,
  AVG(s.total_amount)                                                            AS avg_check,
  MIN(s.sale_date)                                                               AS first_sale_date,
  MAX(s.sale_date)                                                               AS last_sale_date,
  ARRAY_REMOVE(
    ARRAY_AGG(
      DISTINCT CASE WHEN s.source_file LIKE 'Алматы%' THEN 'Алматы' ELSE 'Астана' END
    ),
    NULL
  )                                                                              AS shop_cities
FROM partner_contacts pc
LEFT JOIN sales s ON s.partner_id = pc.id
WHERE 'partner' = ANY(pc.roles)
GROUP BY pc.id, pc.canonical_name, pc.agency_id;

-- Unique index required for CONCURRENTLY refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_partner_aggregates_pk
  ON mv_partner_aggregates (partner_id);

-- Support ORDER BY total_revenue DESC (listPartners top_revenue filter)
CREATE INDEX IF NOT EXISTS idx_mv_partner_aggregates_revenue
  ON mv_partner_aggregates (total_revenue DESC);

-- Support agency-level rollup
CREATE INDEX IF NOT EXISTS idx_mv_partner_aggregates_agency
  ON mv_partner_aggregates (agency_id);


-- ─── 3. refresh_sales_mvs() ─────────────────────────────────────────────────
--
-- RPC function called by:
--   - POST /admin/sales-crm/refresh-mvs  (manual, x-api-key)
--   - Bridge cron mvRefresh.js            (daily 04:30 Almaty)
--
-- Uses CONCURRENTLY so existing data stays queryable during refresh.
-- CONCURRENTLY requires the unique indexes above.
-- Estimated runtime: < 2s on current dataset (4300 sales / 4500 partner_contacts).
-- Will grow sub-linearly with data (aggregation is O(n) scan, not join).

CREATE OR REPLACE FUNCTION refresh_sales_mvs()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sales_monthly;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_partner_aggregates;
END;
$$;

-- Grant execute to authenticated users so sb.rpc('refresh_sales_mvs') works
-- from service_role path (which is what Bridge admin endpoints use).
-- Authenticated users in dashboard cannot call this — it's gated by adminOnly middleware.
GRANT EXECUTE ON FUNCTION refresh_sales_mvs() TO service_role;


-- ─── 4. Initial populate (non-concurrent, safe for first run) ───────────────
--
-- CONCURRENTLY refresh requires at least one completed populate.
-- We do a standard blocking refresh here (fast on empty MV, only blocks writers).

REFRESH MATERIALIZED VIEW mv_sales_monthly;
REFRESH MATERIALIZED VIEW mv_partner_aggregates;


-- ============================================================
-- Verification:
-- ============================================================
-- SELECT COUNT(*) FROM mv_sales_monthly;
--   → should be > 0 (one row per month × shop_city × delivery_city × channel × manager)
--
-- SELECT COUNT(*) FROM mv_partner_aggregates;
--   → should equal COUNT(*) FROM partner_contacts WHERE 'partner' = ANY(roles)
--
-- SELECT indexname FROM pg_indexes WHERE tablename IN ('mv_sales_monthly', 'mv_partner_aggregates');
--   → idx_mv_sales_monthly_pk, idx_mv_sales_monthly_shop, idx_mv_sales_monthly_month
--   → idx_mv_partner_aggregates_pk, idx_mv_partner_aggregates_revenue, idx_mv_partner_aggregates_agency
--
-- SELECT routine_name FROM information_schema.routines WHERE routine_name = 'refresh_sales_mvs';
--   → refresh_sales_mvs
--
-- Test CONCURRENT refresh (should complete without error):
--   SELECT refresh_sales_mvs();
-- ============================================================
