-- ============================================================
-- Migration 0026: Additional Materialized Views for getSalesAnalytics
--
-- Context (Adil 2026-05-13): «продажи раз в месяц — нет смысла каждый раз
-- пересчитывать, пусть берёт готовый результат». Stage B1.1 — убрать
-- последние loadAllParallel('sales') calls из getSalesAnalytics.
--
-- После миграции 0018 у нас уже есть mv_sales_monthly (для timeline+KPI)
-- и mv_partner_aggregates (lifetime per-partner). Не хватало:
--   - per-agency aggregates по месяцам → top_studios + agency movers
--   - per-contact (partner/customer role) aggregates по месяцам
--     → top_partners + contact movers + segments (one-time vs repeat)
--   - per-category aggregates по месяцам → categories chart
--
-- Все 3 MV — refresh CONCURRENTLY не работает (NULLable cols в unique
-- indexes — см. 0019 fix). Поэтому plain REFRESH, ~1-3s lock на чтение.
-- Cron mvRefresh.js (04:30 Almaty) подхватит автоматически через RPC.
--
-- Idempotent: IF NOT EXISTS на VIEW + INDEX. Run в Supabase SQL Editor
-- (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================


-- ─── 1. mv_sales_agency_monthly ─────────────────────────────────────────────
--
-- Per-agency aggregates по месяцам. Покрывает:
--   - top_studios chart (агрегируем по agency_id, sum revenue, top-15)
--   - movers per agency (curr vs prev month)
--
-- Только rows с agency_id IS NOT NULL — sales без agency для top_studios
-- не нужны.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_agency_monthly AS
SELECT
  s.agency_id,
  date_trunc('month', s.sale_date)::date                                       AS month,
  CASE WHEN s.source_file LIKE 'Алматы%' THEN 'Алматы' ELSE 'Астана' END      AS shop_city,
  CASE WHEN s.partner_id IS NOT NULL THEN 'b2b' ELSE 'b2c' END                AS channel,
  COUNT(*)                                                                     AS orders_count,
  SUM(s.total_amount)                                                          AS total_revenue
FROM sales s
WHERE s.sale_date IS NOT NULL
  AND s.agency_id IS NOT NULL
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_agency_monthly_pk
  ON mv_sales_agency_monthly (agency_id, month, shop_city, channel);

CREATE INDEX IF NOT EXISTS idx_mv_sales_agency_monthly_filter
  ON mv_sales_agency_monthly (shop_city, channel, month);


-- ─── 2. mv_sales_contact_monthly ────────────────────────────────────────────
--
-- Per-contact aggregates по месяцам, с ролью (partner / customer). Одна
-- продажа может попасть В ОБЕ роли если partner_id != customer_id, или
-- только в одну если поля совпадают / null. UNION ниже дедуплицирует.
--
-- Покрывает:
--   - top_partners chart (top-15 по revenue)
--   - movers per contact (curr vs prev month)
--   - segments one_time / repeat (count distinct customers per role)

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_contact_monthly AS
SELECT
  contact_id,
  role,
  month,
  shop_city,
  channel,
  SUM(orders_count)::int  AS orders_count,
  SUM(total_revenue)      AS total_revenue
FROM (
  -- partner role
  SELECT
    s.partner_id                                                             AS contact_id,
    'partner'::text                                                          AS role,
    date_trunc('month', s.sale_date)::date                                   AS month,
    CASE WHEN s.source_file LIKE 'Алматы%' THEN 'Алматы' ELSE 'Астана' END  AS shop_city,
    CASE WHEN s.partner_id IS NOT NULL THEN 'b2b' ELSE 'b2c' END            AS channel,
    1                                                                         AS orders_count,
    COALESCE(s.total_amount, 0)                                              AS total_revenue
  FROM sales s
  WHERE s.sale_date IS NOT NULL AND s.partner_id IS NOT NULL
  UNION ALL
  -- customer role
  SELECT
    s.customer_id                                                            AS contact_id,
    'customer'::text                                                         AS role,
    date_trunc('month', s.sale_date)::date                                   AS month,
    CASE WHEN s.source_file LIKE 'Алматы%' THEN 'Алматы' ELSE 'Астана' END  AS shop_city,
    CASE WHEN s.partner_id IS NOT NULL THEN 'b2b' ELSE 'b2c' END            AS channel,
    1                                                                         AS orders_count,
    COALESCE(s.total_amount, 0)                                              AS total_revenue
  FROM sales s
  WHERE s.sale_date IS NOT NULL AND s.customer_id IS NOT NULL
) sub
GROUP BY contact_id, role, month, shop_city, channel;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sales_contact_monthly_pk
  ON mv_sales_contact_monthly (contact_id, role, month, shop_city, channel);

CREATE INDEX IF NOT EXISTS idx_mv_sales_contact_monthly_filter
  ON mv_sales_contact_monthly (shop_city, channel, month);

CREATE INDEX IF NOT EXISTS idx_mv_sales_contact_monthly_revenue
  ON mv_sales_contact_monthly (total_revenue DESC);


-- ─── 3. mv_sale_items_category_monthly ──────────────────────────────────────
--
-- Per-category aggregates по месяцам. JOIN-им sale_items к sales чтобы
-- получить shop_city + channel (sale_items сами не имеют этих полей).
--
-- Покрывает categories chart на Обзоре.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sale_items_category_monthly AS
SELECT
  COALESCE(si.category, 'other')                                             AS category,
  date_trunc('month', s.sale_date)::date                                     AS month,
  CASE WHEN s.source_file LIKE 'Алматы%' THEN 'Алматы' ELSE 'Астана' END    AS shop_city,
  CASE WHEN s.partner_id IS NOT NULL THEN 'b2b' ELSE 'b2c' END              AS channel,
  COUNT(*)                                                                    AS items_count,
  SUM(COALESCE(si.amount, 0))                                                AS total_revenue
FROM sale_items si
JOIN sales s ON s.id = si.sale_id
WHERE s.sale_date IS NOT NULL
GROUP BY 1, 2, 3, 4;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_sale_items_category_monthly_pk
  ON mv_sale_items_category_monthly (category, month, shop_city, channel);

CREATE INDEX IF NOT EXISTS idx_mv_sale_items_category_monthly_filter
  ON mv_sale_items_category_monthly (shop_city, channel, month);


-- ─── 4. Update refresh_sales_mvs() to refresh all 5 MVs ────────────────────
--
-- Без CONCURRENTLY (см. 0019 — NULL deduplication issue с unique indexes).
-- Plain REFRESH блокирует SELECT на ~3-5s. Запускается в 04:30 Almaty
-- (низкий трафик) cron-ом mvRefresh.js + вручную через
-- POST /admin/sales-crm/refresh-mvs (x-api-key required).

CREATE OR REPLACE FUNCTION refresh_sales_mvs()
RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW mv_sales_monthly;
  REFRESH MATERIALIZED VIEW mv_partner_aggregates;
  REFRESH MATERIALIZED VIEW mv_sales_agency_monthly;
  REFRESH MATERIALIZED VIEW mv_sales_contact_monthly;
  REFRESH MATERIALIZED VIEW mv_sale_items_category_monthly;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ─── 5. Initial populate ───────────────────────────────────────────────────
--
-- CREATE MATERIALIZED VIEW автоматически populates при создании, но если
-- IF NOT EXISTS вернул false (MV уже была) — данные могут быть устаревшие.
-- Безопасно сделать explicit refresh.

SELECT refresh_sales_mvs();


-- ============================================================
-- Verification:
-- ============================================================
-- SELECT COUNT(*) FROM mv_sales_agency_monthly;
--   → ~50-200 (per agency × month × city × channel)
--
-- SELECT COUNT(*) FROM mv_sales_contact_monthly;
--   → ~2000-5000 (per contact × role × month × city × channel)
--
-- SELECT COUNT(*) FROM mv_sale_items_category_monthly;
--   → ~200-500 (per category × month × city × channel)
--
-- SELECT contact_id, role, SUM(total_revenue) FROM mv_sales_contact_monthly
-- GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5;
--   → top revenue contacts (sanity check vs current top_partners output)
-- ============================================================
