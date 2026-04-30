-- ============================================================
-- Migration 0012: sale_items category + unified sales_view
--
-- 1. Adds `category` (text) to sale_items for heuristic categorization
--    (sink / faucet / disposer / water_filter / dispenser / roll_mat / ...).
--    Categories are NOT canonical SKU-mapped — это эвристика по тексту названия.
--    Точные SKU матчатся через products table (когда появится справочник).
--
-- 2. Creates `sales_view` — denormalized view with sale + customer + partner +
--    agency + items aggregated as JSON, для удобного просмотра в Supabase Editor.
--
-- Run in Supabase SQL Editor (project WPAdil).
-- ============================================================

-- ─── 1. Add category to sale_items ───────────────────────────
ALTER TABLE sale_items
  ADD COLUMN IF NOT EXISTS category TEXT;

CREATE INDEX IF NOT EXISTS idx_sale_items_category
  ON sale_items (category)
  WHERE category IS NOT NULL;

-- ─── 2. Unified view for browsing ────────────────────────────
CREATE OR REPLACE VIEW sales_view AS
SELECT
  s.id,
  s.sale_date,
  s.order_num,
  s.source_file,
  s.total_amount,
  s.city,
  s.address,
  s.manager,
  s.payment_method,
  s.status_text,
  s.delivery_date,
  s.delivery_status,
  s.inventory_text,
  s.commission_text,
  s.comment,
  s.lead_source,

  -- Customer
  c.canonical_name AS customer_name,
  c.primary_phone  AS customer_phone,
  c.aliases        AS customer_aliases,
  c.id             AS customer_id,

  -- Partner (designer/woodworker/...)
  p.canonical_name AS partner_name,
  p.primary_phone  AS partner_phone,
  p.aliases        AS partner_aliases,
  p.id             AS partner_id,

  -- Agency (studio)
  a.canonical_name AS agency_name,
  a.id             AS agency_id,

  -- Raw fields (для audit)
  s.customer_raw,
  s.partner_raw,

  -- Items (aggregated JSON)
  (
    SELECT json_agg(
      json_build_object(
        'position',  si.position_idx,
        'name',      si.raw_name,
        'sku',       si.sku,
        'qty',       si.qty,
        'price',     si.price_per_unit,
        'amount',    si.amount,
        'category',  si.category
      ) ORDER BY si.position_idx
    )
    FROM sale_items si WHERE si.sale_id = s.id
  ) AS items,

  -- Items count + categories breakdown (для быстрых KPI)
  (SELECT count(*) FROM sale_items si WHERE si.sale_id = s.id) AS items_count,
  (SELECT array_agg(DISTINCT category) FILTER (WHERE category IS NOT NULL)
     FROM sale_items si WHERE si.sale_id = s.id) AS categories,

  s.imported_at
FROM sales s
LEFT JOIN partner_contacts c ON s.customer_id = c.id
LEFT JOIN partner_contacts p ON s.partner_id  = p.id
LEFT JOIN agencies         a ON s.agency_id   = a.id;

-- Sanity check
SELECT
  (SELECT count(*) FROM sale_items WHERE category IS NOT NULL) AS items_categorized_so_far,
  (SELECT count(*) FROM sales_view) AS view_rows;
