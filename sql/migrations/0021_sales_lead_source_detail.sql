-- ============================================================
-- Migration 0021: sales.lead_source_detail + receipt_issued
--
-- chat_ai уже получил lead_source_detail (мигр. 0020). sales должен иметь
-- то же — чтобы можно было join'нуть «лид с omoikiri_ad_grinder_discount»
-- → конкретные продажи и считать ROI per кампания.
--
-- Plus receipt_issued для 2023 import (колонка «Чек выбит» в Excel).
--
-- Idempotent. Run in Supabase SQL Editor (project WPAdil).
-- ============================================================

ALTER TABLE sales
  ADD COLUMN IF NOT EXISTS lead_source_detail TEXT;

ALTER TABLE sales
  ADD COLUMN IF NOT EXISTS receipt_issued BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_sales_lead_source_detail
  ON sales(lead_source_detail, sale_date DESC)
  WHERE lead_source_detail IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sales_lead_source
  ON sales(lead_source, sale_date DESC)
  WHERE lead_source IS NOT NULL;

-- ============================================================
-- Verification:
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name='sales'
--      AND column_name IN ('lead_source','lead_source_detail','receipt_issued');
-- ============================================================
