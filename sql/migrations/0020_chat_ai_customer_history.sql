-- ============================================================
-- Migration 0020: chat_ai customer history columns
--
-- Расширяем chat_ai полями про предыдущие покупки клиента, чтобы
-- daily-wa-analysis мог считать "источник диалогов" точнее: новый клиент,
-- постоянный клиент, лид с рекламной кампании, и т.д.
--
-- Adil 2026-05-04: «он может видеть, заказывал ли у нас клиент ранее или
-- нет. То есть добавить переменные новые и может новый блок к анализу».
--
-- Источник истины — partner_contacts: есть запись с total_purchases_count>0
-- по совпадению phone digits → клиент покупал.
--
-- Idempotent. Run in Supabase SQL Editor (project WPAdil).
-- ============================================================

-- 1. is_existing_customer — был ли клиент в нашей базе продаж на момент анализа.
--    NULL = ещё не определено (старые записи / re-analyze идут с NULL до пере-анализа)
ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS is_existing_customer BOOLEAN;

-- 2. previous_orders_count — сколько заказов у клиента до этого диалога
ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS previous_orders_count INTEGER;

-- 3. previous_orders_amount — суммарная выручка с клиента (₸)
ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS previous_orders_amount NUMERIC(14, 2);

-- 4. last_purchase_date — последняя покупка клиента (для recency)
ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS last_purchase_date DATE;

-- 5. lead_source_detail — расширенная классификация источника лида.
--    lead_source остаётся прежним enum-like ('unknown', 'instagram', etc),
--    detail — свободное поле для конкретики ('omoikiri_ad_grinder', 'returning',
--    'referral_kirill', 'organic_search'). NULL = нет дополнительного контекста.
ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS lead_source_detail TEXT;

-- Индекс для аналитики — "Постоянные клиенты, купившие в последние 90 дней":
CREATE INDEX IF NOT EXISTS idx_chat_ai_existing_customer
  ON chat_ai(is_existing_customer, last_purchase_date DESC)
  WHERE is_existing_customer = TRUE;

-- Индекс для фильтра "Новые лиды с рекламы за последние N дней":
CREATE INDEX IF NOT EXISTS idx_chat_ai_lead_source_detail
  ON chat_ai(lead_source_detail, analyzed_at DESC)
  WHERE lead_source_detail IS NOT NULL;

-- ============================================================
-- Verification:
-- ============================================================
-- SELECT column_name, data_type, is_nullable
--   FROM information_schema.columns
--   WHERE table_name='chat_ai'
--     AND column_name IN ('is_existing_customer', 'previous_orders_count',
--                         'previous_orders_amount', 'last_purchase_date',
--                         'lead_source_detail');
