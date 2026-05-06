-- ============================================================
-- Migration 0023: Meta Creatives — destination columns
--
-- Добавляет 3 столбца в meta_creatives для хранения parsed
-- destination info из object_story_spec:
--
--   landing_url               — конечный URL куда ведёт объявление
--   whatsapp_phone            — номер WhatsApp если CTW-объявление
--   whatsapp_message_template — предзаполненный текст из ?text=
--
-- Lazy-fetch: поля заполняются НЕ при bulk-sync, а по запросу
-- через GET /meta-ads/creatives/:id/details (syncCreativeDetails).
-- Причина: object_story_spec нельзя запросить в bulk (Meta error #100
-- на limit:25+ записей). Fetch per-creative на одиночный creative_id проходит.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS — безопасно повторять.
-- Sources: ObsidianVault/research/library/backend-libs/meta-marketing-api.md
--          (раздел Risks: "Please reduce the amount of data" — error #100)
--
-- Last verified: 2026-05-05
-- ============================================================

-- ------------------------------------------------------------
-- 0. effective_status для meta_campaigns и meta_ad_sets
--    Отличие от status: учитывает иерархию родительских объектов.
--    Нужно для delta sync (_syncActiveCampaigns фильтрует по effective_status).
-- ------------------------------------------------------------
ALTER TABLE meta_campaigns
  ADD COLUMN IF NOT EXISTS effective_status TEXT;

ALTER TABLE meta_ad_sets
  ADD COLUMN IF NOT EXISTS effective_status TEXT;

-- Индекс для delta sync запросов по effective_status
CREATE INDEX IF NOT EXISTS idx_meta_campaigns_effective_status
  ON meta_campaigns (ad_account_id, effective_status)
  WHERE effective_status IS NOT NULL;

-- ------------------------------------------------------------
-- 1. landing_url — URL куда ведёт кликабельный CTA
--    Примеры: "https://wa.me/77001234567?text=..." (CTW)
--             "https://omoikiri.kz/products/sink" (обычный трафик)
-- ------------------------------------------------------------
ALTER TABLE meta_creatives
  ADD COLUMN IF NOT EXISTS landing_url TEXT;

-- ------------------------------------------------------------
-- 2. whatsapp_phone — нормализованный номер WA если CTW-реклама
--    Формат: "+77001234567" (только цифры с +, без скобок и пробелов)
--    NULL если не CTW (или ещё не lazy-fetched)
-- ------------------------------------------------------------
ALTER TABLE meta_creatives
  ADD COLUMN IF NOT EXISTS whatsapp_phone TEXT;

-- ------------------------------------------------------------
-- 3. whatsapp_message_template — URL-decoded текст из ?text= параметра
--    Заполняется когда landing_url = wa.me/... с ?text=
--    NULL если нет text-параметра или не CTW
-- ------------------------------------------------------------
ALTER TABLE meta_creatives
  ADD COLUMN IF NOT EXISTS whatsapp_message_template TEXT;

-- ------------------------------------------------------------
-- 4. Индекс для группировки "все креативы с этим WA-номером"
--    Partial — только строки где whatsapp_phone IS NOT NULL,
--    чтобы не занимать место для ~95% строк где поле NULL.
--    Использование: SELECT * FROM meta_creatives WHERE whatsapp_phone = $1
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_meta_creatives_whatsapp_phone
  ON meta_creatives (whatsapp_phone)
  WHERE whatsapp_phone IS NOT NULL;

-- ============================================================
-- Verification:
-- ============================================================
--
-- Проверить наличие 3 новых колонок:
-- SELECT column_name, data_type, is_nullable
--   FROM information_schema.columns
--   WHERE table_schema = 'public'
--     AND table_name = 'meta_creatives'
--     AND column_name IN ('landing_url', 'whatsapp_phone', 'whatsapp_message_template')
--   ORDER BY column_name;
-- Ожидается 3 строки, data_type = 'text', is_nullable = 'YES'.
--
-- Проверить наличие индекса:
-- SELECT indexname, indexdef
--   FROM pg_indexes
--   WHERE tablename = 'meta_creatives'
--     AND indexname = 'idx_meta_creatives_whatsapp_phone';
-- Ожидается 1 строка с WHERE whatsapp_phone IS NOT NULL.
--
-- Проверить что таблица total-number of columns вырасла с 15 до 18:
-- SELECT COUNT(*) FROM information_schema.columns
--   WHERE table_schema = 'public' AND table_name = 'meta_creatives';
-- ============================================================
