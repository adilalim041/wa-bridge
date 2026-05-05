-- ============================================================
-- Migration 0022: Meta Marketing API — core schema
--
-- Создаём хранилище для синхронизации рекламных кабинетов Facebook/Instagram.
-- Phase 1 (2026-05-04): single-tenant, Adil + ad account act_1347507236049165.
-- Phase 2 (future): multi-tenant — добавить policy USING(user_id = auth.uid()).
--
-- Решения зафиксированы в pre-flight (2026-05-04):
--   - Все money-поля в minor units (центах/тыйынах) — делить на 100 при показе.
--   - Timezone insights-агрегации = timezone_name кабинета (Asia/Omsk + USD у Adil).
--   - Idempotency: UPSERT на (ad_account_id, meta_*_id) + sync_locks таблица.
--   - Concurrent calls ≤ 3, sync 1 раз в 6h per ad_account.
--   - access_token — plain text в phase 1; phase 2 шифровать на уровне приложения.
--
-- Источник истины: ObsidianVault/research/library/backend-libs/meta-marketing-api.md
-- Idempotent. Run in Supabase SQL Editor (project WPAdil).
-- ============================================================

-- ------------------------------------------------------------
-- Хелпер: автообновление updated_at (может уже существовать)
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- 1. meta_ad_accounts — рекламные кабинеты
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_ad_accounts (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id           uuid,                                   -- FK auth.users, nullable в phase 1
  meta_account_id   text        NOT NULL,                   -- "act_1347507236049165"
  account_name      text        NOT NULL,
  currency          text        NOT NULL,                   -- "USD", "KZT"
  timezone_name     text        NOT NULL,                   -- "Asia/Omsk"
  access_token      text        NOT NULL,                   -- plain text, phase 1
  is_active         boolean     NOT NULL DEFAULT true,
  last_sync_at      timestamptz,
  last_sync_status  text,                                   -- 'ok' | 'partial' | 'error'
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (meta_account_id)
);

ALTER TABLE meta_ad_accounts ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_ad_accounts;
CREATE POLICY "service_role_all"
  ON meta_ad_accounts
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy USING(user_id = auth.uid())

CREATE TRIGGER set_meta_ad_accounts_updated_at
  BEFORE UPDATE ON meta_ad_accounts
  FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();

-- ============================================================
-- 2. meta_campaigns — рекламные кампании
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_campaigns (
  id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  ad_account_id     uuid        NOT NULL REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  meta_campaign_id  text        NOT NULL,
  name              text        NOT NULL,
  objective         text,                                   -- nullable, Meta может не вернуть
  status            text        NOT NULL,                   -- ACTIVE/PAUSED/DELETED/ARCHIVED
  daily_budget      bigint,                                 -- minor units, nullable
  lifetime_budget   bigint,
  created_time      timestamptz,
  last_seen_at      timestamptz NOT NULL DEFAULT now(),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ad_account_id, meta_campaign_id)
);

ALTER TABLE meta_campaigns ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_campaigns;
CREATE POLICY "service_role_all"
  ON meta_campaigns
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

CREATE INDEX IF NOT EXISTS idx_meta_campaigns_account_status
  ON meta_campaigns (ad_account_id, status);

CREATE TRIGGER set_meta_campaigns_updated_at
  BEFORE UPDATE ON meta_campaigns
  FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();

-- ============================================================
-- 3. meta_ad_sets — группы объявлений
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_ad_sets (
  id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  ad_account_id       uuid        NOT NULL REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  campaign_id         uuid        NOT NULL REFERENCES meta_campaigns(id) ON DELETE CASCADE,
  meta_adset_id       text        NOT NULL,
  name                text        NOT NULL,
  status              text        NOT NULL,
  daily_budget        bigint,
  lifetime_budget     bigint,
  optimization_goal   text,
  billing_event       text,
  bid_strategy        text,
  targeting           jsonb,                                -- raw payload
  placements          jsonb,
  is_advantage_plus   boolean     NOT NULL DEFAULT false,
  schedule_start      timestamptz,
  schedule_end        timestamptz,
  last_seen_at        timestamptz NOT NULL DEFAULT now(),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ad_account_id, meta_adset_id)
);

ALTER TABLE meta_ad_sets ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_ad_sets;
CREATE POLICY "service_role_all"
  ON meta_ad_sets
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

CREATE TRIGGER set_meta_ad_sets_updated_at
  BEFORE UPDATE ON meta_ad_sets
  FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();

-- ============================================================
-- 4. meta_creatives — рекламные креативы
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_creatives (
  id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  ad_account_id       uuid        NOT NULL REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  meta_creative_id    text        NOT NULL,
  title               text,
  body                text,
  cta_type            text,
  image_url           text,                                 -- от Meta, может протухнуть
  thumbnail_url       text,
  video_id            text,
  cached_image_url    text,                                 -- Cloudinary URL после кэширования (phase 2)
  object_story_spec   jsonb,                                -- полный raw payload
  last_seen_at        timestamptz NOT NULL DEFAULT now(),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ad_account_id, meta_creative_id)
);

ALTER TABLE meta_creatives ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_creatives;
CREATE POLICY "service_role_all"
  ON meta_creatives
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

CREATE TRIGGER set_meta_creatives_updated_at
  BEFORE UPDATE ON meta_creatives
  FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();

-- ============================================================
-- 5. meta_ads — отдельные объявления
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_ads (
  id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  ad_account_id   uuid        NOT NULL REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  campaign_id     uuid        NOT NULL REFERENCES meta_campaigns(id) ON DELETE CASCADE,
  ad_set_id       uuid        NOT NULL REFERENCES meta_ad_sets(id) ON DELETE CASCADE,
  creative_id     uuid        REFERENCES meta_creatives(id) ON DELETE SET NULL,
  meta_ad_id      text        NOT NULL,
  name            text        NOT NULL,
  status          text        NOT NULL,
  created_time    timestamptz,
  last_seen_at    timestamptz NOT NULL DEFAULT now(),
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ad_account_id, meta_ad_id)
);

ALTER TABLE meta_ads ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_ads;
CREATE POLICY "service_role_all"
  ON meta_ads
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

CREATE INDEX IF NOT EXISTS idx_meta_ads_campaign
  ON meta_ads (campaign_id);

CREATE INDEX IF NOT EXISTS idx_meta_ads_adset
  ON meta_ads (ad_set_id);

CREATE TRIGGER set_meta_ads_updated_at
  BEFORE UPDATE ON meta_ads
  FOR EACH ROW EXECUTE FUNCTION trigger_set_timestamp();

-- ============================================================
-- 6. meta_insights_daily — агрегированные метрики по дням
-- ============================================================
-- Примечание: bigserial PK + UNIQUE constraint = idempotent UPSERT через
-- ON CONFLICT (ad_account_id, level, object_id, date_start) DO UPDATE.
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_insights_daily (
  id            bigserial   PRIMARY KEY,
  ad_account_id uuid        NOT NULL REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  level         text        NOT NULL CHECK (level IN ('campaign', 'adset', 'ad')),
  object_id     text        NOT NULL,                       -- meta_campaign_id / meta_adset_id / meta_ad_id
  date_start    date        NOT NULL,                       -- по timezone_name кабинета
  impressions   bigint      NOT NULL DEFAULT 0,
  clicks        bigint      NOT NULL DEFAULT 0,
  spend         bigint      NOT NULL DEFAULT 0,             -- minor units
  reach         bigint      NOT NULL DEFAULT 0,
  frequency     numeric(10, 4),
  ctr           numeric(10, 4),
  cpm           bigint,                                     -- minor units
  cpc           bigint,                                     -- minor units
  actions       jsonb,                                      -- {lead:12, purchase:3, ...}
  synced_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (ad_account_id, level, object_id, date_start)
);

ALTER TABLE meta_insights_daily ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_insights_daily;
CREATE POLICY "service_role_all"
  ON meta_insights_daily
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

-- Основной запрос для UI: "последние 7/30 дней по кабинету"
CREATE INDEX IF NOT EXISTS idx_meta_insights_daily_account_date
  ON meta_insights_daily (ad_account_id, date_start DESC);

-- Drill-down: метрики конкретной кампании/адсета/объявления
CREATE INDEX IF NOT EXISTS idx_meta_insights_daily_level_object
  ON meta_insights_daily (level, object_id);

-- ============================================================
-- 7. meta_sync_locks — распределённая блокировка sync per кабинет
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_sync_locks (
  ad_account_id   uuid        PRIMARY KEY REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  locked_at       timestamptz NOT NULL DEFAULT now(),
  locked_by       text        NOT NULL,                     -- instance id (Railway container)
  heartbeat_at    timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE meta_sync_locks ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_sync_locks;
CREATE POLICY "service_role_all"
  ON meta_sync_locks
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

-- ============================================================
-- 8. meta_sync_log — журнал операций синхронизации
-- ============================================================
CREATE TABLE IF NOT EXISTS meta_sync_log (
  id              bigserial   PRIMARY KEY,
  ad_account_id   uuid        NOT NULL REFERENCES meta_ad_accounts(id) ON DELETE CASCADE,
  sync_type       text        NOT NULL,                     -- 'campaigns'/'adsets'/'ads'/'creatives'/'insights'/'full'
  status          text        NOT NULL,                     -- 'ok'/'partial'/'error'/'started'
  started_at      timestamptz NOT NULL DEFAULT now(),
  completed_at    timestamptz,
  records_synced  integer     NOT NULL DEFAULT 0,
  error_code      text,
  error_message   text,
  error_details   jsonb
);

ALTER TABLE meta_sync_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "service_role_all" ON meta_sync_log;
CREATE POLICY "service_role_all"
  ON meta_sync_log
  FOR ALL
  TO service_role
  USING (true);
-- Phase 2 (multi-tenant): добавить policy через JOIN на meta_ad_accounts.user_id

-- Дашборд "последний sync по кабинету"
CREATE INDEX IF NOT EXISTS idx_meta_sync_log_account_started
  ON meta_sync_log (ad_account_id, started_at DESC);

-- ============================================================
-- Verification:
-- ============================================================
-- Ожидается 8 таблиц с префиксом meta_:
-- SELECT table_name
--   FROM information_schema.tables
--   WHERE table_schema = 'public'
--     AND table_name LIKE 'meta_%'
--   ORDER BY table_name;
--
-- Ожидается RLS enabled на всех 8:
-- SELECT tablename, rowsecurity
--   FROM pg_tables
--   WHERE schemaname = 'public'
--     AND tablename LIKE 'meta_%'
--   ORDER BY tablename;
--
-- Ожидается 8 политик service_role_all:
-- SELECT tablename, policyname
--   FROM pg_policies
--   WHERE schemaname = 'public'
--     AND tablename LIKE 'meta_%'
--   ORDER BY tablename;
--
-- Ожидается 6 индексов помимо PK:
-- SELECT indexname, tablename
--   FROM pg_indexes
--   WHERE schemaname = 'public'
--     AND tablename LIKE 'meta_%'
--     AND indexname NOT LIKE '%_pkey'
--   ORDER BY tablename, indexname;
