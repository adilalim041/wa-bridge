-- ============================================================
-- Migration 0008 — expand settings: new dicts, managers, message_templates
-- Date: 2026-04-22
--
-- Purpose:
--   Phase F.1 — add 4 new columns to tenant_settings, plus two new
--   per-tenant tables: managers and message_templates.
--
-- WARNING: DO NOT run this migration automatically.
--          Execute manually in Supabase Dashboard → SQL Editor.
--          Verify with probe queries at the bottom before deploying.
--
-- Idempotency: all DDL uses IF NOT EXISTS / IF EXISTS / OR REPLACE guards.
--              Safe to re-run after partial failure.
-- ============================================================


-- ============================================================
-- A) Extend tenant_settings with 4 new columns
--
-- lead_sources:    array of string labels for deal lead origin
-- refusal_reasons: array of string labels for why a deal was lost
-- task_types:      array of string keys for CRM task categories
-- company_profile: freeform JSON object with brand info
-- ============================================================

ALTER TABLE tenant_settings
  ADD COLUMN IF NOT EXISTS lead_sources    jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS refusal_reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS task_types      jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS company_profile jsonb NOT NULL DEFAULT '{}'::jsonb;

-- No new RLS needed — existing tenant_settings_own policy (FOR ALL) covers new columns automatically.


-- ============================================================
-- B) Table: managers
--
-- Per-tenant directory of sales managers.
-- session_ids is a jsonb array of WhatsApp session_id strings
-- that this manager operates (used for routing reports, analytics).
-- ============================================================

CREATE TABLE IF NOT EXISTS managers (
  id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name        text        NOT NULL,
  email       text,
  phone       text,
  session_ids jsonb       NOT NULL DEFAULT '[]'::jsonb,
  is_active   boolean     NOT NULL DEFAULT true,
  sort_order  integer     NOT NULL DEFAULT 0,
  notes       text,
  created_at  timestamptz NOT NULL DEFAULT NOW(),
  updated_at  timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_managers_user_id ON managers(user_id);

-- RLS: each tenant sees only their own managers
ALTER TABLE managers ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS managers_own ON managers;
CREATE POLICY managers_own ON managers
  FOR ALL
  USING     (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

-- set_updated_at() is defined in 0004 — reuse it
DROP TRIGGER IF EXISTS managers_updated_at ON managers;
CREATE TRIGGER managers_updated_at
  BEFORE UPDATE ON managers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================
-- C) Table: message_templates
--
-- Per-tenant WhatsApp message templates with placeholder support.
-- body may contain {{имя}}, {{город}}, etc. — substitution is done
-- client-side; this table is storage only.
-- ============================================================

CREATE TABLE IF NOT EXISTS message_templates (
  id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  title       text        NOT NULL,
  body        text        NOT NULL,
  category    text        NOT NULL DEFAULT 'general',
  sort_order  integer     NOT NULL DEFAULT 0,
  created_at  timestamptz NOT NULL DEFAULT NOW(),
  updated_at  timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_message_templates_user_id ON message_templates(user_id);
-- Composite index: most GET requests filter by both user_id and category
CREATE INDEX IF NOT EXISTS idx_message_templates_category ON message_templates(user_id, category);

-- RLS: each tenant sees only their own templates
ALTER TABLE message_templates ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS message_templates_own ON message_templates;
CREATE POLICY message_templates_own ON message_templates
  FOR ALL
  USING     (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

-- set_updated_at() reused from 0004
DROP TRIGGER IF EXISTS message_templates_updated_at ON message_templates;
CREATE TRIGGER message_templates_updated_at
  BEFORE UPDATE ON message_templates
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================
-- Post-run verification probes
-- ============================================================

-- 1) New columns added to tenant_settings:
--    SELECT column_name, data_type, column_default
--    FROM information_schema.columns
--    WHERE table_schema = 'public'
--      AND table_name = 'tenant_settings'
--      AND column_name IN ('lead_sources','refusal_reasons','task_types','company_profile');
--    Expected: 4 rows, all data_type = 'jsonb'

-- 2) New tables exist:
--    SELECT tablename FROM pg_tables
--    WHERE schemaname = 'public'
--      AND tablename IN ('managers', 'message_templates');
--    Expected: 2 rows

-- 3) RLS enabled on new tables:
--    SELECT tablename, rowsecurity FROM pg_tables
--    WHERE schemaname = 'public'
--      AND tablename IN ('managers', 'message_templates');
--    Expected: both rowsecurity = true

-- 4) Policies exist:
--    SELECT tablename, policyname FROM pg_policies
--    WHERE schemaname = 'public'
--      AND tablename IN ('managers', 'message_templates');
--    Expected: 2 rows (managers_own, message_templates_own)

-- 5) Indexes exist:
--    SELECT indexname FROM pg_indexes
--    WHERE schemaname = 'public'
--      AND indexname IN ('idx_managers_user_id','idx_message_templates_user_id','idx_message_templates_category');
--    Expected: 3 rows

-- 6) CI guard — no table with RLS but no policy:
--    SELECT tablename FROM pg_tables
--    WHERE schemaname = 'public'
--      AND rowsecurity = true
--      AND tablename NOT IN (
--        SELECT tablename FROM pg_policies WHERE schemaname = 'public'
--      );
--    Expected: 0 rows
