-- ============================================================
-- Migration 0004 — tenant_settings + funnel_stages
-- Date: 2026-04-21
--
-- Purpose:
--   Phase A of "configurable UI" — move hardcoded roles/cities/tags/funnel
--   stages from source code into per-tenant Supabase tables.
--
-- WARNING: DO NOT run this migration automatically.
--          Execute manually in Supabase Dashboard → SQL Editor.
--          Verify with probe queries at the bottom before deploying.
--
-- Idempotency: all DDL uses IF NOT EXISTS / IF EXISTS guards.
--              Safe to re-run after partial failure.
-- ============================================================


-- ============================================================
-- Table: tenant_settings
--
-- Stores per-tenant arrays of strings: contact roles, cities, chat tags.
-- user_id is a FK to auth.users — one row per authenticated tenant.
-- Defaults ship as Omoikiri-flavored values for backward compat;
-- templates for new clients start empty ([] or minimal defaults).
-- ============================================================

CREATE TABLE IF NOT EXISTS tenant_settings (
  user_id    uuid        PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  roles      jsonb       NOT NULL DEFAULT '["клиент","партнёр","менеджер","другое"]'::jsonb,
  cities     jsonb       NOT NULL DEFAULT '[]'::jsonb,
  tags       jsonb       NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- RLS: each tenant sees only their own row
ALTER TABLE tenant_settings ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_settings_own ON tenant_settings;
CREATE POLICY tenant_settings_own ON tenant_settings
  FOR ALL
  USING     (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

-- Auto-update updated_at on any UPDATE
-- (no trigger runtime deps — uses a simple approach compatible with Supabase)
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tenant_settings_updated_at ON tenant_settings;
CREATE TRIGGER tenant_settings_updated_at
  BEFORE UPDATE ON tenant_settings
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================
-- Table: funnel_stages
--
-- Per-tenant ordered list of CRM funnel stages (AmoCRM-style).
-- sort_order drives column ordering in the funnel board.
-- is_final marks "terminal" stages: won deals, lost deals, spam.
-- color is a CSS hex string for UI rendering.
-- ============================================================

CREATE TABLE IF NOT EXISTS funnel_stages (
  id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    uuid        NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  name       text        NOT NULL,
  color      text        NOT NULL DEFAULT '#3b82f6',
  sort_order int         NOT NULL DEFAULT 0,
  is_final   boolean     DEFAULT false,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Index for fast user-scoped ORDER BY sort_order reads
CREATE INDEX IF NOT EXISTS funnel_stages_user_order
  ON funnel_stages(user_id, sort_order);

-- Unique stage name per tenant to prevent duplicates
CREATE UNIQUE INDEX IF NOT EXISTS funnel_stages_user_name
  ON funnel_stages(user_id, name);

-- RLS: each tenant sees only their own stages
ALTER TABLE funnel_stages ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS funnel_stages_own ON funnel_stages;
CREATE POLICY funnel_stages_own ON funnel_stages
  FOR ALL
  USING     (auth.uid() = user_id)
  WITH CHECK (auth.uid() = user_id);

-- Auto-update updated_at
-- set_updated_at() function already created above
DROP TRIGGER IF EXISTS funnel_stages_updated_at ON funnel_stages;
CREATE TRIGGER funnel_stages_updated_at
  BEFORE UPDATE ON funnel_stages
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================
-- Post-run verification probes
-- Run these in Supabase SQL Editor after applying this migration.
-- ============================================================

-- 1) Both tables exist:
--    SELECT tablename FROM pg_tables
--    WHERE schemaname = 'public'
--      AND tablename IN ('tenant_settings', 'funnel_stages')
--    ORDER BY tablename;
--    Expected: 2 rows

-- 2) RLS is enabled on both:
--    SELECT tablename, rowsecurity FROM pg_tables
--    WHERE schemaname = 'public'
--      AND tablename IN ('tenant_settings', 'funnel_stages');
--    Expected: both have rowsecurity = true

-- 3) Policies exist:
--    SELECT tablename, policyname, cmd, roles
--    FROM pg_policies
--    WHERE schemaname = 'public'
--      AND tablename IN ('tenant_settings', 'funnel_stages')
--    ORDER BY tablename;
--    Expected: 2 rows, one per table, policyname = 'tenant_settings_own' / 'funnel_stages_own'

-- 4) Index exists:
--    SELECT indexname FROM pg_indexes
--    WHERE schemaname = 'public'
--      AND tablename = 'funnel_stages'
--      AND indexname IN ('funnel_stages_user_order', 'funnel_stages_user_name');
--    Expected: 2 rows

-- 5) CI regression guard — no silent-deny traps:
--    SELECT tablename FROM pg_tables
--    WHERE schemaname = 'public'
--      AND rowsecurity = true
--      AND tablename NOT IN (
--        SELECT tablename FROM pg_policies WHERE schemaname = 'public'
--      );
--    Expected: 0 rows
