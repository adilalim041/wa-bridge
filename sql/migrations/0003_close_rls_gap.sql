-- ============================================================
-- Migration 0003 — close RLS coverage gap
-- Audit: 2026-04-21, finding [HIGH] "RLS coverage gap — 5 таблиц в silent-deny"
-- Ref: ObsidianVault/projects/omoikiri/audits/2026-04-21-audit.md, line 81-92
--
-- Problem:
--   Migration 0002 created authenticated_all policies for 12 tables.
--   Five tables had RLS ENABLED (from the original supabase_rls.sql) but
--   had ZERO policies — a silent-deny trap: supabase-js returns data:[]
--   without an error when the userClient is used, making Phase 2 debugging
--   nearly impossible.
--
--   Tables affected:
--     manager_sessions  — app table, live manager→session mapping
--     ai_queue          — app table, kept for possible queue-based processing
--     contacts          — app table, used by queries.js getContactName/saveContact
--     auth_state        — service-only, Baileys credentials, never read via JWT
--     session_lock      — service-only, leader election, never read via JWT
--
-- Fix:
--   Part A: Add authenticated_all policy to 3 app tables
--           (same pattern as 0002: permissive, single-tenant, no user_id scoping yet)
--   Part B: DISABLE RLS on 2 service-only tables
--           (they are ONLY accessed by the Bridge worker via service_role key,
--            never via user JWT — RLS on them adds zero security and causes
--            hard-to-debug failures if ever called through userClient by mistake)
--
-- Safe to re-run: DROP POLICY IF EXISTS + IF NOT EXISTS table check + DISABLE is idempotent
-- ============================================================


-- ============================================================
-- Part A — authenticated_all policies for app tables
-- ============================================================

DO $$
DECLARE
  t text;
  app_tables text[] := ARRAY[
    'manager_sessions',
    'ai_queue',
    'contacts'
  ];
BEGIN
  FOREACH t IN ARRAY app_tables LOOP
    -- Skip gracefully if table does not exist in this environment
    -- (keeps migration portable: template clones may not have all tables yet)
    IF NOT EXISTS (
      SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = t
    ) THEN
      RAISE NOTICE 'skip — table % does not exist', t;
      CONTINUE;
    END IF;

    -- Idempotent: drop first in case the migration is re-run after a partial failure
    EXECUTE format('DROP POLICY IF EXISTS authenticated_all ON %I', t);

    EXECUTE format(
      'CREATE POLICY authenticated_all ON %I
         FOR ALL
         TO authenticated
         USING (true)
         WITH CHECK (true)',
      t
    );

    RAISE NOTICE 'authenticated_all policy created on %', t;
  END LOOP;
END$$;


-- ============================================================
-- Part B — DISABLE RLS on service-only tables
--
-- auth_state  : Baileys WhatsApp credentials. Super-sensitive.
--               Read/written exclusively by the Bridge worker via
--               service_role key. Never exposed to user JWTs.
--               Having RLS enabled with no policy = silent-deny if
--               accidentally called through userClient.
--
-- session_lock: Leader-election / heartbeat table. Internal to Bridge.
--               Same rationale — service_role only.
--
-- DISABLE is idempotent: running it when RLS is already off is a no-op.
-- ============================================================

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'auth_state'
  ) THEN
    ALTER TABLE auth_state DISABLE ROW LEVEL SECURITY;
    RAISE NOTICE 'RLS disabled on auth_state (service_only — service_role pathway only)';
  ELSE
    RAISE NOTICE 'skip — table auth_state does not exist';
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'session_lock'
  ) THEN
    ALTER TABLE session_lock DISABLE ROW LEVEL SECURITY;
    RAISE NOTICE 'RLS disabled on session_lock (service_only — service_role pathway only)';
  ELSE
    RAISE NOTICE 'skip — table session_lock does not exist';
  END IF;
END$$;


-- ============================================================
-- Post-run verification probes
-- Run these manually in Supabase SQL Editor after executing this migration.
-- ============================================================

-- 1) Confirm the three app-table policies were created:
--    Expected: 3 rows (manager_sessions, ai_queue, contacts)
--
--    SELECT tablename, policyname, roles, cmd
--    FROM pg_policies
--    WHERE schemaname = 'public'
--      AND policyname = 'authenticated_all'
--      AND tablename IN ('manager_sessions', 'ai_queue', 'contacts')
--    ORDER BY tablename;

-- 2) Confirm the two service tables have RLS off:
--    Expected: 2 rows with rowsecurity = false
--
--    SELECT tablename, rowsecurity
--    FROM pg_tables
--    WHERE schemaname = 'public'
--      AND tablename IN ('auth_state', 'session_lock');

-- 3) CI/smoke regression guard — must return 0 rows when coverage is complete.
--    A non-zero result means a table has RLS enabled but no policy (silent-deny trap).
--    Uncomment and run when CI is wired:
--
--    SELECT tablename
--    FROM pg_tables
--    WHERE schemaname = 'public'
--      AND rowsecurity = true
--      AND tablename NOT IN (
--        SELECT tablename FROM pg_policies WHERE schemaname = 'public'
--      );
