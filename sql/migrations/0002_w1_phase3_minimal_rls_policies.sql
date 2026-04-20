-- ============================================================
-- W1.1 Phase 3 — minimal RLS policies
-- Audit: 2026-04-21 probe confirmed RLS is already ENABLED on 11 tables
-- but they have NO policies → `authenticated` role returns 0 rows.
-- `tasks` has RLS disabled AND anon has SELECT → public data leak (22 rows).
--
-- This migration adds the simplest working policy for the current
-- single-tenant Omoikiri deployment: any authenticated Supabase user
-- has full read/write access to app tables. Matches existing backend
-- behaviour (one API_KEY = one shared user).
--
-- Multi-tenant SaaS policies (per-tenant session scoping) come later,
-- once `manager_sessions.user_id` is backfilled.
--
-- After this runs: Phase 2 code-migration (req.userClient ?? supabase)
-- becomes safe to deploy endpoint-by-endpoint without dashboard blackout.
-- ============================================================

-- 1) tasks — plug the anon leak + align RLS posture with siblings
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;

-- 2) Single-tenant authenticated-full-access policies
-- Idempotent: DROP first in case partial run re-executed.

DO $$
DECLARE
  t text;
  target_tables text[] := ARRAY[
    'messages', 'chats', 'contacts_crm', 'chat_ai',
    'dialog_sessions', 'manager_analytics', 'session_config',
    'tasks', 'chat_tags', 'manager_reports', 'calls', 'audit_log'
  ];
BEGIN
  FOREACH t IN ARRAY target_tables LOOP
    -- Skip if table doesn't exist (keeps migration portable across clones)
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = t) THEN
      RAISE NOTICE 'skip missing table %', t;
      CONTINUE;
    END IF;

    EXECUTE format('DROP POLICY IF EXISTS authenticated_all ON %I', t);
    EXECUTE format(
      'CREATE POLICY authenticated_all ON %I
         FOR ALL
         TO authenticated
         USING (true)
         WITH CHECK (true)',
      t
    );
    RAISE NOTICE 'policy created on %', t;
  END LOOP;
END$$;

-- 3) Revoke anon SELECT on tables we expect private.
-- (anon role should never read app data — frontend always goes through
-- backend OR uses authenticated JWT. If a table exists and anon has SELECT,
-- an external client with the public anon key could slurp it.)
REVOKE SELECT ON tasks FROM anon;
-- Other tables already have RLS so anon sees 0 rows even with grant; the
-- grant itself is harmless without a policy, but tasks was the visible leak.

-- 4) Sanity: confirm policies
-- After running, this query should return one row per table.
--   SELECT schemaname, tablename, policyname, roles, cmd
--   FROM pg_policies
--   WHERE schemaname = 'public' AND policyname = 'authenticated_all'
--   ORDER BY tablename;
