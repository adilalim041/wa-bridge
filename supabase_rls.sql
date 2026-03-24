-- ============================================================
-- Omoikiri.AI — RLS Policies
-- Run this in Supabase SQL Editor (Dashboard → SQL Editor)
-- ============================================================
--
-- Strategy: Enable RLS on all tables, deny anon access.
-- Backend uses service_role key which bypasses RLS.
-- Frontend (dashboard) goes through backend API, never direct DB.
--
-- This means: even if someone gets the anon key, they can't
-- read or write anything in the database.
-- ============================================================

-- 1. Enable RLS on all tables
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE auth_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_lock ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE manager_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE chats ENABLE ROW LEVEL SECURITY;
ALTER TABLE contacts_crm ENABLE ROW LEVEL SECURITY;
ALTER TABLE dialog_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE chat_ai ENABLE ROW LEVEL SECURITY;
ALTER TABLE ai_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE manager_analytics ENABLE ROW LEVEL SECURITY;

-- 2. No policies = anon key has ZERO access
-- service_role key bypasses RLS automatically
--
-- When ready for SaaS multi-tenant, add policies like:
-- CREATE POLICY "users see own sessions" ON messages
--   FOR SELECT USING (session_id IN (
--     SELECT session_id FROM manager_sessions
--     WHERE user_id = auth.uid()
--   ));
