-- ============================================================
-- Migration 0013: Problem Dismissal Columns + Football Cases View
--
-- Purpose: Powers the "проблемные переписки carousel" feature on the
-- Analytics dashboard. Adds three columns to chat_ai for tracking when
-- a manager (or auto-resolver) decides a flagged conversation is no
-- longer active as a problem, and creates a view that surfaces
-- cross-session "footballing" — one customer being passed between
-- multiple manager sessions.
--
-- Idempotent via ADD COLUMN IF NOT EXISTS / CREATE OR REPLACE VIEW.
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================

-- 1. chat_ai: dismissal columns ------------------------------------------------
-- problem_dismissed_action:
--   NULL    — chat is still flagged (in carousel)
--   'won'   — manager closed it because deal moved forward / was completed
--   'lost'  — manager closed it knowing the lead was lost (shows on "💸 Просранные лиды")
-- Server determines won/lost from current deal_stage at dismiss time.
ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS problem_dismissed_action TEXT
    CHECK (problem_dismissed_action IN ('won', 'lost')),
  ADD COLUMN IF NOT EXISTS problem_dismissed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS problem_dismissed_by TEXT;
  -- problem_dismissed_by holds: Supabase user_id (uuid string) when manual,
  -- or 'auto:daily-wa-analysis' when SKILL.md pre-step auto-resolves.

-- Partial index — only dismissed rows occupy index space.
-- Used by GET /chats-by-filter?type=lost (lists problem_dismissed_action='lost').
CREATE INDEX IF NOT EXISTS idx_chat_ai_problem_dismissed
  ON chat_ai(problem_dismissed_action, problem_dismissed_at DESC)
  WHERE problem_dismissed_action IS NOT NULL;


-- 2. v_football_cases — cross-session "ping-pong" customers --------------------
-- A "football" case = same remote_jid received outbound messages (from_me=true)
-- from ≥2 different session_id within the last 7 days. The view aggregates
-- per remote_jid so the API can join and surface the case once, not per session.
--
-- Group jids (start with 120363... — internal WhatsApp groups) and
-- abnormally long jids are excluded server-side via isGarbageJid in the API
-- handler — view stays lean.
--
-- Window is fixed at 7 days. If we later want a config knob, replace with a
-- function that takes interval as arg.
CREATE OR REPLACE VIEW v_football_cases AS
SELECT
  remote_jid,
  array_agg(DISTINCT session_id ORDER BY session_id) AS sessions,
  count(DISTINCT session_id) AS session_count,
  max(timestamp) AS last_outbound_at
FROM messages
WHERE from_me = true
  AND timestamp > NOW() - INTERVAL '7 days'
  AND remote_jid NOT LIKE '120%'
  AND length(remote_jid) <= 15
GROUP BY remote_jid
HAVING count(DISTINCT session_id) >= 2;

-- Authenticated users (RLS path) and service_role (worker path) need SELECT.
-- Anon stays denied.
GRANT SELECT ON v_football_cases TO authenticated, service_role;


-- ============================================================
-- Verification queries (run manually after applying):
-- ============================================================
-- \d chat_ai     -- confirm 3 new columns
-- \d v_football_cases
-- SELECT count(*) FROM v_football_cases;            -- size of footballing set
-- SELECT * FROM v_football_cases LIMIT 5;
-- SELECT count(*) FROM chat_ai WHERE problem_dismissed_action IS NOT NULL;  -- expect 0 initially
