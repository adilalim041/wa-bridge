-- ============================================================
-- Migration 0017: Problem Dismissal — additional index for open problems
--
-- Context: Migration 0013 already added the three chat_ai columns
-- (problem_dismissed_action, problem_dismissed_at, problem_dismissed_by)
-- and the v_football_cases view. This migration adds one more partial index
-- specifically for the GET /ai/issues queries that scan open (undismissed)
-- problems ordered by analysis date.
--
-- Migration 0013 has idx_chat_ai_problem_dismissed (WHERE action IS NOT NULL)
-- for the 'lost' category lookups. This file adds the complementary index
-- for active categories (slow / no_followup / critical / football) which all
-- filter WHERE problem_dismissed_at IS NULL.
--
-- Idempotent: CREATE INDEX IF NOT EXISTS.
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================

-- Partial index for open problem scans (categories: slow, no_followup, critical, football).
-- Covers the WHERE dismissed IS NULL + ORDER BY analysis_date DESC pattern used
-- by GET /ai/issues for all active categories.
CREATE INDEX IF NOT EXISTS idx_chat_ai_problem_open
  ON chat_ai (analysis_date DESC, id DESC)
  WHERE problem_dismissed_at IS NULL;

-- ============================================================
-- Verification:
-- ============================================================
-- \d chat_ai                             -- confirm problem_dismissed_* columns exist (from 0013)
-- \d v_football_cases                    -- confirm view exists (from 0013)
-- SELECT indexname FROM pg_indexes WHERE tablename = 'chat_ai';
--   → should include: idx_chat_ai_problem_dismissed (0013)
--                     idx_chat_ai_problem_open      (this file)
-- ============================================================
