-- ============================================================
-- Migration 0023: DROP TABLE ai_queue
--
-- Adil 2026-05-04 audit (L-2): ai_queue — dead code 25+ дней.
-- 3001 fossil rows enqueued но никогда не consumed (нет worker'а).
-- Daily analysis читает dialog_sessions напрямую, не через ai_queue.
--
-- Idempotent. Run in Supabase SQL Editor (project WPAdil).
-- ============================================================

DROP TABLE IF EXISTS ai_queue CASCADE;

-- ============================================================
-- Verification:
--   SELECT count(*) FROM information_schema.tables
--    WHERE table_name = 'ai_queue';  -- expect 0
-- ============================================================
