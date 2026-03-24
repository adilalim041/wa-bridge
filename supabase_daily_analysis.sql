-- ============================================================
-- Daily analysis migration
-- Run in Supabase SQL Editor
-- ============================================================

-- 1. Add analysis_date column
ALTER TABLE chat_ai ADD COLUMN IF NOT EXISTS analysis_date DATE DEFAULT CURRENT_DATE;

-- 2. Drop old unique constraint (dialog_session_id only)
ALTER TABLE chat_ai DROP CONSTRAINT IF EXISTS chat_ai_dialog_session_id_key;

-- 3. Add new composite unique (same dialog can have analyses on different days)
ALTER TABLE chat_ai ADD CONSTRAINT chat_ai_dialog_date_unique
  UNIQUE (dialog_session_id, analysis_date);

-- 4. Index for fast date lookups
CREATE INDEX IF NOT EXISTS idx_chat_ai_date ON chat_ai(analysis_date DESC);
CREATE INDEX IF NOT EXISTS idx_chat_ai_session_date ON chat_ai(session_id, analysis_date DESC);
