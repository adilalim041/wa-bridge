-- ============================================================
-- Migration 0014: chat_ai_feedback — manager corrections to AI analysis
--
-- Powers the "ИИ ошибся" button in the problem carousel. When Adil
-- flags an analysis as wrong, we capture the chat_ai row + a free-text
-- explanation. The daily-wa-analysis skill reads these on every run
-- (recent N entries) and uses them as guidelines so the model corrects
-- its own systematic errors over time.
--
-- Idempotent. Run in Supabase SQL Editor (project WPAdil).
-- ============================================================

CREATE TABLE IF NOT EXISTS chat_ai_feedback (
  id          BIGSERIAL PRIMARY KEY,
  chat_ai_id  UUID NOT NULL REFERENCES chat_ai(id) ON DELETE CASCADE,
  -- kind narrows what the analysis got wrong, so the prompt hint can be
  -- categorical: "don't flag X as Y unless Z". Free comment_ru gives the
  -- nuance the categories miss.
  kind        TEXT NOT NULL
              CHECK (kind IN (
                'wrong_category',     -- shouldn't be in this carousel page at all
                'wrong_summary',      -- summary_ru misrepresents the conversation
                'wrong_severity',     -- flagged too hot/cold; or critical when it isn't
                'wrong_assignment',   -- wrong customer_type / deal_stage
                'other'
              )),
  comment_ru  TEXT,
  created_by  TEXT NOT NULL,         -- supabase user_id (UUID string) or '__service__'
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Daily analysis fetches recent feedback ordered by recency to keep prompt
-- size bounded (e.g. last 50). Index supports that.
CREATE INDEX IF NOT EXISTS idx_chat_ai_feedback_recent
  ON chat_ai_feedback(created_at DESC);

-- "Has this row already been flagged?" lookup, used by the dashboard to
-- show a muted/disabled state on the button after submission.
CREATE INDEX IF NOT EXISTS idx_chat_ai_feedback_chat
  ON chat_ai_feedback(chat_ai_id);

ALTER TABLE chat_ai_feedback DISABLE ROW LEVEL SECURITY;
-- Single-tenant for now — same posture as chat_ai. When SaaS multi-tenant
-- lands, gate by chat_ai.session_id → manager_sessions.user_id chain.

GRANT SELECT, INSERT ON chat_ai_feedback TO authenticated, service_role;
GRANT USAGE ON SEQUENCE chat_ai_feedback_id_seq TO authenticated, service_role;

-- ============================================================
-- Verification (run manually after applying):
-- ============================================================
-- \d chat_ai_feedback
-- SELECT count(*) FROM chat_ai_feedback;       -- expect 0
-- INSERT INTO chat_ai_feedback (chat_ai_id, kind, comment_ru, created_by)
--   VALUES ((SELECT id FROM chat_ai LIMIT 1), 'wrong_category', 'тест', '__service__');
-- DELETE FROM chat_ai_feedback WHERE comment_ru = 'тест';
