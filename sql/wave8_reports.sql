-- ============================================================
-- Wave 8 — Manager PDF Reports + Coaching Audit Journal
-- Run in Supabase SQL Editor (Omoikiri WPAdil project)
-- ============================================================
-- Two changes:
--   8A. manager_reports — audit journal of all exported PDFs sent to managers
--   8B. chat_ai.report_sent_at — quick-lookup flag on chat_ai
-- ============================================================

-- ---------- 8A: manager_reports ----------------------------

CREATE TABLE IF NOT EXISTS manager_reports (
  id                  uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
  chat_ai_id          uuid         REFERENCES chat_ai(id) ON DELETE CASCADE,
  dialog_session_id   uuid         REFERENCES dialog_sessions(id),
  client_remote_jid   text         NOT NULL,
  target_session_id   text         NOT NULL,
  sender_session_id   text         NOT NULL,
  coaching_comment    text,
  pdf_cloudinary_url  text,
  filename            text,
  baileys_message_id  text,
  status              text         NOT NULL DEFAULT 'sent'
                        CHECK (status IN ('sent', 'failed', 'pending')),
  error_message       text,
  sent_at             timestamptz  NOT NULL DEFAULT now(),
  created_at          timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_manager_reports_chat_ai
  ON manager_reports(chat_ai_id);

CREATE INDEX IF NOT EXISTS idx_manager_reports_target
  ON manager_reports(target_session_id, sent_at DESC);

CREATE INDEX IF NOT EXISTS idx_manager_reports_sent_at
  ON manager_reports(sent_at DESC);

ALTER TABLE manager_reports ENABLE ROW LEVEL SECURITY;

-- ---------- 8B: chat_ai.report_sent_at ---------------------

ALTER TABLE chat_ai
  ADD COLUMN IF NOT EXISTS report_sent_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_chat_ai_report_sent
  ON chat_ai(report_sent_at)
  WHERE report_sent_at IS NOT NULL;
