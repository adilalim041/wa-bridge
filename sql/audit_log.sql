-- Audit log for production monitoring
-- Run in Supabase SQL Editor before deploying

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    action TEXT NOT NULL,
    session_id TEXT,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_session ON audit_log(session_id, created_at DESC);

-- RLS: service_role only (backend writes, no anon access)
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;

-- Auto-cleanup: delete entries older than 90 days (run monthly via cron or Supabase scheduled function)
-- DELETE FROM audit_log WHERE created_at < now() - interval '90 days';
