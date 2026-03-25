-- Run this in Supabase SQL Editor
-- Optimizes response time lookups for manager analytics

CREATE INDEX IF NOT EXISTS idx_manager_analytics_pending
ON manager_analytics(session_id, remote_jid, manager_response_at);

-- Optimizes message queries by session + jid + timestamp (used in chat loading)
CREATE INDEX IF NOT EXISTS idx_messages_session_jid_ts
ON messages(session_id, remote_jid, timestamp DESC);
