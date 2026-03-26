-- Run this in Supabase SQL Editor
-- Last updated: 2026-03-26

-- Messages: main lookup (chats, unified messages, analytics)
CREATE INDEX IF NOT EXISTS idx_messages_session_jid_ts
ON messages(session_id, remote_jid, timestamp DESC);

-- Messages: analytics daily trend
CREATE INDEX IF NOT EXISTS idx_messages_session_ts
ON messages(session_id, timestamp);

-- Messages: classify batch load by remote_jid
CREATE INDEX IF NOT EXISTS idx_messages_jid_ts
ON messages(remote_jid, timestamp DESC);

-- Chat AI: funnel endpoint (latest analysis per contact)
CREATE INDEX IF NOT EXISTS idx_chat_ai_session_jid_date
ON chat_ai(session_id, remote_jid, analysis_date DESC);

-- Dialog sessions: AI analysis lookup
CREATE INDEX IF NOT EXISTS idx_dialog_sessions_session_jid
ON dialog_sessions(session_id, remote_jid, last_message_at DESC);

-- Manager analytics: response time calculation
CREATE INDEX IF NOT EXISTS idx_manager_analytics_pending
ON manager_analytics(session_id, remote_jid, manager_response_at);

-- Chats: session listing
CREATE INDEX IF NOT EXISTS idx_chats_session_updated
ON chats(session_id, last_message_at DESC);
