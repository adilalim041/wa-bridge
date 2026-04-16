-- ============================================================
-- WhatsApp Calls Tracking
-- Run in Supabase SQL Editor
-- ============================================================
-- Stores metadata about WhatsApp calls (voice + video).
-- We can't record audio (E2E encrypted P2P), but we log:
-- - who called, when, type (voice/video), duration, status
--
-- Status lifecycle:
--   offer → accept (answered) → terminate (ended normally)
--   offer → reject (declined by recipient)
--   offer → timeout (nobody answered)
--   offer → terminate (caller cancelled)
-- ============================================================

CREATE TABLE IF NOT EXISTS calls (
    id BIGSERIAL PRIMARY KEY,
    call_id TEXT NOT NULL,                    -- Baileys call ID
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,                 -- Who we called with
    from_me BOOLEAN DEFAULT false,            -- true = manager initiated
    is_video BOOLEAN DEFAULT false,
    is_group BOOLEAN DEFAULT false,
    status TEXT NOT NULL,                     -- offer, accept, reject, timeout, terminate
    offered_at TIMESTAMPTZ,                   -- when call started ringing
    answered_at TIMESTAMPTZ,                  -- when accepted (if answered)
    ended_at TIMESTAMPTZ,                     -- when call ended
    duration_sec INTEGER,                     -- calculated from answered_at to ended_at
    missed BOOLEAN DEFAULT false,             -- true if reject/timeout on incoming
    raw_data JSONB,                           -- full event for debugging
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (call_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_calls_session ON calls(session_id, offered_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_remote_jid ON calls(session_id, remote_jid, offered_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_missed ON calls(session_id, missed, offered_at DESC) WHERE missed = true;
CREATE INDEX IF NOT EXISTS idx_calls_from_me ON calls(session_id, from_me);

-- RLS: service_role only (backend writes via Supabase)
ALTER TABLE calls ENABLE ROW LEVEL SECURITY;

-- Convenient view: unified feed (messages + calls) for chat view
-- (Actually we'll do the union in the app layer for flexibility)

-- Example queries:
-- Missed calls today:
-- SELECT * FROM calls WHERE session_id = 'xxx' AND missed AND offered_at >= CURRENT_DATE;
--
-- Total call time by manager today:
-- SELECT session_id, SUM(duration_sec)/60 AS total_minutes
-- FROM calls WHERE offered_at >= CURRENT_DATE AND duration_sec > 0
-- GROUP BY session_id;
