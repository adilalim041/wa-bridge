-- ============================================================
-- Migration 0010: include calls in get_chats_with_last_message
--
-- Changes:
--   1. Add LATERAL JOIN on `calls` to surface the most recent call per chat.
--   2. Return four new columns: last_call_offered_at, last_call_missed,
--      last_call_from_me, last_call_duration_sec.
--   3. Change ORDER BY to GREATEST(last_message_at, last_call_offered_at)
--      so a chat with a missed call newer than the last message floats to top.
--
-- Idempotent via explicit DROP first: Postgres refuses CREATE OR REPLACE
-- when RETURNS TABLE signature changes (here we added 4 new columns), so
-- we must drop and recreate. DROP FUNCTION is metadata-only — does not
-- touch any row in any table.
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================

-- Drop first because RETURNS TABLE changed (4 new call columns).
-- IF EXISTS makes it safe to re-run on a fresh DB where function never existed.
DROP FUNCTION IF EXISTS get_chats_with_last_message(TEXT, INT);

CREATE OR REPLACE FUNCTION get_chats_with_last_message(
  p_session_id TEXT,
  p_limit      INT DEFAULT 300
)
RETURNS TABLE (
  remote_jid            TEXT,
  chat_type             TEXT,
  display_name          TEXT,
  participant_count     INT,
  phone_number          TEXT,
  is_muted              BOOLEAN,
  muted_until           TIMESTAMPTZ,
  tags                  TEXT[],
  tag_confirmed         BOOLEAN,
  -- last message columns (unchanged)
  last_message_body     TEXT,
  last_message_type     TEXT,
  last_timestamp        TIMESTAMPTZ,
  last_from_me          BOOLEAN,
  last_push_name        TEXT,
  last_sender           TEXT,
  last_media_url        TEXT,
  last_media_type       TEXT,
  last_file_name        TEXT,
  unread_count          BIGINT,
  -- CRM columns (unchanged)
  crm_first_name        TEXT,
  crm_last_name         TEXT,
  crm_role              TEXT,
  crm_avatar_url        TEXT,
  -- NEW: last call columns
  last_call_offered_at  TIMESTAMPTZ,
  last_call_missed      BOOLEAN,
  last_call_from_me     BOOLEAN,
  last_call_duration_sec INT
) LANGUAGE sql STABLE AS $$
  SELECT
    c.remote_jid,
    c.chat_type,
    c.display_name,
    c.participant_count,
    COALESCE(c.phone_number, c.remote_jid)   AS phone_number,
    COALESCE(c.is_muted, false)              AS is_muted,
    c.muted_until,
    COALESCE(c.tags, '{}')                   AS tags,
    COALESCE(c.tag_confirmed, false)         AS tag_confirmed,
    -- last message (LATERAL — 1 row per chat, uses messages index)
    lm.body                                  AS last_message_body,
    COALESCE(lm.message_type, 'text')        AS last_message_type,
    COALESCE(lm.timestamp, c.last_message_at) AS last_timestamp,
    COALESCE(lm.from_me, false)              AS last_from_me,
    lm.push_name                             AS last_push_name,
    lm.sender                                AS last_sender,
    lm.media_url                             AS last_media_url,
    lm.media_type                            AS last_media_type,
    lm.file_name                             AS last_file_name,
    -- unread count (LATERAL)
    COALESCE(ur.cnt, 0)                      AS unread_count,
    -- CRM
    crm.first_name                           AS crm_first_name,
    crm.last_name                            AS crm_last_name,
    crm.role                                 AS crm_role,
    crm.avatar_url                           AS crm_avatar_url,
    -- NEW: last call (LATERAL — 1 row per chat, uses calls index on session_id+remote_jid+offered_at)
    lc.offered_at                            AS last_call_offered_at,
    lc.missed                                AS last_call_missed,
    lc.from_me                               AS last_call_from_me,
    lc.duration_sec                          AS last_call_duration_sec

  FROM chats c

  -- Last message per chat
  LEFT JOIN LATERAL (
    SELECT m.body, m.message_type, m.from_me, m.push_name,
           m.timestamp, m.sender, m.media_url, m.media_type, m.file_name
    FROM messages m
    WHERE m.session_id = p_session_id
      AND m.remote_jid = c.remote_jid
    ORDER BY m.timestamp DESC
    LIMIT 1
  ) lm ON true

  -- Unread count per chat
  LEFT JOIN LATERAL (
    SELECT COUNT(*) AS cnt
    FROM messages m2
    WHERE m2.session_id = p_session_id
      AND m2.remote_jid = c.remote_jid
      AND m2.from_me    = false
      AND m2.read_at   IS NULL
  ) ur ON true

  -- CRM contact
  LEFT JOIN contacts_crm crm
    ON  crm.session_id = p_session_id
    AND crm.remote_jid = c.remote_jid

  -- NEW: last call per chat
  LEFT JOIN LATERAL (
    SELECT ca.offered_at, ca.missed, ca.from_me, ca.duration_sec
    FROM calls ca
    WHERE ca.session_id = p_session_id
      AND ca.remote_jid = c.remote_jid
    ORDER BY ca.offered_at DESC
    LIMIT 1
  ) lc ON true

  WHERE c.session_id = p_session_id
    AND (c.is_hidden IS NULL OR c.is_hidden = false)

  -- Sort by most recent event: message OR call, whichever is newer.
  -- COALESCE(..., '-infinity') handles NULLs safely for both sides.
  ORDER BY
    GREATEST(
      COALESCE(c.last_message_at,        '-infinity'::TIMESTAMPTZ),
      COALESCE(lc.offered_at,            '-infinity'::TIMESTAMPTZ)
    ) DESC NULLS LAST

  LIMIT p_limit;
$$;

-- ============================================================
-- Recommended index (add if not already present) — makes the
-- LATERAL on calls fast even with thousands of rows per session.
-- Safe to run twice (IF NOT EXISTS).
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_calls_session_jid_offered
  ON calls (session_id, remote_jid, offered_at DESC);
