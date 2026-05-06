-- ============================================================
-- Migration 0024: ack_status column for outgoing messages
--
-- WhatsApp delivery receipts (ack) have 5 levels:
--   0 = pending   (message queued locally, not yet on WA servers)
--   1 = sent      (server received — single grey tick ✓)
--   2 = delivered (recipient device received — double grey tick ✓✓)
--   3 = read      (recipient opened chat — double blue tick ✓✓ blue)
--   4 = played    (voice/video note played)
--
-- Only outgoing messages (from_me = true) carry ack semantics.
-- Incoming messages always NULL — frontend must not render ticks for them.
--
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- This migration is idempotent — safe to run more than once.
-- ============================================================

ALTER TABLE messages
  ADD COLUMN IF NOT EXISTS ack_status SMALLINT;

COMMENT ON COLUMN messages.ack_status IS
  'WhatsApp delivery ack: 0=pending, 1=sent(✓), 2=delivered(✓✓), 3=read(✓✓blue), 4=played. NULL for incoming messages (from_me=false).';

-- Backfill: existing outgoing messages that have been persisted must have at
-- least reached WA servers (otherwise they would not be in our DB). Set to 1
-- (sent) as a conservative floor — real ack updates will raise it higher as
-- Baileys re-delivers receipts on reconnect.
UPDATE messages
  SET ack_status = 1
WHERE from_me = true
  AND ack_status IS NULL;

-- No extra index needed for ack UPDATE path.
-- The existing UNIQUE idx on (session_id, message_id) + partial WHERE from_me=true
-- is already efficient: the UPDATE hits at most 1 row per event.

-- ============================================================
-- Extend get_chats_with_last_message RPC to include last_ack_status.
--
-- Frontend uses this to display delivery ticks in the chat list preview
-- (only meaningful when the last message is outgoing: from_me = true).
-- When the last message is incoming, last_ack_status will be NULL and
-- the frontend must not render any tick.
--
-- RETURNS TABLE changed (one new column) → must DROP and recreate.
-- DROP FUNCTION is metadata-only — no data touched.
-- ============================================================

DROP FUNCTION IF EXISTS get_chats_with_last_message(TEXT, INT);

CREATE OR REPLACE FUNCTION get_chats_with_last_message(
  p_session_id TEXT,
  p_limit      INT DEFAULT 300
)
RETURNS TABLE (
  remote_jid             TEXT,
  chat_type              TEXT,
  display_name           TEXT,
  participant_count      INT,
  phone_number           TEXT,
  is_muted               BOOLEAN,
  muted_until            TIMESTAMPTZ,
  tags                   TEXT[],
  tag_confirmed          BOOLEAN,
  -- last message columns
  last_message_body      TEXT,
  last_message_type      TEXT,
  last_timestamp         TIMESTAMPTZ,
  last_from_me           BOOLEAN,
  last_push_name         TEXT,
  last_sender            TEXT,
  last_media_url         TEXT,
  last_media_type        TEXT,
  last_file_name         TEXT,
  unread_count           BIGINT,
  -- CRM columns
  crm_first_name         TEXT,
  crm_last_name          TEXT,
  crm_role               TEXT,
  crm_avatar_url         TEXT,
  -- call columns (from migration 0010)
  last_call_offered_at   TIMESTAMPTZ,
  last_call_missed       BOOLEAN,
  last_call_from_me      BOOLEAN,
  last_call_duration_sec INT,
  -- NEW: ack status of last outgoing message (NULL when last msg is incoming)
  last_ack_status        SMALLINT
) LANGUAGE sql STABLE AS $$
  SELECT
    c.remote_jid,
    c.chat_type,
    c.display_name,
    c.participant_count,
    COALESCE(c.phone_number, c.remote_jid)    AS phone_number,
    COALESCE(c.is_muted, false)               AS is_muted,
    c.muted_until,
    COALESCE(c.tags, '{}')                    AS tags,
    COALESCE(c.tag_confirmed, false)          AS tag_confirmed,
    -- last message (LATERAL — 1 row per chat, uses messages index)
    lm.body                                   AS last_message_body,
    COALESCE(lm.message_type, 'text')         AS last_message_type,
    COALESCE(lm.timestamp, c.last_message_at) AS last_timestamp,
    COALESCE(lm.from_me, false)               AS last_from_me,
    lm.push_name                              AS last_push_name,
    lm.sender                                 AS last_sender,
    lm.media_url                              AS last_media_url,
    lm.media_type                             AS last_media_type,
    lm.file_name                              AS last_file_name,
    -- unread count (LATERAL)
    COALESCE(ur.cnt, 0)                       AS unread_count,
    -- CRM
    crm.first_name                            AS crm_first_name,
    crm.last_name                             AS crm_last_name,
    crm.role                                  AS crm_role,
    crm.avatar_url                            AS crm_avatar_url,
    -- last call (LATERAL — from migration 0010)
    lc.offered_at                             AS last_call_offered_at,
    lc.missed                                 AS last_call_missed,
    lc.from_me                                AS last_call_from_me,
    lc.duration_sec                           AS last_call_duration_sec,
    -- NEW: ack_status of the last outgoing message only.
    -- When last message is incoming (from_me=false) this returns NULL —
    -- frontend must check fromMe before rendering ticks.
    CASE WHEN COALESCE(lm.from_me, false) = true THEN lm.ack_status ELSE NULL END
                                              AS last_ack_status

  FROM chats c

  -- Last message per chat (now also selects ack_status)
  LEFT JOIN LATERAL (
    SELECT m.body, m.message_type, m.from_me, m.push_name,
           m.timestamp, m.sender, m.media_url, m.media_type, m.file_name,
           m.ack_status
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

  -- Last call per chat (from migration 0010)
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

  ORDER BY
    GREATEST(
      COALESCE(c.last_message_at,   '-infinity'::TIMESTAMPTZ),
      COALESCE(lc.offered_at,       '-infinity'::TIMESTAMPTZ)
    ) DESC NULLS LAST

  LIMIT p_limit;
$$;

-- Grant execute to service_role (required for Supabase RPC calls from backend)
GRANT EXECUTE ON FUNCTION get_chats_with_last_message(TEXT, INT) TO service_role;
GRANT EXECUTE ON FUNCTION get_chats_with_last_message(TEXT, INT) TO authenticated;
