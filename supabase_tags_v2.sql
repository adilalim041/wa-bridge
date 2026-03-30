-- ============================================================
-- Wave 8A: Simplified tag system (4 tags + tag_confirmed)
-- Run in Supabase SQL Editor
-- ============================================================

-- Add tag_confirmed column to chats table
ALTER TABLE chats ADD COLUMN IF NOT EXISTS tag_confirmed BOOLEAN DEFAULT FALSE;

-- Create index for filtering by confirmation status
CREATE INDEX IF NOT EXISTS idx_chats_tag_confirmed ON chats(tag_confirmed);

-- Update the RPC function to include tag_confirmed
CREATE OR REPLACE FUNCTION get_chats_with_last_message(
  p_session_id TEXT,
  p_limit INT DEFAULT 300
)
RETURNS TABLE (
  remote_jid TEXT,
  chat_type TEXT,
  display_name TEXT,
  participant_count INT,
  phone_number TEXT,
  is_muted BOOLEAN,
  muted_until TIMESTAMPTZ,
  tags TEXT[],
  tag_confirmed BOOLEAN,
  last_message_body TEXT,
  last_message_type TEXT,
  last_timestamp TIMESTAMPTZ,
  last_from_me BOOLEAN,
  last_push_name TEXT,
  last_sender TEXT,
  last_media_url TEXT,
  last_media_type TEXT,
  last_file_name TEXT,
  unread_count BIGINT,
  crm_first_name TEXT,
  crm_last_name TEXT,
  crm_role TEXT,
  crm_avatar_url TEXT
) LANGUAGE sql STABLE AS $$
  SELECT
    c.remote_jid,
    c.chat_type,
    c.display_name,
    c.participant_count,
    COALESCE(c.phone_number, c.remote_jid) AS phone_number,
    COALESCE(c.is_muted, false) AS is_muted,
    c.muted_until,
    COALESCE(c.tags, '{}') AS tags,
    COALESCE(c.tag_confirmed, false) AS tag_confirmed,
    lm.body AS last_message_body,
    COALESCE(lm.message_type, 'text') AS last_message_type,
    COALESCE(lm.timestamp, c.last_message_at) AS last_timestamp,
    COALESCE(lm.from_me, false) AS last_from_me,
    lm.push_name AS last_push_name,
    lm.sender AS last_sender,
    lm.media_url AS last_media_url,
    lm.media_type AS last_media_type,
    lm.file_name AS last_file_name,
    COALESCE(ur.cnt, 0) AS unread_count,
    crm.first_name AS crm_first_name,
    crm.last_name AS crm_last_name,
    crm.role AS crm_role,
    crm.avatar_url AS crm_avatar_url
  FROM chats c
  -- Last message per chat (LATERAL = 1 row per chat, uses index)
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
      AND m2.from_me = false
      AND m2.read_at IS NULL
  ) ur ON true
  -- CRM contact
  LEFT JOIN contacts_crm crm
    ON crm.session_id = p_session_id
    AND crm.remote_jid = c.remote_jid
  WHERE c.session_id = p_session_id
    AND (c.is_hidden IS NULL OR c.is_hidden = false)
  ORDER BY c.last_message_at DESC NULLS LAST
  LIMIT p_limit;
$$;
