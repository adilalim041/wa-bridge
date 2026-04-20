-- ============================================================
-- Wave 7 — Baileys Enrichment + Tag Sync
-- Run in Supabase SQL Editor (Omoikiri WPAdil project)
-- ============================================================
-- Four mini-migrations bundled together:
--   7A. chat_tags  — tags live on the contact (remote_jid), not per session
--   7B. messages.read_by_recipient_at — when client read OUR outgoing msg
--   7C. messages.deleted_at / deleted_by_me — soft-delete audit trail
--   7D. messages.quoted_* — reply context
-- ============================================================

-- ---------- 7A: chat_tags ----------------------------------

CREATE TABLE IF NOT EXISTS chat_tags (
    remote_jid TEXT PRIMARY KEY,
    tags TEXT[] NOT NULL DEFAULT '{}',
    tag_confirmed BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_tags_confirmed ON chat_tags(tag_confirmed) WHERE tag_confirmed = true;

ALTER TABLE chat_tags ENABLE ROW LEVEL SECURITY;

-- Backfill: union existing chats.tags per remote_jid into chat_tags.
-- If different sessions had different tags for the same number, we take the
-- union (any tag from any session wins). tag_confirmed flips true if ANY
-- session had it confirmed.
INSERT INTO chat_tags (remote_jid, tags, tag_confirmed, updated_at)
SELECT
    remote_jid,
    ARRAY(SELECT DISTINCT unnest(array_agg(DISTINCT t))) AS tags,
    bool_or(tag_confirmed) AS tag_confirmed,
    MAX(updated_at) AS updated_at
FROM (
    SELECT c.remote_jid, unnest(c.tags) AS t, c.tag_confirmed, c.updated_at
    FROM chats c
    WHERE c.tags IS NOT NULL AND array_length(c.tags, 1) > 0
) s
GROUP BY remote_jid
ON CONFLICT (remote_jid) DO UPDATE
    SET tags = (
        SELECT ARRAY(SELECT DISTINCT unnest(chat_tags.tags || EXCLUDED.tags))
    ),
    tag_confirmed = chat_tags.tag_confirmed OR EXCLUDED.tag_confirmed,
    updated_at = GREATEST(chat_tags.updated_at, EXCLUDED.updated_at);

-- NOTE: we intentionally keep chats.tags column for now. After verifying
-- Wave 7 in production for ~1 week we can drop it in a follow-up migration:
--   ALTER TABLE chats DROP COLUMN tags;
--   ALTER TABLE chats DROP COLUMN tag_confirmed;

-- ---------- 7B: read_by_recipient_at ------------------------

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS read_by_recipient_at TIMESTAMPTZ;

-- Fast lookup when updating receipts (by session + message_id)
CREATE INDEX IF NOT EXISTS idx_messages_session_msgid ON messages(session_id, message_id);

-- ---------- 7C: deletions (audit, don't hide) ---------------

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS deleted_by_me BOOLEAN;

CREATE INDEX IF NOT EXISTS idx_messages_deleted ON messages(session_id, remote_jid, deleted_at)
    WHERE deleted_at IS NOT NULL;

-- ---------- 7D: quoted / reply context ----------------------

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS quoted_message_id TEXT;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS quoted_snippet TEXT;

ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS quoted_from_me BOOLEAN;
