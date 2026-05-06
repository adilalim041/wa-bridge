-- ============================================================
-- Migration 0025: ack_status DEFAULT 0 + retro-fix NULL rows
--
-- Bug discovered after 0024 deploy: new outgoing messages were inserted
-- WITHOUT explicit ack_status, so the column stayed NULL. Bridge UPDATE
-- handler uses `.lt('ack_status', newStatus)` for idempotency — but in
-- PostgreSQL, `NULL < any_number = NULL` (not TRUE), so the row never
-- matches and the ack progression freezes. UI fallback `?? 1` made every
-- new outgoing message look stuck on a single ✓ forever.
--
-- Fix:
--   1. Set DEFAULT 0 — every future row starts at "pending", filter works.
--   2. Backfill any current NULL outgoing rows to 0 so handler can lift them.
--
-- Idempotent.
-- ============================================================

ALTER TABLE messages
  ALTER COLUMN ack_status SET DEFAULT 0;

UPDATE messages
  SET ack_status = 0
WHERE from_me = true
  AND ack_status IS NULL;
