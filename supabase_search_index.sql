-- Index for message body text search (ilike optimization)
-- Note: ilike with leading wildcard (%query%) cannot use btree index
-- For large datasets, consider pg_trgm extension for trigram-based index
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_messages_body_trgm ON messages USING gin(body gin_trgm_ops);

-- This makes ilike '%query%' searches use the trigram index instead of full table scan
