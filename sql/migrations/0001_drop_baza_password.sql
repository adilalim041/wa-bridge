-- Drop unused plain-text password column from baza_users.
-- Rationale: Basic Auth path in src/baza/middleware/bazaAuth.js was removed
-- (never used — no BAZA frontend existed). All BAZA API access is via X-Api-Key.
-- Future BAZA auth (when merged into Omoikiri) will go through Supabase Auth.
--
-- Safe to run: no code reads this column after bazaAuth.js cleanup.

ALTER TABLE baza_users DROP COLUMN IF EXISTS password;
