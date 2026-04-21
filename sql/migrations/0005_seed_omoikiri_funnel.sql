-- ============================================================================
-- 0005_seed_omoikiri_funnel.sql
-- Opt-in: seed 9 Omoikiri-compatible funnel stages for the current user.
--
-- HOW TO USE:
--   1. Open Supabase Dashboard → SQL Editor for the WPAdil project.
--   2. Replace the email below with your account email if different.
--   3. Paste and run this script.
--
-- NOTE: Supabase SQL Editor runs as the `postgres` role, so auth.uid()
-- returns NULL. We resolve user_id from auth.users by email instead.
--
-- EFFECT: Deletes all existing funnel_stages for this user, then inserts
-- the 9 Omoikiri stages. Stage names match legacy chat_ai.deal_stage values
-- so existing Kanban data appears immediately without any data migration.
--
-- New tenants (UORA, template): skip this script — the backend auto-seeds
-- a single "Новый лид" stage on first access to Settings → Funnel.
-- ============================================================================

DO $$
DECLARE
  uid uuid := (SELECT id FROM auth.users WHERE email = 'adilalim041@gmail.com');
BEGIN
  IF uid IS NULL THEN
    RAISE EXCEPTION 'User not found. Edit the email literal in this script to match your Supabase auth user.';
  END IF;

  DELETE FROM funnel_stages WHERE user_id = uid;

  INSERT INTO funnel_stages (user_id, name, color, sort_order, is_final) VALUES
    (uid, 'needs_review',       '#f97316', 0, false),
    (uid, 'first_contact',      '#6b7280', 1, false),
    (uid, 'consultation',       '#3b82f6', 2, false),
    (uid, 'model_selection',    '#8b5cf6', 3, false),
    (uid, 'price_negotiation',  '#f59e0b', 4, false),
    (uid, 'payment',            '#10b981', 5, false),
    (uid, 'delivery',           '#06b6d4', 6, false),
    (uid, 'completed',          '#22c55e', 7, true),
    (uid, 'refused',            '#ef4444', 8, true);

  RAISE NOTICE 'Seeded 9 Omoikiri funnel stages for user %', uid;
END $$;
