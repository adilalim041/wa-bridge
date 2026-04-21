-- ============================================================================
-- 0005_seed_omoikiri_funnel.sql
-- Opt-in: seed 9 Omoikiri-compatible funnel stages for the current user.
--
-- HOW TO USE:
--   1. Open Supabase Dashboard → SQL Editor for the WPAdil project.
--   2. Log in to the dashboard first (so auth.uid() resolves to Adil's user).
--   3. Paste and run this script.
--
-- For psql / service-role context: replace auth.uid() with a literal UUID:
--   DECLARE uid uuid := '<your-user-uuid>';
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
  uid uuid := auth.uid();
BEGIN
  IF uid IS NULL THEN
    RAISE EXCEPTION 'auth.uid() returned NULL. Run this script while logged in to the Supabase dashboard, or replace auth.uid() with your literal user UUID.';
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
