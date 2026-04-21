-- ============================================================================
-- 0007_seed_cities_and_tags.sql
-- Seeds tenant_settings.cities + tenant_settings.tags for existing tenants.
-- - Cities: uses your old env default "Астана, Алматы" — edit if different.
-- - Tags: pulls DISTINCT tags from existing chat_tags table so your existing
--   chat labels reappear in the new tag management UI.
--
-- Run in Supabase Dashboard → SQL Editor. Replace email below if needed.
-- ============================================================================

DO $$
DECLARE
  uid uuid := (SELECT id FROM auth.users WHERE email = 'adilalim041@gmail.com');
  existing_tags jsonb;
BEGIN
  IF uid IS NULL THEN
    RAISE EXCEPTION 'User not found.';
  END IF;

  -- Collect unique tags from chat_tags (deduplicated, sorted alphabetically)
  SELECT COALESCE(jsonb_agg(DISTINCT tag ORDER BY tag), '[]'::jsonb)
    INTO existing_tags
    FROM (
      SELECT DISTINCT unnest(tags) AS tag
        FROM chat_tags
       WHERE tags IS NOT NULL AND array_length(tags, 1) > 0
    ) sub;

  -- Upsert tenant_settings: merge cities + tags, preserve existing roles
  INSERT INTO tenant_settings (user_id, cities, tags, updated_at)
  VALUES (
    uid,
    '["Астана","Алматы"]'::jsonb,
    existing_tags,
    NOW()
  )
  ON CONFLICT (user_id) DO UPDATE
    SET cities     = EXCLUDED.cities,
        tags       = EXCLUDED.tags,
        updated_at = NOW();

  RAISE NOTICE 'Seeded cities + % tags for user %', jsonb_array_length(existing_tags), uid;
END $$;
