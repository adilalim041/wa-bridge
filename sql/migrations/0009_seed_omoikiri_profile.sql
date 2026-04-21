-- ============================================================
-- Migration 0009 — seed Omoikiri company profile + dictionaries
-- Date: 2026-04-22
--
-- Purpose:
--   Opt-in seed for adilalim041@gmail.com tenant.
--   Populates the 4 new tenant_settings columns added in 0008.
--
-- IMPORTANT: Run 0008 FIRST. Then run this script.
--
-- WARNING: This is a data-only script, not a schema migration.
--          Safe to re-run — uses INSERT ... ON CONFLICT DO UPDATE.
-- ============================================================

DO $$
DECLARE
  uid uuid := (SELECT id FROM auth.users WHERE email = 'adilalim041@gmail.com');
BEGIN
  IF uid IS NULL THEN
    RAISE EXCEPTION 'User adilalim041@gmail.com not found in auth.users. '
                    'Run this script in the correct Supabase project (WPAdil / gehiqhnzbumtbvhncblj).';
  END IF;

  -- Upsert the 4 new columns for Adil's tenant.
  -- Existing columns (roles, cities, tags) are preserved via DO NOTHING on the
  -- non-conflicting columns — only the 4 new ones are SET.
  INSERT INTO tenant_settings (
    user_id,
    lead_sources,
    refusal_reasons,
    task_types,
    company_profile,
    updated_at
  )
  VALUES (
    uid,

    -- Lead sources: channels through which clients come to Omoikiri
    '["Instagram","Рекомендация","Дизайнер","Шоурум Астана","Шоурум Алматы","Сайт","WhatsApp","Другое"]'::jsonb,

    -- Refusal reasons: why a potential deal fell through
    '["Дорого","Выбрали конкурента","Передумали","Не сезон","Нет нужной модели","Не отвечает","Другое"]'::jsonb,

    -- Task types: CRM action categories (keys, not display labels)
    '["follow_up","call_back","send_quote","send_catalog","visit_showroom","custom"]'::jsonb,

    -- Company profile: seeded from knowledgeBase data in src/ai/chatEndpoint.js
    -- Omoikiri is a premium Japanese kitchen fixtures brand distributed in Kazakhstan.
    -- Showrooms in Astana and Almaty. Brand colors: red #C8102E, blue #0077C8.
    jsonb_build_object(
      'name',          'Omoikiri',
      'description',   'Официальная дистрибуция японской кухонной сантехники Omoikiri в Казахстане. '
                       'Мойки, смесители и аксессуары премиум-класса. '
                       'Целевая аудитория: дизайнеры интерьера и их клиенты, '
                       'а также конечные покупатели в Астане и Алматы.',
      'website',       'https://omoikiri.kz',
      'phone',         '+7 701 413 51 51',
      'email',         'adilalim041@gmail.com',
      'working_hours', 'Пн–Сб 10:00–19:00 (UTC+5, Астана/Алматы)',
      'showrooms',     jsonb_build_array(
        jsonb_build_object(
          'city',    'Астана',
          'address', 'г. Астана (уточните адрес у менеджера)',
          'hours',   'Пн–Сб 10:00–19:00'
        ),
        jsonb_build_object(
          'city',    'Алматы',
          'address', 'г. Алматы (уточните адрес у менеджера)',
          'hours',   'Пн–Сб 10:00–19:00'
        )
      )
    ),

    NOW()
  )
  ON CONFLICT (user_id) DO UPDATE
    SET lead_sources    = EXCLUDED.lead_sources,
        refusal_reasons = EXCLUDED.refusal_reasons,
        task_types      = EXCLUDED.task_types,
        company_profile = EXCLUDED.company_profile,
        updated_at      = NOW();

  RAISE NOTICE '✓ Seeded tenant_settings for user % (Omoikiri)', uid;
  RAISE NOTICE '  → lead_sources:    8 values (Instagram, Рекомендация, Дизайнер, ...)';
  RAISE NOTICE '  → refusal_reasons: 7 values (Дорого, Конкурент, ...)';
  RAISE NOTICE '  → task_types:      6 values (follow_up, call_back, ...)';
  RAISE NOTICE '  → company_profile: name=Omoikiri, 2 showrooms (Астана + Алматы)';
  RAISE NOTICE 'NEXT: update showroom addresses in the dashboard Settings → Профиль компании.';
END $$;
