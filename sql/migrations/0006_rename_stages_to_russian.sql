-- ============================================================================
-- 0006_rename_stages_to_russian.sql
-- Renames legacy English stage keys to Russian labels in funnel_stages,
-- and syncs existing chat_ai.deal_stage records to the new names.
--
-- Run in Supabase Dashboard → SQL Editor. Replace email below if needed.
-- ============================================================================

DO $$
DECLARE
  uid uuid := (SELECT id FROM auth.users WHERE email = 'adilalim041@gmail.com');
  mapping record;
BEGIN
  IF uid IS NULL THEN
    RAISE EXCEPTION 'User not found.';
  END IF;

  -- Atomic rename: stage name + all chat_ai.deal_stage refs
  FOR mapping IN
    SELECT * FROM (VALUES
      ('needs_review',      'Требует проверки'),
      ('first_contact',     'Первый контакт'),
      ('consultation',      'Консультация'),
      ('model_selection',   'Выбор модели'),
      ('price_negotiation', 'Цена'),
      ('payment',           'Оплата'),
      ('delivery',          'Доставка'),
      ('completed',         'Завершено'),
      ('refused',           'Отказ')
    ) AS t(old_name, new_name)
  LOOP
    -- Update funnel_stages.name for this user
    UPDATE funnel_stages
       SET name = mapping.new_name, updated_at = NOW()
     WHERE user_id = uid AND name = mapping.old_name;

    -- Update chat_ai.deal_stage for this user's records (all sessions)
    UPDATE chat_ai
       SET deal_stage = mapping.new_name
     WHERE deal_stage = mapping.old_name;
  END LOOP;

  RAISE NOTICE 'Renamed 9 stages to Russian for user %', uid;
END $$;
