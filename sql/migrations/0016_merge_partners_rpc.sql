-- ============================================================
-- Migration 0016: merge_partners RPC — атомарная функция объединения контактов
--
-- Closes HIGH-2 from 2026-04-30 sales-crm audit:
-- предыдущая JS-версия mergePartners (src/lib/salesCrm.js) делала
-- 5 sequential UPDATE/DELETE через PostgREST. На 3 из 5 (sales.customer_id,
-- sales.partner_id, followups.contact_id) НЕ проверялся error → mid-flight
-- failure оставлял половину данных переехавшими, половину на source.
--
-- Решение: PL/pgSQL функция, выполняется в одной транзакции.
-- Любой RAISE откатывает всё.
--
-- SECURITY INVOKER (default) — функция исполняется с правами вызывающего.
-- JWT-юзер сохраняет свой RLS-контекст. Если RLS у партнёра заблокирует
-- UPDATE — функция упадёт, ничего не закоммитится.
--
-- Идемпотентна: если source_id уже не существует (повторный вызов после
-- успешного merge) → RAISE 'source contact not found'. Без побочки.
--
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================

CREATE OR REPLACE FUNCTION merge_partners(
  source_id UUID,
  target_id UUID
) RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_source partner_contacts%ROWTYPE;
  v_target partner_contacts%ROWTYPE;
  v_merged_aliases  TEXT[];
  v_merged_roles    TEXT[];
  v_merged_phones   TEXT[];
  v_merged_jids     TEXT[];
  v_new_primary_phone TEXT;
  v_new_agency_id   UUID;
  v_sales_customer  INT;
  v_sales_partner   INT;
  v_followups_moved INT;
BEGIN
  IF source_id IS NULL OR target_id IS NULL THEN
    RAISE EXCEPTION 'source_id and target_id are required';
  END IF;
  IF source_id = target_id THEN
    RAISE EXCEPTION 'source and target are the same';
  END IF;

  -- Lock both rows up-front to prevent racing merges.
  SELECT * INTO v_source FROM partner_contacts WHERE id = source_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'source contact not found';
  END IF;

  SELECT * INTO v_target FROM partner_contacts WHERE id = target_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'target contact not found';
  END IF;

  -- Объединяем массивы (uniq).
  v_merged_aliases := ARRAY(
    SELECT DISTINCT unnest(
      COALESCE(v_target.aliases, '{}') ||
      COALESCE(v_source.aliases, '{}') ||
      ARRAY[v_source.canonical_name]::TEXT[]
    ) WHERE unnest IS NOT NULL AND unnest <> ''
  );
  v_merged_roles := ARRAY(
    SELECT DISTINCT unnest(
      COALESCE(v_target.roles, '{}') ||
      COALESCE(v_source.roles, '{}')
    )
  );
  v_merged_phones := ARRAY(
    SELECT DISTINCT unnest(
      COALESCE(v_target.phones, '{}') ||
      COALESCE(v_source.phones, '{}') ||
      ARRAY[v_target.primary_phone, v_source.primary_phone]::TEXT[]
    ) WHERE unnest IS NOT NULL AND unnest <> ''
  );
  v_merged_jids := ARRAY(
    SELECT DISTINCT unnest(
      COALESCE(v_target.linked_chat_jids, '{}') ||
      COALESCE(v_source.linked_chat_jids, '{}')
    )
  );

  v_new_primary_phone := COALESCE(v_target.primary_phone, v_source.primary_phone);
  v_new_agency_id     := COALESCE(v_target.agency_id, v_source.agency_id);

  -- 1. Обновляем target.
  UPDATE partner_contacts SET
    aliases          = v_merged_aliases,
    roles            = v_merged_roles,
    phones           = v_merged_phones,
    linked_chat_jids = v_merged_jids,
    primary_phone    = v_new_primary_phone,
    agency_id        = v_new_agency_id,
    updated_at       = NOW()
  WHERE id = target_id;

  -- 2. Перепривязываем sales.customer_id.
  WITH upd AS (
    UPDATE sales SET customer_id = target_id
    WHERE customer_id = source_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_sales_customer FROM upd;

  -- 3. Перепривязываем sales.partner_id.
  WITH upd AS (
    UPDATE sales SET partner_id = target_id
    WHERE partner_id = source_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_sales_partner FROM upd;

  -- 4. Перепривязываем followups.contact_id.
  WITH upd AS (
    UPDATE followups SET contact_id = target_id
    WHERE contact_id = source_id
    RETURNING 1
  )
  SELECT COUNT(*) INTO v_followups_moved FROM upd;

  -- 5. Удаляем source.
  DELETE FROM partner_contacts WHERE id = source_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'source delete failed (was modified concurrently)';
  END IF;

  RETURN jsonb_build_object(
    'ok',                  true,
    'merged_into',         target_id,
    'sales_customer_moved', v_sales_customer,
    'sales_partner_moved',  v_sales_partner,
    'followups_moved',      v_followups_moved
  );
END;
$$;

COMMENT ON FUNCTION merge_partners IS
'Атомарно сливает source partner_contact в target. Перепривязывает sales и followups, удаляет source. RAISE на любом шаге откатит всё. См. salesCrm.mergePartners() — JS-обёртка.';
