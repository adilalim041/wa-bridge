-- ============================================================
-- Migration 0015: Partner ↔ WhatsApp chat link views
--
-- Phase 1 of sales-CRM integration. Создаёт удобные views для:
--   1. Связи partner_contacts с WhatsApp-сессиями (jid + session_id)
--   2. "Полной карточки партнёра" — продажи + чаты + последний AI-анализ
--   3. Списка followups готовых к действию (due ≤ 7 дней + контактные данные)
--
-- Идемпотентно через CREATE OR REPLACE VIEW.
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================

-- ─── 1. v_partner_chat_link ─────────────────────────────────
-- Один контакт может писать в несколько WhatsApp-сессий менеджеров.
-- Этот view разворачивает linked_chat_jids в плоские строки (jid, session_id, last_msg_at).
CREATE OR REPLACE VIEW v_partner_chat_link AS
SELECT
  c.id              AS contact_id,
  c.canonical_name  AS contact_name,
  c.primary_phone,
  c.roles,
  c.agency_id,
  m.session_id,
  m.remote_jid,
  COUNT(*)::int     AS message_count,
  MAX(m.timestamp)  AS last_message_at,
  MIN(m.timestamp)  AS first_message_at
FROM partner_contacts c
JOIN messages m ON m.remote_jid = ANY (c.linked_chat_jids)
WHERE c.linked_chat_jids <> '{}'
GROUP BY c.id, c.canonical_name, c.primary_phone, c.roles, c.agency_id,
         m.session_id, m.remote_jid;


-- ─── 2. v_partner_full ──────────────────────────────────────
-- "Карточка партнёра целиком" — read-optimized для UI.
-- Соединяет partner_contacts + agencies + sales-aggregates + chat-aggregates.
CREATE OR REPLACE VIEW v_partner_full AS
SELECT
  c.id,
  c.canonical_name,
  c.primary_phone,
  c.aliases,
  c.phones,
  c.roles,
  c.city,
  c.notes,
  c.tags,
  c.agency_id,
  a.canonical_name AS agency_name,
  -- sales aggregates
  COALESCE(s.orders_count, 0)        AS orders_count,
  COALESCE(s.total_revenue, 0)       AS total_revenue,
  s.first_purchase_date,
  s.last_purchase_date,
  -- chat aggregates
  c.linked_chat_jids,
  COALESCE(ch.chat_sessions, ARRAY[]::text[]) AS chat_sessions,
  COALESCE(ch.total_messages, 0)              AS total_messages,
  ch.last_chat_at
FROM partner_contacts c
LEFT JOIN agencies a ON a.id = c.agency_id
LEFT JOIN LATERAL (
  SELECT
    COUNT(*)::int        AS orders_count,
    SUM(total_amount)    AS total_revenue,
    MIN(sale_date)       AS first_purchase_date,
    MAX(sale_date)       AS last_purchase_date
  FROM sales
  WHERE customer_id = c.id OR partner_id = c.id
) s ON TRUE
LEFT JOIN LATERAL (
  SELECT
    array_agg(DISTINCT session_id)        AS chat_sessions,
    SUM(message_count)::int               AS total_messages,
    MAX(last_message_at)                  AS last_chat_at
  FROM v_partner_chat_link l
  WHERE l.contact_id = c.id
) ch ON TRUE;


-- ─── 3. v_followups_due ─────────────────────────────────────
-- Followups готовые к действию: due_date ≤ today+30, не выполнены,
-- + контакт + последняя продажа + WhatsApp-сессия (если есть).
-- Используется на главной странице дашборда.
CREATE OR REPLACE VIEW v_followups_due AS
SELECT
  f.id              AS followup_id,
  f.due_date,
  f.type            AS followup_type,
  f.note,
  f.related_sale_id,
  f.related_sku,
  c.id              AS contact_id,
  c.canonical_name  AS contact_name,
  c.primary_phone,
  c.aliases,
  c.linked_chat_jids,
  -- Какая сессия писала с этим контактом последней (если есть чат)
  (
    SELECT session_id
    FROM v_partner_chat_link l
    WHERE l.contact_id = c.id
    ORDER BY last_message_at DESC NULLS LAST
    LIMIT 1
  ) AS last_session_id,
  s.sale_date       AS related_sale_date,
  s.total_amount    AS related_sale_total,
  CASE
    WHEN f.due_date <= CURRENT_DATE THEN 'overdue'
    WHEN f.due_date <= CURRENT_DATE + INTERVAL '7 days' THEN 'this_week'
    WHEN f.due_date <= CURRENT_DATE + INTERVAL '30 days' THEN 'this_month'
    ELSE 'later'
  END AS urgency
FROM followups f
JOIN partner_contacts c ON c.id = f.contact_id
LEFT JOIN sales s ON s.id = f.related_sale_id
WHERE f.completed_at IS NULL;


-- ─── Permissions ────────────────────────────────────────────
GRANT SELECT ON v_partner_chat_link TO authenticated, service_role;
GRANT SELECT ON v_partner_full      TO authenticated, service_role;
GRANT SELECT ON v_followups_due     TO authenticated, service_role;


-- ─── Sanity ─────────────────────────────────────────────────
SELECT
  (SELECT COUNT(*) FROM v_partner_chat_link) AS chat_links,
  (SELECT COUNT(*) FROM v_partner_full WHERE total_messages > 0 AND orders_count > 0) AS partners_with_chat_AND_sales,
  (SELECT COUNT(*) FROM v_followups_due WHERE urgency IN ('overdue', 'this_week', 'this_month')) AS due_followups;
