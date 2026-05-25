-- ============================================================
-- Migration 0027: Advertising lead events
--
-- Purpose:
--   Store a separate "ad lead event" layer for WhatsApp analytics.
--   A chat can contain multiple ad events: first inbound message on an ad
--   account, or a later inbound message matching an ad template.
--
-- This is intentionally separate from chat_ai:
--   chat_ai = AI analysis of a whole dialog/session
--   ad_lead_events = deterministic ad attribution event + response/conversion
-- ============================================================

CREATE TABLE IF NOT EXISTS ad_lead_sessions (
  session_id   TEXT PRIMARY KEY,
  display_name TEXT,
  city         TEXT,
  is_active    BOOLEAN NOT NULL DEFAULT TRUE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ad_lead_patterns (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pattern_key    TEXT NOT NULL UNIQUE,
  label          TEXT NOT NULL,
  pattern_text   TEXT NOT NULL,
  match_type     TEXT NOT NULL DEFAULT 'icontains',
  campaign_label TEXT,
  is_active      BOOLEAN NOT NULL DEFAULT TRUE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ad_lead_events (
  id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id                  TEXT NOT NULL,
  remote_jid                  TEXT NOT NULL,
  trigger_message_id          TEXT NOT NULL,
  trigger_message_db_id       BIGINT,
  trigger_at                  TIMESTAMPTZ NOT NULL,
  trigger_body                TEXT,
  trigger_kind                TEXT NOT NULL,
  pattern_key                 TEXT,
  campaign_label              TEXT,
  first_response_at           TIMESTAMPTZ,
  first_response_work_minutes INTEGER,
  response_status             TEXT NOT NULL DEFAULT 'pending',
  last_message_at             TIMESTAMPTZ,
  last_message_from_me        BOOLEAN,
  followup_status             TEXT,
  matched_contact_id          UUID REFERENCES partner_contacts(id) ON DELETE SET NULL,
  matched_sale_ids            UUID[] NOT NULL DEFAULT '{}',
  matched_sales_count         INTEGER NOT NULL DEFAULT 0,
  matched_revenue             BIGINT NOT NULL DEFAULT 0,
  meta_campaign_id            TEXT,
  meta_adset_id               TEXT,
  meta_ad_id                  TEXT,
  meta_creative_id            TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (session_id, trigger_message_id)
);

CREATE INDEX IF NOT EXISTS idx_ad_lead_events_trigger_at
  ON ad_lead_events (trigger_at DESC);

CREATE INDEX IF NOT EXISTS idx_ad_lead_events_session_trigger
  ON ad_lead_events (session_id, trigger_at DESC);

CREATE INDEX IF NOT EXISTS idx_ad_lead_events_remote
  ON ad_lead_events (remote_jid);

CREATE INDEX IF NOT EXISTS idx_ad_lead_events_response_status
  ON ad_lead_events (response_status);

ALTER TABLE ad_lead_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE ad_lead_patterns ENABLE ROW LEVEL SECURITY;
ALTER TABLE ad_lead_events ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'public' AND tablename = 'ad_lead_sessions' AND policyname = 'authenticated_all'
  ) THEN
    CREATE POLICY "authenticated_all" ON ad_lead_sessions FOR ALL TO authenticated USING (true) WITH CHECK (true);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'public' AND tablename = 'ad_lead_patterns' AND policyname = 'authenticated_all'
  ) THEN
    CREATE POLICY "authenticated_all" ON ad_lead_patterns FOR ALL TO authenticated USING (true) WITH CHECK (true);
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies WHERE schemaname = 'public' AND tablename = 'ad_lead_events' AND policyname = 'authenticated_all'
  ) THEN
    CREATE POLICY "authenticated_all" ON ad_lead_events FOR ALL TO authenticated USING (true) WITH CHECK (true);
  END IF;
END $$;

INSERT INTO ad_lead_sessions (session_id, display_name, city)
VALUES
  ('almaty-rabochiy-reklama', 'Алматы — Рабочий Реклама', 'Алматы'),
  ('astana-renat-rabochiy-reklama', 'Астана — Ренат рабочий Реклама', 'Астана')
ON CONFLICT (session_id) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  city = EXCLUDED.city,
  is_active = TRUE,
  updated_at = NOW();

INSERT INTO ad_lead_patterns (pattern_key, label, pattern_text, campaign_label)
VALUES
  ('grinder_interest', 'Измельчитель', 'Меня интересует Измельчитель от бренда Omoikiri', 'Рекламная заявка: измельчитель'),
  ('discount_sanitary', 'Сантехника по скидке', 'Хочу сантехнику от ОМОЙКИРИ по скидке', 'Рекламная заявка: скидка'),
  ('luxury_sanitary', 'Люксовая сантехника', 'Впервые вижу, мне нужна ЛЮКСОВАЯ сантехника', 'Рекламная заявка: люксовая сантехника')
ON CONFLICT (pattern_key) DO UPDATE SET
  label = EXCLUDED.label,
  pattern_text = EXCLUDED.pattern_text,
  campaign_label = EXCLUDED.campaign_label,
  is_active = TRUE,
  updated_at = NOW();

CREATE OR REPLACE FUNCTION ad_working_minutes_utc5(start_at TIMESTAMPTZ, end_at TIMESTAMPTZ)
RETURNS INTEGER
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  start_local TIMESTAMP;
  end_local TIMESTAMP;
  cursor_local TIMESTAMP;
  day_start TIMESTAMP;
  day_end TIMESTAMP;
  segment_end TIMESTAMP;
  total INTEGER := 0;
  guard INTEGER := 0;
BEGIN
  IF start_at IS NULL OR end_at IS NULL OR end_at <= start_at THEN
    RETURN 0;
  END IF;

  start_local := (start_at AT TIME ZONE 'UTC') + INTERVAL '5 hours';
  end_local := (end_at AT TIME ZONE 'UTC') + INTERVAL '5 hours';
  cursor_local := start_local;

  WHILE cursor_local < end_local AND guard < 60 LOOP
    guard := guard + 1;
    day_start := date_trunc('day', cursor_local) + INTERVAL '10 hours';
    day_end := date_trunc('day', cursor_local) + INTERVAL '20 hours';

    IF cursor_local < day_start THEN
      cursor_local := day_start;
    END IF;

    IF cursor_local >= day_end THEN
      cursor_local := date_trunc('day', cursor_local + INTERVAL '1 day') + INTERVAL '10 hours';
      CONTINUE;
    END IF;

    segment_end := LEAST(end_local, day_end);
    total := total + GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (segment_end - cursor_local)) / 60)::INTEGER);
    cursor_local := CASE
      WHEN segment_end >= day_end THEN date_trunc('day', cursor_local + INTERVAL '1 day') + INTERVAL '10 hours'
      ELSE segment_end
    END;
  END LOOP;

  RETURN total;
END;
$$;

CREATE OR REPLACE FUNCTION refresh_ad_lead_events()
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  inserted_count INTEGER := 0;
BEGIN
  WITH inbound AS (
    SELECT
      m.*,
      ROW_NUMBER() OVER (PARTITION BY m.session_id, m.remote_jid ORDER BY m.timestamp ASC, m.id ASC) AS inbound_rank
    FROM messages m
    JOIN ad_lead_sessions s ON s.session_id = m.session_id AND s.is_active = TRUE
    WHERE m.from_me = FALSE
      AND m.remote_jid NOT LIKE '%-%'
  ),
  pattern_candidates AS (
    SELECT
      i.*,
      'template'::TEXT AS trigger_kind,
      p.pattern_key,
      p.campaign_label,
      2 AS priority
    FROM inbound i
    JOIN ad_lead_patterns p
      ON p.is_active = TRUE
     AND LOWER(COALESCE(i.body, '')) LIKE '%' || LOWER(p.pattern_text) || '%'
  ),
  first_candidates AS (
    SELECT
      i.*,
      'ad_account_first_inbound'::TEXT AS trigger_kind,
      NULL::TEXT AS pattern_key,
      'Рекламный WhatsApp-аккаунт'::TEXT AS campaign_label,
      1 AS priority
    FROM inbound i
    WHERE i.inbound_rank = 1
  ),
  candidate_events AS (
    SELECT DISTINCT ON (session_id, message_id)
      *
    FROM (
      SELECT * FROM pattern_candidates
      UNION ALL
      SELECT * FROM first_candidates
    ) x
    ORDER BY session_id, message_id, priority DESC
  ),
  enriched AS (
    SELECT
      e.*,
      regexp_replace(split_part(e.remote_jid, '@', 1), '\D', '', 'g') AS phone_digits,
      r.timestamp AS first_response_at,
      ad_working_minutes_utc5(e.timestamp, r.timestamp) AS first_response_work_minutes,
      lm.timestamp AS last_message_at,
      lm.from_me AS last_message_from_me
    FROM candidate_events e
    LEFT JOIN LATERAL (
      SELECT m.timestamp
      FROM messages m
      WHERE m.session_id = e.session_id
        AND m.remote_jid = e.remote_jid
        AND m.from_me = TRUE
        AND m.timestamp > e.timestamp
      ORDER BY m.timestamp ASC, m.id ASC
      LIMIT 1
    ) r ON TRUE
    LEFT JOIN LATERAL (
      SELECT m.timestamp, m.from_me
      FROM messages m
      WHERE m.session_id = e.session_id
        AND m.remote_jid = e.remote_jid
      ORDER BY m.timestamp DESC, m.id DESC
      LIMIT 1
    ) lm ON TRUE
  ),
  contact_match AS (
    SELECT
      e.*,
      c.id AS matched_contact_id
    FROM enriched e
    LEFT JOIN LATERAL (
      SELECT pc.id
      FROM partner_contacts pc
      WHERE pc.primary_phone = e.phone_digits
         OR e.phone_digits = ANY(pc.phones)
      LIMIT 1
    ) c ON TRUE
  ),
  sales_match AS (
    SELECT
      e.id,
      e.message_id,
      e.session_id,
      e.remote_jid,
      e.body,
      e.timestamp,
      e.trigger_kind,
      e.pattern_key,
      e.campaign_label,
      e.first_response_at,
      e.first_response_work_minutes,
      e.last_message_at,
      e.last_message_from_me,
      e.matched_contact_id,
      COALESCE(array_agg(s.id) FILTER (WHERE s.id IS NOT NULL), '{}') AS matched_sale_ids,
      COUNT(s.id)::INTEGER AS matched_sales_count,
      COALESCE(SUM(s.total_amount), 0)::BIGINT AS matched_revenue
    FROM contact_match e
    LEFT JOIN sales s
      ON e.matched_contact_id IS NOT NULL
     AND (s.customer_id = e.matched_contact_id OR s.partner_id = e.matched_contact_id)
     AND s.sale_date >= ((e.timestamp AT TIME ZONE 'UTC') + INTERVAL '5 hours')::DATE
    GROUP BY
      e.id, e.message_id, e.session_id, e.remote_jid, e.body, e.timestamp,
      e.trigger_kind, e.pattern_key, e.campaign_label, e.first_response_at,
      e.first_response_work_minutes, e.last_message_at, e.last_message_from_me,
      e.matched_contact_id
  ),
  upserted AS (
    INSERT INTO ad_lead_events (
      session_id,
      remote_jid,
      trigger_message_id,
      trigger_message_db_id,
      trigger_at,
      trigger_body,
      trigger_kind,
      pattern_key,
      campaign_label,
      first_response_at,
      first_response_work_minutes,
      response_status,
      last_message_at,
      last_message_from_me,
      followup_status,
      matched_contact_id,
      matched_sale_ids,
      matched_sales_count,
      matched_revenue,
      updated_at
    )
    SELECT
      session_id,
      remote_jid,
      message_id,
      id,
      timestamp,
      body,
      trigger_kind,
      pattern_key,
      campaign_label,
      first_response_at,
      first_response_work_minutes,
      CASE
        WHEN first_response_at IS NULL THEN 'no_response'
        WHEN first_response_work_minutes > 60 THEN 'very_slow'
        WHEN first_response_work_minutes > 15 THEN 'slow'
        ELSE 'ok'
      END,
      last_message_at,
      last_message_from_me,
      CASE
        WHEN first_response_at IS NULL THEN 'no_response'
        WHEN last_message_from_me = FALSE AND last_message_at < NOW() - INTERVAL '24 hours' THEN 'client_waiting'
        ELSE 'responded'
      END,
      matched_contact_id,
      matched_sale_ids,
      matched_sales_count,
      matched_revenue,
      NOW()
    FROM sales_match
    ON CONFLICT (session_id, trigger_message_id) DO UPDATE SET
      first_response_at = EXCLUDED.first_response_at,
      first_response_work_minutes = EXCLUDED.first_response_work_minutes,
      response_status = EXCLUDED.response_status,
      last_message_at = EXCLUDED.last_message_at,
      last_message_from_me = EXCLUDED.last_message_from_me,
      followup_status = EXCLUDED.followup_status,
      matched_contact_id = EXCLUDED.matched_contact_id,
      matched_sale_ids = EXCLUDED.matched_sale_ids,
      matched_sales_count = EXCLUDED.matched_sales_count,
      matched_revenue = EXCLUDED.matched_revenue,
      updated_at = NOW()
    RETURNING 1
  )
  SELECT COUNT(*) INTO inserted_count FROM upserted;

  RETURN jsonb_build_object('upserted', inserted_count, 'refreshed_at', NOW());
END;
$$;
