-- ============================================================
-- Migration 0011: Partner sales tracking + designer/agency cards
--
-- Omoikiri-only feature (НЕ для template/UORA). Создаёт:
--   - agencies        — студии/агентства (whitelist + auto-detect)
--   - partner_contacts        — единая таблица для партнёров и клиентов
--                       (один контакт может иметь обе роли)
--   - sales           — заказы из Excel-отчётов
--   - sale_items      — позиции заказа (артикулы для 2026+, текст для 2024-2025)
--   - products        — справочник артикулов (растёт прогрессивно из 2026 файлов)
--   - followups       — напоминания (картриджи, upsell, satisfaction-check)
--
-- Базовое правило дедупа: только идентичные номера = один контакт.
-- Без номера = отдельная запись для каждого упоминания.
-- Возможные дубли помечаются manually в possible_duplicates jsonb.
--
-- Run in Supabase SQL Editor (project WPAdil, ref gehiqhnzbumtbvhncblj).
-- ============================================================

-- ─── agencies ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agencies (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_name  TEXT NOT NULL,
  aliases         TEXT[] NOT NULL DEFAULT '{}',
  city            TEXT,
  notes           TEXT,
  member_count    INT NOT NULL DEFAULT 0,
  total_sales_amount BIGINT NOT NULL DEFAULT 0,
  total_sales_count  INT NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agencies_canonical_name
  ON agencies (LOWER(canonical_name));

-- ─── partner_contacts ────────────────────────────────────────────────
-- Единая таблица для всех людей. Один контакт может быть partner И customer.
CREATE TABLE IF NOT EXISTS partner_contacts (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  canonical_name  TEXT NOT NULL,
  aliases         TEXT[] NOT NULL DEFAULT '{}',
  primary_phone   TEXT,                              -- нормализованный (7XXXXXXXXXX)
  phones          TEXT[] NOT NULL DEFAULT '{}',      -- все встреченные номера
  roles           TEXT[] NOT NULL DEFAULT '{}',      -- ['partner'] / ['customer'] / оба
  agency_id       UUID REFERENCES agencies(id) ON DELETE SET NULL,
  city            TEXT,
  notes           TEXT,
  tags            TEXT[] NOT NULL DEFAULT '{}',
  -- denormalized aggregates
  total_purchases_count  INT NOT NULL DEFAULT 0,
  total_purchases_amount BIGINT NOT NULL DEFAULT 0,
  first_purchase_date    DATE,
  last_purchase_date     DATE,
  -- cross-session WhatsApp linking (whose remote_jid matches phone)
  linked_chat_jids TEXT[] NOT NULL DEFAULT '{}',
  -- possible duplicates flagged for manual review
  possible_duplicate_of  UUID[] NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Phone is unique only when not null (multiple phoneless partner_contacts allowed)
CREATE UNIQUE INDEX IF NOT EXISTS idx_partner_contacts_primary_phone_unique
  ON partner_contacts (primary_phone)
  WHERE primary_phone IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_partner_contacts_canonical_name_lower
  ON partner_contacts (LOWER(canonical_name));

CREATE INDEX IF NOT EXISTS idx_partner_contacts_agency
  ON partner_contacts (agency_id) WHERE agency_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_partner_contacts_roles
  ON partner_contacts USING GIN (roles);

-- ─── products (артикулы) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
  sku             TEXT PRIMARY KEY,
  canonical_name  TEXT NOT NULL,
  aliases         TEXT[] NOT NULL DEFAULT '{}',
  category        TEXT,                  -- sink|faucet|disposer|filter|cartridge|dispenser|roll_mat|...
  price_default   BIGINT,
  cartridge_replacement_months INT,      -- для фильтров — срок замены картриджа
  notes           TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── sales ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_file     TEXT NOT NULL,
  order_num       TEXT NOT NULL,                     -- № п/п из Excel (для дедупа)
  sale_date       DATE,
  total_amount    BIGINT NOT NULL DEFAULT 0,
  customer_id     UUID REFERENCES partner_contacts(id) ON DELETE SET NULL,
  partner_id      UUID REFERENCES partner_contacts(id) ON DELETE SET NULL,
  agency_id       UUID REFERENCES agencies(id) ON DELETE SET NULL,
  -- raw fields для аудита (на случай косяков парсера)
  customer_raw    TEXT,
  partner_raw     TEXT,
  manager         TEXT,
  payment_method  TEXT,
  status_text     TEXT,                              -- сохраняем как есть («Доставлен 03.09.2024»)
  inventory_text  TEXT,
  city            TEXT,
  address         TEXT,
  comment         TEXT,
  commission_text TEXT,                              -- «Дозатор в подарок» / «600 000 - каспи 0-0-12»
  lead_source     TEXT,                              -- 'instagram' / 'altyn_agash' / NULL
  -- delivery metadata extracted from status_text
  delivery_date   DATE,                              -- если в status_text есть дата
  delivery_status TEXT,                              -- delivered / pending / refused
  imported_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (source_file, order_num)
);

CREATE INDEX IF NOT EXISTS idx_sales_partner ON sales (partner_id);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales (customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_agency ON sales (agency_id);
CREATE INDEX IF NOT EXISTS idx_sales_date ON sales (sale_date DESC);
CREATE INDEX IF NOT EXISTS idx_sales_imported_at ON sales (imported_at);

-- ─── sale_items ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sale_items (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sale_id         UUID NOT NULL REFERENCES sales(id) ON DELETE CASCADE,
  position_idx    INT NOT NULL DEFAULT 0,            -- порядок в заказе (для 2026)
  sku             TEXT,                              -- 4993782 (только 2026)
  raw_name        TEXT,                              -- «Yamakawa 75 GB» (free text для 2024-2025)
  qty             INT,
  price_per_unit  BIGINT,
  amount          BIGINT,
  matched_product_sku TEXT REFERENCES products(sku) ON DELETE SET NULL,
  match_confidence NUMERIC(3,2),                     -- 0.00..1.00
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sale_items_sale ON sale_items (sale_id);
CREATE INDEX IF NOT EXISTS idx_sale_items_sku ON sale_items (sku) WHERE sku IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sale_items_matched ON sale_items (matched_product_sku) WHERE matched_product_sku IS NOT NULL;

-- ─── followups (напоминания) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS followups (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contact_id      UUID NOT NULL REFERENCES partner_contacts(id) ON DELETE CASCADE,
  due_date        DATE NOT NULL,
  type            TEXT NOT NULL,                     -- cartridge_replacement | upsell | satisfaction_check | birthday | custom
  related_sale_id UUID REFERENCES sales(id) ON DELETE SET NULL,
  related_sku     TEXT REFERENCES products(sku) ON DELETE SET NULL,
  note            TEXT,
  completed_at    TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_followups_contact ON followups (contact_id);
CREATE INDEX IF NOT EXISTS idx_followups_due_pending ON followups (due_date) WHERE completed_at IS NULL;

-- ─── RLS — temporary closed (Omoikiri single-tenant for now) ────────
ALTER TABLE agencies      ENABLE ROW LEVEL SECURITY;
ALTER TABLE partner_contacts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE products      ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales         ENABLE ROW LEVEL SECURITY;
ALTER TABLE sale_items    ENABLE ROW LEVEL SECURITY;
ALTER TABLE followups     ENABLE ROW LEVEL SECURITY;

-- Single-tenant policies — все authenticated видят всё (как остальные таблицы)
CREATE POLICY "authenticated_all" ON agencies   FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "authenticated_all" ON partner_contacts   FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "authenticated_all" ON products   FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "authenticated_all" ON sales      FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "authenticated_all" ON sale_items FOR ALL TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "authenticated_all" ON followups  FOR ALL TO authenticated USING (true) WITH CHECK (true);

-- ─── updated_at triggers ─────────────────────────────────────
-- Reuse existing trigger function from migration 0004 (already exists).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'set_updated_at') THEN
    EXECUTE 'CREATE TRIGGER trg_agencies_updated  BEFORE UPDATE ON agencies  FOR EACH ROW EXECUTE FUNCTION set_updated_at()';
    EXECUTE 'CREATE TRIGGER trg_partner_contacts_updated  BEFORE UPDATE ON partner_contacts  FOR EACH ROW EXECUTE FUNCTION set_updated_at()';
    EXECUTE 'CREATE TRIGGER trg_products_updated  BEFORE UPDATE ON products  FOR EACH ROW EXECUTE FUNCTION set_updated_at()';
  END IF;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

-- ─── Sanity check ────────────────────────────────────────────
SELECT
  (SELECT count(*) FROM information_schema.tables WHERE table_name IN
    ('agencies','partner_contacts','products','sales','sale_items','followups')) AS tables_created,
  (SELECT count(*) FROM information_schema.columns WHERE table_name = 'sales') AS sales_columns,
  (SELECT count(*) FROM information_schema.columns WHERE table_name = 'partner_contacts') AS partner_contacts_columns;
