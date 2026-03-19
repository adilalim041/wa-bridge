-- ============================================================
-- Omoikiri.AI — Full Database Schema
-- Last updated: 2026-03-19
-- ============================================================

-- Сообщения WhatsApp
CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,
    from_me BOOLEAN NOT NULL DEFAULT false,
    body TEXT,
    message_type TEXT NOT NULL DEFAULT 'text',
    push_name TEXT,
    sender TEXT,
    chat_type TEXT DEFAULT 'personal',
    media_url TEXT,
    media_type TEXT,
    file_name TEXT,
    read_at TIMESTAMPTZ,
    dialog_session_id BIGINT,
    ai_processed BOOLEAN DEFAULT false,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (message_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_remote_jid ON messages(remote_jid);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_remote_jid_timestamp ON messages(remote_jid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_remote ON messages(session_id, remote_jid, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_dialog_session ON messages(dialog_session_id);
CREATE INDEX IF NOT EXISTS idx_messages_ai_processed ON messages(ai_processed);

-- Контакты (телефонная книга WhatsApp)
CREATE TABLE IF NOT EXISTS contacts (
    id BIGSERIAL PRIMARY KEY,
    phone TEXT UNIQUE NOT NULL,
    name TEXT,
    first_seen_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
CREATE INDEX IF NOT EXISTS idx_contacts_updated ON contacts(updated_at DESC);

-- Auth state (Baileys credentials)
CREATE TABLE IF NOT EXISTS auth_state (
    key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_auth_state_session ON auth_state(session_id);

-- Session lock (distributed locking)
CREATE TABLE IF NOT EXISTS session_lock (
    session_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    locked_at TIMESTAMPTZ DEFAULT now(),
    heartbeat_at TIMESTAMPTZ DEFAULT now()
);

-- Конфигурация сессий WhatsApp
CREATE TABLE IF NOT EXISTS session_config (
    session_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    phone_number TEXT,
    is_active BOOLEAN DEFAULT true,
    auto_start BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Привязка менеджеров к сессиям
CREATE TABLE IF NOT EXISTS manager_sessions (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    session_id TEXT NOT NULL REFERENCES session_config(session_id),
    role TEXT DEFAULT 'viewer',
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(user_id, session_id)
);

CREATE INDEX IF NOT EXISTS idx_manager_sessions_user ON manager_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_manager_sessions_session ON manager_sessions(session_id);

-- Чаты
CREATE TABLE IF NOT EXISTS chats (
    remote_jid TEXT NOT NULL,
    session_id TEXT NOT NULL,
    chat_type TEXT NOT NULL DEFAULT 'personal',
    display_name TEXT,
    participant_count INTEGER,
    phone_number TEXT,
    last_message_at TIMESTAMPTZ,
    is_muted BOOLEAN DEFAULT false,
    muted_until TIMESTAMPTZ,
    is_hidden BOOLEAN DEFAULT false,
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (remote_jid, session_id)
);

CREATE INDEX IF NOT EXISTS idx_chats_session ON chats(session_id, last_message_at DESC);

-- CRM контакты
CREATE TABLE IF NOT EXISTS contacts_crm (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,
    phone TEXT,
    first_name TEXT NOT NULL,
    last_name TEXT,
    role TEXT DEFAULT 'клиент',
    company TEXT,
    city TEXT,
    responsible_manager TEXT,
    avatar_url TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (session_id, remote_jid)
);

-- Диалоговые сессии (группировка сообщений, gap > 4 часов = новый диалог)
CREATE TABLE IF NOT EXISTS dialog_sessions (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    last_message_at TIMESTAMPTZ NOT NULL,
    message_count INTEGER DEFAULT 1,
    status TEXT DEFAULT 'open',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dialog_sessions_lookup ON dialog_sessions(session_id, remote_jid, status);

-- AI анализ диалогов
CREATE TABLE IF NOT EXISTS chat_ai (
    id BIGSERIAL PRIMARY KEY,
    dialog_session_id BIGINT UNIQUE NOT NULL,
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,
    intent TEXT,
    lead_temperature TEXT,
    lead_source TEXT,
    dialog_topic TEXT,
    deal_stage TEXT,
    sentiment TEXT,
    risk_flags TEXT[] DEFAULT '{}',
    summary_ru TEXT,
    action_required BOOLEAN DEFAULT false,
    action_suggestion TEXT,
    confidence NUMERIC DEFAULT 0,
    message_count_analyzed INTEGER DEFAULT 0,
    analyzed_at TIMESTAMPTZ DEFAULT now()
);

-- Очередь на AI обработку
CREATE TABLE IF NOT EXISTS ai_queue (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,
    message_id TEXT,
    dialog_session_id BIGINT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ai_queue_status ON ai_queue(status, created_at);

-- Аналитика менеджеров (время ответа)
CREATE TABLE IF NOT EXISTS manager_analytics (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    remote_jid TEXT NOT NULL,
    dialog_session_id BIGINT,
    customer_message_at TIMESTAMPTZ,
    manager_response_at TIMESTAMPTZ,
    response_time_seconds INTEGER,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_manager_analytics_session ON manager_analytics(session_id, created_at DESC);
