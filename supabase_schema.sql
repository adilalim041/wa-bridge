-- Таблица сообщений
CREATE TABLE IF NOT EXISTS messages (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT UNIQUE NOT NULL,
    remote_jid TEXT NOT NULL,
    from_me BOOLEAN NOT NULL DEFAULT false,
    body TEXT,
    message_type TEXT NOT NULL DEFAULT 'text',
    push_name TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_messages_remote_jid ON messages(remote_jid);
CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_messages_remote_jid_timestamp ON messages(remote_jid, timestamp DESC);

-- Таблица контактов
CREATE TABLE IF NOT EXISTS contacts (
    id BIGSERIAL PRIMARY KEY,
    phone TEXT UNIQUE NOT NULL,
    name TEXT,
    first_seen_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
CREATE INDEX IF NOT EXISTS idx_contacts_updated ON contacts(updated_at DESC);

DELETE FROM messages WHERE body IS NULL;

CREATE TABLE IF NOT EXISTS auth_state (
    key TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_auth_state_session ON auth_state(session_id);

CREATE TABLE IF NOT EXISTS session_lock (
    session_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    locked_at TIMESTAMPTZ DEFAULT now(),
    heartbeat_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE messages ADD COLUMN IF NOT EXISTS session_id TEXT;
CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_remote ON messages(session_id, remote_jid, timestamp DESC);

CREATE TABLE IF NOT EXISTS session_config (
    session_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    phone_number TEXT,
    is_active BOOLEAN DEFAULT true,
    auto_start BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

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

CREATE TABLE IF NOT EXISTS chats (
    remote_jid TEXT NOT NULL,
    session_id TEXT NOT NULL,
    chat_type TEXT NOT NULL DEFAULT 'personal',
    display_name TEXT,
    participant_count INTEGER,
    phone_number TEXT,
    last_message_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (remote_jid, session_id)
);

CREATE INDEX IF NOT EXISTS idx_chats_session ON chats(session_id, last_message_at DESC);

ALTER TABLE messages ADD COLUMN IF NOT EXISTS sender TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS chat_type TEXT DEFAULT 'personal';
ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_url TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS media_type TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS file_name TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ;

-- Migration example from single-session to multi-session:
-- INSERT INTO session_config (session_id, display_name, phone_number, is_active, auto_start)
-- VALUES ('omoikiri-main', 'Астана Основной', '77014135151', true, true)
-- ON CONFLICT (session_id) DO NOTHING;
--
-- UPDATE messages
-- SET session_id = 'omoikiri-main'
-- WHERE session_id IS NULL;
