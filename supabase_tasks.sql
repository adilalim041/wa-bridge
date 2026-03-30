-- Tasks & Reminders table
CREATE TABLE IF NOT EXISTS tasks (
    id BIGSERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    remote_jid TEXT,
    title TEXT NOT NULL,
    description TEXT,
    task_type TEXT DEFAULT 'follow_up',
    priority TEXT DEFAULT 'medium',
    status TEXT DEFAULT 'pending',
    due_date TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    assigned_to TEXT,
    created_by TEXT DEFAULT 'manual',
    deal_value NUMERIC,
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_due ON tasks(status, due_date);
CREATE INDEX IF NOT EXISTS idx_tasks_remote_jid ON tasks(remote_jid);
CREATE INDEX IF NOT EXISTS idx_tasks_session ON tasks(session_id);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date) WHERE status = 'pending';

-- Deal value column on contacts_crm
ALTER TABLE contacts_crm ADD COLUMN IF NOT EXISTS deal_value NUMERIC;
