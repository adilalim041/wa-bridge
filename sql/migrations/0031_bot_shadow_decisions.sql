-- Phase 1 of the WhatsApp AI manager.
--
-- This table stores metadata-only shadow decisions. It intentionally does not
-- copy message bodies or conversation history. Contacts denied by policy are
-- therefore never added to an AI training/RAG dataset.

CREATE TABLE IF NOT EXISTS public.bot_message_decisions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  session_id text NOT NULL,
  message_id text NOT NULL,
  remote_jid text NOT NULL,
  mode text NOT NULL DEFAULT 'shadow'
    CHECK (mode IN ('shadow', 'approval', 'live')),
  action text NOT NULL
    CHECK (action IN ('eligible', 'skip', 'reply', 'escalate', 'error')),
  reason text NOT NULL,
  contact_tags text[] NOT NULL DEFAULT '{}',
  evaluated_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (session_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_bot_message_decisions_evaluated
  ON public.bot_message_decisions (evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_bot_message_decisions_action
  ON public.bot_message_decisions (action, reason, evaluated_at DESC);

ALTER TABLE public.bot_message_decisions ENABLE ROW LEVEL SECURITY;

REVOKE ALL PRIVILEGES ON TABLE public.bot_message_decisions FROM anon, authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.bot_message_decisions TO service_role;

DROP POLICY IF EXISTS service_role_all ON public.bot_message_decisions;
CREATE POLICY service_role_all
  ON public.bot_message_decisions
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
