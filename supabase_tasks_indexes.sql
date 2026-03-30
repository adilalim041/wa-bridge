-- Indexes for task enrichment performance
CREATE INDEX IF NOT EXISTS idx_contacts_crm_remote_jid ON contacts_crm(remote_jid);
CREATE INDEX IF NOT EXISTS idx_chats_remote_jid ON chats(remote_jid);
