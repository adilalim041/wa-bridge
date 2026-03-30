-- Stage tracking for CRM funnel
ALTER TABLE chat_ai ADD COLUMN IF NOT EXISTS stage_source TEXT DEFAULT 'ai_classify';
ALTER TABLE chat_ai ADD COLUMN IF NOT EXISTS stage_changed_at TIMESTAMPTZ DEFAULT now();

-- Valid sources: 'ai_daily', 'ai_classify', 'manual'
COMMENT ON COLUMN chat_ai.stage_source IS 'Who set deal_stage: ai_daily, ai_classify, manual';

-- Index for querying manual overrides
CREATE INDEX IF NOT EXISTS idx_chat_ai_stage_source ON chat_ai(stage_source) WHERE stage_source = 'manual';
