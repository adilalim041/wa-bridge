import { z } from 'zod';
import { CUSTOMER_TYPES } from './tagConstants.js';

// === Tool input schemas for chatEndpoint.js ===
// Validated BEFORE any DB call — defense against hallucinated limits, injection via IDs,
// and resource exhaustion from unbounded parameters.

const sessionIdField = z.string().min(1).max(50).optional();
const remoteJidField = z.string().min(1).max(100);

export const toolInputSchemas = {
  get_chats: z.object({
    session_id: sessionIdField,
    limit: z.coerce.number().int().min(1).max(100).default(20),
    name_search: z.string().max(100).optional(),
  }),

  get_messages: z.object({
    session_id: sessionIdField,
    remote_jid: remoteJidField,
    limit: z.coerce.number().int().min(1).max(200).default(30),
  }),

  get_ai_analysis: z.object({
    session_id: sessionIdField,
    remote_jid: z.string().max(100).optional(),
    lead_temperature: z.enum(['hot', 'warm', 'cold', 'dead']).optional(),
    sentiment: z.enum(['positive', 'neutral', 'negative', 'aggressive']).optional(),
    limit: z.coerce.number().int().min(1).max(100).default(20),
  }),

  get_manager_analytics: z.object({
    session_id: sessionIdField,
    days: z.coerce.number().int().min(1).max(365).default(7),
  }),

  get_contacts: z.object({
    session_id: sessionIdField,
    role: z.string().max(50).optional(),
  }),

  find_problems: z.object({
    session_id: sessionIdField,
    hours_no_response: z.coerce.number().int().min(1).max(720).default(2),
  }),

  update_deal_stage: z.object({
    session_id: sessionIdField,
    remote_jid: remoteJidField,
    // deal_stage is tenant-defined — no hardcoded enum; soft limit 40 chars
    deal_stage: z.string().min(1).max(40).transform((s) => s.trim()),
  }),

  update_tags: z.object({
    session_id: sessionIdField,
    remote_jid: remoteJidField,
    tags: z.array(z.string().max(50)).max(10),
  }),

  create_task: z.object({
    session_id: sessionIdField,
    remote_jid: z.string().max(100).optional(),
    title: z.string().min(1).max(200),
    due_date: z.string().min(1).max(50),
    description: z.string().max(2000).optional(),
    task_type: z.enum(['follow_up', 'call_back', 'send_quote', 'send_catalog', 'visit_showroom', 'custom']).optional(),
    priority: z.enum(['low', 'medium', 'high', 'urgent']).optional(),
    deal_value: z.coerce.number().min(0).max(1_000_000_000).optional(),
    notes: z.string().max(2000).optional(),
  }),
};

// === Daily analysis response schema ===
// Validates the JSON returned by Claude when analyzing a dialog

export const DailyAnalysisSchema = z.object({
  intent: z.enum([
    'price_inquiry', 'complaint', 'availability', 'measurement_request',
    'delivery', 'consultation', 'collaboration', 'small_talk', 'spam', 'other',
  ]).catch('other'),

  lead_temperature: z.enum(['hot', 'warm', 'cold', 'dead']).catch('cold'),

  lead_source: z.enum([
    'instagram_ad', 'google_ad', 'word_of_mouth', 'repeat_client',
    'designer_partner', 'showroom_visit', 'incoming_call', 'unknown',
  ]).catch('unknown'),

  customer_type: z.enum(CUSTOMER_TYPES).catch('unknown'),

  dialog_topic: z.enum([
    'sink_sale', 'faucet_sale', 'complaint', 'service',
    'consultation', 'partnership', 'other',
  ]).catch('other'),

  // TODO dynamic stages per tenant — prompt is hardcoded with 9 Omoikiri stages,
  // backend no longer whitelists. .catch() keeps fallback for malformed AI output.
  deal_stage: z.string().min(1).max(40).catch('needs_review'),

  sentiment: z.enum(['positive', 'neutral', 'negative', 'aggressive']).catch('neutral'),

  risk_flags: z.array(
    z.enum(['client_unhappy', 'manager_rude', 'slow_response', 'potential_return', 'lost_lead'])
  ).catch([]),

  consultation_quality: z.object({
    score: z.number().min(0).max(100).nullable().catch(null),
    questions_asked: z.array(z.string()).catch([]),
    questions_missed: z.array(z.string()).catch([]),
    upsell_offered: z.boolean().catch(false),
  }).catch({ score: null, questions_asked: [], questions_missed: [], upsell_offered: false }),

  followup_status: z.enum(['not_needed', 'done', 'missed', 'pending']).catch('not_needed'),

  manager_issues: z.array(
    z.enum([
      'slow_first_response', 'no_followup', 'poor_consultation', 'no_photos',
      'no_showroom_invite', 'no_upsell', 'rude_tone', 'formal_tone', 'no_alternative',
    ])
  ).catch([]),

  summary_ru: z.string().max(500).nullable().catch(null),
  action_required: z.boolean().catch(false),
  action_suggestion: z.string().max(500).nullable().catch(null),
  confidence: z.number().min(0).max(1).catch(0),
});

// === Batch classify response schema ===
// Validates the JSON array returned by Haiku when classifying chats

export const ClassifyItemSchema = z.object({
  id: z.number(),
  customer_type: z.enum(CUSTOMER_TYPES),
  // Tenant-defined stage name; optional (classify may omit)
  deal_stage: z.string().min(1).max(40).optional(),
});

export const ClassifyBatchSchema = z.array(ClassifyItemSchema);

/**
 * Parse and validate Claude's JSON response with Zod.
 * Returns { success: true, data } or { success: false, error }.
 */
export function parseAIResponse(rawText, schema) {
  try {
    const clean = rawText
      .replace(/```json\s*/g, '')
      .replace(/```\s*/g, '')
      .trim();

    const parsed = JSON.parse(clean);
    const result = schema.safeParse(parsed);

    if (result.success) {
      return { success: true, data: result.data };
    }

    return {
      success: false,
      error: `Zod validation failed: ${result.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ')}`,
      raw: parsed, // keep raw for logging
    };
  } catch (err) {
    return { success: false, error: `JSON parse failed: ${err.message}` };
  }
}
