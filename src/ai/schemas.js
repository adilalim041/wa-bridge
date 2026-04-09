import { z } from 'zod';

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

  customer_type: z.enum(['end_client', 'partner', 'colleague', 'unknown']).catch('unknown'),

  dialog_topic: z.enum([
    'sink_sale', 'faucet_sale', 'complaint', 'service',
    'consultation', 'partnership', 'other',
  ]).catch('other'),

  deal_stage: z.enum([
    'needs_review', 'first_contact', 'consultation', 'model_selection',
    'price_negotiation', 'payment', 'delivery', 'completed', 'refused',
  ]).catch('needs_review'),

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
  customer_type: z.enum(['end_client', 'partner', 'colleague', 'unknown']),
  deal_stage: z.enum([
    'needs_review', 'first_contact', 'consultation', 'model_selection',
    'price_negotiation', 'payment', 'delivery', 'completed', 'refused',
  ]).optional(),
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
