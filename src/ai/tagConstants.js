/**
 * Single source of truth for AI customer_type ↔ chat_tags mappings.
 *
 * Previously these lists lived in 3+ places that drifted apart:
 *  - SYSTEM_PROMPT in aiWorker.js (told Claude which values to emit)
 *  - DailyAnalysisSchema / ClassifyItemSchema in schemas.js (Zod validation)
 *  - CUSTOMER_TYPE_TAG in aiWorker.js (mapping to Russian tag name)
 *  - Dashboard analytics filter (which tags count as "real business")
 *
 * Any edit in one without the others = chats silently fall into "неизвестно"
 * and disappear from the analytics dashboard. Discovered 2026-04-20 when
 * Almaty-Реклама showed 1 hot lead instead of ~100 because AI emitted
 * legacy values (end_user, b2b_partner, internal) that weren't mapped.
 *
 * Rule: the only way to add a new customer_type is to edit this file.
 */

/**
 * Canonical set of customer_type values emitted by the AI today.
 * If adding: update this, rebuild the prompt (aiWorker.js), re-run tests.
 */
export const CUSTOMER_TYPES = [
  'end_client',
  'partner',
  'colleague',
  'spam',
  'unknown',
];

/**
 * Legacy values that appear in historical chat_ai rows but are NOT emitted
 * by the current AI prompt. We keep them mapped so that one-off migrations
 * (e.g. _reclassify_from_chat_ai.mjs) and defensive reads don't silently
 * drop them into "неизвестно".
 */
export const LEGACY_CUSTOMER_TYPES = {
  end_customer: 'end_client',
  end_user:     'end_client',
  contractor:   'end_client',
  designer:     'partner',      // Omoikiri: дизайнеры = партнёры
  b2b_partner:  'partner',
  internal:     'colleague',
  personal:     'unknown',      // personal chats → unknown
  other:        'unknown',
};

/**
 * Mapping: canonical customer_type → Russian tag stored in chat_tags.tags[0].
 */
export const CUSTOMER_TYPE_TAG = {
  end_client: 'клиент',
  partner:    'партнёр',
  colleague:  'сотрудник',
  spam:       'спам',
  unknown:    'неизвестно',
};

/**
 * Full resolver that accepts both canonical and legacy values.
 * Returns the Russian tag name or undefined if truly unknown.
 */
export function resolveTag(customerType) {
  if (!customerType) return undefined;
  const canonical = CUSTOMER_TYPE_TAG[customerType]
    ? customerType
    : LEGACY_CUSTOMER_TYPES[customerType];
  return canonical ? CUSTOMER_TYPE_TAG[canonical] : undefined;
}

/**
 * Set of all tag values that the auto-tagger can produce.
 * Used by aiWorker to decide whether a chat tag is AI-owned (overwritable)
 * vs user-confirmed.
 */
export const AI_AUTO_TAGS = new Set(Object.values(CUSTOMER_TYPE_TAG));

/**
 * "Real business" tags — chats with these tags show up in analytics dashboard.
 * Non-business tags (сотрудник / неизвестно) are filtered out.
 */
export const BUSINESS_TAGS = new Set(['клиент', 'партнёр']);

/**
 * Prompt fragment listing allowed customer_type values.
 * Injected into SYSTEM_PROMPT so the prompt stays in sync with CUSTOMER_TYPES.
 */
export const CUSTOMER_TYPE_PROMPT_LIST = CUSTOMER_TYPES.join(', ');
