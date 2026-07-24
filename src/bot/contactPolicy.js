import { BUSINESS_TAGS, CUSTOMER_TYPE_TAG } from '../ai/tagConstants.js';

const normalizeTag = (tag) => String(tag || '').trim().toLowerCase();

const ALLOWED_TAGS = new Set([...BUSINESS_TAGS].map(normalizeTag));
const DENIED_TAGS = new Set([
  CUSTOMER_TYPE_TAG.colleague,
  CUSTOMER_TYPE_TAG.spam,
  CUSTOMER_TYPE_TAG.unknown,
  'личное',
].map(normalizeTag));

export function evaluateBotContactPolicy({
  fromMe = false,
  chatType = 'personal',
  tags = [],
  tagLookupOk = true,
  messageAgeMs = 0,
  maxMessageAgeMs = 15 * 60 * 1000,
} = {}) {
  if (fromMe) {
    return { eligible: false, reason: 'outgoing_message' };
  }

  if (chatType !== 'personal') {
    return { eligible: false, reason: 'non_personal_chat' };
  }

  if (!Number.isFinite(messageAgeMs) || messageAgeMs < 0 || messageAgeMs > maxMessageAgeMs) {
    return { eligible: false, reason: 'stale_message' };
  }

  if (!tagLookupOk) {
    return { eligible: false, reason: 'tag_lookup_failed' };
  }

  const normalizedTags = [...new Set((tags || []).map(normalizeTag).filter(Boolean))];

  if (normalizedTags.some((tag) => DENIED_TAGS.has(tag))) {
    return { eligible: false, reason: 'denied_contact_tag' };
  }

  if (normalizedTags.some((tag) => ALLOWED_TAGS.has(tag))) {
    return { eligible: true, reason: 'business_contact' };
  }

  return { eligible: false, reason: 'unclassified_contact' };
}

export const BOT_ALLOWED_TAGS = ALLOWED_TAGS;
export const BOT_DENIED_TAGS = DENIED_TAGS;
