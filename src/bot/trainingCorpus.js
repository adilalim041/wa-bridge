const BUSINESS_TAGS = new Set(['клиент', 'партнёр']);
const BLOCKED_TAGS = new Set(['сотрудник', 'коллега', 'личное', 'спам', 'неизвестно']);
const SUPPORTED_CUSTOMER_TYPES = new Set(['end_client', 'partner']);

function normalizeTags(tags) {
  return (Array.isArray(tags) ? tags : [])
    .map((tag) => String(tag || '').trim().toLowerCase())
    .filter(Boolean);
}

export function isEligibleBusinessContact(tags) {
  const normalized = normalizeTags(tags);
  if (normalized.some((tag) => BLOCKED_TAGS.has(tag))) return false;
  return normalized.some((tag) => BUSINESS_TAGS.has(tag));
}

export function redactTrainingText(value) {
  let text = String(value || '').trim();
  if (!text) return '';

  text = text
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, '[email]')
    .replace(/\b(?:https?:\/\/|www\.)\S+/gi, '[url]')
    .replace(/(?:\+?\d[\d\s().-]{8,}\d)/g, (match) => {
      const digits = match.replace(/\D/g, '');
      return digits.length >= 10 ? '[phone]' : match;
    })
    .replace(/\b\d{12,15}\b/g, '[id]');

  return text.replace(/\s+/g, ' ').trim();
}

function messageRole(message) {
  return message?.from_me ? 'assistant' : 'user';
}

function isUsableMessage(message) {
  const type = String(message?.message_type || '').toLowerCase();
  if (['protocol', 'reaction', 'revoked'].includes(type)) return false;
  return Boolean(redactTrainingText(message?.body));
}

export function buildConversationTurns(messages) {
  const ordered = [...(Array.isArray(messages) ? messages : [])]
    .filter(isUsableMessage)
    .sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));

  const turns = [];
  for (const message of ordered) {
    const role = messageRole(message);
    const content = redactTrainingText(message.body);
    if (!content) continue;

    const previous = turns.at(-1);
    if (previous?.role === role) {
      previous.content = `${previous.content}\n${content}`;
    } else {
      turns.push({ role, content });
    }
  }

  while (turns[0]?.role === 'assistant') turns.shift();
  while (turns.at(-1)?.role === 'user') turns.pop();

  for (const turn of turns) {
    turn.content = redactTrainingText(turn.content);
  }

  return turns.length >= 2 ? turns : [];
}

export function isEligibleAnalysis(analysis) {
  if (!SUPPORTED_CUSTOMER_TYPES.has(String(analysis?.customer_type || ''))) return false;
  if ((analysis?.manager_issues || []).length > 0) return false;
  if ((analysis?.risk_flags || []).length > 0) return false;

  const score = Number(analysis?.consultation_score);
  return Number.isFinite(score) && score >= 7;
}

export function buildTrainingExample({ analysis, tags, messages }) {
  if (!isEligibleBusinessContact(tags)) return null;
  if (!isEligibleAnalysis(analysis)) return null;

  const turns = buildConversationTurns(messages);
  if (!turns.length) return null;

  return {
    schema: 'omoikiri-wa-training/v1',
    metadata: {
      contact_type: analysis.customer_type,
      intent: String(analysis.intent || 'unknown'),
      lead_source: String(analysis.lead_source || 'unknown'),
      deal_stage: String(analysis.deal_stage || 'unknown'),
      consultation_score: Number(analysis.consultation_score),
      message_count: turns.length,
    },
    turns,
  };
}
