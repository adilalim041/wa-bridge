/**
 * coachingGenerator.js
 *
 * Builds the "Резюме диалога" string shown to the manager in the PDF and the
 * preview modal. Adil's explicit ask: do NOT call Claude here — just surface
 * the same factual text the dashboard already shows in the dialog card
 * (chat_ai.summary_ru + action_suggestion). Deterministic, instant, no failure
 * mode where the textarea ends up empty.
 */

const FALLBACK_COMMENT =
  'Анализ диалога ещё не готов. Откройте карточку диалога и дополните резюме вручную перед отправкой.';

/**
 * @param {object} params
 * @param {object} params.chatAi  Full chat_ai record.
 * @param {Array}  [params.messages]  Unused — kept for API compatibility.
 * @param {string} [params.clientName]  Unused — kept for API compatibility.
 * @returns {Promise<string>} Russian summary, 1–4 sentences. Never throws.
 */
export async function generateCoachingComment({ chatAi } = {}) {
  const summary = (chatAi?.summary_ru ?? '').toString().trim();
  const action = (
    chatAi?.action_suggestion ??
    chatAi?.actionSuggestion ??
    ''
  ).toString().trim();

  const parts = [];
  if (summary) parts.push(summary);
  if (action) parts.push(`Следующий шаг: ${action}`);

  if (parts.length === 0) return FALLBACK_COMMENT;
  return parts.join(' ');
}
