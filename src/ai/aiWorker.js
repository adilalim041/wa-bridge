import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { KNOWLEDGE_BASE } from './knowledgeBase.js';

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = 'claude-sonnet-4-20250514';
const BATCH_INTERVAL_MS = 30 * 1000;
const MAX_CONTEXT_MESSAGES = 20;
const SESSION_IDLE_MS = 10 * 1000;

let workerTimer = null;
let initialRunTimer = null;

const SYSTEM_PROMPT = `Ты — AI-аналитик компании Omoikiri Kazakhstan (японская кухонная сантехника: мойки, смесители).
Ты анализируешь переписки менеджеров с клиентами в WhatsApp.

Проанализируй диалог и верни ТОЛЬКО JSON (без markdown, без пояснений):

{
  "intent": "одно из: price_inquiry, complaint, availability, measurement_request, delivery, consultation, collaboration, small_talk, spam, other",
  "lead_temperature": "одно из: hot, warm, cold, dead",
  "lead_source": "одно из: instagram_ad, google_ad, word_of_mouth, repeat_client, designer_partner, showroom_visit, incoming_call, unknown",
  "dialog_topic": "одно из: sink_sale, faucet_sale, complaint, service, consultation, partnership, other",
  "deal_stage": "одно из: first_contact, consultation, model_selection, price_negotiation, payment, delivery, completed, refused",
  "sentiment": "одно из: positive, neutral, negative, aggressive",
  "risk_flags": ["массив из возможных: client_unhappy, manager_rude, slow_response, potential_return, lost_lead"],
  "summary_ru": "Краткое резюме на русском, 2-3 предложения максимум",
  "action_required": true/false,
  "action_suggestion": "Что менеджеру стоит сделать дальше (на русском), или null",
  "confidence": 0.0-1.0
}

Правила:
- hot = клиент спрашивает цену, наличие, готов покупать
- warm = интересуется, задаёт вопросы, но не решился
- cold = просто спросил и пропал, или small talk
- dead = отказался, не отвечает давно
- lead_source определяй по контексту первых сообщений (если упоминают Instagram/рекламу/шоурум)
- risk_flags = пустой массив если всё нормально
- Используй справочную информацию ниже для точного определения моделей продукции и оценки соответствия стандартам продаж
- Отвечай ТОЛЬКО JSON, ничего больше

${KNOWLEDGE_BASE}`;

async function callClaude(messages) {
  if (!ANTHROPIC_API_KEY) {
    logger.warn('ANTHROPIC_API_KEY not set, skipping AI analysis');
    return null;
  }

  try {
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: AI_MODEL,
        max_tokens: 1024,
        system: SYSTEM_PROMPT,
        messages: [{ role: 'user', content: messages }],
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      logger.error({ status: response.status, body: errorText }, 'Claude API error');
      return null;
    }

    const data = await response.json();
    const text = data.content?.[0]?.text || '';
    const clean = text.replace(/```json\s*|```\s*/g, '').trim();
    return JSON.parse(clean);
  } catch (error) {
    logger.error({ err: error }, 'Failed to call Claude API');
    return null;
  }
}

function formatDialogForAI(messages, contactInfo) {
  let context = '';

  if (contactInfo) {
    context += `[Контакт: ${contactInfo.first_name || 'Неизвестно'}`;
    if (contactInfo.role) context += `, роль: ${contactInfo.role}`;
    if (contactInfo.company) context += `, компания: ${contactInfo.company}`;
    if (contactInfo.city) context += `, город: ${contactInfo.city}`;
    context += ']\n\n';
  }

  for (const msg of messages) {
    const time = new Date(msg.timestamp).toLocaleString('ru-RU', {
      day: '2-digit',
      month: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
    });
    const sender = msg.from_me ? 'МЕНЕДЖЕР' : 'КЛИЕНТ';
    const body = msg.body || `[${msg.message_type || 'медиа'}]`;
    context += `[${time}] ${sender}: ${body}\n`;
  }

  return context;
}

async function processDialogSession(dialogSessionId, sessionId, remoteJid) {
  try {
    const { data: messageRows, error: msgError } = await supabase
      .from('messages')
      .select('body, from_me, timestamp, message_type, push_name')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .eq('dialog_session_id', dialogSessionId)
      .order('timestamp', { ascending: false })
      .limit(MAX_CONTEXT_MESSAGES);

    if (msgError || !messageRows?.length) {
      logger.error({ err: msgError, dialogSessionId }, 'Failed to fetch messages for AI');
      return false;
    }

    const messages = [...messageRows].reverse();

    const { data: contact } = await supabase
      .from('contacts_crm')
      .select('first_name, last_name, role, company, city')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .maybeSingle();

    const dialogText = formatDialogForAI(messages, contact);
    const analysis = await callClaude(dialogText);

    if (!analysis) {
      return false;
    }

    const { error: saveError } = await supabase
      .from('chat_ai')
      .upsert(
        {
          dialog_session_id: dialogSessionId,
          session_id: sessionId,
          remote_jid: remoteJid,
          intent: analysis.intent || 'other',
          lead_temperature: analysis.lead_temperature || 'cold',
          lead_source: analysis.lead_source || 'unknown',
          dialog_topic: analysis.dialog_topic || 'other',
          deal_stage: analysis.deal_stage || 'first_contact',
          sentiment: analysis.sentiment || 'neutral',
          risk_flags: Array.isArray(analysis.risk_flags) ? analysis.risk_flags : [],
          summary_ru: analysis.summary_ru || null,
          action_required: Boolean(analysis.action_required),
          action_suggestion: analysis.action_suggestion || null,
          confidence: Number.isFinite(Number(analysis.confidence)) ? Number(analysis.confidence) : 0,
          analyzed_at: new Date().toISOString(),
          message_count_analyzed: messages.length,
        },
        { onConflict: 'dialog_session_id' }
      );

    if (saveError) {
      logger.error({ err: saveError, dialogSessionId }, 'Failed to save AI analysis');
      return false;
    }

    logger.info(
      {
        dialogSessionId,
        intent: analysis.intent,
        temperature: analysis.lead_temperature,
      },
      'AI analysis complete'
    );
    return true;
  } catch (error) {
    logger.error({ err: error, dialogSessionId }, 'Unexpected error in AI processing');
    return false;
  }
}

async function processQueue() {
  try {
    const { data: pending, error } = await supabase
      .from('ai_queue')
      .select('id, dialog_session_id, session_id, remote_jid, created_at')
      .eq('status', 'pending')
      .order('created_at', { ascending: true })
      .limit(50);

    if (error || !pending?.length) {
      if (error) {
        logger.error({ err: error }, 'Failed to fetch AI queue');
      }
      return;
    }

    const latestBySession = new Map();
    for (const item of pending) {
      if (!item.dialog_session_id) {
        continue;
      }

      const existing = latestBySession.get(item.dialog_session_id);
      if (!existing || new Date(item.created_at) > new Date(existing.created_at)) {
        latestBySession.set(item.dialog_session_id, item);
      }
    }

    for (const item of latestBySession.values()) {
      const idleFor = Date.now() - new Date(item.created_at).getTime();
      if (idleFor < SESSION_IDLE_MS) {
        continue;
      }

      await supabase
        .from('ai_queue')
        .update({ status: 'processing' })
        .eq('dialog_session_id', item.dialog_session_id)
        .eq('status', 'pending');

      const success = await processDialogSession(
        item.dialog_session_id,
        item.session_id,
        item.remote_jid
      );

      await supabase
        .from('ai_queue')
        .update({
          status: success ? 'done' : 'failed',
          processed_at: new Date().toISOString(),
        })
        .eq('dialog_session_id', item.dialog_session_id)
        .eq('status', 'processing');

      if (success) {
        await supabase
          .from('messages')
          .update({ ai_processed: true })
          .eq('dialog_session_id', item.dialog_session_id)
          .eq('ai_processed', false);
      }
    }
  } catch (error) {
    logger.error({ err: error }, 'Error processing AI queue');
  }
}

export function startAIWorker() {
  if (!ANTHROPIC_API_KEY) {
    logger.warn('ANTHROPIC_API_KEY not set — AI worker disabled');
    return;
  }

  if (workerTimer || initialRunTimer) {
    logger.warn('AI worker already running');
    return;
  }

  logger.info({ interval: BATCH_INTERVAL_MS }, 'Starting AI worker');

  initialRunTimer = setTimeout(() => {
    initialRunTimer = null;
    processQueue().catch((error) => {
      logger.error({ err: error }, 'Initial AI worker run failed');
    });
  }, 10000);

  workerTimer = setInterval(() => {
    processQueue().catch((error) => {
      logger.error({ err: error }, 'Scheduled AI worker run failed');
    });
  }, BATCH_INTERVAL_MS);
}

export function stopAIWorker() {
  if (initialRunTimer) {
    clearTimeout(initialRunTimer);
    initialRunTimer = null;
  }

  if (workerTimer) {
    clearInterval(workerTimer);
    workerTimer = null;
  }
}
