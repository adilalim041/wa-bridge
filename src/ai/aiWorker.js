import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { KNOWLEDGE_BASE } from './knowledgeBase.js';

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = 'claude-sonnet-4-20250514';
const MAX_CONTEXT_MESSAGES = 20;
const REANALYZE_COOLDOWN_MS = 60 * 60 * 1000; // don't re-analyze same dialog within 1 hour

let workerTimer = null;
let initialRunTimer = null;

const SYSTEM_PROMPT = `Ты — AI-аналитик компании Omoikiri Kazakhstan (японская кухонная сантехника: мойки, смесители).
Ты анализируешь переписки менеджеров с клиентами в WhatsApp.

Проанализируй диалог и верни ТОЛЬКО JSON (без markdown, без пояснений):

{
  "intent": "одно из: price_inquiry, complaint, availability, measurement_request, delivery, consultation, collaboration, small_talk, spam, other",
  "lead_temperature": "одно из: hot, warm, cold, dead",
  "lead_source": "одно из: instagram_ad, google_ad, word_of_mouth, repeat_client, designer_partner, showroom_visit, incoming_call, unknown",
  "customer_type": "одно из: end_client, designer, partner, contractor, spam, unknown",
  "dialog_topic": "одно из: sink_sale, faucet_sale, complaint, service, consultation, partnership, other",
  "deal_stage": "одно из: first_contact, consultation, model_selection, price_negotiation, payment, delivery, completed, refused",
  "sentiment": "одно из: positive, neutral, negative, aggressive",
  "risk_flags": ["массив из возможных: client_unhappy, manager_rude, slow_response, potential_return, lost_lead"],
  "consultation_quality": {
    "score": 0-100,
    "questions_asked": ["какие ключевые вопросы менеджер задал из чек-листа"],
    "questions_missed": ["какие обязательные вопросы менеджер НЕ задал"],
    "upsell_offered": true/false
  },
  "followup_status": "одно из: not_needed, done, missed, pending",
  "manager_issues": ["массив из возможных: slow_first_response, no_followup, poor_consultation, no_photos, no_showroom_invite, no_upsell, rude_tone, formal_tone, no_alternative"],
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

Правила для customer_type:
- end_client = покупает для себя/своего дома
- designer = дизайнер интерьера, подбирает для клиента
- partner = представитель магазина, оптовик, дилер
- contractor = подрядчик, строитель
- spam = нерелевантное обращение, бот, реклама
- unknown = недостаточно данных для определения

Правила для consultation_quality:
- score 0-100 на основе чек-листа из стандартов продаж
- Для моек проверь 7 вопросов: размер тумбы, материал столешницы, тип монтажа, кол-во чаш, цвет/материал мойки, нужен ли смеситель, название модели
- Для смесителей проверь 4 вопроса: совместимость с мойкой, выдвижной излив, цвет/покрытие, бюджет
- upsell_offered = предложил ли менеджер сопутствующие товары (дозатор, диспоузер, корзина, разделочная доска)
- Если диалог не про продажу (small_talk, spam) — score = null

Правила для followup_status:
- done = менеджер сам написал повторно после паузы в диалоге
- missed = клиент проявил интерес, но менеджер НЕ написал повторно (прошло >24 часов)
- pending = диалог свежий, follow-up ещё не требуется
- not_needed = разговор активный, завершён сделкой или отказом

Правила для manager_issues (конкретные ошибки менеджера):
- slow_first_response = первый ответ менеджера дольше 2 часов
- no_followup = не написал повторно заинтересованному клиенту
- poor_consultation = не задал обязательные вопросы из чек-листа
- no_photos = клиент просил фото/каталог, менеджер не отправил
- no_showroom_invite = не предложил посетить шоурум
- no_upsell = не предложил сопутствующие товары
- rude_tone = грубый или неприветливый тон
- formal_tone = слишком сухой/формальный ответ без эмпатии
- no_alternative = сказал "нет в наличии" без предложения альтернативы
- manager_issues = пустой массив если менеджер работает хорошо

Используй справочную информацию ниже для точного определения моделей продукции и оценки соответствия стандартам продаж.
Отвечай ТОЛЬКО JSON, ничего больше.

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

async function getResponseTimeContext(sessionId, remoteJid) {
  try {
    const { data } = await supabase
      .from('manager_analytics')
      .select('response_time_seconds')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .not('response_time_seconds', 'is', null)
      .order('created_at', { ascending: false })
      .limit(20);

    if (!data?.length) return '';

    const times = data.map((r) => r.response_time_seconds);
    const avg = Math.round(times.reduce((a, b) => a + b, 0) / times.length);
    const max = Math.max(...times);

    const fmt = (s) => (s < 60 ? `${s}с` : s < 3600 ? `${Math.round(s / 60)}м` : `${(s / 3600).toFixed(1)}ч`);
    return `[Время ответа менеджера: среднее ${fmt(avg)}, самый медленный ${fmt(max)}]\n`;
  } catch {
    return '';
  }
}

function formatDialogForAI(messages, contactInfo, responseTimeCtx) {
  let context = '';

  if (contactInfo) {
    context += `[Контакт: ${contactInfo.first_name || 'Неизвестно'}`;
    if (contactInfo.role) context += `, роль: ${contactInfo.role}`;
    if (contactInfo.company) context += `, компания: ${contactInfo.company}`;
    if (contactInfo.city) context += `, город: ${contactInfo.city}`;
    context += ']\n';
  }

  if (responseTimeCtx) context += responseTimeCtx;
  context += '\n';

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

    const responseTimeCtx = await getResponseTimeContext(sessionId, remoteJid);
    const dialogText = formatDialogForAI(messages, contact, responseTimeCtx);
    const analysis = await callClaude(dialogText);

    if (!analysis) {
      return false;
    }

    const cq = analysis.consultation_quality || {};
    const baseRow = {
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
      // New fields (require migration)
      customer_type: analysis.customer_type || 'unknown',
      consultation_score: Number.isFinite(Number(cq.score)) ? Number(cq.score) : null,
      consultation_details: {
        questions_asked: Array.isArray(cq.questions_asked) ? cq.questions_asked : [],
        questions_missed: Array.isArray(cq.questions_missed) ? cq.questions_missed : [],
        upsell_offered: Boolean(cq.upsell_offered),
      },
      followup_status: analysis.followup_status || 'not_needed',
      manager_issues: Array.isArray(analysis.manager_issues) ? analysis.manager_issues : [],
    };

    let { error: saveError } = await supabase
      .from('chat_ai')
      .upsert(baseRow, { onConflict: 'dialog_session_id' });

    // Fallback: if new columns don't exist yet, save without them
    if (saveError && saveError.message?.includes('column')) {
      const { customer_type, consultation_score, consultation_details, followup_status, manager_issues, ...fallbackRow } = baseRow;
      const fallbackResult = await supabase
        .from('chat_ai')
        .upsert(fallbackRow, { onConflict: 'dialog_session_id' });
      saveError = fallbackResult.error;
      if (!saveError) {
        logger.warn('Saved AI analysis without new columns — run migration to enable extended fields');
      }
    }

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
    // Unstick items stuck in 'processing' for more than 5 minutes
    const stuckCutoff = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { data: stuckItems } = await supabase
      .from('ai_queue')
      .update({ status: 'pending' })
      .eq('status', 'processing')
      .lt('created_at', stuckCutoff)
      .select('dialog_session_id');

    if (stuckItems?.length) {
      logger.warn({ count: stuckItems.length }, 'Unstuck AI queue items back to pending');
    }

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
      // Skip if already analyzed recently (cooldown)
      const { data: existingAnalysis } = await supabase
        .from('chat_ai')
        .select('analyzed_at')
        .eq('dialog_session_id', item.dialog_session_id)
        .maybeSingle();

      if (existingAnalysis?.analyzed_at) {
        const sinceLastAnalysis = Date.now() - new Date(existingAnalysis.analyzed_at).getTime();
        if (sinceLastAnalysis < REANALYZE_COOLDOWN_MS) {
          // Mark as done — already analyzed recently, skip
          await supabase
            .from('ai_queue')
            .update({ status: 'done', processed_at: new Date().toISOString() })
            .eq('dialog_session_id', item.dialog_session_id)
            .eq('status', 'pending');
          continue;
        }
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

// Run analysis on-demand (triggered by API call or schedule)
let isRunning = false;

export async function runAnalysisNow() {
  if (!ANTHROPIC_API_KEY) {
    logger.warn('ANTHROPIC_API_KEY not set — AI analysis disabled');
    return { success: false, error: 'API key not configured' };
  }

  if (isRunning) {
    return { success: false, error: 'Analysis already in progress' };
  }

  isRunning = true;
  const startTime = Date.now();
  let processed = 0;
  let failed = 0;

  try {
    // Process all pending items in queue (not just one batch)
    let hasMore = true;
    while (hasMore) {
      const beforeCount = processed + failed;
      await processQueue();

      // Count what was processed this round
      const { count } = await supabase
        .from('ai_queue')
        .select('*', { count: 'exact', head: true })
        .eq('status', 'pending');

      hasMore = (count || 0) > 0;

      // Safety: count total processed
      const { count: doneCount } = await supabase
        .from('ai_queue')
        .select('*', { count: 'exact', head: true })
        .eq('status', 'done')
        .gte('processed_at', new Date(startTime).toISOString());

      const { count: failCount } = await supabase
        .from('ai_queue')
        .select('*', { count: 'exact', head: true })
        .eq('status', 'failed')
        .gte('processed_at', new Date(startTime).toISOString());

      processed = doneCount || 0;
      failed = failCount || 0;

      // Safety limit — max 100 analyses per run
      if (processed + failed >= 100) {
        logger.warn('AI analysis hit safety limit of 100 per run');
        break;
      }
    }
  } catch (error) {
    logger.error({ err: error }, 'Error during on-demand AI analysis');
  } finally {
    isRunning = false;
  }

  const durationSec = Math.round((Date.now() - startTime) / 1000);
  logger.info({ processed, failed, durationSec }, 'AI analysis run complete');

  return { success: true, processed, failed, durationSec };
}

export function isAnalysisRunning() {
  return isRunning;
}

// Kept for backward compat — now a no-op (analysis is on-demand)
export function startAIWorker() {
  if (!ANTHROPIC_API_KEY) {
    logger.warn('ANTHROPIC_API_KEY not set — AI worker disabled');
    return;
  }
  logger.info('AI worker ready (on-demand mode — call POST /ai/analyze to run)');
}

export function stopAIWorker() {
  // nothing to stop — no timers
}
