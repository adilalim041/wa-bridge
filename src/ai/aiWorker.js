import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { KNOWLEDGE_BASE } from './knowledgeBase.js';

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const AI_MODEL = 'claude-sonnet-4-20250514';
const MAX_CONTEXT_MESSAGES = 30;  // messages from target day
const PREV_DAY_CONTEXT = 10;      // extra messages from before target day for context
const DAILY_ANALYSIS_HOUR = Number(process.env.DAILY_ANALYSIS_HOUR || 23); // 23:00 default

let dailyTimer = null;
let isRunning = false;

const SYSTEM_PROMPT = `Ты — AI-аналитик компании Omoikiri Kazakhstan (японская кухонная сантехника: мойки, смесители).
Ты анализируешь переписки менеджеров с клиентами в WhatsApp.

Проанализируй диалог и верни ТОЛЬКО JSON (без markdown, без пояснений):

{
  "intent": "одно из: price_inquiry, complaint, availability, measurement_request, delivery, consultation, collaboration, small_talk, spam, other",
  "lead_temperature": "одно из: hot, warm, cold, dead",
  "lead_source": "одно из: instagram_ad, google_ad, word_of_mouth, repeat_client, designer_partner, showroom_visit, incoming_call, unknown",
  "customer_type": "одно из: end_client, designer, partner, contractor, colleague, personal, spam, unknown",
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

ВАЖНО — Рабочие часы менеджера: 10:00–20:00 по времени Алматы (UTC+5).
- Сообщения от клиентов вне рабочих часов (до 10:00 или после 20:00) НЕ считаются как медленный ответ, если менеджер ответил на следующий рабочий день до 11:00.
- slow_first_response ставь ТОЛЬКО если менеджер не ответил в течение 2 часов ВНУТРИ рабочего времени.
- Если клиент написал в 23:00 а менеджер ответил в 10:15 — это НОРМАЛЬНО, НЕ slow_first_response.
- Если клиент написал в 14:00 а менеджер ответил в 17:00 — это slow_first_response (3 часа в рабочее время).

Правила для customer_type:
- end_client = покупает для себя/своего дома
- designer = дизайнер интерьера, подбирает для клиента
- partner = представитель магазина, оптовик, дилер
- contractor = подрядчик, строитель
- colleague = коллега, сотрудник компании, внутренняя переписка
- personal = личная переписка, друзья, родственники, не по работе
- spam = нерелевантное обращение, бот, реклама
- unknown = недостаточно данных для определения (слишком мало сообщений)

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

  const MAX_RETRIES = 2;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
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
        const status = response.status;
        const errorText = await response.text();

        // Retry on rate limit (429) or server overload (529/503)
        if ((status === 429 || status === 529 || status === 503) && attempt < MAX_RETRIES) {
          const retryAfter = parseInt(response.headers.get('retry-after') || '0', 10);
          const delay = Math.max(retryAfter * 1000, 3000 * (attempt + 1));
          logger.warn({ status, attempt, delay }, `Claude API ${status} — retrying in ${delay}ms`);
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }

        logger.error({ status, body: errorText }, 'Claude API error');
        return null;
      }

      const data = await response.json();
      const text = data.content?.[0]?.text || '';
      const clean = text.replace(/```json\s*|```\s*/g, '').trim();
      return JSON.parse(clean);
    } catch (error) {
      if (attempt < MAX_RETRIES) {
        logger.warn({ err: error, attempt }, 'Claude API network error — retrying');
        await new Promise((r) => setTimeout(r, 3000 * (attempt + 1)));
        continue;
      }
      logger.error({ err: error }, 'Failed to call Claude API after retries');
      return null;
    }
  }

  return null;
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

function todayDateStr() {
  // Almaty timezone: UTC+5
  const now = new Date(Date.now() + 5 * 60 * 60 * 1000);
  return now.toISOString().slice(0, 10);
}

async function analyzeDialogForDate(dialogSessionId, sessionId, remoteJid, analysisDate) {
  try {
    const dayStart = `${analysisDate}T00:00:00+05:00`;
    const dayEnd = `${analysisDate}T23:59:59+05:00`;

    // 1. Get messages FROM the target day
    const { data: dayMessages, error: dayErr } = await supabase
      .from('messages')
      .select('body, from_me, timestamp, message_type, push_name')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .gte('timestamp', dayStart)
      .lte('timestamp', dayEnd)
      .order('timestamp', { ascending: true })
      .limit(MAX_CONTEXT_MESSAGES);

    // 2. Get earlier messages for context (before target day)
    const { data: prevMessages } = await supabase
      .from('messages')
      .select('body, from_me, timestamp, message_type, push_name')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .lt('timestamp', dayStart)
      .order('timestamp', { ascending: false })
      .limit(PREV_DAY_CONTEXT);

    if (dayErr || !dayMessages?.length) {
      return false;
    }

    // Combine: previous context (chronological) + today's messages
    const contextMsgs = prevMessages ? [...prevMessages].reverse() : [];
    const messages = [...contextMsgs, ...dayMessages];

    const { data: contact } = await supabase
      .from('contacts_crm')
      .select('first_name, last_name, role, company, city')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .maybeSingle();

    const responseTimeCtx = await getResponseTimeContext(sessionId, remoteJid);
    const contextNote = contextMsgs.length > 0
      ? `[Контекст: ${contextMsgs.length} сообщений из предыдущих дней для понимания истории диалога. Анализируй в первую очередь сообщения за ${analysisDate}.]\n`
      : '';
    const dialogText = contextNote + formatDialogForAI(messages, contact, responseTimeCtx);
    const analysis = await callClaude(dialogText);

    if (!analysis) {
      return false;
    }

    const cq = analysis.consultation_quality || {};
    const row = {
      dialog_session_id: dialogSessionId,
      session_id: sessionId,
      remote_jid: remoteJid,
      analysis_date: analysisDate,
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
      message_count_analyzed: dayMessages.length,
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

    const { error: saveError } = await supabase
      .from('chat_ai')
      .upsert(row, { onConflict: 'dialog_session_id,analysis_date' });

    if (saveError) {
      logger.error({ err: saveError, dialogSessionId }, 'Failed to save AI analysis');
      return false;
    }

    // Auto-tag chat based on AI analysis
    await applyAutoTag(sessionId, remoteJid, row.customer_type);

    return true;
  } catch (error) {
    logger.error({ err: error, dialogSessionId }, 'Unexpected error in AI processing');
    return false;
  }
}

// Map AI customer_type → tag name (Russian)
const CUSTOMER_TYPE_TAG = {
  end_client: 'клиент',
  designer: 'дизайнер',
  partner: 'партнёр',
  contractor: 'подрядчик',
  colleague: 'коллега',
  personal: 'личное',
  spam: 'спам',
  unknown: 'неизвестно',
};

const AI_AUTO_TAGS = new Set(Object.values(CUSTOMER_TYPE_TAG));

async function applyAutoTag(sessionId, remoteJid, customerType) {
  const newTag = CUSTOMER_TYPE_TAG[customerType];
  if (!newTag) return;

  try {
    // Fetch current tags
    const { data: chat } = await supabase
      .from('chats')
      .select('tags')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .maybeSingle();

    const existingTags = Array.isArray(chat?.tags) ? chat.tags : [];

    // Remove any previous AI auto-tags, then add the new one
    const manualTags = existingTags.filter((t) => !AI_AUTO_TAGS.has(t));
    const merged = [...new Set([...manualTags, newTag])].slice(0, 10);

    await supabase
      .from('chats')
      .upsert(
        { session_id: sessionId, remote_jid: remoteJid, tags: merged, updated_at: new Date().toISOString() },
        { onConflict: 'session_id,remote_jid' }
      );
  } catch (err) {
    logger.warn({ err, sessionId, remoteJid }, 'Failed to apply auto-tag');
  }
}

const REANALYSIS_MESSAGE_THRESHOLD = 10;

// Re-analyze dialogs that were tagged "unknown" once enough new messages arrive
async function findUnknownDialogsForReanalysis(analysisDate) {
  try {
    // Get dialogs last analyzed as "unknown"
    const { data: unknowns } = await supabase
      .from('chat_ai')
      .select('dialog_session_id, session_id, remote_jid, message_count_analyzed')
      .eq('customer_type', 'unknown')
      .order('analyzed_at', { ascending: false })
      .limit(200);

    if (!unknowns?.length) return [];

    const toReanalyze = [];

    for (const row of unknowns) {
      // Count current messages in the dialog session
      const { count } = await supabase
        .from('messages')
        .select('message_id', { count: 'exact', head: true })
        .eq('session_id', row.session_id)
        .eq('remote_jid', row.remote_jid)
        .gte('timestamp', `${analysisDate}T00:00:00+05:00`)
        .lte('timestamp', `${analysisDate}T23:59:59+05:00`);

      const totalNow = count || 0;
      const prevAnalyzed = row.message_count_analyzed || 0;

      if (totalNow >= prevAnalyzed + REANALYSIS_MESSAGE_THRESHOLD) {
        toReanalyze.push(row);
      }
    }

    return toReanalyze;
  } catch (err) {
    logger.warn({ err }, 'Failed to find unknown dialogs for re-analysis');
    return [];
  }
}

// Find all dialog sessions with activity on a given date and analyze them
export async function runDailyAnalysis(date) {
  if (!ANTHROPIC_API_KEY) {
    return { success: false, error: 'API key not configured' };
  }

  if (isRunning) {
    return { success: false, error: 'Analysis already in progress', running: true };
  }

  isRunning = true;
  const analysisDate = date || todayDateStr();
  const startTime = Date.now();
  let processed = 0;
  let failed = 0;
  let skipped = 0;

  try {
    const dayStart = `${analysisDate}T00:00:00+05:00`;
    const dayEnd = `${analysisDate}T23:59:59+05:00`;

    // Find dialog sessions that had messages on this date
    const { data: activeSessions, error } = await supabase
      .from('dialog_sessions')
      .select('id, session_id, remote_jid, message_count')
      .gte('last_message_at', dayStart)
      .lte('started_at', dayEnd)
      .order('last_message_at', { ascending: false });

    if (error) {
      logger.error({ err: error }, 'Failed to fetch active dialog sessions');
      isRunning = false;
      return { success: false, error: error.message };
    }

    if (!activeSessions?.length) {
      isRunning = false;
      return { success: true, processed: 0, failed: 0, skipped: 0, date: analysisDate, message: 'No dialogs found for this date' };
    }

    // Skip dialogs with only 1 message (just "Привет" etc)
    const meaningful = activeSessions.filter((s) => (s.message_count || 0) >= 2);
    skipped = activeSessions.length - meaningful.length;

    // Check which ones already have analysis for this date
    const dialogIds = meaningful.map((s) => s.id);
    const { data: existingAnalyses } = await supabase
      .from('chat_ai')
      .select('dialog_session_id')
      .in('dialog_session_id', dialogIds)
      .eq('analysis_date', analysisDate);

    const alreadyDone = new Set((existingAnalyses || []).map((a) => a.dialog_session_id));

    for (const session of meaningful) {
      if (alreadyDone.has(session.id)) {
        skipped++;
        continue;
      }

      // Safety limit — 500 per run (enough for 8 sessions × ~60 dialogs each)
      if (processed + failed >= 500) {
        logger.warn('Daily analysis hit safety limit of 500');
        break;
      }

      const success = await analyzeDialogForDate(
        session.id,
        session.session_id,
        session.remote_jid,
        analysisDate
      );

      if (success) {
        processed++;
      } else {
        failed++;
      }
    }

    // Re-analyze "unknown" customer_type dialogs that now have 10+ new messages
    const unknowns = await findUnknownDialogsForReanalysis(analysisDate);
    let reanalyzed = 0;
    for (const row of unknowns) {
      if (processed + failed + reanalyzed >= 500) break;
      const ok = await analyzeDialogForDate(row.dialog_session_id, row.session_id, row.remote_jid, analysisDate);
      if (ok) reanalyzed++;
    }
    if (reanalyzed > 0) {
      logger.info({ reanalyzed }, 'Re-analyzed previously unknown dialogs');
    }
  } catch (error) {
    logger.error({ err: error }, 'Error during daily analysis');
  } finally {
    isRunning = false;
  }

  const durationSec = Math.round((Date.now() - startTime) / 1000);
  logger.info({ processed, failed, skipped, durationSec, date: analysisDate }, 'Daily AI analysis complete');

  return { success: true, processed, failed, skipped, date: analysisDate, durationSec };
}

export function isAnalysisRunning() {
  return isRunning;
}

// Backfill auto-tags for all existing analyses that don't have tags yet
export async function backfillAutoTags() {
  try {
    // Get all unique (session_id, remote_jid, customer_type) from latest analyses
    const { data: analyses, error } = await supabase
      .from('chat_ai')
      .select('session_id, remote_jid, customer_type')
      .not('customer_type', 'is', null)
      .order('analyzed_at', { ascending: false });

    if (error || !analyses?.length) {
      return { success: false, error: error?.message || 'No analyses found', tagged: 0 };
    }

    // Deduplicate — keep only the latest analysis per (session_id, remote_jid)
    const seen = new Set();
    const unique = [];
    for (const row of analyses) {
      const key = `${row.session_id}::${row.remote_jid}`;
      if (!seen.has(key)) {
        seen.add(key);
        unique.push(row);
      }
    }

    let tagged = 0;
    for (const row of unique) {
      await applyAutoTag(row.session_id, row.remote_jid, row.customer_type);
      tagged++;
    }

    logger.info({ tagged }, 'Backfill auto-tags complete');
    return { success: true, tagged };
  } catch (err) {
    logger.error({ err }, 'Backfill auto-tags failed');
    return { success: false, error: err.message, tagged: 0 };
  }
}

// Lightweight: classify untagged chats by reading their messages directly
const CLASSIFY_PROMPT = `Определи тип собеседника по переписке. Верни ТОЛЬКО JSON:
{"customer_type": "одно из: end_client, designer, partner, contractor, colleague, personal, spam, unknown"}

Правила:
- end_client = покупает для себя/своего дома
- designer = дизайнер интерьера, подбирает для клиента
- partner = представитель магазина, оптовик, дилер
- contractor = подрядчик, строитель
- colleague = коллега, сотрудник компании
- personal = личная переписка, друзья, родственники
- spam = нерелевантное, бот, реклама
- unknown = недостаточно данных

Отвечай ТОЛЬКО JSON.`;

export async function classifyUntaggedChats() {
  if (!ANTHROPIC_API_KEY) return { success: false, error: 'No API key', classified: 0 };

  try {
    // Find chats with no AI auto-tags
    const { data: chats } = await supabase
      .from('chats')
      .select('session_id, remote_jid, tags')
      .order('updated_at', { ascending: false })
      .limit(500);

    if (!chats?.length) return { success: true, classified: 0 };

    const untagged = chats.filter((c) => {
      const tags = Array.isArray(c.tags) ? c.tags : [];
      return !tags.some((t) => AI_AUTO_TAGS.has(t));
    });

    if (!untagged.length) return { success: true, classified: 0, message: 'All chats already tagged' };

    let classified = 0;
    let failed = 0;

    for (const chat of untagged) {
      // Get last 15 messages for classification
      const { data: msgs } = await supabase
        .from('messages')
        .select('body, from_me, timestamp, message_type')
        .eq('session_id', chat.session_id)
        .eq('remote_jid', chat.remote_jid)
        .order('timestamp', { ascending: false })
        .limit(15);

      if (!msgs?.length || msgs.length < 2) continue;

      const reversed = [...msgs].reverse();
      let text = '';
      for (const m of reversed) {
        const sender = m.from_me ? 'МЕНЕДЖЕР' : 'КЛИЕНТ';
        text += `${sender}: ${m.body || `[${m.message_type || 'медиа'}]`}\n`;
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
            max_tokens: 100,
            system: CLASSIFY_PROMPT,
            messages: [{ role: 'user', content: text }],
          }),
        });

        if (!response.ok) {
          failed++;
          if (response.status === 429) {
            await new Promise((r) => setTimeout(r, 5000));
          }
          continue;
        }

        const data = await response.json();
        const raw = (data.content?.[0]?.text || '').replace(/```json\s*|```\s*/g, '').trim();
        const result = JSON.parse(raw);

        if (result.customer_type && CUSTOMER_TYPE_TAG[result.customer_type]) {
          await applyAutoTag(chat.session_id, chat.remote_jid, result.customer_type);
          classified++;
        }
      } catch {
        failed++;
      }
    }

    logger.info({ classified, failed, total: untagged.length }, 'Classify untagged chats complete');
    return { success: true, classified, failed, total: untagged.length };
  } catch (err) {
    logger.error({ err }, 'classifyUntaggedChats failed');
    return { success: false, error: err.message, classified: 0 };
  }
}

// Schedule daily analysis at configured hour (Almaty time)
function scheduleDailyRun() {
  const now = new Date(Date.now() + 5 * 60 * 60 * 1000); // Almaty UTC+5
  const nextRun = new Date(now);
  nextRun.setUTCHours(DAILY_ANALYSIS_HOUR - 5, 0, 0, 0); // Convert Almaty hour to UTC

  if (nextRun <= new Date()) {
    // Already past today's run time — schedule for tomorrow
    nextRun.setDate(nextRun.getDate() + 1);
  }

  const msUntilRun = nextRun.getTime() - Date.now();
  const hoursUntil = (msUntilRun / 3600000).toFixed(1);

  logger.info({ nextRun: nextRun.toISOString(), hoursUntil }, `Daily analysis scheduled at ${DAILY_ANALYSIS_HOUR}:00 Almaty time`);

  dailyTimer = setTimeout(async () => {
    logger.info('Running scheduled daily analysis');
    try {
      const result = await runDailyAnalysis();
      logger.info(result, 'Scheduled daily analysis finished');
    } catch (error) {
      logger.error({ err: error }, 'Scheduled daily analysis failed');
    }
    // Schedule next day
    scheduleDailyRun();
  }, msUntilRun);
}

export function startAIWorker() {
  if (!ANTHROPIC_API_KEY) {
    logger.warn('ANTHROPIC_API_KEY not set — AI worker disabled');
    return;
  }
  scheduleDailyRun();
  logger.info('AI worker ready (daily auto + on-demand via POST /ai/analyze)');
}

export function stopAIWorker() {
  if (dailyTimer) {
    clearTimeout(dailyTimer);
    dailyTimer = null;
  }
}
