/**
 * Daily-run orchestrator — sandbox-safe замена прямых curl-запросов из
 * Claude Code SKILL.md в Supabase REST API.
 *
 * Все шаги ежедневного анализа теперь идут через Bridge:
 *   1. autoDismissResolved   — закрыть проблемы где менеджер уже ответил
 *   2. getRecentFeedback     — последние 50 chat_ai_feedback (guidelines для Claude)
 *   3. getPendingDialogs     — непроанализированные dialog_sessions
 *   4. saveAnalysis          — batch upsert в chat_ai
 *   5. getStuckDeals         — горячие/тёплые лиды, не двигались N дней
 *   6. composeDigest         — текстовая сводка для Telegram + JSON
 *
 * SECURITY: все админские, mounted под /admin/* (x-api-key only) — никогда
 * не светят service_role в локальный транскрипт Claude Code.
 *
 * Использует service-role supabase client напрямую (admin-path), RLS bypass
 * корректен — Adil единственный авторизованный consumer этого pipeline.
 */

import { supabase } from '../storage/supabase.js';
import { sendTelegramMessage, isTelegramConfigured } from '../notifications/telegramBot.js';
import { applyAutoTag } from '../ai/aiWorker.js';

// ───────────────────────────────────────────────────────────────────────────
// 1. autoDismissResolved
// ───────────────────────────────────────────────────────────────────────────
//
// Идея: если в карусели висит проблема (manager_issues / risk_flags) старше
// 24h и менеджер уже отвечал клиенту ПОСЛЕ analyzed_at — проблема ушла сама.
// Закрываем как `won`. Иначе оставляем + возвращаем как «висит».
export async function autoDismissResolved() {
  const cutoff = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  const { data: problems, error: fetchErr } = await supabase
    .from('chat_ai')
    .select('id, session_id, remote_jid, analyzed_at, manager_issues, risk_flags')
    .is('problem_dismissed_at', null)
    .lt('analyzed_at', cutoff)
    .or('manager_issues.cs.{slow_first_response},manager_issues.cs.{no_followup},risk_flags.cs.{lost_lead},risk_flags.cs.{client_unhappy}')
    .limit(200);
  if (fetchErr) throw new Error(`autoDismissResolved fetch: ${fetchErr.message}`);

  let dismissed = 0;
  const stillStuck = [];

  for (const p of problems || []) {
    const { data: msg } = await supabase
      .from('messages')
      .select('id, timestamp')
      .eq('session_id', p.session_id)
      .eq('remote_jid', p.remote_jid)
      .eq('from_me', true)
      .gt('timestamp', p.analyzed_at)
      .limit(1)
      .maybeSingle();

    if (msg) {
      const { error: updErr } = await supabase
        .from('chat_ai')
        .update({
          problem_dismissed_action: 'won',
          problem_dismissed_at: new Date().toISOString(),
          problem_dismissed_by: 'auto:daily-wa-analysis',
        })
        .eq('id', p.id);
      if (!updErr) dismissed++;
    } else {
      const days = Math.floor(
        (Date.now() - new Date(p.analyzed_at).getTime()) / (24 * 60 * 60 * 1000)
      );
      stillStuck.push({
        id: p.id,
        session_id: p.session_id,
        remote_jid: p.remote_jid,
        days_stuck: days,
        manager_issues: p.manager_issues,
        risk_flags: p.risk_flags,
      });
    }
  }

  return {
    total_unresolved: (problems || []).length,
    dismissed_won: dismissed,
    still_stuck_count: stillStuck.length,
    still_stuck: stillStuck.slice(0, 20),
  };
}

// ───────────────────────────────────────────────────────────────────────────
// 2. getRecentFeedback
// ───────────────────────────────────────────────────────────────────────────
//
// Adil оставляет фидбек на ошибки прошлых анализов через UI →
// chat_ai_feedback.{kind,comment_ru}. Claude Code читает этот лог перед
// новым прогоном и использует как guidelines.
export async function getRecentFeedback(limit = 50) {
  const cap = Math.min(Math.max(parseInt(limit) || 50, 1), 200);
  const { data, error } = await supabase
    .from('chat_ai_feedback')
    .select('id, kind, comment_ru, created_at, chat_ai_id')
    .order('created_at', { ascending: false })
    .limit(cap);
  if (error) throw new Error(`getRecentFeedback: ${error.message}`);
  return { count: (data || []).length, feedback: data || [] };
}

// ───────────────────────────────────────────────────────────────────────────
// 3. getPendingDialogs
// ───────────────────────────────────────────────────────────────────────────
//
// Возвращает диалоги С НОВОЙ АКТИВНОСТЬЮ за окно since_hours (default 24h).
// Включает 2 кейса:
//   1. NEW       — нет записи в chat_ai (первый анализ)
//   2. RE-ANALYZE — chat_ai есть, но `messages.timestamp > chat_ai.analyzed_at`
//                   (теги/intent могли устареть — нужно пере-проанализировать)
//
// Раньше (2026-05-01 предыдущая итерация): возвращало ВСЕ pending за всю
// историю → 109 диалогов из которых половина — мёртвые first_contact и
// colleague-чаты годовой давности. Adil: «зачем ты читал все непрочитанные?»
// Теперь — окно по `last_message_at`.
export async function getPendingDialogs({
  limit = 100,
  sessionId,
  sinceHours = 24,
} = {}) {
  const cap = Math.min(Math.max(parseInt(limit) || 100, 1), 500);
  const hours = Math.min(Math.max(parseInt(sinceHours) || 24, 1), 24 * 30); // max 30 days
  const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();

  let sessQ = supabase.from('session_config').select('session_id').eq('is_active', true);
  if (sessionId) sessQ = sessQ.eq('session_id', sessionId);
  const { data: sessions, error: sessErr } = await sessQ;
  if (sessErr) throw new Error(`getPendingDialogs sessions: ${sessErr.message}`);
  if (!sessions || sessions.length === 0) return { count: 0, dialogs: [], since: cutoff };

  const out = [];
  let newCount = 0, reCount = 0;

  for (const s of sessions) {
    // Только диалоги с активностью в окне
    const { data: dialogs } = await supabase
      .from('dialog_sessions')
      .select('id, remote_jid, last_message_at, message_count, started_at')
      .eq('session_id', s.session_id)
      .gte('message_count', 2)
      .gte('last_message_at', cutoff)
      .order('last_message_at', { ascending: false })
      .limit(cap);
    if (!dialogs || dialogs.length === 0) continue;

    // Какие из них уже в chat_ai (и когда последний анализ)
    const dialogIds = dialogs.map((d) => d.id);
    const { data: analyzed } = await supabase
      .from('chat_ai')
      .select('dialog_session_id, analyzed_at')
      .in('dialog_session_id', dialogIds);
    const analyzedMap = new Map((analyzed || []).map((a) => [a.dialog_session_id, a.analyzed_at]));

    for (const d of dialogs) {
      const lastAnalyzedAt = analyzedMap.get(d.id);
      if (!lastAnalyzedAt) {
        out.push({ ...d, session_id: s.session_id, reason: 'new' });
        newCount++;
      } else if (new Date(d.last_message_at) > new Date(lastAnalyzedAt)) {
        // Новые сообщения пришли после прошлого анализа → re-analyze для свежих тегов
        out.push({ ...d, session_id: s.session_id, reason: 're_analyze', last_analyzed_at: lastAnalyzedAt });
        reCount++;
      }
      if (out.length >= cap) break;
    }
    if (out.length >= cap) break;
  }

  return {
    count: out.length,
    new_count: newCount,
    re_analyze_count: reCount,
    since: cutoff,
    since_hours: hours,
    dialogs: out,
  };
}

// ───────────────────────────────────────────────────────────────────────────
// 4. saveAnalysis
// ───────────────────────────────────────────────────────────────────────────
//
// Batch insert проанализированных диалогов в chat_ai + опционально:
//   - inline auto-tagging (по customer_type) — chats.tags обновляются СРАЗУ,
//     отдельный /ai/backfill-tags больше не нужен
//   - mark_as_read=true: помечает messages.read_at = NOW() ТОЛЬКО для тех
//     (session_id, remote_jid) которые попали в этот save (stealth — без WA
//     read-receipts). Не трогает остальные unread.
//
// Раньше (2026-05-01 предыдущая итерация): отдельный шаг backfill-tags
// зависал на >60s, и отдельный read-all loop помечал ВСЕ 3905 unread
// (включая чаты которые мы НЕ анализировали). Adil: «зачем ты читал все».
// Теперь оба действия — внутри save-analysis с явным flag.
const REQUIRED_FIELDS = ['session_id', 'remote_jid', 'dialog_session_id', 'intent', 'lead_temperature'];

export async function saveAnalysis(records, { markAsRead = false } = {}) {
  if (!Array.isArray(records)) throw new Error('records must be an array');
  if (records.length === 0) return { saved: 0, ids: [] };
  if (records.length > 100) throw new Error('max 100 records per batch');

  const nowIso = new Date().toISOString();
  for (const r of records) {
    for (const f of REQUIRED_FIELDS) {
      if (!r[f]) throw new Error(`record missing required field: ${f}`);
    }
    if (!r.analyzed_at) r.analyzed_at = nowIso;
  }

  // Step 1: bulk insert; на 23505 — per-record fallback на update.
  let savedCount = 0, updatedCount = 0;
  const ids = [];
  const { data: bulkData, error: bulkErr } = await supabase
    .from('chat_ai')
    .insert(records)
    .select('id, dialog_session_id');

  if (!bulkErr) {
    savedCount = bulkData?.length || 0;
    ids.push(...(bulkData || []).map((d) => d.id));
  } else if (bulkErr.code === '23505') {
    for (const r of records) {
      const { data: ins, error: insErr } = await supabase
        .from('chat_ai')
        .insert(r)
        .select('id, dialog_session_id')
        .maybeSingle();
      if (!insErr && ins) {
        savedCount++;
        ids.push(ins.id);
        continue;
      }
      if (insErr?.code === '23505') {
        const { data: upd, error: updErr } = await supabase
          .from('chat_ai')
          .update(r)
          .eq('dialog_session_id', r.dialog_session_id)
          .select('id, dialog_session_id')
          .maybeSingle();
        if (!updErr && upd) {
          updatedCount++;
          ids.push(upd.id);
        }
      }
    }
  } else {
    throw new Error(`saveAnalysis: ${bulkErr.message}`);
  }

  // Step 2: inline auto-tagging (по customer_type) для каждой записи.
  // applyAutoTag сам уважает tagConfirmed (не перезаписывает manual теги).
  let tagged = 0;
  for (const r of records) {
    if (r.customer_type) {
      try {
        await applyAutoTag(r.session_id, r.remote_jid, r.customer_type);
        tagged++;
      } catch (e) {
        // Tag failure не валит save — логируем silently.
      }
    }
  }

  // Step 3: bulk mark-as-read только для analyzed JIDs (если попросили).
  // Single SQL UPDATE на messages с фильтром по списку (session_id, remote_jid)
  // парам — гораздо быстрее loop'а через per-chat /read-all endpoint.
  let markedRead = 0;
  if (markAsRead) {
    // Группируем по session_id чтобы одним запросом на каждую сессию.
    const bySession = new Map();
    for (const r of records) {
      if (!bySession.has(r.session_id)) bySession.set(r.session_id, new Set());
      bySession.get(r.session_id).add(r.remote_jid);
    }
    for (const [sid, jidSet] of bySession.entries()) {
      const jids = Array.from(jidSet);
      const { count, error: mErr } = await supabase
        .from('messages')
        .update({ read_at: nowIso })
        .eq('session_id', sid)
        .eq('from_me', false)
        .is('read_at', null)
        .in('remote_jid', jids)
        .select('*', { count: 'exact', head: true });
      if (!mErr) markedRead += (count || 0);
    }
  }

  return { saved: savedCount, updated: updatedCount, tagged, marked_read: markedRead, ids };
}

// ───────────────────────────────────────────────────────────────────────────
// 5. getStuckDeals
// ───────────────────────────────────────────────────────────────────────────
//
// Горячие/тёплые лиды без movement >= N дней. «Movement» = chat_ai.analyzed_at.
// Дополнительно проверяем что от менеджера не было сообщений после analyzed_at,
// чтобы убрать те которые двинулись (просто не пере-анализированы).
export async function getStuckDeals({ days = 7 } = {}) {
  const cap = Math.min(Math.max(parseInt(days) || 7, 1), 60);
  const cutoff = new Date(Date.now() - cap * 24 * 60 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from('chat_ai')
    .select('id, session_id, remote_jid, analyzed_at, lead_temperature, deal_stage, summary_ru, action_required, action_suggestion')
    .in('lead_temperature', ['hot', 'warm'])
    .not('deal_stage', 'in', '(completed,refused,delivery)')
    .lt('analyzed_at', cutoff)
    .is('problem_dismissed_at', null)
    .order('analyzed_at', { ascending: true })
    .limit(100);
  if (error) throw new Error(`getStuckDeals: ${error.message}`);

  // Фильтр: только personal jids. Группы (`@g.us`) и LID-jids (15+ цифр без
  // `@s.whatsapp.net` или с `@lid`) — мусор от старых записей, не реальные сделки.
  const isPersonalJid = (jid) => {
    if (!jid) return false;
    if (jid.includes('@g.us')) return false;
    if (jid.includes('@lid')) return false;
    // Personal: 11 цифр + @s.whatsapp.net (KZ format)
    if (jid.includes('@s.whatsapp.net')) {
      const phone = jid.split('@')[0];
      return phone.length >= 10 && phone.length <= 13 && /^\d+$/.test(phone);
    }
    // Без `@` — старый формат, проверим длину
    const phone = jid.split('@')[0];
    return phone.length >= 10 && phone.length <= 13 && /^\d+$/.test(phone);
  };

  const filtered = (data || []).filter((c) => isPersonalJid(c.remote_jid));

  const stuck = [];
  for (const c of filtered) {
    const { data: lastMsg } = await supabase
      .from('messages')
      .select('id, timestamp, from_me')
      .eq('session_id', c.session_id)
      .eq('remote_jid', c.remote_jid)
      .gt('timestamp', c.analyzed_at)
      .order('timestamp', { ascending: false })
      .limit(1)
      .maybeSingle();

    // Truly stuck: либо вообще нет новых сообщений, либо последнее — от клиента
    // (менеджер не ответил после AI-анализа).
    if (!lastMsg || !lastMsg.from_me) {
      const days_stuck = Math.floor(
        (Date.now() - new Date(c.analyzed_at).getTime()) / (24 * 60 * 60 * 1000)
      );
      stuck.push({ ...c, days_stuck, last_msg_from_client: lastMsg ? !lastMsg.from_me : null });
    }
  }

  return { count: stuck.length, deals: stuck.slice(0, 30) };
}

// ───────────────────────────────────────────────────────────────────────────
// 6. composeDigest
// ───────────────────────────────────────────────────────────────────────────
//
// Текстовая сводка за последние 24h: critical / hot / manager-issues / stuck.
// Опционально шлёт в Telegram (если sendTelegram=true И TG настроен).
export async function composeDigest({ sendTelegram = false } = {}) {
  const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

  // 1. Новые анализы
  const { count: newAnalyses } = await supabase
    .from('chat_ai')
    .select('*', { count: 'exact', head: true })
    .gte('analyzed_at', since);

  // 2. Критично за 24h
  const { data: critical } = await supabase
    .from('chat_ai')
    .select('id, session_id, remote_jid, summary_ru, sentiment, intent, action_suggestion')
    .gte('analyzed_at', since)
    .or('sentiment.eq.aggressive,intent.eq.complaint')
    .limit(20);

  // 3. Горячие лиды за 24h
  const { data: hot } = await supabase
    .from('chat_ai')
    .select('id, session_id, remote_jid, summary_ru, dialog_topic, action_suggestion, deal_stage')
    .gte('analyzed_at', since)
    .eq('lead_temperature', 'hot')
    .limit(20);

  // 4. Проблемы менеджеров за 24h (manager_issues непуст)
  const { data: mgrIssues } = await supabase
    .from('chat_ai')
    .select('id, session_id, remote_jid, summary_ru, manager_issues')
    .gte('analyzed_at', since)
    .not('manager_issues', 'eq', '{}')
    .limit(30);

  // 5. Зависшие сделки (используем уже готовую функцию)
  const stuck = await getStuckDeals({ days: 7 });

  const date = new Date().toISOString().slice(0, 10);
  const digest = {
    date,
    new_analyses: newAnalyses || 0,
    critical: { count: (critical || []).length, items: critical || [] },
    hot_leads: { count: (hot || []).length, items: hot || [] },
    manager_issues: { count: (mgrIssues || []).length, items: mgrIssues || [] },
    stuck_deals: stuck,
  };

  // Текстовая версия для Telegram (HTML-safe)
  const lines = [
    `📊 <b>Дневной анализ Omoikiri — ${date}</b>`,
    '',
    `Новых записей: <b>${digest.new_analyses}</b>`,
    `🔴 Критично: <b>${digest.critical.count}</b>`,
    `🔥 Горячих лидов: <b>${digest.hot_leads.count}</b>`,
    `⚠️ Проблем менеджеров: <b>${digest.manager_issues.count}</b>`,
    `🐌 Зависших сделок (>7д): <b>${digest.stuck_deals.count}</b>`,
  ];

  if (digest.critical.count > 0) {
    lines.push('', '<b>🔴 Критично:</b>');
    for (const c of digest.critical.items.slice(0, 5)) {
      const phone = String(c.remote_jid || '').split('@')[0];
      lines.push(`  • +${phone} — ${escapeHtml(truncate(c.summary_ru, 90))}`);
    }
  }
  if (digest.hot_leads.count > 0) {
    lines.push('', '<b>🔥 Горячие:</b>');
    for (const h of digest.hot_leads.items.slice(0, 5)) {
      const phone = String(h.remote_jid || '').split('@')[0];
      lines.push(`  • +${phone} — ${escapeHtml(truncate(h.summary_ru, 90))}`);
    }
  }
  if (digest.stuck_deals.count > 0) {
    lines.push('', '<b>🐌 Зависли:</b>');
    for (const s of digest.stuck_deals.deals.slice(0, 5)) {
      const phone = String(s.remote_jid || '').split('@')[0];
      lines.push(`  • +${phone} — ${s.days_stuck}д, ${s.deal_stage} — ${escapeHtml(truncate(s.summary_ru, 70))}`);
    }
  }

  digest.text = lines.join('\n');

  if (sendTelegram) {
    if (!isTelegramConfigured()) {
      digest.telegram_sent = false;
      digest.telegram_error = 'TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not configured';
    } else {
      try {
        await sendTelegramMessage(digest.text, 'HTML');
        digest.telegram_sent = true;
      } catch (e) {
        digest.telegram_sent = false;
        digest.telegram_error = e.message;
      }
    }
  }

  return digest;
}

// ─── helpers ────────────────────────────────────────────────────────────────
function truncate(s, n) {
  if (!s) return '';
  return s.length > n ? s.slice(0, n - 1) + '…' : s;
}
function escapeHtml(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
