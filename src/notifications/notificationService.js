import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { sendTelegramMessage, isTelegramConfigured } from './telegramBot.js';

let lastOverdueHash = '';
let lastUnansweredHash = '';

// ── Overdue task checker ──
async function checkOverdueTasks() {
  if (!isTelegramConfigured()) return;

  try {
    const now = new Date().toISOString();
    const { data: overdue } = await supabase
      .from('tasks')
      .select('id, title, remote_jid, due_date, session_id')
      .eq('status', 'pending')
      .lt('due_date', now)
      .order('due_date', { ascending: true })
      .limit(10);

    if (!overdue?.length) return;

    // Get contact names
    const jids = [...new Set(overdue.filter((t) => t.remote_jid).map((t) => t.remote_jid))];
    const nameMap = new Map();
    if (jids.length) {
      const { data: contacts } = await supabase
        .from('contacts_crm')
        .select('remote_jid, first_name, last_name')
        .in('remote_jid', jids);
      for (const c of contacts || []) {
        nameMap.set(c.remote_jid, `${c.first_name || ''} ${c.last_name || ''}`.trim());
      }
    }

    let msg = `<b>Просроченные задачи (${overdue.length})</b>\n\n`;
    for (const task of overdue.slice(0, 5)) {
      const contact = nameMap.get(task.remote_jid) || '';
      const phone = task.remote_jid ? task.remote_jid.replace(/@.*$/, '') : '';
      const dueDate = new Date(task.due_date);
      const hoursAgo = Math.round((Date.now() - dueDate.getTime()) / 3600000);

      msg += `\u2022 <b>${task.title}</b>`;
      if (contact) msg += ` \u2014 ${contact}`;
      if (phone) msg += ` (+${phone})`;
      msg += `\n  Срок: ${hoursAgo}ч назад\n`;
    }
    if (overdue.length > 5) {
      msg += `\n... и ещё ${overdue.length - 5}`;
    }

    // Deduplicate: don't send if same as last notification
    const hash = overdue.map(t => t.id).sort().join(',');
    if (hash === lastOverdueHash) return;
    lastOverdueHash = hash;

    await sendTelegramMessage(msg);
    logger.info({ count: overdue.length }, 'Sent overdue tasks notification');
  } catch (err) {
    logger.error({ err }, 'Failed to check overdue tasks');
  }
}

// ── Unanswered customer checker ──
async function checkUnansweredChats() {
  if (!isTelegramConfigured()) return;

  try {
    // Working hours: 10:00 - 20:00 Almaty (UTC+5)
    const now = new Date();
    const almatyHour = (now.getUTCHours() + 5) % 24;
    if (almatyHour < 10 || almatyHour >= 20) return; // Outside working hours

    const twoHoursAgo = new Date(Date.now() - 2 * 3600000).toISOString();
    const eightHoursAgo = new Date(Date.now() - 8 * 3600000).toISOString();

    // The chats table does not have from_me. We need to find chats where the
    // latest message is from the customer (from_me = false) and is 2-8 hours old.
    // Strategy: query recent messages grouped by remote_jid, filter for last msg from customer.
    const { data: recentMsgs } = await supabase
      .from('messages')
      .select('remote_jid, session_id, from_me, timestamp, body, push_name')
      .gt('timestamp', eightHoursAgo)
      .not('remote_jid', 'like', '%@g.us')
      .not('remote_jid', 'like', '%@lid')
      .order('timestamp', { ascending: false })
      .limit(500);

    if (!recentMsgs?.length) return;

    // Group by remote_jid, keep only the latest message per chat
    const latestByJid = new Map();
    for (const m of recentMsgs) {
      if (!latestByJid.has(m.remote_jid)) {
        latestByJid.set(m.remote_jid, m);
      }
    }

    // Filter: last message is FROM customer (not from_me) and older than 2 hours
    const unanswered = [];
    for (const [jid, msg] of latestByJid) {
      if (msg.from_me) continue; // Manager already replied
      if (new Date(msg.timestamp) > new Date(twoHoursAgo)) continue; // Too recent
      // Filter out LID-like JIDs (digits only, 7-13 is real phone)
      const digits = jid.replace(/@.*$/, '').replace(/\D/g, '');
      if (digits.length < 7 || digits.length > 13) continue;

      unanswered.push({
        remote_jid: jid,
        session_id: msg.session_id,
        push_name: msg.push_name,
        last_message_body: msg.body,
        last_message_at: msg.timestamp,
      });
    }

    if (!unanswered.length) return;

    // Enrich with display names from chats table
    const jids = unanswered.map((u) => u.remote_jid);
    const { data: chatRows } = await supabase
      .from('chats')
      .select('remote_jid, display_name')
      .in('remote_jid', jids);
    const nameMap = new Map();
    for (const c of chatRows || []) {
      if (c.display_name) nameMap.set(c.remote_jid, c.display_name);
    }

    const toNotify = unanswered.slice(0, 5);
    let msg = `<b>Клиенты без ответа</b>\n\n`;
    for (const chat of toNotify) {
      const phone = chat.remote_jid.replace(/@.*$/, '');
      const name = nameMap.get(chat.remote_jid) || chat.push_name || phone;
      const hoursAgo = Math.round((Date.now() - new Date(chat.last_message_at).getTime()) / 3600000);
      const preview = (chat.last_message_body || '').slice(0, 60);

      msg += `\u2022 <b>${name}</b> (+${phone}) \u2014 ${hoursAgo}ч`;
      if (preview) msg += `\n  "${preview}"`;
      msg += '\n';
    }

    // Deduplicate: don't send if same contacts as last notification
    const hash = toNotify.map(u => u.remote_jid).sort().join(',');
    if (hash === lastUnansweredHash) return;
    lastUnansweredHash = hash;

    await sendTelegramMessage(msg);
    logger.info({ count: unanswered.length }, 'Sent unanswered chats notification');
  } catch (err) {
    logger.error({ err }, 'Failed to check unanswered chats');
  }
}

// ── Hot lead alert ──
export async function notifyHotLead(contactName, phone, topic, suggestion) {
  if (!isTelegramConfigured()) return;

  let msg = `<b>Hot lead</b>\n`;
  msg += `${contactName || phone}`;
  if (phone) msg += ` (+${phone})`;
  msg += '\n';
  if (topic) msg += `Тема: ${topic}\n`;
  if (suggestion) msg += `Рекомендация: ${suggestion}\n`;

  await sendTelegramMessage(msg);
}

// ── Daily summary ──
export async function sendDailySummary(analysisResult) {
  if (!isTelegramConfigured()) return;

  try {
    const today = new Date().toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
    const { processed = 0, failed = 0, skipped = 0, durationSec = 0 } = analysisResult || {};

    // Count overdue tasks
    const { count: overdueCount } = await supabase
      .from('tasks')
      .select('id', { count: 'exact', head: true })
      .eq('status', 'pending')
      .lt('due_date', new Date().toISOString());

    // Count hot leads from today's analyses
    const todayStr = new Date(Date.now() + 5 * 60 * 60 * 1000).toISOString().slice(0, 10);
    const { count: hotCount } = await supabase
      .from('chat_ai')
      .select('id', { count: 'exact', head: true })
      .eq('analysis_date', todayStr)
      .eq('lead_temperature', 'hot');

    // Count manager issues from today
    const { data: issueRows } = await supabase
      .from('chat_ai')
      .select('manager_issues')
      .eq('analysis_date', todayStr)
      .not('manager_issues', 'eq', '{}');
    const issueCount = (issueRows || []).filter((r) => r.manager_issues?.length > 0).length;

    let msg = `<b>Дневной отчёт (${today})</b>\n\n`;
    msg += `\u2022 Диалогов проанализировано: <b>${processed}</b>\n`;
    if (hotCount > 0) msg += `\u2022 Горячих лидов: <b>${hotCount}</b>\n`;
    if (issueCount > 0) msg += `\u2022 Ошибок менеджеров: <b>${issueCount}</b>\n`;
    if (failed > 0) msg += `\u2022 Не удалось обработать: <b>${failed}</b>\n`;
    if (overdueCount > 0) msg += `\u2022 Просроченных задач: <b>${overdueCount}</b>\n`;
    msg += `\u2022 Время анализа: ${durationSec}с\n`;
    msg += '\nАнализ завершён';

    await sendTelegramMessage(msg);
    logger.info('Sent daily summary to Telegram');
  } catch (err) {
    logger.error({ err }, 'Failed to send daily summary');
  }
}

// ── Periodic checker (runs every 30 min) ──
let checkerInterval = null;

export function startNotificationChecker() {
  if (!isTelegramConfigured()) {
    logger.info('Telegram not configured, skipping notification checker');
    return;
  }

  logger.info('Starting notification checker (every 30 min)');

  // Run first check 1 minute after boot (let sessions connect first)
  setTimeout(() => {
    checkOverdueTasks();
    checkUnansweredChats();
  }, 60_000);

  checkerInterval = setInterval(() => {
    checkOverdueTasks();
    checkUnansweredChats();
  }, 30 * 60_000); // Every 30 minutes
}

export function stopNotificationChecker() {
  if (checkerInterval) {
    clearInterval(checkerInterval);
    checkerInterval = null;
  }
}
