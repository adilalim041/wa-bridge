import { config } from './config.js';
import { supabase } from './storage/supabase.js';

const TELEGRAM_BOT_TOKEN = config.telegramBotToken;
const TELEGRAM_CHAT_ID = config.telegramChatId;

const healthIntervals = new Map();
const zombieCheckIntervals = new Map();
const ZOMBIE_THRESHOLD_MS = 6 * 60 * 60 * 1000; // 6 hours no messages = zombie
const lastConnectedAt = new Map();
const alertSent = new Map();

const DOWNTIME_THRESHOLD = 15 * 60 * 1000; // 15 minutes — don't alarm for quick reconnects
const ALERT_COOLDOWN = 2 * 60 * 60 * 1000; // 2 hours between alerts per session
const lastAlertTime = new Map(); // sessionId -> timestamp of last alert
const disconnectedAt = new Map(); // sessionId -> timestamp when disconnect started
const disconnectLog = new Map(); // sessionId -> [{at, duration}] for morning summary

// Ban detection tracking
const dailyDisconnects = new Map(); // sessionId -> { count, loggedOut, connectionReplaced, date }
const BAN_ALERT_THRESHOLD = 5; // alert if >5 loggedOut per day
const CASCADE_BAN_WINDOW_MS = 15 * 60 * 1000; // 15 min sliding window
const CASCADE_BAN_THRESHOLD = 3; // 3 loggedOut events in window = cascade ban
const recentLoggedOuts = []; // Array of { sessionId, at } for cascade detection
let cascadeShutdownTriggered = false;

export function trackDisconnectEvent(sessionId, statusCode) {
  const today = new Date().toISOString().slice(0, 10);
  let entry = dailyDisconnects.get(sessionId);
  if (!entry || entry.date !== today) {
    entry = { count: 0, loggedOut: 0, connectionReplaced: 0, alerted: false, date: today };
  }
  entry.count++;
  // Only loggedOut (401) is a real ban indicator
  // connectionReplaced (440) is a known Baileys v7 false positive — do NOT count
  const isLoggedOut = statusCode === 401 || statusCode === 515;
  if (isLoggedOut) entry.loggedOut++;
  if (statusCode === 440) entry.connectionReplaced++; // track but don't alert
  dailyDisconnects.set(sessionId, entry);

  // Alert only for real loggedOut events, and only ONCE per day per session
  if (entry.loggedOut >= BAN_ALERT_THRESHOLD && !entry.alerted) {
    entry.alerted = true;
    sendTelegramAlert(
      `🛑 Ban risk [${sessionId}]: ${entry.loggedOut} loggedOut events today. Check account!`
    ).catch(() => {});
  }

  // Cascade ban detection: 3+ sessions getting loggedOut in 15 min = shutdown ALL
  if (isLoggedOut) {
    const now = Date.now();
    // Add this event and clean old ones
    recentLoggedOuts.push({ sessionId, at: now });
    while (recentLoggedOuts.length && now - recentLoggedOuts[0].at > CASCADE_BAN_WINDOW_MS) {
      recentLoggedOuts.shift();
    }

    // Count UNIQUE sessions in window
    const uniqueSessions = new Set(recentLoggedOuts.map((e) => e.sessionId));
    if (uniqueSessions.size >= CASCADE_BAN_THRESHOLD && !cascadeShutdownTriggered) {
      cascadeShutdownTriggered = true;
      const sessionList = Array.from(uniqueSessions).join(', ');
      sendTelegramAlert(
        `🚨 CASCADE BAN DETECTED: ${uniqueSessions.size} sessions banned in 15 min (${sessionList}).\n\nShutting down ALL sessions to protect remaining accounts. Check Railway logs + manually review before restarting.`
      ).catch(() => {});
      console.error('CASCADE BAN — shutting down all sessions:', sessionList);

      // Shutdown all sessions via sessionManager (no restart)
      if (sessionManagerRef?.stopAll) {
        sessionManagerRef.stopAll().catch((err) => {
          console.error('Cascade shutdown failed:', err.message);
        });
      }
    }
  }
}

// Mass disconnect detection
let massDisconnectTimer = null;
export function checkMassDisconnect(allSessionsCount) {
  const disconnectedNow = disconnectedAt.size;
  if (disconnectedNow >= 3 && disconnectedNow >= allSessionsCount * 0.5) {
    if (!massDisconnectTimer) {
      massDisconnectTimer = setTimeout(() => {
        // Still many disconnected after 60s?
        if (disconnectedAt.size >= 3) {
          sendTelegramAlert(
            `🚨 Mass disconnect: ${disconnectedAt.size}/${allSessionsCount} sessions down! Possible network/Railway issue.`
          ).catch(() => {});
        }
        massDisconnectTimer = null;
      }, 60000);
    }
  }
}

export async function sendTelegramAlert(message) {
  console.log(`[ALERT] ${message}`);

  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    return;
  }

  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: 'HTML',
      }),
    });

    if (!response.ok) {
      console.error('Telegram alert failed:', response.status);
    }
  } catch (error) {
    console.error('Telegram alert error:', error.message);
  }
}

export function updateConnectionStatus(sessionId, connected) {
  if (connected) {
    // Record reconnect event in disconnect log
    const dcStart = disconnectedAt.get(sessionId);
    if (dcStart) {
      const duration = Date.now() - dcStart;
      // Only log disconnects > 60s (skip brief reconnect flickers)
      if (duration > 60_000) {
        if (!disconnectLog.has(sessionId)) {
          disconnectLog.set(sessionId, []);
        }
        disconnectLog.get(sessionId).push({ at: dcStart, duration });
      }
      disconnectedAt.delete(sessionId);
    }

    lastConnectedAt.set(sessionId, Date.now());

    // Send "restored" only if a disconnect alert was actually sent and cooldown allows it
    if (alertSent.get(sessionId)) {
      const lastAlert = lastAlertTime.get(sessionId) || 0;
      if (Date.now() - lastAlert > ALERT_COOLDOWN) {
        lastAlertTime.set(sessionId, Date.now());
        sendTelegramAlert(`WA Bridge [${sessionId}]: Connection restored!`).catch(() => {});
      }
    }
    alertSent.set(sessionId, false);
  } else {
    // Record disconnect start time
    if (!disconnectedAt.has(sessionId)) {
      disconnectedAt.set(sessionId, Date.now());
    }
  }
}

// Zombie connection detection: session reports "connected" but no messages arrive
// This happens when WhatsApp silently drops the connection without sending a disconnect event
let sessionManagerRef = null;
export function setSessionManagerRef(sm) { sessionManagerRef = sm; }

// Track last restart per session to prevent restart loops
const lastZombieRestart = new Map();
const MIN_RESTART_INTERVAL = 6 * 60 * 60 * 1000; // 6h minimum between restarts

async function checkZombieConnection(sessionId) {
  try {
    // Only check during working hours (10:00-20:00 Almaty) — nights have no messages normally
    const now = new Date();
    const almatyHour = (now.getUTCHours() + 5) % 24;
    if (almatyHour < 10 || almatyHour >= 20) return;

    // Rate-limit restarts: max once per 6 hours per session
    const lastRestart = lastZombieRestart.get(sessionId) || 0;
    if (Date.now() - lastRestart < MIN_RESTART_INTERVAL) return;

    // Only check if session reports as connected
    const state = sessionManagerRef?.getSessionState?.(sessionId);
    if (!state?.connected) return;

    // Check last message timestamp from DB
    const { data } = await supabase
      .from('messages')
      .select('timestamp')
      .eq('session_id', sessionId)
      .order('timestamp', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (!data?.timestamp) return;

    const lastMsgAge = Date.now() - new Date(data.timestamp).getTime();
    if (lastMsgAge > ZOMBIE_THRESHOLD_MS) {
      const hours = Math.round(lastMsgAge / 3600000);
      lastZombieRestart.set(sessionId, Date.now());
      sendTelegramAlert(
        `🧟 Zombie detected [${sessionId}]: ${hours}h without messages during work hours. Restarting...`
      ).catch(() => {});

      // Force restart
      if (sessionManagerRef?.stopSession && sessionManagerRef?.startSession) {
        await sessionManagerRef.stopSession(sessionId);
        await new Promise((r) => setTimeout(r, 3000));
        await sessionManagerRef.startSession(sessionId);
        // No "restarted" alert — reduces notification spam
      }
    }
  } catch (err) {
    console.error(`Zombie check failed for ${sessionId}:`, err.message);
  }
}

export function startHealthMonitor(sessionId) {
  const CHECK_INTERVAL = 5 * 60 * 1000;

  if (healthIntervals.has(sessionId)) {
    return;
  }

  lastConnectedAt.set(sessionId, Date.now());
  alertSent.set(sessionId, false);

  const timer = setInterval(() => {
    // Downtime = how long the session has been in disconnected state.
    // If not in disconnectedAt, session is currently connected → no downtime.
    // (Previously used lastConnectedAt which only updates on new 'open' events,
    //  causing false alerts for stably-connected sessions after 15+ minutes.)
    const dcStart = disconnectedAt.get(sessionId);
    if (!dcStart) {
      return; // Session is connected, nothing to alert about
    }

    const downtime = Date.now() - dcStart;
    const lastAlert = lastAlertTime.get(sessionId) || 0;
    if (downtime > DOWNTIME_THRESHOLD && !alertSent.get(sessionId) && (Date.now() - lastAlert > ALERT_COOLDOWN)) {
      alertSent.set(sessionId, true);
      lastAlertTime.set(sessionId, Date.now());
      const minutes = Math.round(downtime / 60000);
      sendTelegramAlert(`WA Bridge [${sessionId}]: Disconnected for ${minutes} minutes!`).catch(() => {});
    }
  }, CHECK_INTERVAL);

  healthIntervals.set(sessionId, timer);

  // Zombie check every 2 hours
  if (!zombieCheckIntervals.has(sessionId)) {
    const zombieTimer = setInterval(() => checkZombieConnection(sessionId), 2 * 60 * 60 * 1000);
    zombieCheckIntervals.set(sessionId, zombieTimer);
    // First check after 30 min (give session time to receive messages)
    setTimeout(() => checkZombieConnection(sessionId), 30 * 60 * 1000);
  }
}

export function stopHealthMonitor(sessionId) {
  if (sessionId) {
    const timer = healthIntervals.get(sessionId);
    if (timer) {
      clearInterval(timer);
      healthIntervals.delete(sessionId);
    }
    const zTimer = zombieCheckIntervals.get(sessionId);
    if (zTimer) {
      clearInterval(zTimer);
      zombieCheckIntervals.delete(sessionId);
    }
    lastConnectedAt.delete(sessionId);
    alertSent.delete(sessionId);
    lastAlertTime.delete(sessionId);
    disconnectedAt.delete(sessionId);
    disconnectLog.delete(sessionId);
    return;
  }

  for (const timer of healthIntervals.values()) {
    clearInterval(timer);
  }
  healthIntervals.clear();
  for (const timer of zombieCheckIntervals.values()) {
    clearInterval(timer);
  }
  zombieCheckIntervals.clear();
  lastConnectedAt.clear();
  alertSent.clear();
  lastAlertTime.clear();
  disconnectedAt.clear();
  disconnectLog.clear();
}

// --- Morning summary scheduler ---

let summaryInterval = null;
let summaryRanToday = false;

export function startSummaryScheduler() {
  // Check every 30 minutes
  summaryInterval = setInterval(() => {
    const almatyHour = (new Date().getUTCHours() + 5) % 24;

    // Send summary at 08:00 Almaty time
    if (almatyHour === 8 && !summaryRanToday) {
      summaryRanToday = true;
      sendDisconnectSummary();
    }
    // Reset flag at midnight
    if (almatyHour === 0) {
      summaryRanToday = false;
    }
  }, 30 * 60_000);
}

export function stopSummaryScheduler() {
  if (summaryInterval) {
    clearInterval(summaryInterval);
    summaryInterval = null;
  }
}

function sendDisconnectSummary() {
  let hasEvents = false;
  let msg = '<b>WA Bridge ночной отчёт:</b>\n\n';

  for (const [sessionId, events] of disconnectLog) {
    if (!events.length) continue;
    hasEvents = true;

    const count = events.length;
    const maxDuration = Math.max(...events.map(e => e.duration));
    const totalDuration = events.reduce((sum, e) => sum + e.duration, 0);

    msg += `\u2022 <b>${sessionId}</b>: ${count} дисконнект${count === 1 ? '' : count < 5 ? 'а' : 'ов'} `;
    msg += `(макс. ${Math.round(maxDuration / 60000)} мин, всего ${Math.round(totalDuration / 60000)} мин)\n`;
  }

  // Also report sessions currently disconnected
  for (const [sessionId, dcStart] of disconnectedAt) {
    hasEvents = true;
    const duration = Math.round((Date.now() - dcStart) / 60000);
    msg += `\u26a0 <b>${sessionId}</b>: отключён уже ${duration} мин\n`;
  }

  if (!hasEvents) return; // Nothing to report

  // Check if all sessions are connected
  if (disconnectedAt.size === 0) {
    msg += '\nВсе подключения активны.';
  }
  sendTelegramAlert(msg).catch(() => {});

  // Clear log after sending (keep disconnectedAt — those sessions are still down)
  disconnectLog.clear();
}

// --- Telegram bot command polling ---

let pollingInterval = null;
let lastUpdateId = 0;

export function startTelegramPolling() {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;

  // Poll every 10 seconds for new messages
  pollingInterval = setInterval(pollTelegramUpdates, 10_000);
  // First poll immediately
  pollTelegramUpdates();
}

export function stopTelegramPolling() {
  if (pollingInterval) {
    clearInterval(pollingInterval);
    pollingInterval = null;
  }
}

async function pollTelegramUpdates() {
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates?offset=${lastUpdateId + 1}&timeout=0&allowed_updates=["message"]`;
    const resp = await fetch(url);
    if (!resp.ok) return;

    const { result } = await resp.json();
    if (!result?.length) return;

    for (const update of result) {
      lastUpdateId = update.update_id;
      const msg = update.message;
      if (!msg?.text || String(msg.chat.id) !== TELEGRAM_CHAT_ID) continue;

      const cmd = msg.text.trim().toLowerCase();
      if (cmd === '/status' || cmd === '/s') {
        await handleStatusCommand();
      }
    }
  } catch {
    // Silent — don't spam logs on network blips
  }
}

async function handleStatusCommand() {
  // Lazy import to avoid circular dependency (sessionManager imports monitor.js)
  const { sessionManager } = await import('./baileys/sessionManager.js');
  const states = sessionManager.getAllStates();
  const sessionIds = Object.keys(states);

  if (!sessionIds.length) {
    return sendTelegramAlert('<b>Статус:</b> Нет активных сессий');
  }

  const now = Date.now();
  let msg = '<b>WA Bridge — Статус</b>\n\n';

  for (const id of sessionIds) {
    const state = states[id];
    const dcStart = disconnectedAt.get(id);

    if (state.connected) {
      const uptime = lastConnectedAt.get(id);
      const uptimeMin = uptime ? Math.round((now - uptime) / 60000) : 0;
      msg += `\u2705 <b>${id}</b>: подключён`;
      if (uptimeMin > 0) msg += ` (${uptimeMin} мин)`;
      if (state.user) msg += ` — ${state.user}`;
      msg += '\n';
    } else if (dcStart) {
      const downMin = Math.round((now - dcStart) / 60000);
      msg += `\u274c <b>${id}</b>: отключён ${downMin} мин`;
      if (state.lastError) msg += `\n   <i>${state.lastError}</i>`;
      msg += '\n';
    } else {
      msg += `\u26a0 <b>${id}</b>: неизвестно`;
      if (state.lastError) msg += ` — ${state.lastError}`;
      msg += '\n';
    }
  }

  // Server uptime
  const uptimeS = process.uptime();
  const uptimeH = Math.floor(uptimeS / 3600);
  const uptimeM = Math.floor((uptimeS % 3600) / 60);
  msg += `\n<i>Сервер: ${uptimeH}ч ${uptimeM}м</i>`;

  await sendTelegramAlert(msg);
}
