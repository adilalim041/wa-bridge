import { config } from './config.js';

const TELEGRAM_BOT_TOKEN = config.telegramBotToken;
const TELEGRAM_CHAT_ID = config.telegramChatId;

const healthIntervals = new Map();
const lastConnectedAt = new Map();
const alertSent = new Map();

const DOWNTIME_THRESHOLD = 20 * 60 * 1000; // 20 minutes — alert only for real outages
const ALERT_COOLDOWN = 60 * 60 * 1000; // 60 minutes between alerts per session
const lastAlertTime = new Map(); // sessionId -> timestamp of last alert
const disconnectedAt = new Map(); // sessionId -> timestamp when disconnect started
const disconnectLog = new Map(); // sessionId -> [{at, duration}] for morning summary

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

export function startHealthMonitor(sessionId) {
  const CHECK_INTERVAL = 5 * 60 * 1000;

  if (healthIntervals.has(sessionId)) {
    return;
  }

  lastConnectedAt.set(sessionId, Date.now());
  alertSent.set(sessionId, false);

  const timer = setInterval(() => {
    const lastSeen = lastConnectedAt.get(sessionId);
    if (!lastSeen) {
      return;
    }

    const downtime = Date.now() - lastSeen;

    const lastAlert = lastAlertTime.get(sessionId) || 0;
    if (downtime > DOWNTIME_THRESHOLD && !alertSent.get(sessionId) && (Date.now() - lastAlert > ALERT_COOLDOWN)) {
      alertSent.set(sessionId, true);
      lastAlertTime.set(sessionId, Date.now());
      const minutes = Math.round(downtime / 60000);
      sendTelegramAlert(`WA Bridge [${sessionId}]: Disconnected for ${minutes} minutes!`).catch(() => {});
    }
  }, CHECK_INTERVAL);

  healthIntervals.set(sessionId, timer);
}

export function stopHealthMonitor(sessionId) {
  if (sessionId) {
    const timer = healthIntervals.get(sessionId);
    if (timer) {
      clearInterval(timer);
      healthIntervals.delete(sessionId);
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
