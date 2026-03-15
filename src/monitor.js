import { config } from './config.js';

const TELEGRAM_BOT_TOKEN = config.telegramBotToken;
const TELEGRAM_CHAT_ID = config.telegramChatId;

const healthIntervals = new Map();
const lastConnectedAt = new Map();
const alertSent = new Map();

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
    lastConnectedAt.set(sessionId, Date.now());
    if (alertSent.get(sessionId)) {
      alertSent.set(sessionId, false);
      sendTelegramAlert(`WA Bridge [${sessionId}]: Connection restored!`).catch(() => {});
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

    if (downtime > CHECK_INTERVAL && !alertSent.get(sessionId)) {
      alertSent.set(sessionId, true);
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
    return;
  }

  for (const timer of healthIntervals.values()) {
    clearInterval(timer);
  }
  healthIntervals.clear();
  lastConnectedAt.clear();
  alertSent.clear();
}
