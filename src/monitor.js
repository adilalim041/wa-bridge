import { config } from './config.js';

const TELEGRAM_BOT_TOKEN = config.telegramBotToken;
const TELEGRAM_CHAT_ID = config.telegramChatId;

let healthInterval = null;
let lastConnectedAt = null;
let alertSent = false;

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

export function updateConnectionStatus(connected) {
  if (connected) {
    lastConnectedAt = Date.now();
    if (alertSent) {
      alertSent = false;
      sendTelegramAlert('Connection restored!').catch(() => {});
    }
  }
}

export function startHealthMonitor() {
  const CHECK_INTERVAL = 5 * 60 * 1000;

  if (healthInterval) {
    return;
  }

  lastConnectedAt = Date.now();

  healthInterval = setInterval(() => {
    if (!lastConnectedAt) {
      return;
    }

    const downtime = Date.now() - lastConnectedAt;

    if (downtime > CHECK_INTERVAL && !alertSent) {
      alertSent = true;
      const minutes = Math.round(downtime / 60000);
      sendTelegramAlert(`WA Bridge: Disconnected for ${minutes} minutes!`).catch(() => {});
    }
  }, CHECK_INTERVAL);
}

export function stopHealthMonitor() {
  if (healthInterval) {
    clearInterval(healthInterval);
    healthInterval = null;
  }
}
