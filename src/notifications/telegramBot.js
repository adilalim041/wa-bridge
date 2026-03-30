import { logger } from '../config.js';

const BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const isConfigured = Boolean(BOT_TOKEN && CHAT_ID);

export async function sendTelegramMessage(text, parseMode = 'HTML') {
  if (!isConfigured) return;
  try {
    const resp = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: CHAT_ID,
        text,
        parse_mode: parseMode,
        disable_web_page_preview: true,
      }),
    });
    if (!resp.ok) {
      const body = await resp.text();
      logger.warn({ status: resp.status, body }, 'Telegram send failed');
    }
  } catch (err) {
    logger.warn({ err }, 'Telegram send error');
  }
}

export function isTelegramConfigured() {
  return isConfigured;
}
