import dotenv from 'dotenv';
import pino from 'pino';

dotenv.config();

export const logger = pino({ level: 'info' });
export const baileysLogger = pino({ level: 'warn' });

const requiredVars = ['SUPABASE_URL', 'SUPABASE_KEY'];

for (const key of requiredVars) {
  if (!process.env[key]?.trim()) {
    throw new Error(`${key} is required`);
  }
}

export const config = {
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseKey: process.env.SUPABASE_KEY,
  port: Number.parseInt(process.env.PORT || '3001', 10),
  sessionId: process.env.SESSION_ID?.trim() || 'omoikiri-main',
  telegramBotToken: process.env.TELEGRAM_BOT_TOKEN || '',
  telegramChatId: process.env.TELEGRAM_CHAT_ID || '',
};
