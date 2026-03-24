import dotenv from 'dotenv';
import pino from 'pino';

dotenv.config();

export const logger = pino({ level: 'info' });
export const baileysLogger = pino({ level: 'warn' });

const requiredVars = ['SUPABASE_URL', 'SUPABASE_KEY', 'SUPABASE_SERVICE_ROLE_KEY', 'API_KEY'];

for (const key of requiredVars) {
  if (!process.env[key]?.trim()) {
    throw new Error(`${key} is required — set it in environment variables`);
  }
}

export const config = {
  supabaseUrl: process.env.SUPABASE_URL,
  supabaseKey: process.env.SUPABASE_KEY,
  supabaseServiceRoleKey: process.env.SUPABASE_SERVICE_ROLE_KEY,
  port: Number.parseInt(process.env.PORT || '3001', 10),
  telegramBotToken: process.env.TELEGRAM_BOT_TOKEN || '',
  telegramChatId: process.env.TELEGRAM_CHAT_ID || '',
  cloudinaryCloudName: process.env.CLOUDINARY_CLOUD_NAME || '',
  cloudinaryApiKey: process.env.CLOUDINARY_API_KEY || '',
  cloudinaryApiSecret: process.env.CLOUDINARY_API_SECRET || '',
};
