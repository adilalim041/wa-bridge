import pino from 'pino';
import qrcode from 'qrcode-terminal';
import { makeWASocket, DisconnectReason } from 'baileys';
import { config, logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { useSupabaseAuthState } from '../storage/authState.js';
import { handleMessage } from './messageHandler.js';

const connectionState = {
  qr: null,
  connected: false,
  user: null,
  lastError: null,
};

export function getConnectionState() {
  return { ...connectionState };
}

export async function startConnection(options = {}) {
  const { onSocket } = options;
  const { state, saveCreds } = await useSupabaseAuthState(config.sessionId);

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: 'silent' }),
    browser: ['WA Bridge', 'Chrome', '120.0.0'],
    version: [2, 3000, 1035194821],
  });

  if (typeof onSocket === 'function') {
    onSocket(sock);
  }

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      connectionState.qr = qr;
      connectionState.connected = false;
      connectionState.user = null;
      connectionState.lastError = null;
      console.log('\nScan this QR code with WhatsApp:\n');
      qrcode.generate(qr, { small: true });
      console.log('\nWaiting for scan...\n');
    }

    if (connection === 'open') {
      connectionState.qr = null;
      connectionState.connected = true;
      connectionState.user = sock.user?.name || sock.user?.id || null;
      connectionState.lastError = null;
      console.log(`Connected to WhatsApp as ${connectionState.user || 'unknown user'}`);
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const reason = lastDisconnect?.error?.output?.payload?.message || 'unknown';
      connectionState.connected = false;
      connectionState.qr = null;
      connectionState.user = null;
      connectionState.lastError = `Status ${statusCode || 'unknown'}: ${reason}`;

      console.log(`Connection closed. Status: ${statusCode}, Reason: ${reason}`);

      if (statusCode === DisconnectReason.loggedOut) {
        const { error } = await supabase
          .from('auth_state')
          .delete()
          .eq('session_id', config.sessionId);

        if (error) {
          logger.error({ err: error }, 'Failed to clear auth state from Supabase');
        }

        console.log('Session expired. Auth state cleared. Restarting for new QR...');
        setTimeout(() => {
          startConnection(options).catch((error) => {
            logger.error({ err: error }, 'Failed to restart WhatsApp connection');
          });
        }, 3000);
      } else {
        const delay = 5000;
        console.log(`Reconnecting in ${delay / 1000}s...`);
        setTimeout(() => {
          startConnection(options).catch((error) => {
            logger.error({ err: error }, 'Failed to reconnect to WhatsApp');
          });
        }, delay);
      }
    }
  });

  sock.ev.on('creds.update', async () => {
    try {
      await saveCreds();
    } catch (error) {
      logger.error({ err: error }, 'Failed to persist WhatsApp credentials');
    }
  });

  sock.ev.on('messages.upsert', async ({ messages }) => {
    for (const message of messages ?? []) {
      await handleMessage(message, sock);
    }
  });

  return { sock, startConnection };
}
