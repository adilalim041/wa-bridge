import pino from 'pino';
import qrcode from 'qrcode-terminal';
import { fetchLatestBaileysVersion, makeWASocket, DisconnectReason } from 'baileys';
import { config, logger } from '../config.js';
import { sendTelegramAlert, updateConnectionStatus, startHealthMonitor } from '../monitor.js';
import { supabase } from '../storage/supabase.js';
import { useSupabaseAuthState } from '../storage/authState.js';
import { setCurrentVersion, startVersionChecker } from '../versionChecker.js';
import { handleMessage } from './messageHandler.js';

const connectionState = {
  qr: null,
  connected: false,
  user: null,
  lastError: null,
};

let reconnectAttempt = 0;
let healthMonitorStarted = false;

function getReconnectDelay() {
  reconnectAttempt += 1;

  if (reconnectAttempt <= 5) {
    return 5000;
  }

  if (reconnectAttempt <= 8) {
    return 5 * 60 * 1000;
  }

  return 30 * 60 * 1000;
}

function resetReconnectAttempts() {
  reconnectAttempt = 0;
}

export function getConnectionState() {
  return { ...connectionState };
}

export async function startConnection(options = {}) {
  const { onSocket } = options;
  const { state, saveCreds } = await useSupabaseAuthState(config.sessionId);
  let waVersion;

  try {
    const { version } = await fetchLatestBaileysVersion();
    waVersion = version;
    console.log(`WhatsApp version: ${version.join('.')}`);
  } catch {
    waVersion = [2, 3000, 1035194821];
    console.log(`Failed to fetch WA version, using fallback: ${waVersion.join('.')}`);
  }

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: 'silent' }),
    browser: ['WA Bridge', 'Chrome', '120.0.0'],
    version: waVersion,
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
      resetReconnectAttempts();
      updateConnectionStatus(true);
      setCurrentVersion(waVersion);

      if (!healthMonitorStarted) {
        startHealthMonitor();
        healthMonitorStarted = true;
      }

      startVersionChecker(() => {
        sock.end(undefined);
      });

      console.log(`Connected to WhatsApp as ${connectionState.user || 'unknown user'}`);
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const reason = lastDisconnect?.error?.output?.payload?.message || 'unknown';
      connectionState.connected = false;
      connectionState.qr = null;
      connectionState.user = null;
      connectionState.lastError = `Status ${statusCode || 'unknown'}: ${reason}`;
      updateConnectionStatus(false);

      console.log(`Connection closed. Status: ${statusCode}, Reason: ${reason}`);

      if (statusCode === DisconnectReason.loggedOut) {
        resetReconnectAttempts();
        const { error } = await supabase
          .from('auth_state')
          .delete()
          .eq('session_id', config.sessionId);

        if (error) {
          logger.error({ err: error }, 'Failed to clear auth state from Supabase');
        }

        sendTelegramAlert('WA Bridge: Session logged out! Need new QR scan.').catch(() => {});
        console.log('Session expired. Auth state cleared. Restarting for new QR...');
        setTimeout(() => {
          startConnection(options).catch((error) => {
            logger.error({ err: error }, 'Failed to restart WhatsApp connection');
          });
        }, 3000);
      } else {
        const delay = getReconnectDelay();
        const delayStr = delay >= 60000 ? `${delay / 60000} min` : `${delay / 1000}s`;
        console.log(`Reconnecting in ${delayStr}... (attempt ${reconnectAttempt})`);

        if (reconnectAttempt === 9) {
          sendTelegramAlert(
            'WA Bridge: 9 failed reconnect attempts. Switching to 30min interval.'
          ).catch(() => {});
        }

        setTimeout(() => {
          startConnection(options).catch((error) => {
            console.error('Failed to reconnect:', error.message);
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
