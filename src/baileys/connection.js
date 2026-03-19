import pino from 'pino';
import qrcode from 'qrcode-terminal';
import { fetchLatestBaileysVersion, makeWASocket, DisconnectReason } from 'baileys';
import { logger } from '../config.js';
import { sendTelegramAlert, startHealthMonitor, updateConnectionStatus } from '../monitor.js';
import { supabase } from '../storage/supabase.js';
import { useSupabaseAuthState } from '../storage/authState.js';
import { emitNewMessage, emitSessionStatus } from '../api/websocket.js';
import { setCurrentVersion, startVersionChecker } from '../versionChecker.js';
import { handleMessage } from './messageHandler.js';

const DEFAULT_STATE = {
  qr: null,
  connected: false,
  user: null,
  lastError: null,
};

const connectionStates = new Map();
const reconnectAttempts = new Map();
const monitoredSessions = new Set();
const activeSessions = new Set();

function shouldPrintQrToTerminal() {
  return activeSessions.size <= 1;
}

function getReconnectDelay(sessionId) {
  const attempt = (reconnectAttempts.get(sessionId) ?? 0) + 1;
  reconnectAttempts.set(sessionId, attempt);

  if (attempt <= 5) {
    return 5000;
  }

  if (attempt <= 8) {
    return 5 * 60 * 1000;
  }

  return 30 * 60 * 1000;
}

function resetReconnectAttempts(sessionId) {
  reconnectAttempts.set(sessionId, 0);
}

function updateState(sessionId, updates) {
  const current = connectionStates.get(sessionId) || DEFAULT_STATE;
  const next = { ...current, ...updates };
  connectionStates.set(sessionId, next);
  emitSessionStatus(sessionId, next);
  return next;
}

export function activateSession(sessionId) {
  activeSessions.add(sessionId);
}

export function deactivateSession(sessionId) {
  activeSessions.delete(sessionId);
}

export function clearConnectionState(sessionId) {
  connectionStates.delete(sessionId);
  reconnectAttempts.delete(sessionId);
  monitoredSessions.delete(sessionId);
  emitSessionStatus(sessionId, { ...DEFAULT_STATE });
}

export function getConnectionState(sessionId) {
  return connectionStates.get(sessionId) || { ...DEFAULT_STATE };
}

export async function startConnection({ sessionId, onSocket }) {
  if (!sessionId) {
    throw new Error('sessionId is required');
  }

  if (!activeSessions.has(sessionId)) {
    return { sock: null, startConnection };
  }

  const { state, saveCreds } = await useSupabaseAuthState(sessionId);
  let waVersion;

  try {
    const { version } = await fetchLatestBaileysVersion();
    waVersion = version;
    console.log(`[${sessionId}] WhatsApp version: ${version.join('.')}`);
  } catch {
    waVersion = [2, 3000, 1035194821];
    console.log(`[${sessionId}] Failed to fetch WA version, using fallback: ${waVersion.join('.')}`);
  }

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: 'silent' }),
    browser: ['Omoikiri CRM', 'Desktop', '1.0'],
    version: waVersion,
    syncFullHistory: true,
    fireInitQueries: true,
    markOnlineOnConnect: true,
    shouldSyncHistoryMessage: () => true,
    getMessage: async (key) => {
      // Baileys calls this for message retries — fetch from Supabase
      try {
        const { data } = await supabase
          .from('messages')
          .select('body, message_type')
          .eq('message_id', key.id)
          .eq('session_id', sessionId)
          .maybeSingle();

        if (data?.body) {
          return { conversation: data.body };
        }
      } catch (err) {
        logger.debug({ err, sessionId, messageId: key.id }, 'getMessage lookup failed');
      }

      return undefined;
    },
  });

  if (typeof onSocket === 'function') {
    onSocket(sock);
  }

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      updateState(sessionId, {
        qr,
        connected: false,
        user: null,
        lastError: null,
      });

      console.log(`[${sessionId}] QR code generated. Scan via /qr/${sessionId}`);
      if (shouldPrintQrToTerminal()) {
        qrcode.generate(qr, { small: true });
      }
    }

    if (connection === 'open') {
      updateState(sessionId, {
        qr: null,
        connected: true,
        user: sock.user?.name || sock.user?.id || null,
        lastError: null,
      });

      resetReconnectAttempts(sessionId);
      updateConnectionStatus(sessionId, true);
      setCurrentVersion(waVersion);

      if (!monitoredSessions.has(sessionId)) {
        startHealthMonitor(sessionId);
        monitoredSessions.add(sessionId);
      }

      startVersionChecker(sessionId, () => {
        if (activeSessions.has(sessionId)) {
          sock.end(undefined);
        }
      });

      console.log(`[${sessionId}] Connected to WhatsApp as ${sock.user?.name || sock.user?.id || 'unknown user'}`);
    }

    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const reason = lastDisconnect?.error?.output?.payload?.message || 'unknown';

      updateState(sessionId, {
        connected: false,
        qr: null,
        user: null,
        lastError: `Status ${statusCode || 'unknown'}: ${reason}`,
      });
      updateConnectionStatus(sessionId, false);

      console.log(`[${sessionId}] Connection closed. Status: ${statusCode}, Reason: ${reason}`);

      if (!activeSessions.has(sessionId)) {
        console.log(`[${sessionId}] Session stopped manually. Skipping reconnect.`);
        return;
      }

      if (statusCode === DisconnectReason.loggedOut) {
        resetReconnectAttempts(sessionId);
        const { error } = await supabase
          .from('auth_state')
          .delete()
          .eq('session_id', sessionId);

        if (error) {
          logger.error({ err: error, sessionId }, 'Failed to clear auth state from Supabase');
        }

        sendTelegramAlert(`WA Bridge [${sessionId}]: Session logged out! Need new QR scan.`).catch(() => {});
        console.log(`[${sessionId}] Session expired. Auth state cleared. Restarting for new QR...`);

        setTimeout(() => {
          if (!activeSessions.has(sessionId)) {
            return;
          }

          startConnection({ sessionId, onSocket }).catch((error) => {
            logger.error({ err: error, sessionId }, 'Failed to restart WhatsApp connection');
          });
        }, 3000);

        return;
      }

      const delay = getReconnectDelay(sessionId);
      const attempt = reconnectAttempts.get(sessionId) ?? 0;
      const delayStr = delay >= 60000 ? `${delay / 60000} min` : `${delay / 1000}s`;
      console.log(`[${sessionId}] Reconnecting in ${delayStr}... (attempt ${attempt})`);

      if (attempt === 9) {
        sendTelegramAlert(
          `WA Bridge [${sessionId}]: 9 failed reconnect attempts. Switching to 30min interval.`
        ).catch(() => {});
      }

      setTimeout(() => {
        if (!activeSessions.has(sessionId)) {
          return;
        }

        startConnection({ sessionId, onSocket }).catch((error) => {
          console.error(`[${sessionId}] Failed to reconnect:`, error.message);
        });
      }, delay);
    }
  });

  sock.ev.on('creds.update', async () => {
    try {
      await saveCreds();
    } catch (error) {
      logger.error({ err: error, sessionId }, 'Failed to persist WhatsApp credentials');
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    console.log(`[${sessionId}] messages.upsert: ${messages?.length ?? 0} messages (type: ${type})`);
    for (const message of messages ?? []) {
      const savedMessage = await handleMessage(message, sock, sessionId);
      if (savedMessage) {
        emitNewMessage(sessionId, savedMessage);
      }
    }
  });

  // History sync — delivers offline/missed messages on reconnect
  sock.ev.on('messaging-history.set', async ({ messages, chats, isLatest, progress, syncType }) => {
    const msgCount = messages?.length ?? 0;
    const chatCount = chats?.length ?? 0;
    console.log(
      `[${sessionId}] messaging-history.set: ${msgCount} messages, ${chatCount} chats ` +
      `(syncType: ${syncType}, progress: ${progress ?? '?'}%, isLatest: ${isLatest})`
    );

    if (!messages?.length) {
      return;
    }

    let saved = 0;
    for (const message of messages) {
      try {
        const result = await handleMessage(message, sock, sessionId);
        if (result) {
          saved++;
          emitNewMessage(sessionId, result);
        }
      } catch (err) {
        logger.error({ err, sessionId, messageId: message?.key?.id }, 'Failed to process history sync message');
      }
    }

    console.log(`[${sessionId}] History sync: saved ${saved}/${msgCount} messages`);
  });

  // Edited messages — update body in Supabase
  sock.ev.on('messages.update', async (updates) => {
    for (const { key, update } of updates ?? []) {
      try {
        if (!key?.id || !key?.remoteJid) {
          continue;
        }

        // Handle edited messages
        const editedMsg = update?.message?.editedMessage?.message;
        if (editedMsg) {
          const newBody =
            editedMsg.conversation ||
            editedMsg.extendedTextMessage?.text ||
            editedMsg.imageMessage?.caption ||
            editedMsg.videoMessage?.caption ||
            null;

          if (newBody) {
            const { error } = await supabase
              .from('messages')
              .update({ body: newBody })
              .eq('message_id', key.id)
              .eq('session_id', sessionId);

            if (error) {
              logger.error({ err: error, sessionId, messageId: key.id }, 'Failed to update edited message');
            } else {
              console.log(`[${sessionId}] Message edited: ${key.id} → "${newBody.substring(0, 50)}"`);
            }
          }
        }
      } catch (err) {
        logger.error({ err, sessionId, messageId: key?.id }, 'Failed to process message update');
      }
    }
  });

  return { sock, startConnection };
}
