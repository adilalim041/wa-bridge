import crypto from 'crypto';
import { WebSocketServer, WebSocket } from 'ws';
import { logger } from '../config.js';
import { verifySupabaseJwt, isJwtAuthAvailable } from './jwtAuth.js';

let wss = null;
let heartbeatInterval = null;

export function setupWebSocket(server) {
  if (wss) {
    return wss;
  }

  wss = new WebSocketServer({ server, path: '/ws' });
  const API_KEY = process.env.API_KEY;

  // Phase A: WebSocket accepts ?access_token=<jwt> OR ?apiKey=<key> (backward compat).
  // Browsers cannot send custom headers on WebSocket upgrade — query params are the
  // only option. Both paths are fail-closed: if neither succeeds, connection is rejected.
  wss.on('connection', async (ws, req) => {
    const url = new URL(req.url, 'http://localhost');

    // ── Attempt 1: JWT via ?access_token= ──────────────────────────────────
    const accessToken = url.searchParams.get('access_token');
    if (accessToken) {
      if (!isJwtAuthAvailable()) {
        logger.warn('WebSocket rejected: access_token provided but JWT not configured');
        ws.close(4001, 'Unauthorized');
        return;
      }
      try {
        const user = await verifySupabaseJwt(accessToken);
        ws.user = user;
        logger.debug({ userId: user.userId }, 'ws jwt auth ok');
        // Fall through to the rest of the connection handler
      } catch (err) {
        logger.warn({ err: err.message }, 'WebSocket rejected: jwt verify failed');
        ws.close(4001, 'Unauthorized');
        return;
      }
    } else {
      // ── Attempt 2: legacy ?apiKey= ────────────────────────────────────────
      if (API_KEY) {
        const clientKey = url.searchParams.get('apiKey');
        const isValid = clientKey && clientKey.length === API_KEY.length &&
          crypto.timingSafeEqual(Buffer.from(clientKey), Buffer.from(API_KEY));
        if (!isValid) {
          logger.warn('WebSocket connection rejected: invalid API key');
          ws.close(4001, 'Unauthorized');
          return;
        }
      }
    }

    logger.info('Dashboard client connected via WebSocket');

    ws.isAlive = true;
    ws.subscribedSessions = null;

    ws.on('pong', () => {
      ws.isAlive = true;
    });

    ws.on('message', (data) => {
      try {
        const message = JSON.parse(data.toString());
        if (message.type === 'subscribe' && Array.isArray(message.sessionIds)) {
          ws.subscribedSessions = new Set(message.sessionIds);
        }
      } catch {
        // ignore invalid client payloads
      }
    });

    ws.on('close', () => {
      logger.debug('Dashboard client disconnected');
    });
  });

  heartbeatInterval = setInterval(() => {
    for (const ws of wss.clients) {
      if (!ws.isAlive) {
        ws.terminate();
        continue;
      }

      ws.isAlive = false;
      ws.ping();
    }
  }, 30000);

  return wss;
}

export function emitNewMessage(sessionId, messageData) {
  if (!wss) {
    return;
  }

  const payload = JSON.stringify({
    type: 'new_message',
    sessionId,
    message: messageData,
  });

  for (const ws of wss.clients) {
    if (ws.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (ws.subscribedSessions && !ws.subscribedSessions.has(sessionId)) {
      continue;
    }

    ws.send(payload);
  }
}

export function emitSessionStatus(sessionId, status) {
  if (!wss) {
    return;
  }

  const payload = JSON.stringify({
    type: 'session_status',
    sessionId,
    status,
  });

  for (const ws of wss.clients) {
    if (ws.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (ws.subscribedSessions && !ws.subscribedSessions.has(sessionId)) {
      continue;
    }

    ws.send(payload);
  }
}

/**
 * Broadcast a delivery-ack update to connected dashboard clients.
 *
 * Emitted after a successful ack_status UPDATE in the database — only
 * fires when the UPDATE actually changed a row (no-op updates are skipped).
 *
 * Frontend listens for type='message.ack' and updates the tick indicators
 * on the matching message bubble without a full refetch.
 *
 * @param {string} sessionId  - Baileys session the message belongs to
 * @param {string} messageId  - Baileys/WA message key id (message_id column)
 * @param {string} remoteJid  - WA JID of the chat
 * @param {number} ackStatus  - New ack level: 1=sent, 2=delivered, 3=read, 4=played
 */
export function emitAckUpdate(sessionId, messageId, remoteJid, ackStatus) {
  if (!wss) {
    return;
  }

  const payload = JSON.stringify({
    type: 'message.ack',
    sessionId,
    messageId,
    remoteJid,
    ackStatus,
  });

  for (const ws of wss.clients) {
    if (ws.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (ws.subscribedSessions && !ws.subscribedSessions.has(sessionId)) {
      continue;
    }

    ws.send(payload);
  }
}

export function emitCallEvent(sessionId, callData) {
  if (!wss) {
    return;
  }

  const payload = JSON.stringify({
    type: 'call_event',
    sessionId,
    call: callData,
  });

  for (const ws of wss.clients) {
    if (ws.readyState !== WebSocket.OPEN) {
      continue;
    }

    if (ws.subscribedSessions && !ws.subscribedSessions.has(sessionId)) {
      continue;
    }

    ws.send(payload);
  }
}

export function stopWebSocket() {
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }

  if (wss) {
    for (const ws of wss.clients) {
      ws.terminate();
    }
    wss.close();
    wss = null;
  }
}
