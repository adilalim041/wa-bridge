import crypto from 'crypto';
import { WebSocketServer, WebSocket } from 'ws';
import { logger } from '../config.js';

let wss = null;
let heartbeatInterval = null;

export function setupWebSocket(server) {
  if (wss) {
    return wss;
  }

  wss = new WebSocketServer({ server, path: '/ws' });
  const API_KEY = process.env.API_KEY;

  wss.on('connection', (ws, req) => {
    // Authenticate via query param: /ws?apiKey=...
    if (API_KEY) {
      const url = new URL(req.url, 'http://localhost');
      const clientKey = url.searchParams.get('apiKey');
      const isValid = clientKey && clientKey.length === API_KEY.length &&
        crypto.timingSafeEqual(Buffer.from(clientKey), Buffer.from(API_KEY));
      if (!isValid) {
        logger.warn('WebSocket connection rejected: invalid API key');
        ws.close(4001, 'Unauthorized');
        return;
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
