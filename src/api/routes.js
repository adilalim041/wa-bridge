import express from 'express';
import QRCode from 'qrcode';
import { logger } from '../config.js';
import { RateLimiter, sendWithDelay } from '../antiban/rateLimiter.js';
import { getConnectionState } from '../baileys/connection.js';
import { getContacts, getMessages } from '../storage/queries.js';

const rateLimiter = new RateLimiter();

function normalizePhone(phone = '') {
  return phone.replace(/\D/g, '');
}

function escapeHtml(value = '') {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function setupRoutes(app, sockProvider) {
  const router = express.Router();

  router.get('/health', (_req, res) => {
    const state = getConnectionState();
    res.json({
      status: 'ok',
      connected: state.connected,
      user: state.user,
      uptime: process.uptime(),
    });
  });

  router.get('/status', (_req, res) => {
    const state = getConnectionState();
    res.json({
      connected: state.connected,
      user: state.user,
      hasQR: Boolean(state.qr),
      lastError: state.lastError,
    });
  });

  router.get('/qr', async (_req, res) => {
    const state = getConnectionState();
    let qrImageUrl = null;

    if (state.qr) {
      try {
        qrImageUrl = await QRCode.toDataURL(state.qr, { width: 300 });
      } catch (error) {
        logger.warn({ err: error }, 'Failed to render QR image');
      }
    }

    const safeUser = escapeHtml(state.user || 'unknown');
    const safeLastError = state.lastError ? escapeHtml(state.lastError) : '';
    let content = `
      <div class="status waiting">Waiting for QR code...</div>
      <p>Bridge is connecting to WhatsApp servers. Please wait.</p>
    `;

    if (state.connected) {
      content = `
        <div class="status connected">Connected as ${safeUser}</div>
        <p>WhatsApp Bridge is running.</p>
      `;
    } else if (state.qr && qrImageUrl) {
      content = `
        <div class="status waiting">Scan QR code with WhatsApp</div>
        <img src="${qrImageUrl}" alt="QR Code" width="300" height="300" />
        <p class="hint">Page refreshes automatically every 3 seconds</p>
      `;
    } else if (state.lastError) {
      content = `
        <div class="status error">Disconnected, reconnecting...</div>
        <p>Bridge is trying to reconnect to WhatsApp.</p>
        <p class="hint">Last error: ${safeLastError}</p>
      `;
    }

    res.set('Cache-Control', 'no-store');
    res.type('html').send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>WA Bridge QR</title>
  <meta http-equiv="refresh" content="3" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      color-scheme: light;
      font-family: "Segoe UI", Arial, sans-serif;
    }
    body {
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background: linear-gradient(135deg, #eff6ff, #f8fafc 45%, #ecfeff);
      color: #111827;
    }
    .card {
      width: min(92vw, 420px);
      padding: 32px;
      border-radius: 20px;
      background: rgba(255, 255, 255, 0.95);
      box-shadow: 0 20px 50px rgba(15, 23, 42, 0.12);
      text-align: center;
    }
    h1 {
      margin: 0 0 12px;
      font-size: 28px;
    }
    .status {
      font-size: 20px;
      font-weight: 700;
      margin-bottom: 16px;
    }
    .connected {
      color: #16a34a;
    }
    .waiting {
      color: #d97706;
    }
    .error {
      color: #dc2626;
    }
    img {
      width: 300px;
      max-width: 100%;
      height: auto;
      border-radius: 14px;
      background: #fff;
      padding: 10px;
      box-sizing: border-box;
    }
    p {
      margin: 10px 0 0;
      line-height: 1.5;
    }
    .hint {
      color: #6b7280;
      font-size: 13px;
    }
  </style>
</head>
<body>
  <main class="card">
    <h1>WA Bridge</h1>
    ${content}
  </main>
</body>
</html>`);
  });

  router.post('/send', async (req, res) => {
    const phone = normalizePhone(req.body?.phone);
    const message = req.body?.message?.toString().trim();

    if (!phone || !message) {
      return res.status(400).json({
        error: 'phone and message are required',
      });
    }

    const jid = `${phone}@s.whatsapp.net`;
    const sock = sockProvider();

    if (!sock?.user) {
      return res.status(503).json({ error: 'WhatsApp is not connected' });
    }

    if (!rateLimiter.canSend(jid) || !rateLimiter.canSendGlobal()) {
      return res.status(429).json({ error: 'Rate limit exceeded' });
    }

    try {
      const result = await sendWithDelay(sock, jid, { text: message });
      rateLimiter.recordSend(jid);

      return res.json({
        success: true,
        messageId: result?.key?.id ?? null,
      });
    } catch (error) {
      logger.error({ err: error, jid }, 'Failed to send WhatsApp message');
      return res.status(500).json({ error: 'Failed to send message' });
    }
  });

  router.get('/messages/:phone', async (req, res) => {
    const phone = normalizePhone(req.params.phone);
    const limit = Number.parseInt(req.query.limit, 10) || 50;
    const offset = Number.parseInt(req.query.offset, 10) || 0;

    try {
      const messages = await getMessages(phone, limit, offset);
      return res.json(messages);
    } catch (error) {
      logger.error({ err: error, phone }, 'Failed to fetch messages');
      return res.status(500).json({ error: 'Failed to fetch messages' });
    }
  });

  router.get('/contacts', async (_req, res) => {
    try {
      const contacts = await getContacts();
      return res.json(contacts);
    } catch (error) {
      logger.error({ err: error }, 'Failed to fetch contacts');
      return res.status(500).json({ error: 'Failed to fetch contacts' });
    }
  });

  app.use('/', router);
}
