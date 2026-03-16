import express from 'express';
import QRCode from 'qrcode';
import { getRateLimiter, sendWithDelay } from '../antiban/rateLimiter.js';
import { sessionManager } from '../baileys/sessionManager.js';
import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { getChatsWithLastMessage, getContacts, getMessages } from '../storage/queries.js';

function normalizeChatId(value = '') {
  const raw = decodeURIComponent(value).trim();
  if (!raw) {
    return '';
  }

  if (raw.endsWith('@g.us') || raw.endsWith('@s.whatsapp.net') || raw.endsWith('@lid')) {
    return raw
      .replace('@s.whatsapp.net', '')
      .replace('@g.us', '')
      .replace('@lid', '');
  }

  if (raw.includes('-')) {
    return raw.replace(/[^0-9-]/g, '');
  }

  return raw.replace(/\D/g, '');
}

function escapeHtml(value = '') {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function renderQrPage(sessionId, displayName, state) {
  let qrImageUrl = null;

  if (state.qr) {
    try {
      qrImageUrl = await QRCode.toDataURL(state.qr, { width: 300 });
    } catch {
      qrImageUrl = null;
    }
  }

  const safeTitle = escapeHtml(displayName || sessionId);
  const safeUser = escapeHtml(state.user || 'unknown');
  const safeLastError = state.lastError ? escapeHtml(state.lastError) : '';

  let content = `
    <div class="status waiting">Waiting for QR code...</div>
    <p>Bridge is connecting to WhatsApp servers. Please wait.</p>
  `;

  if (state.connected) {
    content = `
      <div class="status connected">Connected as ${safeUser}</div>
      <p>Session <strong>${escapeHtml(sessionId)}</strong> is running.</p>
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
      <p>Last error: ${safeLastError}</p>
    `;
  }

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>WA Bridge QR - ${safeTitle}</title>
  <meta http-equiv="refresh" content="3" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root { font-family: "Segoe UI", Arial, sans-serif; color-scheme: light; }
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
      width: min(92vw, 440px);
      padding: 32px;
      border-radius: 20px;
      background: rgba(255,255,255,0.96);
      box-shadow: 0 20px 50px rgba(15, 23, 42, 0.12);
      text-align: center;
    }
    h1 { margin: 0 0 8px; font-size: 30px; }
    h2 { margin: 0 0 20px; font-size: 18px; color: #475569; }
    .status { font-size: 20px; font-weight: 700; margin-bottom: 14px; }
    .connected { color: #16a34a; }
    .waiting { color: #d97706; }
    .error { color: #dc2626; }
    img {
      width: 300px;
      max-width: 100%;
      height: auto;
      border-radius: 14px;
      background: #fff;
      padding: 10px;
      box-sizing: border-box;
    }
    p { line-height: 1.5; }
    .hint { color: #6b7280; font-size: 13px; }
    a { color: #2563eb; text-decoration: none; }
  </style>
</head>
<body>
  <main class="card">
    <h1>WA Bridge</h1>
    <h2>${safeTitle}</h2>
    ${content}
    <p class="hint"><a href="/sessions">Back to sessions</a></p>
  </main>
</body>
</html>`;
}

export function setupRoutes(app) {
  const router = express.Router();

  router.get('/health', async (_req, res) => {
    const { data: configs } = await supabase
      .from('session_config')
      .select('session_id')
      .order('created_at', { ascending: true });

    const sessions = {};
    for (const config of configs ?? []) {
      const state = sessionManager.getSessionState(config.session_id);
      sessions[config.session_id] = {
        connected: state.connected,
        user: state.user,
        hasQR: Boolean(state.qr),
      };
    }

    res.json({
      status: 'ok',
      sessions,
      uptime: process.uptime(),
    });
  });

  router.get('/sessions', async (_req, res) => {
    const { data: configs, error } = await supabase
      .from('session_config')
      .select('*')
      .order('created_at', { ascending: true });

    if (error) {
      logger.error({ err: error }, 'Failed to load sessions');
      return res.status(500).json({ error: 'Failed to load sessions' });
    }

    const result = (configs ?? []).map((config) => {
      const state = sessionManager.getSessionState(config.session_id);
      return {
        sessionId: config.session_id,
        displayName: config.display_name,
        phoneNumber: config.phone_number,
        isActive: config.is_active,
        autoStart: config.auto_start,
        connected: state.connected,
        user: state.user,
        hasQR: Boolean(state.qr),
      };
    });

    return res.json(result);
  });

  router.post('/sessions', async (req, res) => {
    const sessionId = req.body?.sessionId?.toString().trim();
    const displayName = req.body?.displayName?.toString().trim();

    if (!sessionId || !displayName) {
      return res.status(400).json({ error: 'sessionId and displayName required' });
    }

    if (!/^[a-z0-9-]+$/.test(sessionId)) {
      return res.status(400).json({ error: 'sessionId must be lowercase alphanumeric with hyphens' });
    }

    const { error } = await supabase.from('session_config').insert({
      session_id: sessionId,
      display_name: displayName,
      is_active: true,
      auto_start: true,
    });

    if (error) {
      logger.error({ err: error, sessionId }, 'Failed to create session');
      return res.status(409).json({ error: 'Session already exists' });
    }

    try {
      const session = await sessionManager.startSession(sessionId);
      if (!session) {
        return res.status(409).json({ error: 'Session is locked by another instance' });
      }

      return res.json({
        success: true,
        sessionId,
        message: `Scan QR at /qr/${sessionId}`,
      });
    } catch (startError) {
      logger.error({ err: startError, sessionId }, 'Failed to start session');
      return res.status(500).json({ error: 'Session created but failed to start' });
    }
  });

  router.delete('/sessions/:sessionId', async (req, res) => {
    const { sessionId } = req.params;

    const { data: config } = await supabase
      .from('session_config')
      .select('session_id')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (!config) {
      return res.status(404).json({ error: 'Session not found' });
    }

    await sessionManager.stopSession(sessionId);
    await supabase.from('manager_sessions').delete().eq('session_id', sessionId);
    await supabase.from('auth_state').delete().eq('session_id', sessionId);

    const { error } = await supabase.from('session_config').delete().eq('session_id', sessionId);
    if (error) {
      logger.error({ err: error, sessionId }, 'Failed to delete session');
      return res.status(500).json({ error: 'Failed to delete session' });
    }

    return res.json({ success: true, sessionId });
  });

  router.get('/sessions/:sessionId/status', async (req, res) => {
    const { sessionId } = req.params;
    const { data: config } = await supabase
      .from('session_config')
      .select('session_id')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (!config) {
      return res.status(404).json({ error: 'Session not found' });
    }

    const state = sessionManager.getSessionState(sessionId);
    return res.json({
      connected: state.connected,
      user: state.user,
      hasQR: Boolean(state.qr),
      lastError: state.lastError,
      uptime: process.uptime(),
    });
  });

  router.get('/sessions/:sessionId/chats', async (req, res) => {
    const { sessionId } = req.params;
    try {
      const chats = await getChatsWithLastMessage(sessionId);
      return res.json(chats);
    } catch (error) {
      logger.error({ err: error, sessionId }, 'Failed to fetch chats');
      return res.status(500).json({ error: 'Failed to fetch chats' });
    }
  });

  router.get('/sessions/:sessionId/messages/:phone', async (req, res) => {
    const { sessionId } = req.params;
    const phone = normalizeChatId(req.params.phone);
    const limit = Number.parseInt(req.query.limit, 10) || 50;
    const offset = Number.parseInt(req.query.offset, 10) || 0;

    try {
      const messages = await getMessages(sessionId, phone, limit, offset);
      return res.json(messages);
    } catch (error) {
      logger.error({ err: error, sessionId, phone }, 'Failed to fetch messages');
      return res.status(500).json({ error: 'Failed to fetch messages' });
    }
  });

  router.get('/sessions/:sessionId/contacts', async (req, res) => {
    const { sessionId } = req.params;
    try {
      const contacts = await getContacts(sessionId);
      return res.json(contacts);
    } catch (error) {
      logger.error({ err: error, sessionId }, 'Failed to fetch contacts');
      return res.status(500).json({ error: 'Failed to fetch contacts' });
    }
  });

  router.post('/sessions/:sessionId/send', async (req, res) => {
    const { sessionId } = req.params;
    const phone = normalizeChatId(req.body?.phone);
    const message = req.body?.message?.toString().trim();

    if (!phone || !message) {
      return res.status(400).json({ error: 'phone and message are required' });
    }

    const session = sessionManager.getSession(sessionId);
    const sock = session?.sock;
    if (!sock?.user) {
      return res.status(503).json({ error: 'WhatsApp session is not connected' });
    }

    const limiter = getRateLimiter(sessionId);
    const jid = phone.includes('-') ? `${phone}@g.us` : `${phone}@s.whatsapp.net`;

    if (!limiter.canSend(jid) || !limiter.canSendGlobal()) {
      return res.status(429).json({ error: 'Rate limit exceeded' });
    }

    try {
      const result = await sendWithDelay(sock, jid, { text: message });
      limiter.recordSend(jid);

      return res.json({
        success: true,
        messageId: result?.key?.id ?? null,
      });
    } catch (error) {
      logger.error({ err: error, sessionId, jid }, 'Failed to send WhatsApp message');
      return res.status(500).json({ error: 'Failed to send message' });
    }
  });

  router.get('/qr/:sessionId', async (req, res) => {
    const { sessionId } = req.params;
    const { data: config } = await supabase
      .from('session_config')
      .select('session_id, display_name')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (!config) {
      return res.status(404).send('Session not found');
    }

    const html = await renderQrPage(sessionId, config.display_name, sessionManager.getSessionState(sessionId));
    res.set('Cache-Control', 'no-store');
    return res.type('html').send(html);
  });

  router.get('/qr', async (_req, res) => {
    const { data: configs } = await supabase
      .from('session_config')
      .select('session_id, display_name')
      .order('created_at', { ascending: true });

    const items = (configs ?? [])
      .map((config) => {
        const state = sessionManager.getSessionState(config.session_id);
        const status = state.connected ? 'Connected' : state.qr ? 'QR Ready' : 'Waiting';
        return `<li><a href="/qr/${escapeHtml(config.session_id)}">${escapeHtml(config.display_name)}</a> <span>${status}</span></li>`;
      })
      .join('');

    res.type('html').send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>WA Bridge Sessions</title>
  <meta http-equiv="refresh" content="5" />
  <style>
    body { font-family: "Segoe UI", Arial, sans-serif; margin: 40px; color: #111827; }
    h1 { margin-bottom: 16px; }
    ul { padding-left: 20px; }
    li { margin-bottom: 10px; }
    span { color: #64748b; margin-left: 8px; }
  </style>
</head>
<body>
  <h1>WA Bridge Sessions</h1>
  <ul>${items || '<li>No sessions configured yet.</li>'}</ul>
</body>
</html>`);
  });

  app.use('/', router);
}
