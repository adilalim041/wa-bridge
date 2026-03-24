import express from 'express';
import multer from 'multer';
import QRCode from 'qrcode';
import { v2 as cloudinary } from 'cloudinary';
import { handleAIChat } from '../ai/chatEndpoint.js';
import { runAnalysisNow, isAnalysisRunning } from '../ai/aiWorker.js';
import { getOrCreateDialogSession } from '../ai/dialogSessions.js';
import { enqueueForAI } from '../ai/queueManager.js';
import { trackResponseTime } from '../ai/responseTracker.js';
import { getRateLimiter, sendWithDelay } from '../antiban/rateLimiter.js';
import { invalidateHiddenCache } from '../baileys/messageHandler.js';
import { sessionManager } from '../baileys/sessionManager.js';
import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { getChatsWithLastMessage, getContacts, getMessages } from '../storage/queries.js';

const CYRILLIC_MAP = {
  а: 'a', б: 'b', в: 'v', г: 'g', д: 'd', е: 'e', ё: 'yo', ж: 'zh',
  з: 'z', и: 'i', й: 'y', к: 'k', л: 'l', м: 'm', н: 'n', о: 'o',
  п: 'p', р: 'r', с: 's', т: 't', у: 'u', ф: 'f', х: 'kh', ц: 'ts',
  ч: 'ch', ш: 'sh', щ: 'sch', ъ: '', ы: 'y', ь: '', э: 'e', ю: 'yu', я: 'ya',
};

function slugify(text) {
  return text
    .toLowerCase()
    .split('')
    .map((c) => CYRILLIC_MAP[c] || c)
    .join('')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 40);
}

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
});

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME || 'do0zl6hbd',
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

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

function buildWhatsAppJid(remoteJid) {
  return remoteJid.includes('-') ? `${remoteJid}@g.us` : `${remoteJid}@s.whatsapp.net`;
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

  // On-demand AI analysis — triggered from dashboard
  router.post('/ai/analyze', async (req, res) => {
    if (isAnalysisRunning()) {
      return res.json({ success: false, error: 'Анализ уже выполняется', running: true });
    }

    // Run async, return immediately
    res.json({ success: true, message: 'Анализ запущен' });

    // Process in background
    runAnalysisNow().then((result) => {
      logger.info(result, 'On-demand AI analysis finished');
    });
  });

  router.get('/ai/analyze/status', (req, res) => {
    res.json({ running: isAnalysisRunning() });
  });

  router.post('/ai/chat', async (req, res) => {
    const inputMessages = Array.isArray(req.body?.messages) ? req.body.messages : null;

    if (!inputMessages || inputMessages.length === 0) {
      return res.status(400).json({ error: 'messages array is required' });
    }

    const cleanMessages = inputMessages.map((message) => ({
      role: message?.role === 'assistant' ? 'assistant' : 'user',
      content:
        typeof message?.content === 'string'
          ? message.content
          : JSON.stringify(message?.content ?? ''),
    }));

    try {
      const result = await handleAIChat(cleanMessages);

      if (result.error) {
        return res.status(500).json({ error: result.error });
      }

      return res.json({
        response: result.response,
        usage: result.usage || null,
      });
    } catch (error) {
      logger.error({ err: error }, 'AI chat endpoint error');
      return res.status(500).json({ error: 'AI chat failed' });
    }
  });

  router.get('/analytics/summary', async (req, res) => {
    const sessionId = req.query.session_id || null;
    const days = Math.min(Number(req.query.days) || 7, 90);
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    try {
      // Build session filter
      const sessionFilter = (query) => sessionId ? query.eq('session_id', sessionId) : query;

      // 1. KPI: response times
      const { data: responseTimes } = await sessionFilter(
        supabase
          .from('manager_analytics')
          .select('response_time_seconds')
          .gte('created_at', since)
          .not('response_time_seconds', 'is', null)
      );

      const rtValues = (responseTimes ?? []).map((r) => r.response_time_seconds).filter((v) => v > 0);
      const avgResponseTime = rtValues.length > 0
        ? Math.round(rtValues.reduce((a, b) => a + b, 0) / rtValues.length)
        : 0;

      // 2. AI analysis breakdown
      const { data: aiData } = await sessionFilter(
        supabase
          .from('chat_ai')
          .select('lead_temperature, deal_stage, sentiment, risk_flags, action_required, session_id, remote_jid, summary_ru, action_suggestion, customer_type, consultation_score, consultation_details, followup_status, manager_issues')
          .gte('analyzed_at', since)
      );

      const ai = aiData ?? [];

      // Lead temperature
      const leads = { hot: 0, warm: 0, cold: 0, dead: 0 };
      for (const row of ai) {
        if (leads[row.lead_temperature] !== undefined) leads[row.lead_temperature]++;
      }

      // Deal stages
      const stages = {};
      for (const row of ai) {
        stages[row.deal_stage] = (stages[row.deal_stage] || 0) + 1;
      }

      // Sentiment
      const sentiment = { positive: 0, neutral: 0, negative: 0, aggressive: 0 };
      for (const row of ai) {
        if (sentiment[row.sentiment] !== undefined) sentiment[row.sentiment]++;
      }

      // Risk flags with example chats
      const riskMap = {};
      for (const row of ai) {
        for (const flag of row.risk_flags ?? []) {
          if (!riskMap[flag]) riskMap[flag] = { count: 0, chats: [] };
          riskMap[flag].count++;
          if (riskMap[flag].chats.length < 3) {
            riskMap[flag].chats.push({
              sessionId: row.session_id,
              remoteJid: row.remote_jid,
              summary: row.summary_ru,
              action: row.action_suggestion,
            });
          }
        }
      }
      const topRisks = Object.entries(riskMap)
        .sort((a, b) => b[1].count - a[1].count)
        .slice(0, 5)
        .map(([flag, data]) => ({ flag, count: data.count, chats: data.chats }));

      // Action required chats
      const actionChats = ai
        .filter((r) => r.action_required)
        .slice(0, 5)
        .map((r) => ({
          sessionId: r.session_id,
          remoteJid: r.remote_jid,
          summary: r.summary_ru,
          action: r.action_suggestion,
        }));

      // Action required count
      const actionRequired = ai.filter((r) => r.action_required).length;

      // Customer types breakdown
      const customerTypes = {};
      for (const row of ai) {
        const ct = row.customer_type || 'unknown';
        customerTypes[ct] = (customerTypes[ct] || 0) + 1;
      }

      // Consultation quality avg
      const cScores = ai.map((r) => r.consultation_score).filter((s) => s != null && s > 0);
      const avgConsultationScore = cScores.length > 0
        ? Math.round(cScores.reduce((a, b) => a + b, 0) / cScores.length)
        : null;

      // Manager issues breakdown
      const issuesMap = {};
      for (const row of ai) {
        for (const issue of row.manager_issues ?? []) {
          issuesMap[issue] = (issuesMap[issue] || 0) + 1;
        }
      }
      const managerIssues = Object.entries(issuesMap)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 10)
        .map(([issue, count]) => ({ issue, count }));

      // Followup stats
      const followupStats = { done: 0, missed: 0, pending: 0, not_needed: 0 };
      for (const row of ai) {
        const fs = row.followup_status || 'not_needed';
        if (followupStats[fs] !== undefined) followupStats[fs]++;
      }

      // 3. Dialog sessions count
      const { count: totalDialogs } = await sessionFilter(
        supabase
          .from('dialog_sessions')
          .select('*', { count: 'exact', head: true })
          .gte('started_at', since)
      );

      // 4. Daily message trend
      const { data: dailyMessages } = await sessionFilter(
        supabase
          .from('messages')
          .select('timestamp')
          .gte('timestamp', since)
          .order('timestamp', { ascending: true })
      );

      const dailyMap = {};
      for (const msg of dailyMessages ?? []) {
        const day = msg.timestamp.substring(0, 10);
        dailyMap[day] = (dailyMap[day] || 0) + 1;
      }
      const dailyStats = Object.entries(dailyMap)
        .map(([date, messages]) => ({ date, messages }))
        .sort((a, b) => a.date.localeCompare(b.date));

      return res.json({
        period: { days, since },
        kpi: {
          avgResponseTime,
          hotLeads: leads.hot,
          totalDialogs: totalDialogs || 0,
          actionRequired,
          avgConsultationScore,
        },
        leads,
        stages,
        sentiment,
        topRisks,
        actionChats,
        dailyStats,
        customerTypes,
        managerIssues,
        followupStats,
      });
    } catch (error) {
      logger.error({ err: error }, 'Analytics summary failed');
      return res.status(500).json({ error: 'Failed to fetch analytics' });
    }
  });

  // One-time cleanup of garbage LID entries (short numeric remote_jid values)
  router.post('/admin/cleanup-lid-garbage', async (req, res) => {
    try {
      const { sessionId } = req.query;

      // Find all chats with short numeric remote_jid (LID garbage)
      let chatQuery = supabase
        .from('chats')
        .select('remote_jid, session_id');

      if (sessionId) {
        chatQuery = chatQuery.eq('session_id', sessionId);
      }

      const { data: allChats, error: findError } = await chatQuery;
      if (findError) {
        return res.status(500).json({ error: findError.message });
      }

      // Filter garbage: short LIDs (<7 digits) or long LIDs (>13 digits, no real phone is that long)
      const garbageEntries = (allChats || []).filter(c => {
        const jid = c.remote_jid;
        if (!/^\d+$/.test(jid)) return false;
        return jid.length < 7 || jid.length > 13;
      });

      if (garbageEntries.length === 0) {
        return res.json({ success: true, message: 'No garbage LID entries found', cleaned: { count: 0 } });
      }

      let deletedMessages = 0;
      let deletedChats = 0;
      let deletedDialogSessions = 0;

      for (const { remote_jid, session_id } of garbageEntries) {
        const { count: msgCount } = await supabase
          .from('messages')
          .delete({ count: 'exact' })
          .eq('remote_jid', remote_jid)
          .eq('session_id', session_id);
        deletedMessages += msgCount || 0;

        const { count: dsCount } = await supabase
          .from('dialog_sessions')
          .delete({ count: 'exact' })
          .eq('remote_jid', remote_jid)
          .eq('session_id', session_id);
        deletedDialogSessions += dsCount || 0;

        const { count: chatCount } = await supabase
          .from('chats')
          .delete({ count: 'exact' })
          .eq('remote_jid', remote_jid)
          .eq('session_id', session_id);
        deletedChats += chatCount || 0;
      }

      res.json({
        success: true,
        cleaned: {
          garbageJids: garbageEntries.map(g => g.remote_jid),
          deletedMessages,
          deletedChats,
          deletedDialogSessions,
        },
      });
    } catch (error) {
      logger.error({ err: error }, 'Cleanup LID garbage failed');
      res.status(500).json({ error: 'Cleanup failed' });
    }
  });

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
    const displayName = req.body?.displayName?.toString().trim();
    const city = req.body?.city?.toString().trim() || '';
    let sessionId = req.body?.sessionId?.toString().trim() || '';

    if (!displayName) {
      return res.status(400).json({ error: 'displayName required' });
    }

    if (!sessionId) {
      sessionId = slugify(city ? `${city} ${displayName}` : displayName);
    }

    if (!/^[a-z0-9-]+$/.test(sessionId)) {
      return res.status(400).json({ error: 'sessionId must be lowercase alphanumeric with hyphens' });
    }

    const finalDisplayName = city ? `${city} — ${displayName}` : displayName;

    const { error } = await supabase.from('session_config').insert({
      session_id: sessionId,
      display_name: finalDisplayName,
      is_active: true,
      auto_start: true,
    });

    if (error) {
      if (error.code === '23505') {
        sessionId = `${sessionId}-${Date.now().toString(36).slice(-4)}`;
        const { error: retryError } = await supabase.from('session_config').insert({
          session_id: sessionId,
          display_name: finalDisplayName,
          is_active: true,
          auto_start: true,
        });

        if (retryError) {
          logger.error({ err: retryError, sessionId }, 'Failed to create session (retry)');
          return res.status(409).json({ error: 'Session already exists' });
        }
      } else {
        logger.error({ err: error, sessionId }, 'Failed to create session');
        return res.status(500).json({ error: error.message });
      }
    }

    try {
      const session = await sessionManager.startSession(sessionId);
      if (!session) {
        return res.status(409).json({ error: 'Session is locked by another instance' });
      }

      return res.json({
        success: true,
        sessionId,
        displayName: finalDisplayName,
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
      qr: state.qr || null,
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

  router.post('/sessions/:sessionId/chats/:phone/mute', async (req, res) => {
    const { sessionId } = req.params;
    const phone = normalizeChatId(req.params.phone);
    const mute = Boolean(req.body?.mute);
    const duration = Number.isFinite(Number(req.body?.duration)) ? Number(req.body.duration) : null;

    const session = sessionManager.getSession(sessionId);
    const sock = session?.sock;
    if (!sock?.user) {
      return res.status(503).json({ error: 'WhatsApp session is not connected' });
    }

    const jid = buildWhatsAppJid(phone);

    try {
      if (mute) {
        const expiration = duration && duration > 0 ? Math.floor(Date.now() / 1000) + duration : -1;
        await sock.chatModify({ mute: expiration }, jid);
      } else {
        await sock.chatModify({ mute: null }, jid);
      }

      const mutedUntil = mute && duration && duration > 0
        ? new Date(Date.now() + duration * 1000).toISOString()
        : null;

      const { error: persistError } = await supabase
        .from('chats')
        .upsert(
          {
            session_id: sessionId,
            remote_jid: phone,
            is_muted: mute,
            muted_until: mutedUntil,
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'session_id,remote_jid' }
        );

      if (persistError) {
        logger.error({ err: persistError, sessionId, jid }, 'Failed to persist mute state');
        return res.status(500).json({ error: 'Failed to persist mute state' });
      }

      return res.json({ success: true, muted: mute, mutedUntil });
    } catch (error) {
      logger.error({ err: error, sessionId, jid }, 'Failed to mute/unmute chat');
      return res.status(500).json({ error: 'Failed to mute/unmute chat' });
    }
  });

  router.post('/sessions/:sessionId/chats/:phone/hide', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);
    const hidden = Boolean(req.body?.hidden);

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    try {
      const { error } = await supabase
        .from('chats')
        .upsert(
          {
            session_id: sessionId,
            remote_jid: remoteJid,
            is_hidden: hidden,
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'session_id,remote_jid' }
        );

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to hide/unhide chat');
        return res.status(500).json({ error: 'Failed to update chat visibility' });
      }

      invalidateHiddenCache(sessionId, remoteJid);

      if (hidden) {
        const { error: deleteError } = await supabase
          .from('messages')
          .delete()
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid);

        if (deleteError) {
          logger.error({ err: deleteError, sessionId, remoteJid }, 'Failed to delete hidden chat messages');
        }
      }

      return res.json({ success: true, hidden });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error hiding chat');
      return res.status(500).json({ error: 'Failed to update chat visibility' });
    }
  });

  router.post('/sessions/:sessionId/chats/:phone/tags', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);
    const inputTags = req.body?.tags;

    if (!remoteJid || !Array.isArray(inputTags)) {
      return res.status(400).json({ error: 'phone and tags array are required' });
    }

    const cleanTags = [...new Set(
      inputTags
        .map((tag) => tag?.toString().trim().toLowerCase())
        .filter(Boolean)
    )].slice(0, 10);

    try {
      const { error } = await supabase
        .from('chats')
        .upsert(
          {
            session_id: sessionId,
            remote_jid: remoteJid,
            tags: cleanTags,
            updated_at: new Date().toISOString(),
          },
          { onConflict: 'session_id,remote_jid' }
        );

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to update tags');
        return res.status(500).json({ error: 'Failed to update tags' });
      }

      return res.json({ success: true, tags: cleanTags });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error updating tags');
      return res.status(500).json({ error: 'Failed to update tags' });
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

  router.post('/sessions/:sessionId/messages/read', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.body?.remoteJid);
    const messageIds = Array.isArray(req.body?.messageIds)
      ? req.body.messageIds.map((value) => value?.toString().trim()).filter(Boolean)
      : [];

    if (!remoteJid || messageIds.length === 0) {
      return res.status(400).json({ error: 'remoteJid and messageIds are required' });
    }

    try {
      const { data: rows, error: fetchError } = await supabase
        .from('messages')
        .select('message_id, sender, chat_type')
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .eq('from_me', false)
        .in('message_id', messageIds);

      if (fetchError) {
        logger.error({ err: fetchError, sessionId, remoteJid }, 'Failed to load messages for read receipt');
        return res.status(500).json({ error: 'Failed to mark messages as read' });
      }

      const readAt = new Date().toISOString();
      const { error: updateError } = await supabase
        .from('messages')
        .update({ read_at: readAt })
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .eq('from_me', false)
        .in('message_id', messageIds);

      if (updateError) {
        logger.error({ err: updateError, sessionId, remoteJid }, 'Failed to update read timestamps');
        return res.status(500).json({ error: 'Failed to mark messages as read' });
      }

      const sock = sessionManager.getSession(sessionId)?.sock;
      let receiptSent = false;

      if (sock?.user && rows?.length) {
        const keys = rows.map((row) => ({
          remoteJid: buildWhatsAppJid(remoteJid),
          id: row.message_id,
          participant: row.chat_type === 'group' && row.sender ? `${row.sender}@s.whatsapp.net` : undefined,
        }));

        try {
          await sock.readMessages(keys);
          receiptSent = true;
        } catch (receiptError) {
          logger.error(
            { err: receiptError, sessionId, remoteJid, messageIds },
            'Failed to send WhatsApp read receipt'
          );
        }
      }

      return res.json({
        success: true,
        readAt,
        receiptSent,
        updated: rows?.length || 0,
      });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error while marking messages as read');
      return res.status(500).json({ error: 'Failed to mark messages as read' });
    }
  });

  router.post('/sessions/:sessionId/chats/:phone/read-all', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    try {
      const [{ count, error: countError }, { data: recentUnread, error: recentError }] = await Promise.all([
        supabase
          .from('messages')
          .select('*', { count: 'exact', head: true })
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid)
          .eq('from_me', false)
          .is('read_at', null),
        supabase
          .from('messages')
          .select('message_id, sender, chat_type')
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid)
          .eq('from_me', false)
          .is('read_at', null)
          .order('timestamp', { ascending: false })
          .limit(20),
      ]);

      if (countError || recentError) {
        logger.error(
          { err: countError || recentError, sessionId, remoteJid },
          'Failed to load unread messages for bulk read'
        );
        return res.status(500).json({ error: 'Failed to mark all messages as read' });
      }

      const readAt = new Date().toISOString();
      const { error: updateError } = await supabase
        .from('messages')
        .update({ read_at: readAt })
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .eq('from_me', false)
        .is('read_at', null);

      if (updateError) {
        logger.error({ err: updateError, sessionId, remoteJid }, 'Failed to mark all as read');
        return res.status(500).json({ error: 'Failed to mark all messages as read' });
      }

      const sock = sessionManager.getSession(sessionId)?.sock;
      if (sock?.user && recentUnread?.length) {
        try {
          const jid = buildWhatsAppJid(remoteJid);
          const keys = recentUnread.map((row) => ({
            remoteJid: jid,
            id: row.message_id,
            participant: row.chat_type === 'group' && row.sender ? `${row.sender}@s.whatsapp.net` : undefined,
          }));

          await sock.readMessages(keys);
        } catch (receiptError) {
          logger.error({ err: receiptError, sessionId, remoteJid }, 'Failed to send bulk read receipt');
        }
      }

      return res.json({ success: true, readAt, updated: count || 0 });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error marking all as read');
      return res.status(500).json({ error: 'Failed to mark all messages as read' });
    }
  });

  router.get('/sessions/:sessionId/contacts/:phone', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    try {
      const { data, error } = await supabase
        .from('contacts_crm')
        .select('*')
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .maybeSingle();

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to fetch contact');
        return res.status(500).json({ error: 'Failed to fetch contact' });
      }

      return res.json(data || null);
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error fetching contact');
      return res.status(500).json({ error: 'Failed to fetch contact' });
    }
  });

  router.post('/sessions/:sessionId/contacts/:phone', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);
    const {
      firstName,
      lastName,
      role,
      company,
      city,
      responsibleManager,
      avatarUrl,
      notes,
      phone: phoneNumber,
    } = req.body ?? {};

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    if (!firstName || !firstName.trim()) {
      return res.status(400).json({ error: 'firstName is required' });
    }

    try {
      const payload = {
        session_id: sessionId,
        remote_jid: remoteJid,
        phone: phoneNumber || remoteJid,
        first_name: firstName.trim(),
        last_name: lastName?.trim() || null,
        role: role?.trim() || 'клиент',
        company: company?.trim() || null,
        city: city?.trim() || null,
        responsible_manager: responsibleManager?.trim() || null,
        avatar_url: avatarUrl || null,
        notes: notes?.trim() || null,
        updated_at: new Date().toISOString(),
      };

      const { data, error } = await supabase
        .from('contacts_crm')
        .upsert(payload, { onConflict: 'session_id,remote_jid' })
        .select()
        .single();

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to save contact');
        return res.status(500).json({ error: 'Failed to save contact' });
      }

      return res.json(data);
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error saving contact');
      return res.status(500).json({ error: 'Failed to save contact' });
    }
  });

  router.post('/sessions/:sessionId/contacts/:phone/avatar', upload.single('avatar'), async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    try {
      const result = await new Promise((resolve, reject) => {
        const uploadStream = cloudinary.uploader.upload_stream(
          {
            folder: 'omoikiri_crm/avatars',
            public_id: `${sessionId}_${remoteJid}`,
            overwrite: true,
            transformation: [
              { width: 200, height: 200, crop: 'fill', gravity: 'face' },
              { quality: 'auto', fetch_format: 'auto' },
            ],
          },
          (error, uploadResult) => {
            if (error) {
              reject(error);
              return;
            }

            resolve(uploadResult);
          }
        );

        uploadStream.end(req.file.buffer);
      });

      const avatarUrl = result?.secure_url || null;

      if (!avatarUrl) {
        return res.status(500).json({ error: 'Failed to upload avatar' });
      }

      const { error } = await supabase
        .from('contacts_crm')
        .update({ avatar_url: avatarUrl, updated_at: new Date().toISOString() })
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid);

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to update avatar URL');
        return res.status(500).json({ error: 'Failed to save avatar' });
      }

      return res.json({ success: true, avatarUrl });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Failed to upload avatar');
      return res.status(500).json({ error: 'Failed to upload avatar' });
    }
  });

  router.delete('/sessions/:sessionId/contacts/:phone', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    try {
      const { error } = await supabase
        .from('contacts_crm')
        .delete()
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid);

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to delete contact');
        return res.status(500).json({ error: 'Failed to delete contact' });
      }

      return res.json({ success: true });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error deleting contact');
      return res.status(500).json({ error: 'Failed to delete contact' });
    }
  });

  router.get('/sessions/:sessionId/contacts-crm', async (req, res) => {
    const { sessionId } = req.params;

    try {
      const { data, error } = await supabase
        .from('contacts_crm')
        .select('*')
        .eq('session_id', sessionId)
        .order('updated_at', { ascending: false });

      if (error) {
        logger.error({ err: error, sessionId }, 'Failed to fetch contacts list');
        return res.status(500).json({ error: 'Failed to fetch contacts' });
      }

      return res.json(data || []);
    } catch (error) {
      logger.error({ err: error, sessionId }, 'Unexpected error fetching contacts');
      return res.status(500).json({ error: 'Failed to fetch contacts' });
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

    if (message.length > 4096) {
      return res.status(400).json({ error: 'Message too long (max 4096 chars)' });
    }

    const session = sessionManager.getSession(sessionId);
    const sock = session?.sock;
    if (!sock?.user) {
      return res.status(503).json({ error: 'WhatsApp session is not connected' });
    }

    const limiter = getRateLimiter(sessionId);
    const jid = buildWhatsAppJid(phone);

    if (!limiter.canSend(jid) || !limiter.canSendGlobal()) {
      return res.status(429).json({ error: 'Rate limit exceeded' });
    }

    try {
      const result = await sendWithDelay(sock, jid, { text: message });
      limiter.recordSend(jid);

      const messageId = result?.key?.id ?? `sent-${Date.now()}`;

      // Save sent message directly to Supabase — don't rely on messages.upsert event
      const now = new Date().toISOString();
      try {
        const { saveMessage, upsertChat } = await import('../storage/queries.js');
        const { getPhoneToSession } = await import('../baileys/messageHandler.js');

        await saveMessage({
          messageId,
          sessionId,
          remoteJid: phone,
          fromMe: true,
          body: message,
          messageType: 'text',
          pushName: sock.user?.name || null,
          sender: null,
          chatType: phone.includes('-') ? 'group' : 'personal',
          mediaUrl: null,
          mediaType: null,
          fileName: null,
          timestamp: now,
        });

        await upsertChat({
          remoteJid: phone,
          sessionId,
          chatType: phone.includes('-') ? 'group' : 'personal',
          displayName: null,
          participantCount: null,
          phoneNumber: phone.includes('-') ? null : phone,
        });

        // Link to dialog session + AI pipeline
        try {
          const dialogSessionId = await getOrCreateDialogSession(sessionId, phone, now);
          if (dialogSessionId) {
            await supabase
              .from('messages')
              .update({ dialog_session_id: dialogSessionId })
              .eq('message_id', messageId)
              .eq('session_id', sessionId);

            await trackResponseTime(sessionId, phone, dialogSessionId, true, now);
            await enqueueForAI(sessionId, phone, messageId, dialogSessionId);
          }
        } catch (aiErr) {
          logger.error({ err: aiErr, sessionId, messageId }, 'AI pipeline failed for /send');
        }

        // Cross-session mirror: save as incoming for the other session
        const phoneMap = getPhoneToSession();
        const mirrorSessionId = phoneMap.get(phone);
        const myPhone = [...phoneMap.entries()].find(([, sid]) => sid === sessionId)?.[0];

        if (mirrorSessionId && mirrorSessionId !== sessionId && myPhone) {
          await saveMessage({
            messageId,
            sessionId: mirrorSessionId,
            remoteJid: myPhone,
            fromMe: false,
            body: message,
            messageType: 'text',
            pushName: sock.user?.name || null,
            sender: null,
            chatType: 'personal',
            mediaUrl: null,
            mediaType: null,
            fileName: null,
            timestamp: now,
          });

          await upsertChat({
            remoteJid: myPhone,
            sessionId: mirrorSessionId,
            chatType: 'personal',
            displayName: null,
            participantCount: null,
            phoneNumber: myPhone,
          });

          logger.info(`[/send] Mirrored to ${mirrorSessionId}: ${phone} → ${myPhone}`);
        }
      } catch (saveErr) {
        logger.error({ err: saveErr, sessionId, messageId }, 'Failed to save sent message to DB');
      }

      return res.json({
        success: true,
        messageId,
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
