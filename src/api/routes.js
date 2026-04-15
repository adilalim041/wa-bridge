import express from 'express';
import multer from 'multer';
import QRCode from 'qrcode';
import { v2 as cloudinary } from 'cloudinary';
import { handleAIChat } from '../ai/chatEndpoint.js';
import { runDailyAnalysis, isAnalysisRunning, backfillAutoTags, classifyUntaggedChats, reclassifyStuckContacts } from '../ai/aiWorker.js';
import { getOrCreateDialogSession } from '../ai/dialogSessions.js';
import { trackResponseTime } from '../ai/responseTracker.js';
import { getRateLimiter, sendWithDelay } from '../antiban/rateLimiter.js';
import { invalidateHiddenCache } from '../baileys/messageHandler.js';
import { sessionManager } from '../baileys/sessionManager.js';
import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';
import { getChatsWithLastMessage, getContacts, getMessages, getQueueStats, getLinkedSessions, getUnifiedMessages } from '../storage/queries.js';
import { sendTelegramMessage, isTelegramConfigured } from '../notifications/telegramBot.js';

const BRAND = process.env.BRAND_NAME || 'Omoikiri';

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

/**
 * Check if a remote_jid represents a real phone contact (not LID, not group).
 * Real phone numbers: 7-13 digits. LIDs: 14+ digits. Groups contain '-'.
 */
function isRealPhoneJid(jid) {
  if (!jid) return false;
  if (jid.includes('@g.us') || jid.includes('@lid') || jid.includes('-')) return false;
  const digits = jid.replace(/@.*$/, '').replace(/\D/g, '');
  return digits.length >= 7 && digits.length <= 13;
}

// Filter out garbage LID JIDs (too short or too long numeric IDs)
function isGarbageJid(jid) {
  if (!jid || typeof jid !== 'string') return true;
  if (jid.includes('-')) return false; // group JIDs are fine
  const digits = jid.replace(/\D/g, '');
  return digits.length < 7 || digits.length > 13;
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

  // On-demand daily analysis — accepts optional ?date=YYYY-MM-DD
  router.post('/ai/analyze', async (req, res) => {
    if (isAnalysisRunning()) {
      return res.json({ success: false, error: 'Анализ уже выполняется', running: true });
    }

    const date = req.query.date || req.body?.date || null; // YYYY-MM-DD or null (= today)
    res.json({ success: true, message: 'Анализ запущен', date: date || 'today' });

    runDailyAnalysis(date).then((result) => {
      logger.info(result, 'On-demand daily analysis finished');
    });
  });

  router.get('/ai/analyze/status', (req, res) => {
    res.json({ running: isAnalysisRunning() });
  });

  // One-time backfill: apply auto-tags from existing AI analyses
  router.post('/ai/backfill-tags', async (req, res) => {
    try {
      const result = await backfillAutoTags();
      res.json(result);
    } catch (err) {
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // Classify untagged chats using lightweight AI (no full analysis needed)
  router.post('/ai/classify-chats', async (req, res) => {
    try {
      const result = await classifyUntaggedChats();
      res.json(result);
    } catch (err) {
      logger.error({ err }, 'classify-chats endpoint error');
      res.status(500).json({ success: false, error: err.message });
    }
  });

  // Get list of dates that have analysis data
  router.get('/ai/analyze/dates', async (req, res) => {
    try {
      const sessionId = req.query.session_id || null;
      let query = supabase
        .from('chat_ai')
        .select('analysis_date')
        .not('analysis_date', 'is', null)
        .order('analysis_date', { ascending: false })
        .limit(90);

      if (sessionId) {
        query = query.eq('session_id', sessionId);
      }

      const { data, error } = await query;

      if (error) {
        return res.status(500).json({ error: error.message });
      }

      // Deduplicate dates and count analyses per date
      const dateMap = new Map();
      for (const row of data || []) {
        const d = row.analysis_date;
        dateMap.set(d, (dateMap.get(d) || 0) + 1);
      }

      const dates = Array.from(dateMap.entries()).map(([date, count]) => ({ date, count }));
      res.json({ dates });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  router.post('/ai/chat', async (req, res) => {
    if (!process.env.ANTHROPIC_API_KEY) {
      return res.status(503).json({
        error: 'AI chat is disabled — set ANTHROPIC_API_KEY env var to enable.',
      });
    }

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
    const dateFrom = req.query.date_from || req.query.date || null; // YYYY-MM-DD
    const dateTo = req.query.date_to || req.query.date || null;     // YYYY-MM-DD (same as from for single day)
    const days = Math.min(Number(req.query.days) || 7, 90);

    // Date range or days-based
    const rangeStart = dateFrom ? `${dateFrom}T00:00:00+05:00` : null;
    const rangeEnd = dateTo ? `${dateTo}T23:59:59+05:00` : null;
    const since = rangeStart || new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    try {
      // Build session filter
      const sessionFilter = (query) => sessionId ? query.eq('session_id', sessionId) : query;

      // 1. KPI: response times (adjusted for working hours 10:00-20:00 Almaty)
      let rtQuery = supabase
        .from('manager_analytics')
        .select('customer_message_at, manager_response_at')
        .gte('created_at', since)
        .not('manager_response_at', 'is', null);
      if (rangeEnd) rtQuery = rtQuery.lte('created_at', rangeEnd);

      const { data: responseTimes } = await sessionFilter(rtQuery);

      // Calculate working-hours-only response time for each entry
      const WORK_START = 10; // 10:00 Almaty
      const WORK_END = 20;   // 20:00 Almaty
      const ALMATY_OFFSET = 5 * 3600000; // UTC+5 in ms

      function calcWorkingSeconds(customerAt, managerAt) {
        if (!customerAt || !managerAt) return 0;
        const cAlmaty = new Date(new Date(customerAt).getTime() + ALMATY_OFFSET);
        const mAlmaty = new Date(new Date(managerAt).getTime() + ALMATY_OFFSET);
        // Clamp customer time to work hours start
        const cHour = cAlmaty.getUTCHours() + cAlmaty.getUTCMinutes() / 60;
        const start = new Date(cAlmaty);
        if (cHour < WORK_START) {
          start.setUTCHours(WORK_START, 0, 0, 0);
        } else if (cHour >= WORK_END) {
          start.setUTCDate(start.getUTCDate() + 1);
          start.setUTCHours(WORK_START, 0, 0, 0);
        }
        let total = 0;
        const cur = new Date(start);
        for (let safety = 0; safety < 30 && cur < mAlmaty; safety++) {
          const dayEnd = new Date(cur);
          dayEnd.setUTCHours(WORK_END, 0, 0, 0);
          if (dayEnd > mAlmaty) {
            total += (mAlmaty - cur) / 1000;
            break;
          }
          total += (dayEnd - cur) / 1000;
          cur.setUTCDate(cur.getUTCDate() + 1);
          cur.setUTCHours(WORK_START, 0, 0, 0);
        }
        return Math.max(0, Math.round(total));
      }

      const rtValues = (responseTimes ?? [])
        .map((r) => calcWorkingSeconds(r.customer_message_at, r.manager_response_at))
        .filter((v) => v > 0);
      const avgResponseTime = rtValues.length > 0
        ? Math.round(rtValues.reduce((a, b) => a + b, 0) / rtValues.length)
        : 0;

      // 2. AI analysis breakdown — filter by analysis_date range or days
      let aiQuery = supabase
        .from('chat_ai')
        .select('lead_temperature, deal_stage, sentiment, risk_flags, action_required, session_id, remote_jid, summary_ru, action_suggestion, customer_type, consultation_score, consultation_details, followup_status, manager_issues');

      if (dateFrom && dateTo && dateFrom === dateTo) {
        aiQuery = aiQuery.eq('analysis_date', dateFrom);
      } else if (dateFrom) {
        aiQuery = aiQuery.gte('analysis_date', dateFrom);
        if (dateTo) aiQuery = aiQuery.lte('analysis_date', dateTo);
      } else {
        // Convert days to date string for analysis_date filter
        const sinceDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000 + 5 * 3600000);
        const sinceDateStr = sinceDate.toISOString().slice(0, 10);
        aiQuery = aiQuery.gte('analysis_date', sinceDateStr);
      }

      const { data: aiData } = await sessionFilter(aiQuery);

      const ai = (aiData ?? []).filter((row) => !isGarbageJid(row.remote_jid));

      // Lead temperature
      const leads = { hot: 0, warm: 0, cold: 0, dead: 0 };
      for (const row of ai) {
        if (leads[row.lead_temperature] !== undefined) leads[row.lead_temperature]++;
      }

      // Deal stages — always include all valid stages (even if 0) for consistent frontend rendering
      const stages = { needs_review: 0, first_contact: 0, consultation: 0, model_selection: 0, price_negotiation: 0, payment: 0, delivery: 0, completed: 0, refused: 0 };
      for (const row of ai) {
        if (stages[row.deal_stage] !== undefined) stages[row.deal_stage]++;
        else stages[row.deal_stage] = 1;
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

      // Action required chats (show up to 50 — counter was showing total but list only 5)
      const actionChats = ai
        .filter((r) => r.action_required)
        .slice(0, 50)
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
      let dialogQuery = supabase
        .from('dialog_sessions')
        .select('*', { count: 'exact', head: true })
        .gte('started_at', since);
      if (rangeEnd) dialogQuery = dialogQuery.lte('started_at', rangeEnd);
      const { count: totalDialogs } = await sessionFilter(dialogQuery);

      // 4. Daily message trend — only fetch timestamp column, limit to 10k max
      let msgQuery = supabase
        .from('messages')
        .select('timestamp')
        .gte('timestamp', since)
        .order('timestamp', { ascending: true })
        .limit(10000);
      if (rangeEnd) msgQuery = msgQuery.lte('timestamp', rangeEnd);
      const { data: dailyMessages } = await sessionFilter(msgQuery);

      const dailyMap = {};
      for (const msg of dailyMessages ?? []) {
        const day = msg.timestamp.substring(0, 10);
        dailyMap[day] = (dailyMap[day] || 0) + 1;
      }
      const dailyStats = Object.entries(dailyMap)
        .map(([date, messages]) => ({ date, messages }))
        .sort((a, b) => a.date.localeCompare(b.date));

      return res.json({
        period: { days, since, dateFrom: dateFrom || null, dateTo: dateTo || null },
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
        hasQR: Boolean(state.qr),
        // Don't expose user names in unauthenticated health endpoint
      };
    }

    res.json({
      status: 'ok',
      sessions,
      uptime: process.uptime(),
      messageQueue: getQueueStats(),
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

  // Force-restart a session (kills socket, reconnects fresh)
  router.post('/sessions/:sessionId/restart', async (req, res) => {
    const { sessionId } = req.params;
    const { data: config } = await supabase
      .from('session_config')
      .select('session_id')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (!config) {
      return res.status(404).json({ error: 'Session not found' });
    }

    logger.info({ sessionId }, 'Force-restarting session');
    await sessionManager.stopSession(sessionId);
    // Small delay to ensure clean disconnect
    await new Promise((r) => setTimeout(r, 2000));
    await sessionManager.startSession(sessionId);
    const state = sessionManager.getSessionState(sessionId);
    return res.json({ success: true, sessionId, connected: state.connected, hasQR: Boolean(state.qr) });
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
    const limit = Math.min(Number(req.query.limit) || 2000, 2000);
    const offset = Number(req.query.offset) || 0;
    try {
      const allChats = await getChatsWithLastMessage(sessionId);
      const paginated = offset > 0 || limit < 2000
        ? allChats.slice(offset, offset + limit)
        : allChats;
      return res.json(paginated);
    } catch (error) {
      logger.error({ err: error, sessionId }, 'Failed to fetch chats');
      return res.status(500).json({ error: 'Failed to fetch chats' });
    }
  });

  // Unified chats across all sessions — server-side aggregation with pagination
  router.get('/chats/all', async (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 100, 500);
    const offset = Number(req.query.offset) || 0;
    try {
      // Get all active sessions
      const { data: sessions } = await supabase
        .from('session_config')
        .select('session_id, display_name')
        .eq('is_active', true);

      if (!sessions?.length) {
        return res.json({ chats: [], total: 0, hasMore: false });
      }

      // Fetch chats from all sessions in parallel (cached, 10s TTL)
      const allPromises = sessions.map(async (s) => {
        const chats = await getChatsWithLastMessage(s.session_id);
        return chats.map((chat) => ({
          ...chat,
          _sessionId: s.session_id,
          _sessionName: s.display_name,
        }));
      });

      const allResults = await Promise.all(allPromises);
      const merged = allResults.flat().sort((a, b) =>
        new Date(b.lastTimestamp) - new Date(a.lastTimestamp)
      );

      const paginated = merged.slice(offset, offset + limit);
      return res.json({
        chats: paginated,
        total: merged.length,
        hasMore: offset + limit < merged.length,
      });
    } catch (error) {
      logger.error({ err: error }, 'Failed to fetch unified chats');
      return res.status(500).json({ error: 'Failed to fetch unified chats' });
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
            tag_confirmed: true, // manual tag update = confirmed
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

  // Confirm (or change + confirm) a chat tag
  router.post('/sessions/:sessionId/chats/:phone/confirm-tag', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);
    const { tag } = req.body; // optional — if provided, change the tag AND confirm

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    const VALID_TAGS = ['клиент', 'сотрудник', 'партнёр', 'неизвестно'];

    try {
      const updates = { tag_confirmed: true, updated_at: new Date().toISOString() };
      if (tag) {
        if (!VALID_TAGS.includes(tag)) {
          return res.status(400).json({ error: 'Invalid tag. Must be one of: ' + VALID_TAGS.join(', ') });
        }
        // Replace ALL tags with just this one confirmed tag
        updates.tags = [tag];
      }

      const { error } = await supabase
        .from('chats')
        .update(updates)
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid);

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to confirm tag');
        return res.status(500).json({ error: 'Failed to confirm tag' });
      }

      return res.json({ success: true });
    } catch (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error confirming tag');
      return res.status(500).json({ error: 'Failed to confirm tag' });
    }
  });

  // Full-text message search across all chats
  router.get('/messages/search', async (req, res) => {
    try {
      const { q, session_id, limit = 50, offset = 0 } = req.query;

      if (!q || q.length < 2) {
        return res.status(400).json({ error: 'Query must be at least 2 characters' });
      }
      if (q.length > 200) {
        return res.status(400).json({ error: 'Query too long (max 200)' });
      }

      // Use ilike for case-insensitive partial match
      let query = supabase
        .from('messages')
        .select('message_id, session_id, remote_jid, body, from_me, message_type, timestamp, push_name')
        .ilike('body', `%${q}%`)
        .not('body', 'is', null)
        .order('timestamp', { ascending: false })
        .range(+offset, +offset + +limit - 1);

      if (session_id && session_id !== '__all__') {
        query = query.eq('session_id', session_id);
      }

      // Filter out LID and group JIDs
      query = query.not('remote_jid', 'like', '%@g.us');
      query = query.not('remote_jid', 'like', '%@lid');

      const { data, error } = await query;
      if (error) throw error;

      // Enrich with contact names
      const jids = [...new Set((data || []).map((m) => m.remote_jid))];
      const nameMap = new Map();

      if (jids.length) {
        // CRM contacts
        const { data: contacts } = await supabase
          .from('contacts_crm')
          .select('remote_jid, first_name, last_name')
          .in('remote_jid', jids);
        for (const c of contacts || []) {
          nameMap.set(c.remote_jid, `${c.first_name || ''} ${c.last_name || ''}`.trim());
        }

        // Chat display names fallback
        const { data: chats } = await supabase
          .from('chats')
          .select('remote_jid, display_name')
          .in('remote_jid', jids);
        for (const c of chats || []) {
          if (!nameMap.has(c.remote_jid) && c.display_name) {
            nameMap.set(c.remote_jid, c.display_name);
          }
        }
      }

      const results = (data || []).map((msg) => ({
        messageId: msg.message_id,
        sessionId: msg.session_id,
        remoteJid: msg.remote_jid,
        body: msg.body,
        fromMe: msg.from_me,
        messageType: msg.message_type,
        timestamp: msg.timestamp,
        pushName: msg.push_name,
        contactName: nameMap.get(msg.remote_jid) || msg.push_name || msg.remote_jid.replace(/@.*$/, ''),
      }));

      // Count total matches (for "N results found" display)
      let countQuery = supabase
        .from('messages')
        .select('message_id', { count: 'exact', head: true })
        .ilike('body', `%${q}%`)
        .not('body', 'is', null)
        .not('remote_jid', 'like', '%@g.us')
        .not('remote_jid', 'like', '%@lid');

      if (session_id && session_id !== '__all__') {
        countQuery = countQuery.eq('session_id', session_id);
      }

      const { count: totalCount } = await countQuery;

      res.json({ results, total: totalCount || results.length, query: q });
    } catch (error) {
      logger.error({ err: error }, 'Failed to search messages');
      res.status(500).json({ error: error.message });
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

  // Cross-session: find all sessions where this contact exists
  router.get('/contacts/:phone/linked-sessions', async (req, res) => {
    const phone = normalizeChatId(req.params.phone);
    try {
      const sessions = await getLinkedSessions(phone);
      return res.json(sessions);
    } catch (error) {
      logger.error({ err: error, phone }, 'Failed to fetch linked sessions');
      return res.status(500).json({ error: 'Failed to fetch linked sessions' });
    }
  });

  // Cross-session: get unified messages from ALL sessions for one contact
  router.get('/contacts/:phone/unified-messages', async (req, res) => {
    const phone = normalizeChatId(req.params.phone);
    const limit = Number.parseInt(req.query.limit, 10) || 50;
    const offset = Number.parseInt(req.query.offset, 10) || 0;
    try {
      const messages = await getUnifiedMessages(phone, limit, offset);
      return res.json(messages);
    } catch (error) {
      logger.error({ err: error, phone }, 'Failed to fetch unified messages');
      return res.status(500).json({ error: 'Failed to fetch unified messages' });
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
    const stealth = req.query.stealth === 'true' || req.body?.stealth === true;

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

      // Stealth mode: update DB (reset unread in dashboard) but do NOT send WhatsApp read receipt
      // This way the client never sees blue checkmarks
      if (!stealth) {
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
      }

      return res.json({ success: true, readAt, updated: count || 0, stealth });
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
      dealValue,
    } = req.body ?? {};

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    if (!firstName || !firstName.trim()) {
      return res.status(400).json({ error: 'firstName is required' });
    }

    // Input length validation — prevent oversized payloads
    const MAX_LEN = 500;
    for (const [key, val] of Object.entries({ firstName, lastName, role, company, city, responsibleManager })) {
      if (val && String(val).length > MAX_LEN) {
        return res.status(400).json({ error: `${key} too long (max ${MAX_LEN} chars)` });
      }
    }
    if (notes && String(notes).length > 5000) {
      return res.status(400).json({ error: 'notes too long (max 5000 chars)' });
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
        deal_value: dealValue != null ? dealValue : undefined,
        updated_at: new Date().toISOString(),
      };

      // Remove undefined keys so they don't overwrite existing values
      for (const key of Object.keys(payload)) {
        if (payload[key] === undefined) delete payload[key];
      }

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
      let query = supabase
        .from('contacts_crm')
        .select('*')
        .order('updated_at', { ascending: false });

      // Support __all__ to fetch contacts across all sessions
      if (sessionId && sessionId !== '__all__') {
        query = query.eq('session_id', sessionId);
      }

      const { data, error } = await query;

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

  // ── CRM Funnel ───────────────────────────────────────────────────────
  // Returns contacts grouped by deal stage (latest AI analysis per chat)
  router.get('/crm/funnel', async (req, res) => {
    const sessionId = req.query.session_id;
    try {
      // Get latest deal_stage per remote_jid from chat_ai
      let query = supabase
        .from('chat_ai')
        .select('session_id, remote_jid, deal_stage, customer_type, lead_temperature, analysis_date, sentiment, stage_source')
        .order('analysis_date', { ascending: false });

      if (sessionId && sessionId !== '__all__') {
        query = query.eq('session_id', sessionId);
      }

      const { data: analyses, error: aiErr } = await query;
      if (aiErr) throw aiErr;

      // Keep only latest analysis per real phone contact (skip LIDs, groups)
      const latestByJid = new Map();
      for (const row of analyses || []) {
        const key = row.remote_jid;
        if (!isRealPhoneJid(key)) continue;
        if (!latestByJid.has(key) || row.analysis_date > latestByJid.get(key).analysis_date) {
          latestByJid.set(key, row);
        }
      }

      // Get CRM contact info and chat display names
      const jids = [...latestByJid.keys()];
      if (!jids.length) return res.json({ stages: {}, contacts: [] });

      // Fetch CRM data
      let crmQuery = supabase.from('contacts_crm').select('*');
      if (sessionId && sessionId !== '__all__') {
        crmQuery = crmQuery.eq('session_id', sessionId);
      }
      const { data: crmContacts } = await crmQuery;
      const crmMap = new Map();
      for (const c of crmContacts || []) {
        crmMap.set(c.remote_jid, c);
      }

      // Fetch chat display names
      let chatQuery = supabase.from('chats').select('session_id, remote_jid, display_name, tags, last_message_body, last_message_timestamp');
      if (sessionId && sessionId !== '__all__') {
        chatQuery = chatQuery.eq('session_id', sessionId);
      }
      const { data: chatRows } = await chatQuery;
      const chatMap = new Map();
      for (const c of chatRows || []) {
        if (!chatMap.has(c.remote_jid)) chatMap.set(c.remote_jid, c);
      }

      // Fetch push_name from messages for contacts without display_name
      const jidsNeedingName = jids.filter((jid) => {
        const crm = crmMap.get(jid);
        const chat = chatMap.get(jid);
        return !crm && !chat?.display_name;
      });
      const pushNameMap = new Map();
      if (jidsNeedingName.length) {
        // Get latest push_name for each jid (from incoming messages)
        for (let i = 0; i < jidsNeedingName.length; i += 50) {
          const batch = jidsNeedingName.slice(i, i + 50);
          const { data: msgs } = await supabase
            .from('messages')
            .select('remote_jid, push_name')
            .in('remote_jid', batch)
            .eq('from_me', false)
            .not('push_name', 'is', null)
            .order('timestamp', { ascending: false })
            .limit(batch.length);
          for (const m of msgs || []) {
            if (m.push_name && !pushNameMap.has(m.remote_jid)) {
              pushNameMap.set(m.remote_jid, m.push_name);
            }
          }
        }
      }

      // Build contacts with stages
      const contacts = [];
      for (const [jid, ai] of latestByJid) {
        const crm = crmMap.get(jid);
        const chat = chatMap.get(jid);
        const phone = jid.replace(/@s\.whatsapp\.net$/, '');
        const name = crm
          ? `${crm.first_name} ${crm.last_name || ''}`.trim()
          : (chat?.display_name || pushNameMap.get(jid) || phone);

        contacts.push({
          remoteJid: jid,
          phone,
          sessionId: ai.session_id,
          displayName: name,
          dealStage: ai.deal_stage || 'needs_review',
          customerType: ai.customer_type,
          leadTemperature: ai.lead_temperature,
          sentiment: ai.sentiment,
          analysisDate: ai.analysis_date,
          lastMessage: chat?.last_message_body,
          lastMessageAt: chat?.last_message_timestamp,
          avatarUrl: crm?.avatar_url,
          city: crm?.city,
          company: crm?.company,
          dealValue: crm?.deal_value || null,
          tags: chat?.tags || [],
          stageSource: ai.stage_source || null,
        });
      }

      // Group by stage
      const stages = {};
      const STAGE_ORDER = ['needs_review', 'first_contact', 'consultation', 'model_selection', 'price_negotiation', 'payment', 'delivery', 'completed', 'refused'];
      for (const s of STAGE_ORDER) stages[s] = [];
      for (const c of contacts) {
        const stage = stages[c.dealStage] ? c.dealStage : 'needs_review';
        stages[stage].push(c);
      }

      return res.json({ stages, total: contacts.length });
    } catch (error) {
      logger.error({ err: error }, 'Failed to fetch CRM funnel');
      return res.status(500).json({ error: 'Failed to fetch funnel data' });
    }
  });

  // Update deal stage manually
  router.post('/crm/deal-stage', async (req, res) => {
    const { sessionId, remoteJid, dealStage } = req.body || {};
    const VALID_STAGES = ['needs_review', 'first_contact', 'consultation', 'model_selection', 'price_negotiation', 'payment', 'delivery', 'completed', 'refused'];

    if (!sessionId || !remoteJid || !VALID_STAGES.includes(dealStage)) {
      return res.status(400).json({ error: 'sessionId, remoteJid, and valid dealStage required' });
    }

    try {
      // Update the latest chat_ai record for this contact
      const { data: latest } = await supabase
        .from('chat_ai')
        .select('id')
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .order('analysis_date', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (latest) {
        await supabase.from('chat_ai').update({
          deal_stage: dealStage,
          stage_source: 'manual',
          stage_changed_at: new Date().toISOString(),
        }).eq('id', latest.id);
      } else {
        // No AI analysis yet — create a minimal record
        await supabase.from('chat_ai').insert({
          session_id: sessionId,
          remote_jid: remoteJid,
          deal_stage: dealStage,
          analysis_date: new Date().toISOString().slice(0, 10),
          customer_type: 'unknown',
          stage_source: 'manual',
          stage_changed_at: new Date().toISOString(),
        });
      }

      return res.json({ success: true });
    } catch (error) {
      logger.error({ err: error }, 'Failed to update deal stage');
      return res.status(500).json({ error: 'Failed to update deal stage' });
    }
  });

  // Re-classify contacts stuck at first_contact (rate limited: 1x per hour)
  router.post('/crm/reclassify-stuck', async (req, res) => {
    try {
      const result = await reclassifyStuckContacts();
      return res.json(result);
    } catch (error) {
      logger.error({ err: error }, 'Failed to reclassify stuck contacts');
      return res.status(500).json({ error: 'Failed to reclassify stuck contacts' });
    }
  });

  // ── Tasks & Reminders CRUD ──────────────────────────────────────────────

  router.get('/tasks/stats', async (req, res) => {
    try {
      const { session_id } = req.query;
      const now = new Date().toISOString();
      const todayStart = new Date();
      todayStart.setHours(0, 0, 0, 0);

      let baseQuery = supabase.from('tasks').select('id, status, due_date, completed_at');
      if (session_id && session_id !== '__all__') {
        baseQuery = baseQuery.eq('session_id', session_id);
      }

      const { data: all } = await baseQuery;
      const tasks = all || [];

      const pending = tasks.filter(t => t.status === 'pending').length;
      const overdue = tasks.filter(t => t.status === 'pending' && t.due_date < now).length;
      const completedToday = tasks.filter(t => t.status === 'completed' && t.completed_at >= todayStart.toISOString()).length;
      const upcomingToday = tasks.filter(t => t.status === 'pending' && t.due_date >= todayStart.toISOString() && t.due_date < new Date(todayStart.getTime() + 86400000).toISOString()).length;
      const total = tasks.length;

      res.json({ total, pending, overdue, completedToday, upcomingToday });
    } catch (error) {
      logger.error({ err: error }, 'Failed to fetch task stats');
      res.status(500).json({ error: error.message });
    }
  });

  router.get('/tasks', async (req, res) => {
    try {
      const { session_id, status, remote_jid, limit, offset } = req.query;
      const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
      const safeOffset = Math.max(Number(offset) || 0, 0);

      let query = supabase
        .from('tasks')
        .select('*')
        .order('due_date', { ascending: true })
        .range(safeOffset, safeOffset + safeLimit - 1);

      if (session_id && session_id !== '__all__') {
        query = query.eq('session_id', session_id);
      }
      if (remote_jid) {
        query = query.eq('remote_jid', remote_jid);
      }
      if (status === 'overdue') {
        query = query.eq('status', 'pending').lt('due_date', new Date().toISOString());
      } else if (status && status !== 'all') {
        query = query.eq('status', status);
      }

      const { data, error } = await query;
      if (error) throw error;

      // Enrich with contact info
      const jids = [...new Set((data || []).filter(t => t.remote_jid).map(t => t.remote_jid))];
      const contactMap = new Map();
      if (jids.length) {
        const [{ data: contacts }, { data: chats }] = await Promise.all([
          supabase.from('contacts_crm').select('remote_jid, first_name, last_name, company').in('remote_jid', jids),
          supabase.from('chats').select('remote_jid, display_name').in('remote_jid', jids),
        ]);
        for (const c of contacts || []) {
          contactMap.set(c.remote_jid, c);
        }
        for (const c of chats || []) {
          if (!contactMap.has(c.remote_jid) && c.display_name) {
            contactMap.set(c.remote_jid, { first_name: c.display_name, last_name: '' });
          }
        }
      }

      const enriched = (data || []).map(task => {
        const contact = contactMap.get(task.remote_jid);
        return {
          ...task,
          contactName: contact ? `${contact.first_name || ''} ${contact.last_name || ''}`.trim() : null,
          contactCompany: contact?.company || null,
        };
      });

      res.json({ tasks: enriched });
    } catch (error) {
      logger.error({ err: error }, 'Failed to fetch tasks');
      res.status(500).json({ error: error.message });
    }
  });

  router.post('/tasks', async (req, res) => {
    try {
      const { sessionId, remoteJid, title, description, taskType, priority, dueDate, assignedTo, dealValue, notes } = req.body;

      if (!sessionId || !title || !dueDate) {
        return res.status(400).json({ error: 'sessionId, title, and dueDate are required' });
      }
      if (title.length > 200) return res.status(400).json({ error: 'Title too long (max 200)' });
      if (description && description.length > 2000) return res.status(400).json({ error: 'Description too long (max 2000)' });

      const VALID_TYPES = ['follow_up', 'call_back', 'send_quote', 'send_catalog', 'visit_showroom', 'custom'];
      const VALID_PRIORITIES = ['low', 'medium', 'high', 'urgent'];

      const { data, error } = await supabase
        .from('tasks')
        .insert({
          session_id: sessionId,
          remote_jid: remoteJid || null,
          title: title.trim(),
          description: description?.trim() || null,
          task_type: VALID_TYPES.includes(taskType) ? taskType : 'follow_up',
          priority: VALID_PRIORITIES.includes(priority) ? priority : 'medium',
          status: 'pending',
          due_date: dueDate,
          assigned_to: assignedTo || null,
          deal_value: dealValue || null,
          notes: notes?.trim() || null,
          created_by: 'manual',
        })
        .select()
        .single();

      if (error) throw error;
      res.json({ task: data });
    } catch (error) {
      logger.error({ err: error }, 'Failed to create task');
      res.status(500).json({ error: error.message });
    }
  });

  router.patch('/tasks/:id', async (req, res) => {
    try {
      const { id } = req.params;
      const updates = { ...req.body, updated_at: new Date().toISOString() };

      // Auto-set completed_at
      if (updates.status === 'completed' && !updates.completed_at) {
        updates.completed_at = new Date().toISOString();
      }
      if (updates.status === 'pending') {
        updates.completed_at = null;
      }

      // Sanitize — only allow valid fields
      const ALLOWED = ['title', 'description', 'task_type', 'priority', 'status', 'due_date', 'completed_at', 'assigned_to', 'deal_value', 'notes', 'remote_jid', 'updated_at'];
      const sanitized = {};
      for (const key of ALLOWED) {
        if (key in updates) sanitized[key] = updates[key];
      }

      if (sanitized.title && sanitized.title.length > 200) {
        return res.status(400).json({ error: 'Title must be under 200 characters' });
      }
      if (sanitized.description && sanitized.description.length > 2000) {
        return res.status(400).json({ error: 'Description must be under 2000 characters' });
      }

      const { data, error } = await supabase
        .from('tasks')
        .update(sanitized)
        .eq('id', id)
        .select()
        .single();

      if (error) throw error;
      res.json({ task: data });
    } catch (error) {
      logger.error({ err: error }, 'Failed to update task');
      res.status(500).json({ error: error.message });
    }
  });

  router.delete('/tasks/:id', async (req, res) => {
    try {
      const { id } = req.params;
      const { session_id } = req.query;

      // If session_id provided, verify the task belongs to that session before deleting
      if (session_id) {
        const { data: task, error: fetchErr } = await supabase
          .from('tasks')
          .select('id, session_id')
          .eq('id', id)
          .maybeSingle();
        if (fetchErr) throw fetchErr;
        if (!task) return res.status(404).json({ error: 'Task not found' });
        if (task.session_id !== session_id) {
          return res.status(403).json({ error: 'Task does not belong to this session' });
        }
      }

      const { error } = await supabase.from('tasks').delete().eq('id', id);
      if (error) throw error;
      res.json({ success: true });
    } catch (error) {
      logger.error({ err: error }, 'Failed to delete task');
      res.status(500).json({ error: error.message });
    }
  });

  // ── End Tasks & Reminders ──────────────────────────────────────────────

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

  // ── Monthly report ────────────────────────────────────────────────────
  router.get('/reports/monthly', async (req, res) => {
    try {
      const { month } = req.query; // format: "2026-03"
      if (!month || !/^\d{4}-\d{2}$/.test(month)) {
        return res.status(400).json({ error: 'month parameter required (format: YYYY-MM)' });
      }

      const startDate = `${month}-01`;
      const endMonth = new Date(startDate);
      endMonth.setMonth(endMonth.getMonth() + 1);
      const endDate = endMonth.toISOString().slice(0, 10);

      // 1. Total unique chats with messages this month (ordered desc so first per key = latest)
      const { data: msgStats } = await supabase
        .from('messages')
        .select('remote_jid, session_id, from_me')
        .gte('timestamp', startDate)
        .lt('timestamp', endDate)
        .not('remote_jid', 'like', '%@g.us')
        .order('timestamp', { ascending: false });

      const uniqueChats = new Set((msgStats || []).map(m => `${m.session_id}:${m.remote_jid}`));
      const totalConversations = uniqueChats.size;

      // Messages from clients vs from managers
      const fromClients = (msgStats || []).filter(m => !m.from_me).length;
      const fromManagers = (msgStats || []).filter(m => m.from_me).length;

      // 2. Tasks created this month
      const { data: tasks } = await supabase
        .from('tasks')
        .select('id, status, task_type, created_by, completed_at')
        .gte('created_at', startDate)
        .lt('created_at', endDate);

      const taskStats = {
        total: (tasks || []).length,
        completed: (tasks || []).filter(t => t.status === 'completed').length,
        pending: (tasks || []).filter(t => t.status === 'pending').length,
        autoFollowUp: (tasks || []).filter(t => t.created_by === 'auto_followup').length,
      };

      // 3. AI analysis results this month
      const { data: analyses } = await supabase
        .from('chat_ai')
        .select('customer_type, lead_temperature, deal_stage, manager_issues')
        .gte('analysis_date', startDate)
        .lt('analysis_date', endDate);

      const customerTypes = {};
      const dealStages = {};
      let hotLeads = 0;
      let managerIssueCount = 0;

      for (const a of analyses || []) {
        const ct = a.customer_type || 'unknown';
        customerTypes[ct] = (customerTypes[ct] || 0) + 1;

        const ds = a.deal_stage || 'unknown';
        dealStages[ds] = (dealStages[ds] || 0) + 1;

        if (a.lead_temperature === 'hot') hotLeads++;
        if (a.manager_issues?.length > 0) managerIssueCount++;
      }

      // 4. Unanswered clients (last message from client, no reply)
      // msgStats is ordered by timestamp desc, so first entry per key = latest message
      const chatLastMsg = new Map();
      for (const m of msgStats || []) {
        const key = `${m.session_id}:${m.remote_jid}`;
        if (!chatLastMsg.has(key)) {
          chatLastMsg.set(key, m);
        }
      }
      const unansweredCount = [...chatLastMsg.values()].filter(m => !m.from_me).length;

      res.json({
        month,
        totalConversations,
        messages: { fromClients, fromManagers, total: fromClients + fromManagers },
        tasks: taskStats,
        analysis: { total: (analyses || []).length, customerTypes, dealStages, hotLeads, managerIssueCount },
        unansweredAtEndOfMonth: unansweredCount,
      });
    } catch (error) {
      logger.error({ err: error }, 'Failed to generate monthly report');
      res.status(500).json({ error: error.message });
    }
  });

  // ── Notification endpoints ──

  router.get('/notifications/status', (_req, res) => {
    res.json({
      telegram: isTelegramConfigured(),
      message: isTelegramConfigured()
        ? 'Telegram configured'
        : 'Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars',
    });
  });

  let lastTestNotification = 0;
  router.post('/notifications/test', async (_req, res) => {
    const now = Date.now();
    if (now - lastTestNotification < 60000) {
      return res.status(429).json({ error: 'Test notification rate limited. Wait 1 minute.' });
    }
    lastTestNotification = now;

    if (!isTelegramConfigured()) {
      return res.json({ success: false, message: 'Telegram not configured' });
    }
    try {
      await sendTelegramMessage(`<b>Тест уведомлений ${BRAND}.AI</b>\n\nУведомления работают!`);
      res.json({ success: true });
    } catch (err) {
      logger.error({ err }, 'Test notification failed');
      res.status(500).json({ success: false, error: err.message });
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
