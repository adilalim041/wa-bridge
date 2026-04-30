import express from 'express';
import rateLimit from 'express-rate-limit';
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
import { getChatsWithLastMessage, getContacts, getMessages, getQueueStats, getLinkedSessions, getUnifiedMessages, getCallsBySession, getCallsByChat, getCallsKpi, formatCallRow, getChatTags, getChatTagsByJids, upsertChatTags, insertManagerReport, listManagerReports, markChatAiReportSent, getChatAiById, getActiveSessions, getTenantSettings, upsertTenantSettings, getFunnelStages, createFunnelStage, updateFunnelStage, deleteFunnelStage, reorderFunnelStages } from '../storage/queries.js';
import { z } from 'zod';
import { sendTelegramMessage, isTelegramConfigured } from '../notifications/telegramBot.js';
import { generateCoachingComment } from '../ai/coachingGenerator.js';
import { uploadReportPdf } from '../reports/uploadPdfToCloudinary.js';
import { resolveSender } from '../reports/routing.js';
import { isSentryEnabled } from '../observability/sentry.js';
import { runCloudinaryCleanup, getCleanupConfig } from '../cleanup/cloudinaryCleanup.js';
import * as salesCrm from '../lib/salesCrm.js';

const BRAND = process.env.BRAND_NAME || 'Omoikiri';

// ---------------------------------------------------------------------------
// In-process cache for /analytics/summary
// ---------------------------------------------------------------------------
// Cache key is composite: `${sessionId}|...|${userId}` where userId is:
//   - '__service__' when called via x-api-key (internal workers, admin, no req.user)
//   - req.user.id (Supabase auth UID) when called via user JWT (dashboard requests)
// This prevents cross-tenant cache bleed: service_role rows must NOT be served to
// a lower-privilege user client that goes through RLS.
const analyticsCache = new Map(); // `${sessionId}|dateFrom|dateTo|days|userId` → { data, expiresAt }
const ANALYTICS_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

function makeAnalyticsCacheKey(sessionId, dateFrom, dateTo, days, userId) {
  return `${sessionId || '_all'}|${dateFrom || ''}|${dateTo || ''}|${days}|${userId ?? '__service__'}`;
}

function getAnalyticsCache(key) {
  const entry = analyticsCache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    analyticsCache.delete(key);
    return null;
  }
  return entry.data;
}

function setAnalyticsCache(key, data) {
  analyticsCache.set(key, { data, expiresAt: Date.now() + ANALYTICS_CACHE_TTL_MS });
}

function invalidateAnalyticsCache() {
  analyticsCache.clear();
}

// ---------------------------------------------------------------------------
// Per-(user, session) rate-limiter for the reconnect endpoint.
// WhatsApp flags frequent reconnects (auth_state wipe + Baileys restart) as
// suspicious device behaviour — ban-risk. 3 reconnects/hour per user+session
// is a hard ceiling. keyGenerator falls back to IP if req.user is absent
// (should not happen for JWT-authenticated endpoints, but defensive).
// ---------------------------------------------------------------------------
const reconnectLimiter = rateLimit({
  windowMs: 60 * 60 * 1000, // 1 hour
  max: 3,
  keyGenerator: (req) => `${req.user?.userId ?? req.ip}:${req.params.sessionId}`,
  standardHeaders: true,
  legacyHeaders: false,
  message: {
    error:
      'Слишком частое переподключение. Попробуй через час — WhatsApp может пометить устройство как подозрительное.',
  },
});

// Call-row serialization is shared with the WebSocket emit path — see
// formatCallRow in src/storage/queries.js. Keep the REST response and the
// real-time event shape in sync via that single source of truth.

// Allowed values for validation
const VALID_TEMPERATURES = new Set(['hot', 'warm', 'cold', 'dead']);
// NOTE: deal_stage is no longer validated against a hardcoded whitelist.
// Valid stages are tenant-defined in the funnel_stages table.
// Soft validation (string, 1-40 chars) is applied at the handler level.
const VALID_MANAGER_ISSUES = new Set([
  'slow_first_response', 'no_followup', 'poor_consultation', 'no_photos',
  'no_showroom_invite', 'no_upsell', 'rude_tone', 'formal_tone', 'no_alternative',
]);

// ---------------------------------------------------------------------------
// Zod schemas for tenant_settings + funnel_stages endpoints
// ---------------------------------------------------------------------------

const labelArray = z
  .array(z.string().min(1).max(30))
  .max(30);

// Longer label array for lead_sources / refusal_reasons / task_types
const labelArrayLong = z
  .array(z.string().min(1).max(60))
  .max(50);

// Showroom shape inside company_profile
const ShowroomSchema = z.object({
  city:    z.string().min(1).max(80),
  address: z.string().min(1).max(300),
  hours:   z.string().max(100).optional(),
});

// company_profile — all fields optional so partial updates work
const CompanyProfileSchema = z.object({
  name:          z.string().max(100).optional(),
  description:   z.string().max(2000).optional(),
  website:       z.string().url().max(300).optional().or(z.literal('')),
  phone:         z.string().max(40).optional(),
  email:         z.string().email().max(200).optional().or(z.literal('')),
  working_hours: z.string().max(200).optional(),
  showrooms:     z.array(ShowroomSchema).max(10).optional(),
}).strict();

const TenantSettingsPutSchema = z.object({
  roles:           labelArray.optional(),
  cities:          labelArray.optional(),
  tags:            labelArray.optional(),
  lead_sources:    labelArrayLong.optional(),
  refusal_reasons: labelArrayLong.optional(),
  task_types:      labelArrayLong.optional(),
  company_profile: CompanyProfileSchema.optional(),
}).refine(
  (body) =>
    body.roles !== undefined ||
    body.cities !== undefined ||
    body.tags !== undefined ||
    body.lead_sources !== undefined ||
    body.refusal_reasons !== undefined ||
    body.task_types !== undefined ||
    body.company_profile !== undefined,
  { message: 'At least one settings field is required' }
);

// ---------------------------------------------------------------------------
// Zod schemas for /settings/managers
// ---------------------------------------------------------------------------

const ManagerCreateSchema = z.object({
  name:        z.string().min(1).max(100),
  email:       z.string().email().max(200).optional().or(z.literal('')),
  phone:       z.string().max(40).optional(),
  session_ids: z.array(z.string().min(1).max(80)).max(20).optional(),
  notes:       z.string().max(1000).optional(),
});

const ManagerPatchSchema = z.object({
  name:        z.string().min(1).max(100).optional(),
  email:       z.string().email().max(200).optional().or(z.literal('')),
  phone:       z.string().max(40).optional(),
  session_ids: z.array(z.string().min(1).max(80)).max(20).optional(),
  is_active:   z.boolean().optional(),
  notes:       z.string().max(1000).optional(),
}).refine(
  (b) =>
    b.name !== undefined ||
    b.email !== undefined ||
    b.phone !== undefined ||
    b.session_ids !== undefined ||
    b.is_active !== undefined ||
    b.notes !== undefined,
  { message: 'At least one field is required' }
);

const ReorderSchema = z.object({
  ids: z.array(z.string().uuid()).min(1).max(200),
});

// ---------------------------------------------------------------------------
// Zod schemas for /settings/templates
// ---------------------------------------------------------------------------

const TemplateCreateSchema = z.object({
  title:    z.string().min(1).max(100),
  body:     z.string().min(1).max(4000),
  category: z.string().min(1).max(60).optional(),
});

const TemplatePatchSchema = z.object({
  title:    z.string().min(1).max(100).optional(),
  body:     z.string().min(1).max(4000).optional(),
  category: z.string().min(1).max(60).optional(),
}).refine(
  (b) => b.title !== undefined || b.body !== undefined || b.category !== undefined,
  { message: 'At least one field (title, body, category) is required' }
);

const hexColorField = z
  .string()
  .regex(/^#[0-9a-fA-F]{6}$/, 'color must be a 6-digit hex value, e.g. #3b82f6');

const FunnelStageCreateSchema = z.object({
  name:     z.string().min(1).max(40),
  color:    hexColorField.optional(),
  is_final: z.boolean().optional(),
});

const FunnelStagePatchSchema = z.object({
  name:     z.string().min(1).max(40).optional(),
  color:    hexColorField.optional(),
  is_final: z.boolean().optional(),
}).refine(
  (body) => body.name !== undefined || body.color !== undefined || body.is_final !== undefined,
  { message: 'At least one field (name, color, is_final) is required' }
);

const FunnelStageReorderSchema = z.object({
  order: z.array(z.string().uuid()).min(1).max(200),
});

/** Format Zod error issues into a human-readable message. */
function zodErrorMessage(err) {
  return err.issues.map((i) => `${i.path.join('.') || 'body'}: ${i.message}`).join('; ');
}

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

  // GET /test-sentry-error — admin-only smoke test for Sentry wiring.
  // Throws synchronously so expressErrorHandler captures it; the request
  // returns 500 and an event should appear in Sentry within seconds.
  // Mounted under x-api-key auth (same as other admin endpoints) so a random
  // visitor can't burn through the Sentry quota.
  router.get('/test-sentry-error', (_req, _res) => {
    const enabled = isSentryEnabled();
    throw new Error(
      `Sentry smoke test from /test-sentry-error at ${new Date().toISOString()} (sentry_enabled=${enabled})`
    );
  });

  // POST /admin/cloudinary-cleanup — manual run (audit + on-demand sweep).
  // ?mode=dry-run    — safe default, lists candidates, deletes nothing.
  // ?mode=delete     — actually deletes (matches scheduler when env=delete).
  // ?retentionDays=N — override TTL for this call only.
  // Always restricted to wa-bridge/ folder by hard-coded prefix in the
  // cleanup module — no way to scope this elsewhere from the API.
  router.post('/admin/cloudinary-cleanup', async (req, res) => {
    const requestedMode = String(req.query.mode || 'dry-run').toLowerCase();
    const allowedModes = ['dry-run', 'delete'];
    if (!allowedModes.includes(requestedMode)) {
      return res.status(400).json({ error: `mode must be one of: ${allowedModes.join(', ')}` });
    }
    const retentionDaysOverride = req.query.retentionDays ? Number(req.query.retentionDays) : undefined;
    if (retentionDaysOverride !== undefined && (!Number.isFinite(retentionDaysOverride) || retentionDaysOverride < 1)) {
      return res.status(400).json({ error: 'retentionDays must be a positive integer' });
    }
    try {
      const summary = await runCloudinaryCleanup({
        mode: requestedMode,
        retentionDays: retentionDaysOverride,
      });
      return res.json(summary);
    } catch (err) {
      logger.error({ err: err.message }, 'admin_cloudinary_cleanup_failed');
      return res.status(500).json({ error: 'Cleanup failed', message: err.message });
    }
  });

  // GET /admin/cloudinary-cleanup/config — what env says, no I/O
  router.get('/admin/cloudinary-cleanup/config', (_req, res) => {
    return res.json(getCleanupConfig());
  });

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
      const db = req.userClient ?? supabase;
      let query = db
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

    // Serve from cache if available
    // userId: explicit from req.user (JWT path) or '__service__' (x-api-key / worker path).
    // '__service__' is the safe fallback — it keeps service-scope results isolated from
    // user-scoped RLS-filtered results. Never use 'anon' here as that would merge unknown
    // callers into a single bucket with their own bleed risk.
    const userId = req.user?.userId ?? '__service__';
    const cacheKey = makeAnalyticsCacheKey(sessionId, dateFrom, dateTo, days, userId);
    const cached = getAnalyticsCache(cacheKey);
    if (cached) {
      logger.debug({ cacheKey }, 'analytics/summary: cache hit');
      return res.json(cached);
    }

    // Date range or days-based
    const rangeStart = dateFrom ? `${dateFrom}T00:00:00+05:00` : null;
    const rangeEnd = dateTo ? `${dateTo}T23:59:59+05:00` : null;
    const since = rangeStart || new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    try {
      const db = req.userClient ?? supabase;
      // Build session filter
      const sessionFilter = (query) => sessionId ? query.eq('session_id', sessionId) : query;

      // Pre-load the set of remote_jids tagged as клиент or партнёр.
      // Analytics should only reflect customer/partner interactions, not employees
      // or unclassified chats.
      const { data: relevantTagRows } = await db
        .from('chat_tags')
        .select('remote_jid')
        .overlaps('tags', ['клиент', 'партнёр']);
      const relevantJids = new Set((relevantTagRows ?? []).map((r) => r.remote_jid));

      // 1. KPI: response times (adjusted for working hours 10:00-20:00 Almaty)
      let rtQuery = db
        .from('manager_analytics')
        .select('customer_message_at, manager_response_at, remote_jid')
        .gte('created_at', since)
        .not('manager_response_at', 'is', null);
      if (rangeEnd) rtQuery = rtQuery.lte('created_at', rangeEnd);

      const { data: responseTimesRaw } = await sessionFilter(rtQuery);
      // Filter: only клиент/партнёр chats
      const responseTimes = (responseTimesRaw ?? []).filter((r) => relevantJids.has(r.remote_jid));

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
      let aiQuery = db
        .from('chat_ai')
        .select('lead_temperature, deal_stage, sentiment, risk_flags, action_required, session_id, remote_jid, summary_ru, action_suggestion, customer_type, consultation_score, consultation_details, followup_status, manager_issues, intent');

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

      // Two views over the same AI rows:
      //  - aiAll: only garbage filtered out — used for criticalChats so that
      //    aggressive/complaint/risk_flags signals reach the dashboard even
      //    when the chat is tagged as colleague or hasn't been tag-confirmed
      //    yet (e.g. landlord rent ultimatum, vendor complaint).
      //  - ai: additionally restricted to клиент/партнёр chats — used for
      //    sales KPIs (lead temperature, deal stages, response times, etc.)
      //    where colleague chats would skew the numbers.
      const aiAll = (aiData ?? []).filter((row) => !isGarbageJid(row.remote_jid));
      const ai = aiAll.filter((row) => relevantJids.has(row.remote_jid));

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

      // Critical chats — aggressive sentiment, non-empty risk_flags, or
      // explicit complaint intent. Computed from aiAll (NOT ai) so that
      // colleague-tagged or untagged chats can still surface as critical:
      // the landlord-rent ultimatum and vendor complaints don't carry
      // клиент/партнёр tags but are exactly what we want to flag.
      function isCritical(row) {
        if (row.sentiment === 'aggressive') return true;
        if (row.intent === 'complaint') return true;
        if (Array.isArray(row.risk_flags) && row.risk_flags.length > 0) return true;
        return false;
      }
      const criticalAll = aiAll.filter(isCritical);
      const criticalCount = criticalAll.length;
      const criticalChats = criticalAll
        .slice(0, 50)
        .map((r) => ({
          sessionId: r.session_id,
          remoteJid: r.remote_jid,
          summary: r.summary_ru,
          action: r.action_suggestion,
          sentiment: r.sentiment,
          intent: r.intent,
          riskFlags: r.risk_flags || [],
        }));

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
      let dialogQuery = db
        .from('dialog_sessions')
        .select('*', { count: 'exact', head: true })
        .gte('started_at', since);
      if (rangeEnd) dialogQuery = dialogQuery.lte('started_at', rangeEnd);
      const { count: totalDialogs } = await sessionFilter(dialogQuery);

      // 4. Daily message trend — only fetch timestamp column, limit to 10k max
      let msgQuery = db
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

      const result = {
        period: { days, since, dateFrom: dateFrom || null, dateTo: dateTo || null },
        kpi: {
          avgResponseTime,
          hotLeads: leads.hot,
          totalDialogs: totalDialogs || 0,
          actionRequired,
          criticalCount,
          avgConsultationScore,
        },
        leads,
        stages,
        sentiment,
        topRisks,
        actionChats,
        criticalChats,
        dailyStats,
        customerTypes,
        managerIssues,
        followupStats,
      };

      setAnalyticsCache(cacheKey, result);
      return res.json(result);
    } catch (error) {
      logger.error({ err: error }, 'Analytics summary failed');
      return res.status(500).json({ error: 'Failed to fetch analytics' });
    }
  });

  // ---------------------------------------------------------------------------
  // GET /analytics/chats-by-filter — filtered chat list for drilldown
  // ---------------------------------------------------------------------------
  router.get('/analytics/chats-by-filter', async (req, res) => {
    const { type, value, session_id, date_from, date_to } = req.query;
    const days = Math.min(Number(req.query.days) || 7, 90);

    // 'slow' / 'no_followup' / 'football' / 'lost' added 2026-04-29 for the
    // "проблемные переписки carousel" feature. They piggy-back on this same
    // endpoint so the existing AnalyticsPopup drilldown UI keeps working.
    const VALID_TYPES = new Set([
      'risk', 'issue', 'action_required', 'temperature', 'followup', 'critical',
      'slow', 'no_followup', 'football', 'lost',
    ]);
    const TYPES_WITHOUT_VALUE = new Set(['action_required', 'critical', 'slow', 'no_followup', 'football', 'lost']);
    if (!type || !VALID_TYPES.has(type)) {
      return res.status(400).json({ error: `type must be one of: ${[...VALID_TYPES].join(', ')}` });
    }
    if (!TYPES_WITHOUT_VALUE.has(type) && !value) {
      return res.status(400).json({ error: 'value is required for this filter type' });
    }

    try {
      const db = req.userClient ?? supabase;

      // Build date range for analysis_date
      let sinceDateStr;
      if (date_from) {
        sinceDateStr = date_from;
      } else {
        const sinceDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000 + 5 * 3600000);
        sinceDateStr = sinceDate.toISOString().slice(0, 10);
      }

      // For 'football' we first resolve the set of remote_jids from v_football_cases
      // (cross-session view), then filter chat_ai to those jids. For all other types
      // chat_ai is the primary source.
      let footballMeta = null; // { jidsSet: Set, perJid: Map<jid, {sessions, count, last_outbound_at}> }
      if (type === 'football') {
        const { data: footballRows, error: footballErr } = await db
          .from('v_football_cases')
          .select('remote_jid, sessions, session_count, last_outbound_at');
        if (footballErr) {
          logger.error({ err: footballErr }, 'chats-by-filter football view query failed');
          return res.status(500).json({ error: 'Failed to query football view' });
        }
        const perJid = new Map();
        for (const r of footballRows ?? []) {
          perJid.set(r.remote_jid, {
            sessions: r.sessions || [],
            count: r.session_count || 0,
            lastOutboundAt: r.last_outbound_at || null,
          });
        }
        footballMeta = { jidsSet: new Set(perJid.keys()), perJid };
        if (footballMeta.jidsSet.size === 0) {
          return res.json({ total: 0, filter: { type, value: null }, chats: [] });
        }
      }

      // Build chat_ai query
      let aiQuery = db
        .from('chat_ai')
        .select('id, session_id, remote_jid, dialog_session_id, analysis_date, lead_temperature, deal_stage, sentiment, risk_flags, manager_issues, action_required, action_suggestion, followup_status, summary_ru, consultation_score, report_sent_at, intent, problem_dismissed_action, problem_dismissed_at')
        .gte('analysis_date', sinceDateStr)
        .limit(100);

      if (date_to) {
        aiQuery = aiQuery.lte('analysis_date', date_to);
      }
      if (session_id) {
        aiQuery = aiQuery.eq('session_id', session_id);
      }

      // Hide dismissed rows from every category EXCEPT 'lost' (which IS the
      // dismissed-as-lost view). Carousel needs this so once a manager clicks
      // "Нет — оставить как проблему" the row disappears from active categories.
      if (type !== 'lost') {
        aiQuery = aiQuery.is('problem_dismissed_at', null);
      }

      // Type-specific filter
      if (type === 'risk') {
        aiQuery = aiQuery.contains('risk_flags', [value]);
      } else if (type === 'issue') {
        aiQuery = aiQuery.contains('manager_issues', [value]);
      } else if (type === 'action_required') {
        aiQuery = aiQuery.eq('action_required', true);
      } else if (type === 'temperature') {
        aiQuery = aiQuery.eq('lead_temperature', value);
      } else if (type === 'followup') {
        aiQuery = aiQuery.eq('followup_status', value);
      } else if (type === 'critical') {
        // Can't express "risk_flags non-empty OR sentiment=aggressive OR intent=complaint"
        // cleanly in PostgREST (array length check isn't a first-class filter). Fetch a
        // superset and filter client-side — we're already limited to 100 rows per call.
        aiQuery = aiQuery.or('sentiment.eq.aggressive,intent.eq.complaint,risk_flags.not.is.null');
      } else if (type === 'slow') {
        aiQuery = aiQuery.contains('manager_issues', ['slow_first_response']);
      } else if (type === 'no_followup') {
        aiQuery = aiQuery.contains('manager_issues', ['no_followup']);
      } else if (type === 'football') {
        aiQuery = aiQuery.in('remote_jid', [...footballMeta.jidsSet]);
      } else if (type === 'lost') {
        aiQuery = aiQuery.eq('problem_dismissed_action', 'lost');
      }

      const { data: aiRowsRaw, error: aiError } = await aiQuery;
      const aiRows = type === 'critical'
        ? (aiRowsRaw || []).filter((r) =>
            r.sentiment === 'aggressive' ||
            r.intent === 'complaint' ||
            (Array.isArray(r.risk_flags) && r.risk_flags.length > 0)
          )
        : aiRowsRaw;
      if (aiError) {
        logger.error({ err: aiError }, 'chats-by-filter: chat_ai query failed');
        return res.status(500).json({ error: 'Failed to query AI data' });
      }

      if (!aiRows || aiRows.length === 0) {
        return res.json({ total: 0, filter: { type, value: value || null }, chats: [] });
      }

      // Collect unique (session_id, remote_jid) pairs for chats lookup
      const jidSet = [...new Set(aiRows.map((r) => `${r.session_id}:::${r.remote_jid}`))];
      const sessionIds = [...new Set(aiRows.map((r) => r.session_id))];

      // Fetch chat metadata (display_name, last_message_at) — tags live in chat_tags now
      const { data: chatsData } = await db
        .from('chats')
        .select('session_id, remote_jid, display_name, last_message_at')
        .in('session_id', sessionIds);

      const chatsMap = {};
      for (const c of chatsData ?? []) {
        chatsMap[`${c.session_id}:::${c.remote_jid}`] = c;
      }

      // W7A: phone-level tags
      const uniqueJids = [...new Set(aiRows.map((r) => r.remote_jid))];
      const tagsMap = await getChatTagsByJids(uniqueJids, db);

      // Fetch session display names
      const { data: sessionsData } = await db
        .from('session_config')
        .select('session_id, display_name')
        .in('session_id', sessionIds);

      const sessionsMap = {};
      for (const s of sessionsData ?? []) {
        sessionsMap[s.session_id] = s.display_name || s.session_id;
      }

      // Build result list — filter out garbage JIDs
      const chats = aiRows
        .filter((r) => !isGarbageJid(r.remote_jid))
        .map((r) => {
          const key = `${r.session_id}:::${r.remote_jid}`;
          const chat = chatsMap[key] || {};
          const digits = r.remote_jid.replace(/\D/g, '');
          const tagEntry = tagsMap[r.remote_jid] || { tags: [], tagConfirmed: false };
          // Football metadata: attached when the row's jid appears in v_football_cases
          // (resolved earlier for type='football', otherwise undefined).
          const football = footballMeta?.perJid?.get(r.remote_jid);
          return {
            sessionId: r.session_id,
            sessionDisplayName: sessionsMap[r.session_id] || r.session_id,
            remoteJid: r.remote_jid,
            displayName: chat.display_name || null,
            phone: isRealPhoneJid(r.remote_jid) ? digits : null,
            lastTimestamp: chat.last_message_at || null,
            summary: r.summary_ru || null,
            action: r.action_suggestion || null,
            temperature: r.lead_temperature || null,
            dealStage: r.deal_stage || null,
            sentiment: r.sentiment || null,
            issues: r.manager_issues || [],
            riskFlags: r.risk_flags || [],
            actionRequired: r.action_required,
            followupStatus: r.followup_status || null,
            analysisDate: r.analysis_date || null,
            consultationScore: r.consultation_score || null,
            chatAiId: r.id,
            tags: tagEntry.tags,
            tagConfirmed: tagEntry.tagConfirmed,
            reportSentAt: r.report_sent_at || null,
            report_sent_at: r.report_sent_at || null,
            problemDismissedAction: r.problem_dismissed_action || null,
            problemDismissedAt: r.problem_dismissed_at || null,
            footballSessions: football?.sessions || null,
            footballSessionCount: football?.count || null,
          };
        });

      return res.json({ total: chats.length, filter: { type, value: value || null }, chats });
    } catch (error) {
      logger.error({ err: error }, 'chats-by-filter failed');
      return res.status(500).json({ error: 'Failed to fetch filtered chats' });
    }
  });

  // ---------------------------------------------------------------------------
  // PATCH /chat_ai/:id — manual correction of AI analysis fields
  // ---------------------------------------------------------------------------
  router.patch('/chat_ai/:id', async (req, res) => {
    const { id } = req.params;
    if (!id || typeof id !== 'string') {
      return res.status(400).json({ error: 'Invalid id' });
    }

    const ALLOWED_FIELDS = new Set([
      'action_required', 'manager_issues', 'risk_flags',
      'lead_temperature', 'deal_stage', 'summary_ru', 'action_suggestion',
    ]);

    // Strip unknown fields
    const updates = {};
    for (const [key, val] of Object.entries(req.body || {})) {
      if (ALLOWED_FIELDS.has(key)) {
        updates[key] = val;
      }
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'No valid fields provided' });
    }

    // Validate individual fields (null/undefined = clear, which is valid)
    if ('lead_temperature' in updates && updates.lead_temperature !== null && !VALID_TEMPERATURES.has(updates.lead_temperature)) {
      return res.status(400).json({
        error: `lead_temperature must be one of: ${[...VALID_TEMPERATURES].join(', ')} or null`,
      });
    }
    if ('deal_stage' in updates && updates.deal_stage !== null) {
      const ds = updates.deal_stage;
      if (typeof ds !== 'string' || ds.trim().length === 0 || ds.trim().length > 40) {
        return res.status(400).json({ error: 'deal_stage must be a string of 1–40 characters' });
      }
      updates.deal_stage = ds.trim();
      // Verify stage exists in the user's funnel configuration
      const userId = req.user?.userId;
      if (userId) {
        const { data: stageRow } = await (req.userClient ?? supabase)
          .from('funnel_stages')
          .select('id')
          .eq('user_id', userId)
          .eq('name', updates.deal_stage)
          .maybeSingle();
        if (!stageRow) {
          return res.status(400).json({
            error: `Стадия "${updates.deal_stage}" не существует в воронке. Сначала создайте её в Настройках.`,
          });
        }
      }
    }
    if ('manager_issues' in updates) {
      if (!Array.isArray(updates.manager_issues)) {
        return res.status(400).json({ error: 'manager_issues must be an array' });
      }
      const invalid = updates.manager_issues.filter((i) => !VALID_MANAGER_ISSUES.has(i));
      if (invalid.length > 0) {
        return res.status(400).json({
          error: `Invalid manager_issues values: ${invalid.join(', ')}. Valid: ${[...VALID_MANAGER_ISSUES].join(', ')}`,
        });
      }
    }
    if ('risk_flags' in updates && !Array.isArray(updates.risk_flags)) {
      return res.status(400).json({ error: 'risk_flags must be an array' });
    }
    if ('action_required' in updates && typeof updates.action_required !== 'boolean') {
      return res.status(400).json({ error: 'action_required must be a boolean' });
    }

    const db = req.userClient ?? supabase;
    try {
      const { data, error } = await db
        .from('chat_ai')
        .update(updates)
        .eq('id', id)
        .select()
        .single();

      if (error) {
        if (error.code === 'PGRST116') {
          return res.status(404).json({ error: 'chat_ai record not found' });
        }
        logger.error({ err: error, id }, 'PATCH chat_ai failed');
        return res.status(500).json({ error: 'Update failed' });
      }

      // Invalidate analytics cache — the updated record may affect summary counts
      invalidateAnalyticsCache();

      logger.info({ id, fields: Object.keys(updates) }, 'chat_ai manually updated');
      return res.json({ ok: true, record: data });
    } catch (error) {
      logger.error({ err: error, id }, 'PATCH chat_ai unexpected error');
      return res.status(500).json({ error: 'Update failed' });
    }
  });

  // ---------------------------------------------------------------------------
  // POST /chat_ai/:id/dismiss-problem — remove a chat from the "проблемные
  // переписки" carousel. The server decides won/lost from the row's current
  // deal_stage at dismiss time (see WIN_STAGES below). Frontend only sends
  // a click — no payload required.
  // ---------------------------------------------------------------------------
  router.post('/chat_ai/:id/dismiss-problem', async (req, res) => {
    const { id } = req.params;
    if (!id || typeof id !== 'string') {
      return res.status(400).json({ error: 'Invalid id' });
    }

    // Stages that mean "deal moved forward" — clicking "Нет" on one of these
    // closes it as a win. Anything else (consultation stuck, refused, NULL,
    // needs_review, etc.) is a "просранный лид" (lost).
    const WIN_STAGES = new Set([
      'completed', 'delivery', 'payment', 'closed_won', 'post_sale',
      'Завершено', 'Доставка', 'Оплата',
    ]);

    try {
      const db = req.userClient ?? supabase;

      // Read current deal_stage to decide win vs lost
      const { data: existing, error: readErr } = await db
        .from('chat_ai')
        .select('id, deal_stage, problem_dismissed_action')
        .eq('id', id)
        .maybeSingle();

      if (readErr) {
        logger.error({ err: readErr, id }, 'dismiss-problem read failed');
        return res.status(500).json({ error: 'Read failed' });
      }
      if (!existing) {
        return res.status(404).json({ error: 'chat_ai record not found' });
      }
      if (existing.problem_dismissed_action) {
        // Already dismissed — idempotent return
        return res.json({ ok: true, action: existing.problem_dismissed_action, alreadyDismissed: true });
      }

      const action = WIN_STAGES.has(existing.deal_stage) ? 'won' : 'lost';
      const userId = req.user?.userId || req.user?.id || '__service__';

      const { error: updateErr } = await db
        .from('chat_ai')
        .update({
          problem_dismissed_action: action,
          problem_dismissed_at: new Date().toISOString(),
          problem_dismissed_by: userId,
        })
        .eq('id', id);

      if (updateErr) {
        logger.error({ err: updateErr, id }, 'dismiss-problem update failed');
        return res.status(500).json({ error: 'Update failed' });
      }

      invalidateAnalyticsCache();
      logger.info({ id, action, userId, dealStage: existing.deal_stage }, 'chat_ai problem dismissed');
      return res.json({ ok: true, action });
    } catch (error) {
      logger.error({ err: error, id }, 'dismiss-problem unexpected error');
      return res.status(500).json({ error: 'Dismiss failed' });
    }
  });

  // ---------------------------------------------------------------------------
  // POST /chat_ai/:id/feedback — manager flags AI analysis as wrong. Stored
  // in chat_ai_feedback so the daily-wa-analysis skill can read recent
  // feedback as in-context guidelines on the next run.
  // ---------------------------------------------------------------------------
  const FEEDBACK_KINDS = new Set(['wrong_category', 'wrong_summary', 'wrong_severity', 'wrong_assignment', 'other']);

  router.post('/chat_ai/:id/feedback', async (req, res) => {
    const { id } = req.params;
    const { kind, comment } = req.body || {};

    if (!id || typeof id !== 'string') {
      return res.status(400).json({ error: 'Invalid id' });
    }
    if (!kind || !FEEDBACK_KINDS.has(kind)) {
      return res.status(400).json({ error: `kind must be one of: ${[...FEEDBACK_KINDS].join(', ')}` });
    }
    const cleanComment = typeof comment === 'string' ? comment.trim().slice(0, 2000) : null;
    if (!cleanComment && kind === 'other') {
      return res.status(400).json({ error: 'comment required when kind=other' });
    }

    try {
      const db = req.userClient ?? supabase;

      // Verify the chat_ai row exists (FK violation otherwise = obscure 500)
      const { data: existing, error: readErr } = await db
        .from('chat_ai')
        .select('id')
        .eq('id', id)
        .maybeSingle();
      if (readErr || !existing) {
        return res.status(404).json({ error: 'chat_ai record not found' });
      }

      const userId = req.user?.userId || req.user?.id || '__service__';
      const { error: insertErr } = await db
        .from('chat_ai_feedback')
        .insert({
          chat_ai_id: id,
          kind,
          comment_ru: cleanComment || null,
          created_by: userId,
        });
      if (insertErr) {
        logger.error({ err: insertErr, id }, 'feedback insert failed');
        return res.status(500).json({ error: 'Save failed' });
      }

      logger.info({ id, kind, userId }, 'chat_ai feedback recorded');
      return res.json({ ok: true });
    } catch (error) {
      logger.error({ err: error, id }, 'feedback unexpected error');
      return res.status(500).json({ error: 'Save failed' });
    }
  });

  // ---------------------------------------------------------------------------
  // Calls API
  // ---------------------------------------------------------------------------

  /**
   * GET /sessions/:sessionId/calls?limit=100&offset=0
   * Returns all calls for a session, newest first.
   */
  router.get('/sessions/:sessionId/calls', async (req, res) => {
    const { sessionId } = req.params;
    const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
    const offset = parseInt(req.query.offset, 10) || 0;
    const db = req.userClient ?? supabase;

    try {
      const calls = await getCallsBySession(sessionId, { limit, offset, db });
      return res.json({ calls: calls.map(formatCallRow) });
    } catch (err) {
      logger.error({ err, sessionId }, 'GET /sessions/:sessionId/calls failed');
      return res.status(500).json({ error: 'Failed to fetch calls' });
    }
  });

  /**
   * GET /sessions/:sessionId/chats/:phone/calls
   * Returns calls for a specific chat (by phone number / remoteJid).
   */
  router.get('/sessions/:sessionId/chats/:phone/calls', async (req, res) => {
    const { sessionId, phone } = req.params;
    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);
    const db = req.userClient ?? supabase;

    try {
      const calls = await getCallsByChat(sessionId, phone, { limit, db });
      return res.json({ calls: calls.map(formatCallRow) });
    } catch (err) {
      logger.error({ err, sessionId, phone }, 'GET /sessions/:sessionId/chats/:phone/calls failed');
      return res.status(500).json({ error: 'Failed to fetch calls' });
    }
  });

  /**
   * GET /analytics/calls-kpi?days=7
   * Returns call KPIs: total, missed, answer rate, avg duration — per session and overall.
   */
  router.get('/analytics/calls-kpi', async (req, res) => {
    const days = Math.min(parseInt(req.query.days, 10) || 7, 90);
    const db = req.userClient ?? supabase;

    try {
      const kpi = await getCallsKpi(days, db);
      if (!kpi) {
        return res.status(500).json({ error: 'Failed to compute calls KPI' });
      }
      return res.json(kpi);
    } catch (err) {
      logger.error({ err, days }, 'GET /analytics/calls-kpi failed');
      return res.status(500).json({ error: 'Failed to compute calls KPI' });
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

  // Delete all manager_analytics rows for chats tagged as сотрудник.
  // Auth: same JWT/x-api-key middleware as all other routes in this router.
  router.post('/admin/cleanup-employee-analytics', async (_req, res) => {
    try {
      const { data: employeeTagRows, error: tagErr } = await supabase
        .from('chat_tags')
        .select('remote_jid')
        .contains('tags', ['сотрудник']);

      if (tagErr) {
        logger.error({ err: tagErr }, 'cleanup-employee-analytics: failed to fetch employee jids');
        return res.status(500).json({ error: tagErr.message });
      }

      const jids = (employeeTagRows ?? []).map((r) => r.remote_jid);

      if (jids.length === 0) {
        return res.json({ deleted: 0 });
      }

      const { count, error: delErr } = await supabase
        .from('manager_analytics')
        .delete({ count: 'exact' })
        .in('remote_jid', jids);

      if (delErr) {
        logger.error({ err: delErr }, 'cleanup-employee-analytics: delete failed');
        return res.status(500).json({ error: delErr.message });
      }

      logger.info({ deleted: count, employeeCount: jids.length }, 'cleanup-employee-analytics: done');
      return res.json({ deleted: count ?? 0 });
    } catch (err) {
      logger.error({ err }, 'cleanup-employee-analytics: unexpected error');
      return res.status(500).json({ error: 'Cleanup failed' });
    }
  });

  // Tag all chats that have no entry in chat_tags (or have empty tags) as 'неизвестно'.
  // Idempotent — safe to call multiple times.
  router.post('/admin/backfill-unknown-tags', async (_req, res) => {
    try {
      const { data: allChats, error: chatsErr } = await supabase
        .from('chats')
        .select('remote_jid');

      if (chatsErr) {
        logger.error({ err: chatsErr }, 'backfill-unknown-tags: failed to fetch chats');
        return res.status(500).json({ error: chatsErr.message });
      }

      const allJids = [...new Set((allChats ?? []).map((c) => c.remote_jid).filter(Boolean))];

      if (allJids.length === 0) {
        return res.json({ tagged: 0 });
      }

      // Fetch existing tag records in one batch
      const { data: existingTags } = await supabase
        .from('chat_tags')
        .select('remote_jid, tags')
        .in('remote_jid', allJids);

      const taggedJids = new Set(
        (existingTags ?? [])
          .filter((r) => Array.isArray(r.tags) && r.tags.length > 0)
          .map((r) => r.remote_jid)
      );

      const toTag = allJids.filter((jid) => !taggedJids.has(jid));

      let tagged = 0;
      for (const jid of toTag) {
        const ok = await upsertChatTags(jid, { tags: ['неизвестно'], tagConfirmed: false });
        if (ok) tagged++;
      }

      logger.info({ tagged, total: allJids.length }, 'backfill-unknown-tags: done');
      return res.json({ tagged });
    } catch (err) {
      logger.error({ err }, 'backfill-unknown-tags: unexpected error');
      return res.status(500).json({ error: 'Backfill failed' });
    }
  });

  // W1.1 Phase 1: auth plumbing probe.
  // Confirms the JWT path sets req.user and req.userClient correctly.
  // ───────────────────────────────────────────────────────────────────────
  // Sales CRM endpoints (Omoikiri-only — миграции 0011/0012/0015).
  // Тонкие обёртки вокруг src/lib/salesCrm.js, который содержит всю логику.
  // ───────────────────────────────────────────────────────────────────────

  // GET /sales-crm/partners?filter=&q=&limit=&offset=
  router.get('/sales-crm/partners', async (req, res) => {
    try {
      const r = await salesCrm.listPartners(req, {
        filter: req.query.filter,
        q: req.query.q || '',
        limit: req.query.limit ? Number(req.query.limit) : undefined,
        offset: req.query.offset ? Number(req.query.offset) : undefined,
      });
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_list_partners_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/partners/:id  — full card
  router.get('/sales-crm/partners/:id', async (req, res) => {
    try {
      const r = await salesCrm.getPartnerCard(req, req.params.id);
      if (!r) return res.status(404).json({ error: 'partner not found' });
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_partner_card_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/agencies  — list of all studios with aggregates
  router.get('/sales-crm/agencies', async (req, res) => {
    try {
      const r = await salesCrm.listAgencies(req);
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_list_agencies_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/agencies/:id  — studio card
  router.get('/sales-crm/agencies/:id', async (req, res) => {
    try {
      const r = await salesCrm.getStudioCard(req, req.params.id);
      if (!r) return res.status(404).json({ error: 'agency not found' });
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_studio_card_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/followups/due?window_days=30
  router.get('/sales-crm/followups/due', async (req, res) => {
    try {
      const r = await salesCrm.getDueFollowups(req, {
        window_days: req.query.window_days ? Number(req.query.window_days) : undefined,
        limit: req.query.limit ? Number(req.query.limit) : undefined,
      });
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_followups_due_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // POST /sales-crm/partners/:id/merge   { target_id }
  // Сливает текущий контакт в target. Все заказы/followups перейдут на target.
  // Source-контакт удаляется. Откатить нельзя без переимпорта.
  router.post('/sales-crm/partners/:id/merge', async (req, res) => {
    try {
      const r = await salesCrm.mergePartners(req, req.params.id, req.body || {});
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_merge_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // POST /sales-crm/partners/:id/agency   { agency_id: uuid|null }
  router.post('/sales-crm/partners/:id/agency', async (req, res) => {
    try {
      const r = await salesCrm.updatePartnerAgency(req, req.params.id, req.body || {});
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_agency_update_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // POST /sales-crm/followups/:id/done   { action, note? }
  router.post('/sales-crm/followups/:id/done', async (req, res) => {
    try {
      const r = await salesCrm.markFollowupDone(req, req.params.id, req.body || {});
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_mark_done_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/analytics?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD
  router.get('/sales-crm/analytics', async (req, res) => {
    try {
      const r = await salesCrm.getSalesAnalytics(req, {
        date_from: req.query.date_from,
        date_to: req.query.date_to,
      });
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_analytics_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/segmentation — RFM + cohorts + cities (Group B)
  router.get('/sales-crm/segmentation', async (req, res) => {
    try {
      const r = await salesCrm.getSegmentation(req);
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_segmentation_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/products — top SKUs + cross-sell + cartridge funnel (Group C)
  router.get('/sales-crm/products', async (req, res) => {
    try {
      const r = await salesCrm.getProductInsights(req);
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_products_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/partners-insights — cold/rising/ROI (Group D)
  router.get('/sales-crm/partners-insights', async (req, res) => {
    try {
      const r = await salesCrm.getPartnerInsights(req);
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_partner_insights_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/partners/by-phone/:phone  — cross-link from chat (jid → partner)
  // Returns 200 always: { partner: {...} | null }. Не-найден — это нормальный
  // случай (большинство WhatsApp-номеров не пересекаются с продажами), не 404.
  router.get('/sales-crm/partners/by-phone/:phone', async (req, res) => {
    try {
      const r = await salesCrm.findPartnerByPhone(req, req.params.phone);
      res.json({ partner: r || null });
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_by_phone_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // GET /sales-crm/search?q=...   — quick lookup by name/phone
  router.get('/sales-crm/search', async (req, res) => {
    try {
      const r = await salesCrm.searchPartner(req, {
        q: req.query.q || '',
        limit: req.query.limit ? Number(req.query.limit) : undefined,
      });
      res.json(r);
    } catch (e) {
      req.log?.warn({ err: e.message }, 'sales_crm_search_failed');
      res.status(400).json({ error: e.message });
    }
  });

  // Used as smoke-test after deploy + Phase 2 migration-in-progress checks.
  router.get('/health/auth', async (req, res) => {
    res.json({
      authenticated: Boolean(req.user),
      user: req.user ? { id: req.user.userId, email: req.user.email, role: req.user.role } : null,
      hasUserClient: Boolean(req.userClient),
      authMode: req.user ? 'jwt' : 'api_key',
    });
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

  router.get('/sessions', async (req, res) => {
    const db = req.userClient ?? supabase;
    const { data: configs, error } = await db
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

    // Ownership check via RLS: req.userClient only sees rows belonging to the
    // calling user — prevents tenant A from deleting tenant B's session.
    // Destructive ops below remain on service_role (supabase) — ownership is
    // already confirmed at this point.
    const db = req.userClient ?? supabase;
    const { data: config } = await db
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

    // Ownership check via RLS: same pattern as DELETE and reconnect.
    const db = req.userClient ?? supabase;
    const { data: config } = await db
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

  // Reconnect an existing session — clears stale auth_state so Baileys issues a fresh QR
  // reconnectLimiter: 3/hour per (userId + sessionId) — prevents double-click / UI-loop
  // spam that WhatsApp reads as suspicious frequent reconnects (ban-risk).
  router.post('/sessions/:sessionId/reconnect', reconnectLimiter, async (req, res) => {
    const { sessionId } = req.params;
    const db = req.userClient ?? supabase;

    // RLS: req.userClient sees only rows where user_id = auth.uid()
    const { data: config } = await db
      .from('session_config')
      .select('session_id')
      .eq('session_id', sessionId)
      .maybeSingle();

    if (!config) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // Clear stale auth_state using service key — same as DisconnectReason.loggedOut handler
    const { error: clearError } = await supabase
      .from('auth_state')
      .delete()
      .eq('session_id', sessionId);

    if (clearError) {
      logger.error({ err: clearError, sessionId }, 'Failed to clear auth_state during reconnect');
      return res.status(500).json({ error: 'Failed to clear auth state' });
    }

    logger.info({ sessionId }, 'User-requested reconnect — cleared auth_state and restarted session');
    await sessionManager.stopSession(sessionId);
    await new Promise((r) => setTimeout(r, 2000));
    await sessionManager.startSession(sessionId);

    return res.json({ success: true, sessionId, connected: false, hasQR: false });
  });

  router.get('/sessions/:sessionId/status', async (req, res) => {
    const { sessionId } = req.params;
    const db = req.userClient ?? supabase;
    const { data: config } = await db
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
    const db = req.userClient ?? supabase;
    try {
      const allChats = await getChatsWithLastMessage(sessionId, db, req.user?.userId ?? '__service__');
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
    const db = req.userClient ?? supabase;
    try {
      // Get all active sessions
      const { data: sessions } = await db
        .from('session_config')
        .select('session_id, display_name')
        .eq('is_active', true);

      if (!sessions?.length) {
        return res.json({ chats: [], total: 0, hasMore: false });
      }

      // Fetch chats from all sessions in parallel (cached, 10s TTL)
      const allPromises = sessions.map(async (s) => {
        const chats = await getChatsWithLastMessage(s.session_id, db, req.user?.userId ?? '__service__');
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

    const db = req.userClient ?? supabase;
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

      const { error: persistError } = await db
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

    const db = req.userClient ?? supabase;
    try {
      const { error } = await db
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
        const { error: deleteError } = await db
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

  // W7A: tags are now phone-level (chat_tags table). The :sessionId param is
  // accepted for backward compat with the old URL shape but ignored — tags
  // apply to the contact across ALL sessions.
  // 2026-04-29: when a manager flips the tag to 'сотрудник' or 'спам',
  // auto-dismiss any active problem-carousel rows for this jid as 'won'
  // (with audit marker '__tag_change__'). Adil's complaint was seeing
  // colleague chats flagged as problems — fixing the tag should fix the
  // carousel without a second click.
  const NON_PROBLEM_TAGS = new Set(['сотрудник', 'спам']);

  router.post('/sessions/:sessionId/chats/:phone/tags', async (req, res) => {
    const { sessionId } = req.params;
    const remoteJid = normalizeChatId(req.params.phone);
    const inputTags = req.body?.tags;

    if (!remoteJid || !Array.isArray(inputTags)) {
      return res.status(400).json({ error: 'phone and tags array are required' });
    }

    const db = req.userClient ?? supabase;
    const ok = await upsertChatTags(remoteJid, { tags: inputTags, tagConfirmed: true }, db);
    if (!ok) {
      return res.status(500).json({ error: 'Failed to update tags' });
    }

    // Cascade: if the new primary tag means "this isn't a real customer
    // conversation" — drop any active flagged-problem rows for this jid
    // so they disappear from the carousel immediately.
    const newPrimary = (inputTags[0] || '').toLowerCase();
    let cascaded = 0;
    if (NON_PROBLEM_TAGS.has(newPrimary)) {
      try {
        const userId = req.user?.userId || req.user?.id || '__service__';
        const { data: updated, error: cascadeErr } = await db
          .from('chat_ai')
          .update({
            problem_dismissed_action: 'won',
            problem_dismissed_at: new Date().toISOString(),
            problem_dismissed_by: `__tag_change__:${userId}`,
          })
          .eq('remote_jid', remoteJid)
          .is('problem_dismissed_at', null)
          .select('id');
        if (cascadeErr) {
          logger.warn({ err: cascadeErr, remoteJid }, 'tag-change cascade dismiss failed');
        } else {
          cascaded = updated?.length || 0;
          if (cascaded > 0) {
            invalidateAnalyticsCache();
            logger.info({ remoteJid, newPrimary, cascaded }, 'tag change cascaded to dismiss carousel rows');
          }
        }
      } catch (err) {
        logger.warn({ err, remoteJid }, 'tag-change cascade unexpected error');
      }
    }

    const { tags, tagConfirmed } = await getChatTags(remoteJid, db);
    return res.json({ success: true, tags, tagConfirmed, sessionId, cascaded });
  });

  // Confirm (or change + confirm) a chat tag — phone-level
  router.post('/sessions/:sessionId/chats/:phone/confirm-tag', async (req, res) => {
    const remoteJid = normalizeChatId(req.params.phone);
    const { tag } = req.body;

    if (!remoteJid) {
      return res.status(400).json({ error: 'phone is required' });
    }

    const VALID_TAGS = ['клиент', 'сотрудник', 'партнёр', 'неизвестно', 'спам'];

    const payload = { tagConfirmed: true };
    if (tag) {
      if (!VALID_TAGS.includes(tag)) {
        return res.status(400).json({ error: 'Invalid tag. Must be one of: ' + VALID_TAGS.join(', ') });
      }
      payload.tags = [tag];
    }

    const db = req.userClient ?? supabase;
    const ok = await upsertChatTags(remoteJid, payload, db);
    if (!ok) {
      return res.status(500).json({ error: 'Failed to confirm tag' });
    }
    return res.json({ success: true });
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

      const db = req.userClient ?? supabase;

      // Use ilike for case-insensitive partial match
      let query = db
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
        const { data: contacts } = await db
          .from('contacts_crm')
          .select('remote_jid, first_name, last_name')
          .in('remote_jid', jids);
        for (const c of contacts || []) {
          nameMap.set(c.remote_jid, `${c.first_name || ''} ${c.last_name || ''}`.trim());
        }

        // Chat display names fallback
        const { data: chats } = await db
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
      let countQuery = db
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
      const db = req.userClient ?? supabase;
      const messages = await getMessages(sessionId, phone, limit, offset, db);
      return res.json(messages);
    } catch (error) {
      logger.error({ err: error, sessionId, phone }, 'Failed to fetch messages');
      return res.status(500).json({ error: 'Failed to fetch messages' });
    }
  });

  // Cross-session: find all sessions where this contact exists
  router.get('/contacts/:phone/linked-sessions', async (req, res) => {
    const db = req.userClient ?? supabase;
    const phone = normalizeChatId(req.params.phone);
    try {
      const sessions = await getLinkedSessions(phone, db);
      return res.json(sessions);
    } catch (error) {
      logger.error({ err: error, phone }, 'Failed to fetch linked sessions');
      return res.status(500).json({ error: 'Failed to fetch linked sessions' });
    }
  });

  // Cross-session: get unified messages from ALL sessions for one contact
  router.get('/contacts/:phone/unified-messages', async (req, res) => {
    const db = req.userClient ?? supabase;
    const phone = normalizeChatId(req.params.phone);
    const limit = Number.parseInt(req.query.limit, 10) || 50;
    const offset = Number.parseInt(req.query.offset, 10) || 0;
    try {
      const messages = await getUnifiedMessages(phone, limit, offset, db);
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

    const db = req.userClient ?? supabase;
    try {
      const { data: rows, error: fetchError } = await db
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
      const { error: updateError } = await db
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

    const db = req.userClient ?? supabase;
    try {
      const [{ count, error: countError }, { data: recentUnread, error: recentError }] = await Promise.all([
        db
          .from('messages')
          .select('*', { count: 'exact', head: true })
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid)
          .eq('from_me', false)
          .is('read_at', null),
        db
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
      const { error: updateError } = await db
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
      const db = req.userClient ?? supabase;
      const { data, error } = await db
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

    const db = req.userClient ?? supabase;
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

      const { data, error } = await db
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

      const db = req.userClient ?? supabase;
      const { error } = await db
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

    const db = req.userClient ?? supabase;
    try {
      const { error } = await db
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
      const db = req.userClient ?? supabase;
      let query = db
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
      const db = req.userClient ?? supabase;

      // Get latest deal_stage per remote_jid from chat_ai
      let query = db
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
      if (!jids.length) {
        // Still need stages_meta even when there are no contacts
        const userId = req.user?.userId;
        let stages_meta = [];
        if (userId) {
          const { data: metaRows } = await db
            .from('funnel_stages')
            .select('id, name, color, sort_order, is_final')
            .eq('user_id', userId)
            .order('sort_order', { ascending: true });
          stages_meta = metaRows || [];
        }
        return res.json({ stages: {}, contacts: [], stages_meta, total: 0 });
      }

      // Fetch CRM data
      let crmQuery = db.from('contacts_crm').select('*');
      if (sessionId && sessionId !== '__all__') {
        crmQuery = crmQuery.eq('session_id', sessionId);
      }
      const { data: crmContacts } = await crmQuery;
      const crmMap = new Map();
      for (const c of crmContacts || []) {
        crmMap.set(c.remote_jid, c);
      }

      // Fetch chat display names (tags now live in chat_tags — W7A)
      let chatQuery = db.from('chats').select('session_id, remote_jid, display_name, last_message_body, last_message_timestamp');
      if (sessionId && sessionId !== '__all__') {
        chatQuery = chatQuery.eq('session_id', sessionId);
      }
      const [{ data: chatRows }, funnelTagsMap] = await Promise.all([
        chatQuery,
        getChatTagsByJids(jids, db),
      ]);
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
          const { data: msgs } = await db
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
          tags: (funnelTagsMap[jid]?.tags) || [],
          stageSource: ai.stage_source || null,
        });
      }

      // Load tenant funnel stages for metadata
      const userId = req.user?.userId;
      let stages_meta = [];
      if (userId) {
        const { data: metaRows } = await db
          .from('funnel_stages')
          .select('id, name, color, sort_order, is_final')
          .eq('user_id', userId)
          .order('sort_order', { ascending: true });
        stages_meta = metaRows || [];
      }

      // Build a set of known stage names for grouping
      const knownStageNames = new Set(stages_meta.map((s) => s.name));

      // Group by stage; unknown stages fall into __unknown bucket
      const stages = {};
      for (const s of stages_meta) stages[s.name] = [];
      stages['__unknown'] = [];

      for (const c of contacts) {
        if (knownStageNames.has(c.dealStage)) {
          stages[c.dealStage].push(c);
        } else {
          stages['__unknown'].push(c);
        }
      }

      // Append virtual __unknown entry to stages_meta if bucket has items
      if (stages['__unknown'].length > 0) {
        stages_meta = [
          ...stages_meta,
          { id: '__unknown', name: 'Без стадии', color: '#64748b', sort_order: 999, is_final: false },
        ];
      } else {
        // Remove empty __unknown bucket from response
        delete stages['__unknown'];
      }

      return res.json({ stages, stages_meta, total: contacts.length });
    } catch (error) {
      logger.error({ err: error }, 'Failed to fetch CRM funnel');
      return res.status(500).json({ error: 'Failed to fetch funnel data' });
    }
  });

  // Update deal stage manually
  router.post('/crm/deal-stage', async (req, res) => {
    const { sessionId, remoteJid, dealStage } = req.body || {};

    if (!sessionId || !remoteJid) {
      return res.status(400).json({ error: 'sessionId and remoteJid are required' });
    }
    if (!dealStage || typeof dealStage !== 'string' || dealStage.trim().length === 0 || dealStage.trim().length > 40) {
      return res.status(400).json({ error: 'dealStage must be a non-empty string of up to 40 characters' });
    }
    const normalizedStage = dealStage.trim();

    const db = req.userClient ?? supabase;
    try {
      // Update the latest chat_ai record for this contact
      const { data: latest } = await db
        .from('chat_ai')
        .select('id')
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .order('analysis_date', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (latest) {
        await db.from('chat_ai').update({
          deal_stage: normalizedStage,
          stage_source: 'manual',
          stage_changed_at: new Date().toISOString(),
        }).eq('id', latest.id);
      } else {
        // No AI analysis yet — create a minimal record
        await db.from('chat_ai').insert({
          session_id: sessionId,
          remote_jid: remoteJid,
          deal_stage: normalizedStage,
          analysis_date: new Date().toISOString().slice(0, 10),
          customer_type: 'unknown',
          stage_source: 'manual',
          stage_changed_at: new Date().toISOString(),
        });
      }

      return res.json({ success: true, dealStage: normalizedStage });
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
      const db = req.userClient ?? supabase;
      const { session_id } = req.query;
      const now = new Date().toISOString();
      const todayStart = new Date();
      todayStart.setHours(0, 0, 0, 0);

      let baseQuery = db.from('tasks').select('id, status, due_date, completed_at');
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
      const db = req.userClient ?? supabase;
      const { session_id, status, remote_jid, limit, offset } = req.query;
      const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
      const safeOffset = Math.max(Number(offset) || 0, 0);

      let query = db
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
          db.from('contacts_crm').select('remote_jid, first_name, last_name, company').in('remote_jid', jids),
          db.from('chats').select('remote_jid, display_name').in('remote_jid', jids),
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
    const db = req.userClient ?? supabase;
    try {
      const { sessionId, remoteJid, title, description, taskType, priority, dueDate, assignedTo, dealValue, notes } = req.body;

      if (!sessionId || !title || !dueDate) {
        return res.status(400).json({ error: 'sessionId, title, and dueDate are required' });
      }
      if (title.length > 200) return res.status(400).json({ error: 'Title too long (max 200)' });
      if (description && description.length > 2000) return res.status(400).json({ error: 'Description too long (max 2000)' });

      const VALID_TYPES = ['follow_up', 'call_back', 'send_quote', 'send_catalog', 'visit_showroom', 'custom'];
      const VALID_PRIORITIES = ['low', 'medium', 'high', 'urgent'];

      const { data, error } = await db
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
    const db = req.userClient ?? supabase;
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

      const { data, error } = await db
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
    const db = req.userClient ?? supabase;
    try {
      const { id } = req.params;
      const { session_id } = req.query;

      // If session_id provided, verify the task belongs to that session before deleting
      if (session_id) {
        const { data: task, error: fetchErr } = await db
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

      const { error } = await db.from('tasks').delete().eq('id', id);
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
    const db = req.userClient ?? supabase;
    try {
      const contacts = await getContacts(sessionId, db);
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

    const db = req.userClient ?? supabase;

    try {
      const result = await sendWithDelay(sock, jid, { text: message });
      limiter.recordSend(jid);

      const messageId = result?.key?.id ?? `sent-${Date.now()}`;

      // Save sent message directly to Supabase — don't rely on messages.upsert event
      // SERVICE-ROLE INTENTIONAL: saveMessage / upsertChat below run on service_role
      // by design. Ingestion parity — these helpers are also called by the Baileys
      // message-event handler (machine-to-machine, no req.user), so they must write
      // through service_role regardless of the caller's auth mode. The inline
      // messages.update for dialog_session_id at line ~2458 DOES go through `db`
      // (user JWT path) because it's a user-facing correction, not an ingestion write.
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
            await db
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
      const db = req.userClient ?? supabase;
      const { month } = req.query; // format: "2026-03"
      if (!month || !/^\d{4}-\d{2}$/.test(month)) {
        return res.status(400).json({ error: 'month parameter required (format: YYYY-MM)' });
      }

      const startDate = `${month}-01`;
      const endMonth = new Date(startDate);
      endMonth.setMonth(endMonth.getMonth() + 1);
      const endDate = endMonth.toISOString().slice(0, 10);

      // 1. Total unique chats with messages this month (ordered desc so first per key = latest)
      const { data: msgStats } = await db
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
      const { data: tasks } = await db
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
      const { data: analyses } = await db
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

  // ---------------------------------------------------------------------------
  // Wave 8 — Manager PDF Reports
  // ---------------------------------------------------------------------------

  /**
   * POST /reports/preview
   * Body: { chatAiId: string }
   *
   * Returns the data needed for the PDF preview:
   *   - chatAi record
   *   - last 50 messages of the dialog
   *   - clientName (from contacts_crm or chats)
   *   - coachingComment (AI-generated, Russian, 3-5 sentences)
   *   - availableTargets: active sessions the report can be sent to
   */
  router.post('/reports/preview', async (req, res) => {
    const { chatAiId } = req.body || {};

    if (!chatAiId || typeof chatAiId !== 'string') {
      return res.status(400).json({ error: 'chatAiId is required' });
    }

    const db = req.userClient ?? supabase;

    try {
      // 1. Load chat_ai record
      const chatAi = await getChatAiById(chatAiId, db);
      if (!chatAi) {
        return res.status(404).json({ error: 'chat_ai record not found' });
      }

      // 2. Load last 50 messages for this dialog session
      let messages = [];
      if (chatAi.dialog_session_id) {
        const { data: msgs, error: msgsErr } = await db
          .from('messages')
          .select('body, from_me, timestamp, message_type, media_url')
          .eq('dialog_session_id', chatAi.dialog_session_id)
          .order('timestamp', { ascending: true })
          .limit(50);

        if (msgsErr) {
          logger.warn({ err: msgsErr, chatAiId }, 'reports/preview: failed to load messages');
        } else {
          messages = msgs ?? [];
        }
      }

      // 3. Resolve client display name
      // Try contacts_crm first, then chats.display_name
      let clientName = 'Клиент';
      const remoteJid = chatAi.remote_jid;

      if (remoteJid) {
        const { data: crm } = await db
          .from('contacts_crm')
          .select('first_name, last_name')
          .eq('remote_jid', remoteJid)
          .maybeSingle();

        if (crm?.first_name) {
          clientName = [crm.first_name, crm.last_name].filter(Boolean).join(' ');
        } else {
          const { data: chat } = await db
            .from('chats')
            .select('display_name')
            .eq('remote_jid', remoteJid)
            .limit(1)
            .maybeSingle();
          if (chat?.display_name) clientName = chat.display_name;
        }
      }

      // 4. Generate coaching comment (non-throwing — falls back on Claude failure)
      const coachingComment = await generateCoachingComment({ messages, chatAi, clientName });

      // 5. Load active sessions for target dropdown
      const activeSessions = await getActiveSessions(db);

      // 6. Build available targets (all active sessions)
      const availableTargets = activeSessions.map((s) => ({
        sessionId: s.session_id,
        displayName: s.display_name || s.session_id,
      }));

      return res.json({
        chatAi,
        messages,
        clientName,
        coachingComment,
        availableTargets,
      });
    } catch (err) {
      logger.error({ err, chatAiId }, 'reports/preview failed');
      return res.status(500).json({ error: 'Failed to generate report preview' });
    }
  });

  /**
   * POST /reports/send
   * Body: {
   *   chatAiId: string,
   *   targetSessionId: string,
   *   pdfBase64: string,
   *   filename: string,
   *   coachingComment: string
   * }
   *
   * Uploads PDF to Cloudinary, sends via Baileys, records in manager_reports,
   * marks chat_ai.report_sent_at.
   */
  router.post('/reports/send', async (req, res) => {
    const {
      chatAiId,
      targetSessionId,
      pdfBase64,
      filename,
      coachingComment,
    } = req.body || {};

    // Input validation
    if (!chatAiId || typeof chatAiId !== 'string') {
      return res.status(400).json({ error: 'chatAiId is required' });
    }
    if (!targetSessionId || typeof targetSessionId !== 'string') {
      return res.status(400).json({ error: 'targetSessionId is required' });
    }
    if (!pdfBase64 || typeof pdfBase64 !== 'string') {
      return res.status(400).json({ error: 'pdfBase64 is required' });
    }
    if (!filename || typeof filename !== 'string') {
      return res.status(400).json({ error: 'filename is required' });
    }
    // Enforce reasonable PDF size: 10MB base64 ≈ 7.5MB decoded
    if (pdfBase64.length > 14_000_000) {
      return res.status(400).json({ error: 'pdfBase64 too large (max ~10MB)' });
    }

    const db = req.userClient ?? supabase;

    try {
      // Load active sessions
      const activeSessions = await getActiveSessions(db);
      const activeSet = new Set(activeSessions.map((s) => s.session_id));

      // Validate target is an active session
      if (!activeSet.has(targetSessionId)) {
        return res.status(409).json({
          error: `Target session "${targetSessionId}" is not active. Choose an active session.`,
        });
      }

      // Idempotency: check for recent duplicate (within 60 seconds)
      // SERVICE-ROLE INTENTIONAL: dedup must execute even if user JWT expired
      // between PDF upload and the audit trail. Tightening RLS policy later
      // (user_id = auth.uid()) would turn this into a silent-deny → duplicate
      // reports sent. Global dedup is a system invariant, not per-user.
      const since60s = new Date(Date.now() - 60_000).toISOString();
      const { data: existingReport } = await supabase
        .from('manager_reports')
        .select('id')
        .eq('chat_ai_id', chatAiId)
        .eq('target_session_id', targetSessionId)
        .eq('status', 'sent')
        .gte('sent_at', since60s)
        .maybeSingle();

      if (existingReport) {
        return res.status(409).json({
          error: 'Report already sent in the last 60 seconds (duplicate request blocked)',
          existingReportId: existingReport.id,
        });
      }

      // Load chat_ai for dialog_session_id + remote_jid
      const chatAi = await getChatAiById(chatAiId, db);
      if (!chatAi) {
        return res.status(404).json({ error: 'chat_ai record not found' });
      }

      // Resolve sender session
      let senderSessionId;
      try {
        senderSessionId = resolveSender(targetSessionId, activeSessions);
      } catch (routingErr) {
        return res.status(409).json({ error: routingErr.message });
      }

      // Get target phone number from session_config
      const targetConfig = activeSessions.find((s) => s.session_id === targetSessionId);
      if (!targetConfig?.phone_number) {
        return res.status(409).json({
          error: `Cannot find phone number for session "${targetSessionId}". Make sure the session is connected and phone_number is set in session_config.`,
        });
      }

      const targetJid = `${targetConfig.phone_number}@s.whatsapp.net`;

      // Get sender socket
      const senderEntry = sessionManager.getSession(senderSessionId);
      const senderSock = senderEntry?.sock;
      if (!senderSock) {
        return res.status(409).json({
          error: `Sender session "${senderSessionId}" is configured but socket is not available. Wait for it to connect.`,
        });
      }

      // Upload PDF to Cloudinary
      let pdfUrl;
      const safeFilename = filename.slice(0, 200).replace(/[^a-zA-Z0-9._-]/g, '_');

      // SERVICE-ROLE INTENTIONAL: insertManagerReport, markChatAiReportSent below all use
      // service_role client (imported at top of file). These are audit-trail writes that must
      // succeed regardless of the caller's JWT state (expired, rotated, etc). Do NOT migrate
      // these to userClient — a failed audit write would leave an untracked sent report.
      try {
        const uploadResult = await uploadReportPdf(pdfBase64, safeFilename);
        pdfUrl = uploadResult.url;
      } catch (uploadErr) {
        logger.error({ err: uploadErr, chatAiId }, 'reports/send: Cloudinary upload failed');

        // Record failed attempt for audit
        await insertManagerReport({
          chat_ai_id: chatAiId,
          dialog_session_id: chatAi.dialog_session_id ?? null,
          client_remote_jid: chatAi.remote_jid ?? 'unknown',
          target_session_id: targetSessionId,
          sender_session_id: senderSessionId,
          coaching_comment: coachingComment ?? null,
          filename: safeFilename,
          status: 'failed',
          error_message: `Cloudinary upload failed: ${uploadErr.message}`,
        });

        return res.status(502).json({ error: 'Failed to upload PDF to Cloudinary. Report not sent.' });
      }

      // Send via Baileys
      let baileysMessageId = null;

      try {
        const pdfBuffer = Buffer.from(pdfBase64, 'base64');
        const sent = await senderSock.sendMessage(targetJid, {
          document: pdfBuffer,
          fileName: safeFilename.endsWith('.pdf') ? safeFilename : `${safeFilename}.pdf`,
          mimetype: 'application/pdf',
          caption: coachingComment ?? '',
        });
        baileysMessageId = sent?.key?.id ?? null;
      } catch (sendErr) {
        logger.error({ err: sendErr, chatAiId, targetJid }, 'reports/send: Baileys sendMessage failed');

        // Record failed attempt
        await insertManagerReport({
          chat_ai_id: chatAiId,
          dialog_session_id: chatAi.dialog_session_id ?? null,
          client_remote_jid: chatAi.remote_jid ?? 'unknown',
          target_session_id: targetSessionId,
          sender_session_id: senderSessionId,
          coaching_comment: coachingComment ?? null,
          pdf_cloudinary_url: pdfUrl,
          filename: safeFilename,
          status: 'failed',
          error_message: `WhatsApp send failed: ${sendErr.message}`,
        });

        return res.status(502).json({ error: 'PDF uploaded but WhatsApp delivery failed. Check session connectivity.' });
      }

      // Record successful send
      const reportId = await insertManagerReport({
        chat_ai_id: chatAiId,
        dialog_session_id: chatAi.dialog_session_id ?? null,
        client_remote_jid: chatAi.remote_jid ?? 'unknown',
        target_session_id: targetSessionId,
        sender_session_id: senderSessionId,
        coaching_comment: coachingComment ?? null,
        pdf_cloudinary_url: pdfUrl,
        filename: safeFilename,
        baileys_message_id: baileysMessageId,
        status: 'sent',
      });

      // Mark chat_ai as reported (enables "dead" badge in UI)
      await markChatAiReportSent(chatAiId);

      // Invalidate analytics cache so dashboard reflects the change
      invalidateAnalyticsCache();

      logger.info(
        { chatAiId, targetSessionId, senderSessionId, reportId, baileysMessageId },
        'reports/send: report sent successfully'
      );

      return res.json({
        success: true,
        messageId: baileysMessageId,
        reportId,
        pdfUrl,
      });
    } catch (err) {
      logger.error({ err, chatAiId }, 'reports/send: unexpected error');
      return res.status(500).json({ error: 'Failed to send report' });
    }
  });

  /**
   * GET /reports
   * Query: ?session_id=X&date_from=YYYY-MM-DD&date_to=YYYY-MM-DD&limit=100
   *
   * Returns the audit journal of sent reports with resolved display names.
   */
  router.get('/reports', async (req, res) => {
    const db = req.userClient ?? supabase;
    const sessionId = req.query.session_id || null;
    const dateFrom = req.query.date_from || null;
    const dateTo = req.query.date_to || null;
    const limit = Math.min(Number(req.query.limit) || 100, 500);

    try {
      const rows = await listManagerReports({ sessionId, dateFrom, dateTo, limit, db });

      // Resolve session display names for target and sender
      const allSessionIds = [...new Set([
        ...rows.map((r) => r.target_session_id),
        ...rows.map((r) => r.sender_session_id),
      ].filter(Boolean))];

      let sessionNameMap = {};
      if (allSessionIds.length > 0) {
        const { data: configs } = await db
          .from('session_config')
          .select('session_id, display_name')
          .in('session_id', allSessionIds);
        for (const c of configs ?? []) {
          sessionNameMap[c.session_id] = c.display_name || c.session_id;
        }
      }

      // Resolve client display names
      const clientJids = [...new Set(rows.map((r) => r.client_remote_jid).filter(Boolean))];
      let clientNameMap = {};
      if (clientJids.length > 0) {
        const { data: chats } = await db
          .from('chats')
          .select('remote_jid, display_name')
          .in('remote_jid', clientJids);
        for (const c of chats ?? []) {
          clientNameMap[c.remote_jid] = c.display_name || c.remote_jid;
        }
      }

      const reports = rows.map((r) => ({
        id: r.id,
        chatAiId: r.chat_ai_id,
        clientName: clientNameMap[r.client_remote_jid] || r.client_remote_jid,
        clientPhone: r.client_remote_jid,
        targetSessionId: r.target_session_id,
        targetDisplayName: sessionNameMap[r.target_session_id] || r.target_session_id,
        senderSessionId: r.sender_session_id,
        senderDisplayName: sessionNameMap[r.sender_session_id] || r.sender_session_id,
        coachingComment: r.coaching_comment,
        pdfUrl: r.pdf_cloudinary_url,
        filename: r.filename,
        baileysMessageId: r.baileys_message_id,
        status: r.status,
        errorMessage: r.error_message,
        sentAt: r.sent_at,
      }));

      return res.json({ total: reports.length, reports });
    } catch (err) {
      logger.error({ err }, 'GET /reports failed');
      return res.status(500).json({ error: 'Failed to fetch reports' });
    }
  });

  // ==========================================================================
  // Settings: tenant_settings
  // ==========================================================================

  /**
   * GET /settings/tenant
   * Returns { roles, cities, tags } for the authenticated tenant.
   * If no row exists yet, returns the schema defaults and does NOT insert.
   */
  router.get('/settings/tenant', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      let settings = await getTenantSettings(userId, db);

      if (!settings) {
        // First visit — return schema defaults without writing to DB.
        // The first PUT will create the row.
        settings = {
          roles:           ['клиент', 'партнёр', 'менеджер', 'другое'],
          cities:          [],
          tags:            [],
          lead_sources:    [],
          refusal_reasons: [],
          task_types:      [],
          company_profile: {},
        };
      } else {
        // Back-fill new columns for rows created before 0008 migration
        settings.lead_sources    = settings.lead_sources    ?? [];
        settings.refusal_reasons = settings.refusal_reasons ?? [];
        settings.task_types      = settings.task_types      ?? [];
        settings.company_profile = settings.company_profile ?? {};
      }

      return res.json(settings);
    } catch (err) {
      logger.error({ err, userId }, 'GET /settings/tenant failed');
      return res.status(500).json({ error: 'Failed to fetch tenant settings' });
    }
  });

  /**
   * PUT /settings/tenant
   * Body: { roles?, cities?, tags? }  — partial update, merges with existing values.
   * Upserts the row (INSERT on first call, UPDATE on subsequent calls).
   */
  router.put('/settings/tenant', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = TenantSettingsPutSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const result = await upsertTenantSettings(userId, parsed.data, db);
      if (!result) {
        return res.status(500).json({ error: 'Failed to save tenant settings' });
      }
      return res.json(result);
    } catch (err) {
      logger.error({ err, userId }, 'PUT /settings/tenant failed');
      return res.status(500).json({ error: 'Failed to save tenant settings' });
    }
  });

  // ==========================================================================
  // Funnel stages
  // ==========================================================================

  /**
   * GET /funnel/stages
   * Returns ordered list of funnel stages for the tenant.
   * If no stages exist, seeds one default "Новый лид" stage and returns it.
   */
  router.get('/funnel/stages', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      let stages = await getFunnelStages(userId, db);

      if (stages.length === 0) {
        // Seed default stage for new tenants so UI is never blank
        const defaultStage = await createFunnelStage(
          userId,
          { name: 'Новый лид', color: '#3b82f6', is_final: false },
          db
        );
        stages = defaultStage ? [defaultStage] : [];
      }

      return res.json(stages);
    } catch (err) {
      logger.error({ err, userId }, 'GET /funnel/stages failed');
      return res.status(500).json({ error: 'Failed to fetch funnel stages' });
    }
  });

  /**
   * POST /funnel/stages
   * Body: { name, color?, is_final? }
   * Appends a new stage at the end (sort_order = max + 1).
   */
  router.post('/funnel/stages', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = FunnelStageCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const stage = await createFunnelStage(userId, parsed.data, db);
      if (!stage) {
        return res.status(500).json({ error: 'Failed to create funnel stage' });
      }
      return res.status(201).json(stage);
    } catch (err) {
      logger.error({ err, userId }, 'POST /funnel/stages failed');
      return res.status(500).json({ error: 'Failed to create funnel stage' });
    }
  });

  /**
   * PATCH /funnel/stages/:id
   * Body: { name?, color?, is_final? }  — partial update.
   */
  router.patch('/funnel/stages/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = FunnelStagePatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const stage = await updateFunnelStage(id, userId, parsed.data, db);
      if (!stage) {
        return res.status(404).json({ error: 'Stage not found or access denied' });
      }
      return res.json(stage);
    } catch (err) {
      logger.error({ err, userId, stageId: id }, 'PATCH /funnel/stages/:id failed');
      return res.status(500).json({ error: 'Failed to update funnel stage' });
    }
  });

  /**
   * DELETE /funnel/stages/:id
   * Returns 409 if any active chat_ai deal is on this stage.
   * Returns 404 if stage not found or belongs to another tenant.
   */
  router.delete('/funnel/stages/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const result = await deleteFunnelStage(id, userId, db);

      if (result.conflict) {
        return res.status(409).json({
          error: `Cannot delete: ${result.conflictCount} active deal(s) are on this stage`,
          conflictCount: result.conflictCount,
        });
      }

      if (!result.deleted) {
        return res.status(404).json({ error: 'Stage not found or access denied' });
      }

      return res.json({ deleted: true });
    } catch (err) {
      logger.error({ err, userId, stageId: id }, 'DELETE /funnel/stages/:id failed');
      return res.status(500).json({ error: 'Failed to delete funnel stage' });
    }
  });

  /**
   * POST /funnel/stages/reorder
   * Body: { order: [id1, id2, id3, ...] }
   * Batch-updates sort_order so each stage gets its position index.
   */
  router.post('/funnel/stages/reorder', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = FunnelStageReorderSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const ok = await reorderFunnelStages(userId, parsed.data.order, db);
      if (!ok) {
        return res.status(500).json({ error: 'Failed to reorder stages' });
      }
      return res.json({ reordered: parsed.data.order.length });
    } catch (err) {
      logger.error({ err, userId }, 'POST /funnel/stages/reorder failed');
      return res.status(500).json({ error: 'Failed to reorder stages' });
    }
  });

  // ==========================================================================
  // Settings: managers
  // ==========================================================================

  /**
   * GET /settings/managers
   * Returns list of managers for the authenticated tenant, sorted by sort_order ASC,
   * then created_at ASC.
   */
  router.get('/settings/managers', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const { data, error } = await db
        .from('managers')
        .select('*')
        .eq('user_id', userId)
        .order('sort_order', { ascending: true })
        .order('created_at', { ascending: true });

      if (error) throw error;
      return res.json(data ?? []);
    } catch (err) {
      logger.error({ err, userId }, 'GET /settings/managers failed');
      return res.status(500).json({ error: 'Failed to fetch managers' });
    }
  });

  /**
   * POST /settings/managers
   * Body: { name, email?, phone?, session_ids?, notes? }
   * Creates a new manager. sort_order is set to max+1 among existing rows.
   */
  router.post('/settings/managers', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ManagerCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      // Determine next sort_order
      const { data: maxRow } = await db
        .from('managers')
        .select('sort_order')
        .eq('user_id', userId)
        .order('sort_order', { ascending: false })
        .limit(1)
        .maybeSingle();

      const sortOrder = maxRow ? maxRow.sort_order + 1 : 0;

      const { data, error } = await db
        .from('managers')
        .insert({
          user_id:     userId,
          name:        parsed.data.name,
          email:       parsed.data.email || null,
          phone:       parsed.data.phone || null,
          session_ids: parsed.data.session_ids ?? [],
          notes:       parsed.data.notes || null,
          sort_order:  sortOrder,
        })
        .select()
        .single();

      if (error) throw error;
      return res.status(201).json(data);
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/managers failed');
      return res.status(500).json({ error: 'Failed to create manager' });
    }
  });

  /**
   * POST /settings/managers/reorder
   * Body: { ids: [uuid, ...] }
   * Atomically resets sort_order so each id gets its array-index position.
   * Must be registered BEFORE PATCH /:id to avoid Express matching "reorder" as :id.
   */
  router.post('/settings/managers/reorder', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ReorderSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      // Atomic batch update: each id gets sort_order = its index position.
      // RLS ensures only the tenant's own rows can be touched.
      await Promise.all(
        parsed.data.ids.map((id, idx) =>
          db
            .from('managers')
            .update({ sort_order: idx })
            .eq('id', id)
            .eq('user_id', userId)
        )
      );
      return res.json({ reordered: parsed.data.ids.length });
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/managers/reorder failed');
      return res.status(500).json({ error: 'Failed to reorder managers' });
    }
  });

  /**
   * PATCH /settings/managers/:id
   * Body: partial manager fields.
   * Returns 404 when the row doesn't exist or belongs to another tenant.
   */
  router.patch('/settings/managers/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ManagerPatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    // Build update payload from only the provided fields
    const patch = {};
    if (parsed.data.name        !== undefined) patch.name        = parsed.data.name;
    if (parsed.data.email       !== undefined) patch.email       = parsed.data.email || null;
    if (parsed.data.phone       !== undefined) patch.phone       = parsed.data.phone || null;
    if (parsed.data.session_ids !== undefined) patch.session_ids = parsed.data.session_ids;
    if (parsed.data.is_active   !== undefined) patch.is_active   = parsed.data.is_active;
    if (parsed.data.notes       !== undefined) patch.notes       = parsed.data.notes || null;

    try {
      const { data, error } = await db
        .from('managers')
        .update(patch)
        .eq('id', id)
        .eq('user_id', userId)
        .select()
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Manager not found or access denied' });
      }
      return res.json(data);
    } catch (err) {
      logger.error({ err, userId, managerId: id }, 'PATCH /settings/managers/:id failed');
      return res.status(500).json({ error: 'Failed to update manager' });
    }
  });

  /**
   * DELETE /settings/managers/:id
   */
  router.delete('/settings/managers/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const { data, error } = await db
        .from('managers')
        .delete()
        .eq('id', id)
        .eq('user_id', userId)
        .select('id')
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Manager not found or access denied' });
      }
      return res.json({ deleted: true });
    } catch (err) {
      logger.error({ err, userId, managerId: id }, 'DELETE /settings/managers/:id failed');
      return res.status(500).json({ error: 'Failed to delete manager' });
    }
  });

  // ==========================================================================
  // Settings: message_templates
  // ==========================================================================

  /**
   * GET /settings/templates
   * Query: ?category=<category>  (optional filter)
   * Returns templates sorted by sort_order ASC, created_at ASC.
   */
  router.get('/settings/templates', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { category } = req.query;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      let query = db
        .from('message_templates')
        .select('*')
        .eq('user_id', userId)
        .order('sort_order', { ascending: true })
        .order('created_at', { ascending: true });

      if (category && typeof category === 'string') {
        query = query.eq('category', category.trim());
      }

      const { data, error } = await query;
      if (error) throw error;
      return res.json(data ?? []);
    } catch (err) {
      logger.error({ err, userId }, 'GET /settings/templates failed');
      return res.status(500).json({ error: 'Failed to fetch templates' });
    }
  });

  /**
   * POST /settings/templates
   * Body: { title, body, category? }
   */
  router.post('/settings/templates', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = TemplateCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const { data: maxRow } = await db
        .from('message_templates')
        .select('sort_order')
        .eq('user_id', userId)
        .order('sort_order', { ascending: false })
        .limit(1)
        .maybeSingle();

      const sortOrder = maxRow ? maxRow.sort_order + 1 : 0;

      const { data, error } = await db
        .from('message_templates')
        .insert({
          user_id:    userId,
          title:      parsed.data.title,
          body:       parsed.data.body,
          category:   parsed.data.category ?? 'general',
          sort_order: sortOrder,
        })
        .select()
        .single();

      if (error) throw error;
      return res.status(201).json(data);
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/templates failed');
      return res.status(500).json({ error: 'Failed to create template' });
    }
  });

  /**
   * POST /settings/templates/reorder
   * Body: { ids: [uuid, ...] }
   * Must be registered BEFORE PATCH /:id.
   */
  router.post('/settings/templates/reorder', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ReorderSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      await Promise.all(
        parsed.data.ids.map((id, idx) =>
          db
            .from('message_templates')
            .update({ sort_order: idx })
            .eq('id', id)
            .eq('user_id', userId)
        )
      );
      return res.json({ reordered: parsed.data.ids.length });
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/templates/reorder failed');
      return res.status(500).json({ error: 'Failed to reorder templates' });
    }
  });

  /**
   * PATCH /settings/templates/:id
   * Body: partial template fields.
   */
  router.patch('/settings/templates/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = TemplatePatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    const patch = {};
    if (parsed.data.title    !== undefined) patch.title    = parsed.data.title;
    if (parsed.data.body     !== undefined) patch.body     = parsed.data.body;
    if (parsed.data.category !== undefined) patch.category = parsed.data.category;

    try {
      const { data, error } = await db
        .from('message_templates')
        .update(patch)
        .eq('id', id)
        .eq('user_id', userId)
        .select()
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Template not found or access denied' });
      }
      return res.json(data);
    } catch (err) {
      logger.error({ err, userId, templateId: id }, 'PATCH /settings/templates/:id failed');
      return res.status(500).json({ error: 'Failed to update template' });
    }
  });

  /**
   * DELETE /settings/templates/:id
   */
  router.delete('/settings/templates/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const { data, error } = await db
        .from('message_templates')
        .delete()
        .eq('id', id)
        .eq('user_id', userId)
        .select('id')
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Template not found or access denied' });
      }
      return res.json({ deleted: true });
    } catch (err) {
      logger.error({ err, userId, templateId: id }, 'DELETE /settings/templates/:id failed');
      return res.status(500).json({ error: 'Failed to delete template' });
    }
  });

  app.use('/', router);
}
