/**
 * Issues module — helpers for GET /ai/issues and POST /ai/issues/:dialog_session_id/dismiss.
 *
 * Powers the "проблемные переписки" carousel on the Analytics dashboard.
 * 5 categories: slow | no_followup | critical | football | lost.
 *
 * Design notes:
 * - Uses req.userClient (RLS-aware) for all DB reads/writes — never service_role
 *   on user-facing paths. This mirrors the pattern from routes.js dismiss-problem.
 * - 1-hour in-memory cache per (userId, category, page, limit).
 *   Invalidated on every dismiss — dismissed rows must disappear immediately.
 * - v_football_cases view (migration 0013) already filters to 7-day window
 *   and requires count(DISTINCT session_id) >= 2. No JS post-filter needed.
 * - 'critical' category: PostgREST can't express "risk_flags non-empty" as a
 *   single filter. Fetch superset via OR and post-filter in JS — same pattern
 *   as analytics/chats-by-filter (decisions.md 2026-04-25).
 * - 'lost': returns only problem_dismissed_action='lost' rows (no dismissed_at filter).
 * - All other categories exclude problem_dismissed_at IS NOT NULL.
 *
 * WIN_STAGES — stages where dismissing = "deal moved forward" (won).
 * Anything else → lost. Kept in sync with dismiss-problem in routes.js.
 *
 * Cache TTL: 1h (problems change slowly; dismissed rows removed on write).
 * Cache key: category|page|limit|userId — per-user to avoid RLS cross-leak.
 */

import { supabase as serviceClient } from '../storage/supabase.js';
import { logger } from '../config.js';

// ─── Cache ───────────────────────────────────────────────────────────────────

const ISSUES_CACHE_TTL_MS = 60 * 60 * 1000; // 1h
const _issuesCache = new Map();

function _cacheKey(userId, category, page, limit) {
  // service-role callers (x-api-key) don't cache — would leak across users
  if (!userId) return null;
  return `issues|${userId}|${category}|${page}|${limit}`;
}

function _cacheGet(key) {
  if (!key) return null;
  const e = _issuesCache.get(key);
  if (!e) return null;
  if (Date.now() > e.expiresAt) { _issuesCache.delete(key); return null; }
  return e.data;
}

function _cacheSet(key, data) {
  if (!key) return;
  _issuesCache.set(key, { data, expiresAt: Date.now() + ISSUES_CACHE_TTL_MS });
  // Light cleanup — prevent unbounded growth
  if (_issuesCache.size > 500) {
    const now = Date.now();
    for (const [k, v] of _issuesCache) if (v.expiresAt < now) _issuesCache.delete(k);
  }
}

export function invalidateIssuesCache() {
  _issuesCache.clear();
}

// ─── Constants ───────────────────────────────────────────────────────────────

const VALID_CATEGORIES = new Set(['slow', 'no_followup', 'critical', 'football', 'lost']);

// Stages where the deal moved forward — dismiss = won.
// All other stages (NULL, consultation stuck, refused, needs_review) = lost.
// Keep in sync with dismiss-problem handler in routes.js.
const WIN_STAGES = new Set([
  'completed', 'delivery', 'payment', 'closed_won', 'post_sale',
  'Завершено', 'Доставка', 'Оплата',
]);

// chat_ai columns we always select (covers all 5 categories)
const CHAT_AI_SELECT = [
  'id',
  'dialog_session_id',
  'session_id',
  'remote_jid',
  'analysis_date',
  'analyzed_at',
  'summary_ru',
  'deal_stage',
  'sentiment',
  'intent',
  'risk_flags',
  'manager_issues',
  'action_required',
  'action_suggestion',
  'problem_dismissed_action',
  'problem_dismissed_at',
].join(', ');

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Format phone digits from JID (e.g. "77001234567@s.whatsapp.net" → "77001234567").
 * Returns null for group JIDs.
 */
function _phoneFromJid(jid) {
  if (!jid || jid.includes('-') || jid.includes('120363')) return null;
  return jid.replace(/[^0-9]/g, '') || null;
}

/**
 * Build display name: prefer CRM display_name, fall back to phone.
 */
function _clientName(chatRow, jid) {
  return chatRow?.display_name || _phoneFromJid(jid) || jid;
}

// ─── Main export: getIssues ───────────────────────────────────────────────────

/**
 * Fetch paginated list of problem conversations for a given category.
 *
 * @param {object} opts
 * @param {string} opts.category - 'slow' | 'no_followup' | 'critical' | 'football' | 'lost'
 * @param {number} opts.page - 0-based page index
 * @param {number} opts.limit - items per page (max 50)
 * @param {object} opts.db - Supabase client (req.userClient or serviceClient)
 * @param {string|null} opts.userId - for cache key (req.user?.userId)
 * @returns {Promise<{category, items, page, limit, total, has_more}>}
 */
export async function getIssues({ category, page, limit, db, userId }) {
  if (!VALID_CATEGORIES.has(category)) {
    throw Object.assign(new Error(`Invalid category: ${category}`), { statusCode: 400 });
  }

  const pg = Math.max(0, Math.floor(page));
  const lim = Math.min(Math.max(1, Math.floor(limit)), 50);

  // Cache check
  const ck = _cacheKey(userId, category, pg, lim);
  const cached = _cacheGet(ck);
  if (cached) return cached;

  // For 'football' we need the remote_jid set from v_football_cases first
  let footballMeta = null; // Map<jid, { sessions, count, last_outbound_at }>
  if (category === 'football') {
    const { data: fbRows, error: fbErr } = await db
      .from('v_football_cases')
      .select('remote_jid, sessions, session_count, last_outbound_at');
    if (fbErr) {
      logger.error({ err: fbErr }, 'getIssues: v_football_cases query failed');
      throw new Error('Football view query failed');
    }
    footballMeta = new Map();
    for (const r of fbRows ?? []) {
      footballMeta.set(r.remote_jid, {
        sessions: r.sessions || [],
        count: r.session_count || 0,
        lastOutboundAt: r.last_outbound_at || null,
      });
    }
    if (footballMeta.size === 0) {
      const result = _emptyResult(category, pg, lim);
      _cacheSet(ck, result);
      return result;
    }
  }

  // Build base query
  let q = db.from('chat_ai').select(CHAT_AI_SELECT);

  // All categories except 'lost' exclude already-dismissed rows
  if (category !== 'lost') {
    q = q.is('problem_dismissed_at', null);
  }

  // Category-specific filters
  if (category === 'slow') {
    q = q.contains('manager_issues', ['slow_first_response']);
  } else if (category === 'no_followup') {
    q = q.contains('manager_issues', ['no_followup']);
  } else if (category === 'critical') {
    // PostgREST can't filter "array non-empty" directly — fetch superset, JS-filter.
    // Superset: sentiment=aggressive OR intent=complaint OR risk_flags not null.
    // The OR uses PostgREST's .or() syntax which is safe here (no user-supplied values).
    q = q.or('sentiment.eq.aggressive,intent.eq.complaint,risk_flags.not.is.null');
  } else if (category === 'football') {
    q = q.in('remote_jid', [...footballMeta.keys()]);
  } else if (category === 'lost') {
    q = q.eq('problem_dismissed_action', 'lost');
  }

  // Order: newest analysis first
  q = q.order('analysis_date', { ascending: false }).order('id', { ascending: false });

  // Fetch all matching rows (we need total count for pagination).
  // Max reasonable size: 200. If category grows past that, we'd add server-side
  // pagination with a count query — not needed at current scale.
  q = q.limit(200);

  const { data: rawRows, error: qErr } = await q;
  if (qErr) {
    logger.error({ err: qErr, category }, 'getIssues: chat_ai query failed');
    throw new Error('chat_ai query failed');
  }

  // JS post-filter for 'critical' (array non-empty check)
  const allRows = category === 'critical'
    ? (rawRows || []).filter((r) =>
        r.sentiment === 'aggressive' ||
        r.intent === 'complaint' ||
        (Array.isArray(r.risk_flags) && r.risk_flags.length > 0)
      )
    : (rawRows || []);

  const total = allRows.length;

  // Paginate
  const slice = allRows.slice(pg * lim, pg * lim + lim);

  if (slice.length === 0) {
    const result = _emptyResult(category, pg, lim, total);
    _cacheSet(ck, result);
    return result;
  }

  // Enrich: fetch chat display names for the sliced jids
  const uniqueJids = [...new Set(slice.map((r) => r.remote_jid))];
  const uniqueSessions = [...new Set(slice.map((r) => r.session_id))];

  const [chatsRes, sessionsRes] = await Promise.all([
    db.from('chats')
      .select('session_id, remote_jid, display_name')
      .in('session_id', uniqueSessions),
    db.from('session_config')
      .select('session_id, display_name')
      .in('session_id', uniqueSessions),
  ]);

  const chatsMap = new Map();
  for (const c of chatsRes.data ?? []) {
    chatsMap.set(`${c.session_id}:::${c.remote_jid}`, c);
  }
  const sessionsMap = new Map();
  for (const s of sessionsRes.data ?? []) {
    sessionsMap.set(s.session_id, s.display_name || s.session_id);
  }

  // Build items
  const items = slice.map((r) => {
    const chatKey = `${r.session_id}:::${r.remote_jid}`;
    const chat = chatsMap.get(chatKey);
    const fb = footballMeta?.get(r.remote_jid) || null;
    return {
      dialog_session_id: r.dialog_session_id,
      chat_ai_id: r.id,
      remote_jid: r.remote_jid,
      client_name: _clientName(chat, r.remote_jid),
      session_id: r.session_id,
      session_display_name: sessionsMap.get(r.session_id) || r.session_id,
      summary_ru: r.summary_ru || null,
      deal_stage: r.deal_stage || null,
      analyzed_at: r.analyzed_at || r.analysis_date || null,
      manager_issues: r.manager_issues || [],
      risk_flags: r.risk_flags || [],
      action_required: r.action_required || false,
      action_suggestion: r.action_suggestion || null,
      problem_dismissed_action: r.problem_dismissed_action || null,
      problem_dismissed_at: r.problem_dismissed_at || null,
      // Football-specific metadata (null for other categories)
      football_sessions: fb?.sessions || null,
      football_session_count: fb?.count || null,
      // total_amount: not joined here (sales data in separate DB segment).
      // Frontend can enrich via /sales-crm/partners if needed.
      total_amount: null,
    };
  });

  const result = {
    category,
    items,
    page: pg,
    limit: lim,
    total,
    has_more: (pg + 1) * lim < total,
  };

  _cacheSet(ck, result);
  return result;
}

function _emptyResult(category, page, limit, total = 0) {
  return { category, items: [], page, limit, total, has_more: false };
}

// ─── dismissIssue ─────────────────────────────────────────────────────────────

/**
 * Dismiss a problem conversation by dialog_session_id.
 * Determines won/lost from current deal_stage in chat_ai.
 * Clears the issues cache on success.
 *
 * @param {string} dialogSessionId - from URL param :dialog_session_id
 * @param {object} db - req.userClient (RLS-scoped)
 * @param {string|null} userId - req.user?.userId for audit trail
 * @returns {Promise<{ok: true, action: 'won'|'lost', alreadyDismissed?: true}>}
 */
export async function dismissIssue(dialogSessionId, db, userId) {
  if (!dialogSessionId || typeof dialogSessionId !== 'string') {
    throw Object.assign(new Error('dialog_session_id is required'), { statusCode: 400 });
  }

  // Read current deal_stage to determine won/lost
  const { data: existing, error: readErr } = await db
    .from('chat_ai')
    .select('id, deal_stage, problem_dismissed_action')
    .eq('dialog_session_id', dialogSessionId)
    .maybeSingle();

  if (readErr) {
    logger.error({ err: readErr, dialogSessionId }, 'dismissIssue: read failed');
    throw new Error('Read failed');
  }
  if (!existing) {
    throw Object.assign(
      new Error('chat_ai record not found for this dialog_session_id'),
      { statusCode: 404 }
    );
  }

  // Idempotent — already dismissed
  if (existing.problem_dismissed_action) {
    return { ok: true, action: existing.problem_dismissed_action, alreadyDismissed: true };
  }

  const action = WIN_STAGES.has(existing.deal_stage) ? 'won' : 'lost';
  const dismissedBy = userId || '__service__';

  const { error: updateErr } = await db
    .from('chat_ai')
    .update({
      problem_dismissed_action: action,
      problem_dismissed_at: new Date().toISOString(),
      problem_dismissed_by: dismissedBy,
    })
    .eq('id', existing.id);

  if (updateErr) {
    logger.error({ err: updateErr, dialogSessionId }, 'dismissIssue: update failed');
    throw new Error('Update failed');
  }

  // Invalidate cache — dismissed rows must vanish from active categories immediately
  invalidateIssuesCache();

  logger.info(
    { dialogSessionId, chatAiId: existing.id, action, userId: dismissedBy, dealStage: existing.deal_stage },
    'issue dismissed'
  );

  return { ok: true, action };
}
