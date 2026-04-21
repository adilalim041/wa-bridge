import { existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from 'fs';
import { logger } from '../config.js';
import { supabase } from './supabase.js';

// --- Failover queue for Supabase outages ---
const MAX_QUEUE_SIZE = 5000;
const FLUSH_INTERVAL_MS = 10_000;
const FLUSH_BATCH_SIZE = 200;
const QUEUE_ALERT_THRESHOLD = 1000;
const QUEUE_BACKUP_PATH = 'data/queue-backup.json';
const failoverQueue = [];
let flushTimer = null;
let alertSent = false;

// --- Heartbeat telemetry (read by heartbeat.js, written here) ---
// lastMessageProcessedAt: ISO string of the most recent successful saveMessage call.
// null until the first message is processed in this process lifetime.
let _lastMessageProcessedAt = null;

/** Called internally by saveMessage on every successful DB upsert. */
function _touchLastMessageProcessed() {
  _lastMessageProcessedAt = new Date().toISOString();
}

/** Returns ISO string of last successful message save, or null if none yet. */
export function getLastMessageProcessedAt() {
  return _lastMessageProcessedAt;
}

/**
 * Supabase health indicator derived from the failover queue depth.
 * Returns true when the queue is empty (Supabase is responding normally).
 * Returns false when messages have accumulated (recent Supabase failures).
 * Does NOT make a new DB roundtrip — reads in-memory state only.
 */
export function getSupabaseOk() {
  return failoverQueue.length === 0;
}

// Load backed-up queue on startup
try {
  if (existsSync(QUEUE_BACKUP_PATH)) {
    const backed = JSON.parse(readFileSync(QUEUE_BACKUP_PATH, 'utf-8'));
    if (Array.isArray(backed) && backed.length > 0) {
      failoverQueue.push(...backed);
      logger.info({ restored: backed.length }, 'Restored messages from queue backup file');
      startFlushTimer();
    }
    unlinkSync(QUEUE_BACKUP_PATH);
  }
} catch (err) {
  logger.warn({ err }, 'Failed to restore queue backup');
}

function startFlushTimer() {
  if (flushTimer) return;
  flushTimer = setInterval(flushQueue, FLUSH_INTERVAL_MS);
}

async function flushQueue() {
  if (failoverQueue.length === 0) {
    if (flushTimer) {
      clearInterval(flushTimer);
      flushTimer = null;
    }
    return;
  }

  const batch = failoverQueue.splice(0, FLUSH_BATCH_SIZE);
  const retryLater = [];

  for (const row of batch) {
    try {
      const { error } = await supabase.from('messages').upsert(row, {
        onConflict: 'message_id,session_id',
        ignoreDuplicates: true,
      });
      if (error) retryLater.push(row);
    } catch {
      retryLater.push(row);
    }
  }

  if (retryLater.length > 0) {
    failoverQueue.unshift(...retryLater);
    logger.warn({ queueSize: failoverQueue.length, failedBatch: retryLater.length }, 'Failover queue: some messages still failing');
  } else {
    logger.info({ flushed: batch.length, remaining: failoverQueue.length }, 'Failover queue: batch flushed');
  }
}

export function getQueueStats() {
  return { size: failoverQueue.length, maxSize: MAX_QUEUE_SIZE, timerActive: flushTimer !== null };
}

export async function flushQueueOnShutdown() {
  if (failoverQueue.length === 0) return;
  logger.info({ queueSize: failoverQueue.length }, 'Flushing failover queue on shutdown...');

  // Try flushing up to 3 times
  for (let attempt = 0; attempt < 3 && failoverQueue.length > 0; attempt++) {
    await flushQueue();
    if (failoverQueue.length > 0) {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  // If messages remain after 3 attempts, persist to disk
  if (failoverQueue.length > 0) {
    try {
      mkdirSync('data', { recursive: true });
      writeFileSync(QUEUE_BACKUP_PATH, JSON.stringify(failoverQueue));
      logger.warn({ saved: failoverQueue.length }, 'Saved remaining queue to disk backup');
    } catch (err) {
      logger.error({ err, lost: failoverQueue.length }, 'CRITICAL: Failed to backup queue — messages may be lost');
    }
  }
}

export async function saveMessage(data) {
  const MAX_RETRIES = 3;
  const row = {
    message_id: data.messageId,
    session_id: data.sessionId,
    remote_jid: data.remoteJid,
    from_me: data.fromMe,
    body: data.body,
    message_type: data.messageType,
    push_name: data.pushName ?? null,
    sender: data.sender ?? null,
    chat_type: data.chatType ?? 'personal',
    media_url: data.mediaUrl ?? null,
    media_type: data.mediaType ?? null,
    file_name: data.fileName ?? null,
    timestamp: data.timestamp,
  };

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const { error } = await supabase.from('messages').upsert(row, {
        onConflict: 'message_id,session_id',
        ignoreDuplicates: true,
      });

      if (!error) {
        _touchLastMessageProcessed();
        return;
      }

      logger.error(
        { err: error, messageId: data.messageId, attempt },
        `Failed to save message (attempt ${attempt}/${MAX_RETRIES})`
      );

      if (attempt < MAX_RETRIES) {
        await new Promise((r) => setTimeout(r, 1000 * attempt));
      }
    } catch (error) {
      logger.error(
        { err: error, messageId: data.messageId, attempt },
        `Unexpected error saving message (attempt ${attempt}/${MAX_RETRIES})`
      );

      if (attempt < MAX_RETRIES) {
        await new Promise((r) => setTimeout(r, 1000 * attempt));
      }
    }
  }

  // All retries exhausted — queue for later flush instead of losing
  if (failoverQueue.length >= MAX_QUEUE_SIZE) {
    // Backup to disk before dropping
    try {
      mkdirSync('data', { recursive: true });
      writeFileSync(QUEUE_BACKUP_PATH, JSON.stringify(failoverQueue));
    } catch { /* best effort */ }
    const dropped = failoverQueue.shift();
    logger.error({ droppedMessageId: dropped.message_id, queueSize: MAX_QUEUE_SIZE }, 'CRITICAL: Failover queue full, dropped oldest message');
  }
  failoverQueue.push(row);
  startFlushTimer();
  logger.warn({ messageId: data.messageId, queueSize: failoverQueue.length }, 'Message queued for retry (Supabase outage)');

  // Alert via Telegram when queue grows beyond threshold
  if (failoverQueue.length >= QUEUE_ALERT_THRESHOLD && !alertSent) {
    alertSent = true;
    import('../notifications/telegramBot.js').then(({ sendTelegramMessage }) => {
      sendTelegramMessage(`⚠️ Failover queue: ${failoverQueue.length} messages queued. Supabase may be down.`).catch(() => {});
    }).catch(() => {});
    // Reset alert flag after 5 minutes
    setTimeout(() => { alertSent = false; }, 5 * 60 * 1000);
  }
}

export async function upsertChat({
  remoteJid,
  sessionId,
  chatType,
  displayName,
  participantCount,
  phoneNumber,
}) {
  try {
    let existingChat = null;
    const needsExistingChat =
      chatType === 'group'
        ? !displayName || participantCount == null
        : !displayName || !phoneNumber;

    if (needsExistingChat) {
      const { data, error: fetchError } = await supabase
        .from('chats')
        .select('display_name, participant_count, phone_number')
        .eq('remote_jid', remoteJid)
        .eq('session_id', sessionId)
        .maybeSingle();

      if (fetchError) {
        logger.error({ err: fetchError, remoteJid, sessionId }, 'Failed to load existing chat metadata');
      } else {
        existingChat = data;
      }
    }

    const { error } = await supabase.from('chats').upsert(
      {
        remote_jid: remoteJid,
        session_id: sessionId,
        chat_type: chatType || 'personal',
        display_name: displayName || existingChat?.display_name || null,
        participant_count: participantCount ?? existingChat?.participant_count ?? null,
        phone_number: phoneNumber || existingChat?.phone_number || null,
        last_message_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
      {
        onConflict: 'remote_jid,session_id',
      }
    );

    if (error) {
      logger.error({ err: error, remoteJid, sessionId }, 'Failed to upsert chat');
    } else {
      // Default tag for new chats — set only if no chat_tags record exists yet.
      // Allows the AI worker to later replace 'неизвестно' with the real role.
      // Groups (remote_jid ends with @g.us) are skipped — AI never classifies them.
      if (!remoteJid.endsWith('@g.us')) {
        const existing = await getChatTags(remoteJid);
        if (existing.tags.length === 0) {
          await upsertChatTags(remoteJid, { tags: ['неизвестно'], tagConfirmed: false });
        }
      }
    }
  } catch (error) {
    logger.error({ err: error, remoteJid, sessionId }, 'Unexpected error while upserting chat');
  }
}

export async function getContactName(remoteJid) {
  try {
    if (!remoteJid) {
      return null;
    }

    const { data, error } = await supabase
      .from('contacts')
      .select('name')
      .eq('phone', remoteJid)
      .maybeSingle();

    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to fetch contact name');
      return null;
    }

    return data?.name || null;
  } catch (error) {
    logger.error({ err: error, remoteJid }, 'Unexpected error while fetching contact name');
    return null;
  }
}

export async function saveContact(remoteJid, pushName) {
  try {
    const payload = {
      phone: remoteJid,
      updated_at: new Date().toISOString(),
    };

    if (pushName) {
      payload.name = pushName;
    }

    const { error } = await supabase.from('contacts').upsert(payload, {
      onConflict: 'phone',
    });

    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to save contact');
    }
  } catch (error) {
    logger.error({ err: error, remoteJid }, 'Unexpected error while saving contact');
  }
}

export async function getMessages(sessionId, remoteJid, limit = 50, offset = 0, db = supabase) {
  // W1.1 Phase 2: accept optional dbClient so RLS-filtered reads work via userClient
  try {
    let query = db
      .from('messages')
      .select('*')
      .eq('session_id', sessionId);

    if (remoteJid) {
      query = query.eq('remote_jid', remoteJid);
    }

    const { data, error } = await query
      .order('timestamp', { ascending: false })
      .range(offset, offset + limit - 1);

    if (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Failed to fetch messages');
      return [];
    }

    return data ?? [];
  } catch (error) {
    logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error while fetching messages');
    return [];
  }
}

// Find all sessions where this contact exists
export async function getLinkedSessions(remoteJid, db = supabase) {
  try {
    const [chatsRes, tagsRes] = await Promise.all([
      db
        .from('chats')
        .select('session_id, display_name, last_message_at')
        .eq('remote_jid', remoteJid),
      getChatTags(remoteJid, db),
    ]);

    if (chatsRes.error) {
      logger.error({ err: chatsRes.error, remoteJid }, 'Failed to fetch linked sessions');
      return [];
    }

    const data = chatsRes.data;
    if (!data?.length) return [];

    // Enrich with session display names
    const sessionIds = [...new Set(data.map((c) => c.session_id))];
    const { data: configs } = await db
      .from('session_config')
      .select('session_id, display_name')
      .in('session_id', sessionIds);

    const configMap = {};
    for (const c of configs || []) configMap[c.session_id] = c.display_name;

    // Tags are phone-level (W7A) — same for every session this contact appears in
    const sharedTags = tagsRes.tags || [];

    return data.map((c) => ({
      sessionId: c.session_id,
      sessionName: configMap[c.session_id] || c.session_id,
      lastMessageAt: c.last_message_at,
      contactName: c.display_name,
      tags: sharedTags,
    }));
  } catch (error) {
    logger.error({ err: error, remoteJid }, 'Unexpected error in getLinkedSessions');
    return [];
  }
}

// ---------------------------------------------------------------------------
// Chat tags (Wave 7A — phone-level, session-independent)
// ---------------------------------------------------------------------------
// Tags follow the PERSON, not the (session, person) pair. If Adil tags Bulat
// as "сотрудник" in session omoikiri-main, Bulat's chat in almaty-nurbolat
// inherits the same tag. Backed by the `chat_tags` table (see
// sql/wave7_enrichment.sql). Reads/writes here are session-agnostic.

export async function getChatTags(remoteJid, db = supabase) {
  if (!remoteJid) return { tags: [], tagConfirmed: false };
  try {
    const { data, error } = await db
      .from('chat_tags')
      .select('tags, tag_confirmed')
      .eq('remote_jid', remoteJid)
      .maybeSingle();
    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to fetch chat_tags');
      return { tags: [], tagConfirmed: false };
    }
    return {
      tags: Array.isArray(data?.tags) ? data.tags : [],
      tagConfirmed: Boolean(data?.tag_confirmed),
    };
  } catch (err) {
    logger.error({ err, remoteJid }, 'Unexpected error in getChatTags');
    return { tags: [], tagConfirmed: false };
  }
}

export async function getChatTagsByJids(remoteJids = [], db = supabase) {
  const uniqueJids = [...new Set((remoteJids || []).filter(Boolean))];
  if (!uniqueJids.length) return {};
  try {
    const { data, error } = await db
      .from('chat_tags')
      .select('remote_jid, tags, tag_confirmed')
      .in('remote_jid', uniqueJids);
    if (error) {
      logger.error({ err: error, count: uniqueJids.length }, 'Failed to batch-fetch chat_tags');
      return {};
    }
    const map = {};
    for (const row of data || []) {
      map[row.remote_jid] = {
        tags: Array.isArray(row.tags) ? row.tags : [],
        tagConfirmed: Boolean(row.tag_confirmed),
      };
    }
    return map;
  } catch (err) {
    logger.error({ err }, 'Unexpected error in getChatTagsByJids');
    return {};
  }
}

export async function upsertChatTags(remoteJid, { tags, tagConfirmed } = {}) {
  if (!remoteJid) return false;
  const cleanTags = Array.isArray(tags)
    ? [...new Set(tags.map((t) => t?.toString().trim().toLowerCase()).filter(Boolean))].slice(0, 10)
    : undefined;
  const payload = {
    remote_jid: remoteJid,
    updated_at: new Date().toISOString(),
  };
  if (cleanTags !== undefined) payload.tags = cleanTags;
  if (typeof tagConfirmed === 'boolean') payload.tag_confirmed = tagConfirmed;
  try {
    const { error } = await supabase
      .from('chat_tags')
      .upsert(payload, { onConflict: 'remote_jid' });
    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to upsert chat_tags');
      return false;
    }
    return true;
  } catch (err) {
    logger.error({ err, remoteJid }, 'Unexpected error in upsertChatTags');
    return false;
  }
}

// ---------------------------------------------------------------------------
// Calls
// ---------------------------------------------------------------------------

/**
 * UPSERT a call record.
 * On conflict (call_id, session_id):
 *   - Updates only non-null incoming fields.
 *   - For 'terminate' status: calculates duration_sec when answered_at exists.
 * Returns the final DB row, or null on error.
 */
/**
 * Convert raw DB call row (snake_case) to the API/frontend shape (camelCase).
 * Shared between REST endpoints and real-time WebSocket emits — both MUST
 * emit the same shape or the frontend breaks silently. A previous mismatch
 * caused real-time call events to never render in-chat (2026-04-20 bug).
 */
export function formatCallRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    callId: row.call_id,
    sessionId: row.session_id,
    remoteJid: row.remote_jid,
    fromMe: row.from_me,
    isVideo: row.is_video,
    isGroup: row.is_group,
    status: row.status,
    offeredAt: row.offered_at,
    answeredAt: row.answered_at ?? null,
    endedAt: row.ended_at ?? null,
    durationSec: row.duration_sec ?? null,
    missed: row.missed,
    createdAt: row.created_at,
  };
}

export async function upsertCall({
  callId,
  sessionId,
  remoteJid,
  fromMe,
  isVideo,
  isGroup,
  status,
  offeredAt,
  answeredAt,
  endedAt,
  durationSec,
  missed,
  rawData,
  terminateAt, // special flag: compute duration on terminate
}) {
  try {
    const now = new Date().toISOString();

    // For terminate: fetch existing row to compute duration if answered
    if (terminateAt) {
      const { data: existing } = await supabase
        .from('calls')
        .select('answered_at')
        .eq('call_id', callId)
        .eq('session_id', sessionId)
        .maybeSingle();

      if (existing?.answered_at) {
        const answeredMs = new Date(existing.answered_at).getTime();
        const endedMs = new Date(terminateAt).getTime();
        durationSec = Math.max(0, Math.round((endedMs - answeredMs) / 1000));
        missed = false;
      } else {
        // Call was never answered — caller hung up before pickup
        missed = true;
      }
      endedAt = terminateAt;
    }

    // Build update object — only include defined fields
    const row = {
      call_id: callId,
      session_id: sessionId,
      remote_jid: remoteJid,
      from_me: fromMe,
      is_video: isVideo,
      is_group: isGroup,
      status,
      updated_at: now,
    };

    if (offeredAt !== undefined) row.offered_at = offeredAt;
    if (answeredAt !== undefined) row.answered_at = answeredAt;
    if (endedAt !== undefined) row.ended_at = endedAt;
    if (durationSec !== undefined) row.duration_sec = durationSec;
    if (missed !== undefined) row.missed = missed;
    if (rawData !== undefined) row.raw_data = rawData;

    const { data, error } = await supabase
      .from('calls')
      .upsert(row, {
        onConflict: 'call_id,session_id',
        ignoreDuplicates: false,
      })
      .select()
      .single();

    if (error) {
      logger.error({ err: error, callId, sessionId, status }, 'Failed to upsert call');
      return null;
    }

    return data;
  } catch (err) {
    logger.error({ err, callId, sessionId }, 'Unexpected error in upsertCall');
    return null;
  }
}

export async function getCallsBySession(sessionId, { limit = 100, offset = 0, db = supabase } = {}) {
  try {
    const { data, error } = await db
      .from('calls')
      .select('*')
      .eq('session_id', sessionId)
      .order('offered_at', { ascending: false })
      .range(offset, offset + limit - 1);

    if (error) {
      logger.error({ err: error, sessionId }, 'Failed to fetch calls by session');
      return [];
    }

    return data ?? [];
  } catch (err) {
    logger.error({ err, sessionId }, 'Unexpected error in getCallsBySession');
    return [];
  }
}

export async function getCallsByChat(sessionId, remoteJid, { limit = 50, db = supabase } = {}) {
  try {
    const { data, error } = await db
      .from('calls')
      .select('*')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .order('offered_at', { ascending: false })
      .limit(limit);

    if (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Failed to fetch calls by chat');
      return [];
    }

    return data ?? [];
  } catch (err) {
    logger.error({ err, sessionId, remoteJid }, 'Unexpected error in getCallsByChat');
    return [];
  }
}

export async function getMissedCallsCount(sessionId, remoteJid, db = supabase) {
  try {
    const { count, error } = await db
      .from('calls')
      .select('*', { count: 'exact', head: true })
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .eq('missed', true);

    if (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Failed to count missed calls');
      return 0;
    }

    return count ?? 0;
  } catch (err) {
    logger.error({ err, sessionId, remoteJid }, 'Unexpected error in getMissedCallsCount');
    return 0;
  }
}

export async function getCallsKpi(days = 7, db = supabase) {
  try {
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

    const { data, error } = await db
      .from('calls')
      .select('session_id, status, missed, duration_sec, from_me')
      .gte('offered_at', since);

    if (error) {
      logger.error({ err: error, days }, 'Failed to fetch calls for KPI');
      return null;
    }

    // Aggregate per session
    const sessionMap = new Map();

    for (const row of data ?? []) {
      if (!sessionMap.has(row.session_id)) {
        sessionMap.set(row.session_id, {
          sessionId: row.session_id,
          total: 0,
          missed: 0,
          answered: 0,
          totalDurationSec: 0,
          answeredCount: 0,
        });
      }
      const s = sessionMap.get(row.session_id);
      s.total++;
      if (row.missed) s.missed++;
      if (row.status === 'accept' || row.duration_sec > 0) {
        s.answered++;
        if (row.duration_sec > 0) {
          s.totalDurationSec += row.duration_sec;
          s.answeredCount++;
        }
      }
    }

    const sessions = Array.from(sessionMap.values()).map((s) => ({
      sessionId: s.sessionId,
      total: s.total,
      missed: s.missed,
      answered: s.answered,
      answerRate: s.total > 0 ? Math.round((s.answered / s.total) * 100) : 0,
      avgDurationSec: s.answeredCount > 0 ? Math.round(s.totalDurationSec / s.answeredCount) : 0,
    }));

    // Overall totals
    const totals = sessions.reduce(
      (acc, s) => {
        acc.total += s.total;
        acc.missed += s.missed;
        acc.answered += s.answered;
        return acc;
      },
      { total: 0, missed: 0, answered: 0 }
    );

    totals.answerRate = totals.total > 0 ? Math.round((totals.answered / totals.total) * 100) : 0;

    return { days, since, totals, sessions };
  } catch (err) {
    logger.error({ err, days }, 'Unexpected error in getCallsKpi');
    return null;
  }
}

export async function getUnifiedMessages(remoteJid, limit = 50, offset = 0, db = supabase) {
  // Reject LID/group jids
  const digits = (remoteJid || '').replace(/@.*$/, '').replace(/\D/g, '');
  if (!remoteJid || digits.length > 13 || remoteJid.includes('@g.us') || remoteJid.includes('@lid')) {
    return [];
  }
  try {
    const { data, error } = await db
      .from('messages')
      .select('*')
      .eq('remote_jid', remoteJid)
      .order('timestamp', { ascending: false })
      .range(offset, offset + limit - 1);

    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to fetch unified messages');
      return [];
    }

    return data ?? [];
  } catch (error) {
    logger.error({ err: error, remoteJid }, 'Unexpected error in getUnifiedMessages');
    return [];
  }
}

export async function getContacts(sessionId, db = supabase) {
  try {
    const { data, error } = await db
      .from('messages')
      .select('remote_jid, push_name, timestamp')
      .eq('session_id', sessionId)
      .order('timestamp', { ascending: false });

    if (error) {
      logger.error({ err: error, sessionId }, 'Failed to fetch contacts');
      return [];
    }

    const contactMap = new Map();
    for (const row of data ?? []) {
      if (!contactMap.has(row.remote_jid)) {
        contactMap.set(row.remote_jid, row.push_name);
      }
    }

    return Array.from(contactMap.entries()).map(([phone, name]) => ({ phone, name }));
  } catch (error) {
    logger.error({ err: error, sessionId }, 'Unexpected error while fetching contacts');
    return [];
  }
}

// In-memory cache for getChatsWithLastMessage — 10s TTL, reduces DB load at 8 sessions
// Cache key is composite: `${sessionId}::${userId}` where userId is:
//   - '__service__' when called with serviceClient (internal workers, admin endpoints)
//   - req.user.id (Supabase auth UID) when called with userClient (dashboard requests)
//   - 'anon' when userId not provided (backward-compat, not used in authenticated paths)
// This prevents cross-tenant cache bleed: service_role rows must NOT be served to
// a lower-privilege user client that goes through RLS.
const chatsCache = new Map(); // `${sessionId}::${userId}` -> { data, timestamp }
const CHATS_CACHE_TTL = 10_000; // 10 seconds

export async function getChatsWithLastMessage(sessionId, db = supabase, userId = null) {
  // W1.1 Phase 2: thread dbClient through all Supabase calls
  // W1.1 audit fix: composite cache key prevents service_role data bleeding into user-scoped reads
  const cacheUserId = db === supabase ? '__service__' : (userId ?? 'anon');
  const cacheKey = `${sessionId}::${cacheUserId}`;
  const cached = chatsCache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CHATS_CACHE_TTL) {
    return cached.data;
  }

  const queryStart = Date.now();
  try {
    // Try optimized RPC first (single SQL query with LATERAL JOINs)
    const { data: rpcData, error: rpcError } = await db
      .rpc('get_chats_with_last_message', {
        p_session_id: sessionId,
        p_limit: 2000,
      });

    if (!rpcError && rpcData) {
      const result = rpcData
        .filter((row) => {
          const jid = row.remote_jid;
          if (!jid || jid.includes('-')) return true; // groups ok
          const digits = jid.replace(/\D/g, '');
          return digits.length >= 7 && digits.length <= 13;
        })
        .map((row) => ({
        remoteJid: row.remote_jid,
        chatType: row.chat_type,
        displayName: row.display_name,
        participantCount: row.participant_count,
        phoneNumber: row.phone_number,
        isMuted: row.is_muted,
        mutedUntil: row.muted_until || null,
        tags: row.tags || [],
        tagConfirmed: row.tag_confirmed || false,
        lastMessage: row.last_message_body || null,
        lastMessageType: row.last_message_type || 'text',
        lastTimestamp: row.last_timestamp,
        fromMe: row.last_from_me,
        pushName: row.last_push_name || null,
        sender: row.last_sender || null,
        mediaUrl: row.last_media_url || null,
        mediaType: row.last_media_type || null,
        fileName: row.last_file_name || null,
        unreadCount: Number(row.unread_count) || 0,
        hasCrmContact: Boolean(row.crm_first_name),
        crmName: row.crm_first_name
          ? `${row.crm_first_name}${row.crm_last_name ? ` ${row.crm_last_name}` : ''}`
          : null,
        crmRole: row.crm_role || null,
        crmAvatarUrl: row.crm_avatar_url || null,
      }));
      // W7A overlay: phone-level chat_tags wins over legacy chats.tags
      await applyChatTagsOverlay(result, db);
      const elapsed = Date.now() - queryStart;
      if (elapsed > 2000) logger.warn({ sessionId, elapsed }, 'Slow getChatsWithLastMessage query');
      chatsCache.set(cacheKey, { data: result, timestamp: Date.now() });
      return result;
    }

    // Fallback: RPC not available yet — use multi-query approach
    if (rpcError) {
      logger.warn({ err: rpcError.message }, 'RPC not available, using fallback queries');
    }

    const { data: chatList, error: chatError } = await db
      .from('chats')
      .select('*')
      .eq('session_id', sessionId)
      .or('is_hidden.is.null,is_hidden.eq.false')
      .order('last_message_at', { ascending: false })
      .limit(2000);

    if (chatError) {
      logger.error({ err: chatError, sessionId }, 'Failed to fetch chats');
      return [];
    }

    if (!chatList?.length) return [];

    // Filter out LID garbage JIDs
    const cleanChats = chatList.filter((c) => {
      const jid = c.remote_jid;
      if (!jid || jid.includes('-')) return true;
      const digits = jid.replace(/\D/g, '');
      return digits.length >= 7 && digits.length <= 13;
    });

    const jids = cleanChats.map((c) => c.remote_jid);

    const { data: crmContacts } = await db
      .from('contacts_crm')
      .select('remote_jid, first_name, last_name, role, avatar_url')
      .eq('session_id', sessionId)
      .in('remote_jid', jids);

    const crmMap = new Map();
    for (const c of crmContacts ?? []) {
      crmMap.set(c.remote_jid, c);
    }

    const BATCH_SIZE = 50;
    const result = [];

    for (let i = 0; i < cleanChats.length; i += BATCH_SIZE) {
      const batch = cleanChats.slice(i, i + BATCH_SIZE);
      const batchResults = await Promise.all(
        batch.map(async (chat) => {
          const [
            { data: messages, error },
            { count, error: unreadError },
          ] = await Promise.all([
            db
              .from('messages')
              .select('body, message_type, from_me, push_name, timestamp, sender, media_url, media_type, file_name')
              .eq('session_id', sessionId)
              .eq('remote_jid', chat.remote_jid)
              .order('timestamp', { ascending: false })
              .limit(1),
            db
              .from('messages')
              .select('*', { count: 'exact', head: true })
              .eq('session_id', sessionId)
              .eq('remote_jid', chat.remote_jid)
              .eq('from_me', false)
              .is('read_at', null),
          ]);

          if (error) {
            logger.error({ err: error, sessionId, remoteJid: chat.remote_jid }, 'Failed to fetch last message');
          }
          if (unreadError) {
            logger.error({ err: unreadError, sessionId, remoteJid: chat.remote_jid }, 'Failed to count unread');
          }

          const lastMessage = messages?.[0] || null;
          const crmContact = crmMap.get(chat.remote_jid) || null;
          const crmName = crmContact
            ? `${crmContact.first_name}${crmContact.last_name ? ` ${crmContact.last_name}` : ''}`
            : null;

          return {
            remoteJid: chat.remote_jid,
            chatType: chat.chat_type,
            displayName: chat.display_name,
            participantCount: chat.participant_count,
            phoneNumber: chat.phone_number || chat.remote_jid,
            isMuted: chat.is_muted || false,
            mutedUntil: chat.muted_until || null,
            // legacy tags field — will be overwritten by applyChatTagsOverlay below
            tags: chat.tags || [],
            tagConfirmed: chat.tag_confirmed || false,
            lastMessage: lastMessage?.body || null,
            lastMessageType: lastMessage?.message_type || 'text',
            lastTimestamp: lastMessage?.timestamp || chat.last_message_at,
            fromMe: lastMessage?.from_me || false,
            pushName: lastMessage?.push_name || null,
            sender: lastMessage?.sender || null,
            mediaUrl: lastMessage?.media_url || null,
            mediaType: lastMessage?.media_type || null,
            fileName: lastMessage?.file_name || null,
            unreadCount: count || 0,
            hasCrmContact: Boolean(crmContact),
            crmName,
            crmRole: crmContact?.role || null,
            crmAvatarUrl: crmContact?.avatar_url || null,
          };
        })
      );
      result.push(...batchResults);
    }

    // W7A overlay: phone-level chat_tags wins over legacy chats.tags
    await applyChatTagsOverlay(result, db);
    return result;
  } catch (error) {
    logger.error({ err: error, sessionId }, 'Unexpected error while fetching chats');
    return [];
  }
}

// Overlay chat_tags (phone-level) onto a chat list result. Mutates in place.
// If a remote_jid has an entry in chat_tags, its tags replace the legacy
// `chats.tags` value. If not, the legacy value stays (for graceful migration).
async function applyChatTagsOverlay(chatList, db = supabase) {
  if (!Array.isArray(chatList) || chatList.length === 0) return;
  const jids = chatList.map((c) => c.remoteJid).filter(Boolean);
  const tagMap = await getChatTagsByJids(jids, db);
  for (const chat of chatList) {
    const entry = tagMap[chat.remoteJid];
    if (entry) {
      chat.tags = entry.tags;
      chat.tagConfirmed = entry.tagConfirmed;
    }
  }
}

// ---------------------------------------------------------------------------
// Wave 8 — Manager Reports
// ---------------------------------------------------------------------------

/**
 * Insert a new manager_reports row and return its id.
 * @param {object} row  Matches the manager_reports table columns (snake_case).
 * @returns {Promise<string|null>} UUID of the inserted row, or null on error.
 */
export async function insertManagerReport(row) {
  try {
    const { data, error } = await supabase
      .from('manager_reports')
      .insert(row)
      .select('id')
      .single();

    if (error) {
      logger.error({ err: error, row }, 'insertManagerReport: failed to insert');
      return null;
    }

    return data.id;
  } catch (err) {
    logger.error({ err }, 'insertManagerReport: unexpected error');
    return null;
  }
}

/**
 * List manager reports with optional filters.
 * Returns plain rows — formatting to camelCase is caller's responsibility.
 *
 * @param {object} opts
 * @param {string} [opts.sessionId]   Filter by target_session_id.
 * @param {string} [opts.dateFrom]    ISO date string (inclusive).
 * @param {string} [opts.dateTo]      ISO date string (inclusive).
 * @param {number} [opts.limit=100]   Max rows.
 * @returns {Promise<Array>}
 */
export async function listManagerReports({ sessionId, dateFrom, dateTo, limit = 100, db = supabase } = {}) {
  try {
    const safeLimit = Math.min(Number(limit) || 100, 500);

    let query = db
      .from('manager_reports')
      .select('*')
      .order('sent_at', { ascending: false })
      .limit(safeLimit);

    if (sessionId) {
      query = query.eq('target_session_id', sessionId);
    }
    if (dateFrom) {
      query = query.gte('sent_at', `${dateFrom}T00:00:00+00:00`);
    }
    if (dateTo) {
      query = query.lte('sent_at', `${dateTo}T23:59:59+00:00`);
    }

    const { data, error } = await query;

    if (error) {
      logger.error({ err: error }, 'listManagerReports: query failed');
      return [];
    }

    return data ?? [];
  } catch (err) {
    logger.error({ err }, 'listManagerReports: unexpected error');
    return [];
  }
}

/**
 * Mark a chat_ai record as having a report sent.
 * @param {string} chatAiId   UUID of the chat_ai row.
 * @param {string} [sentAt]   ISO timestamp (defaults to now()).
 */
export async function markChatAiReportSent(chatAiId, sentAt) {
  try {
    const ts = sentAt ?? new Date().toISOString();
    const { error } = await supabase
      .from('chat_ai')
      .update({ report_sent_at: ts })
      .eq('id', chatAiId);

    if (error) {
      logger.error({ err: error, chatAiId }, 'markChatAiReportSent: update failed');
    }
  } catch (err) {
    logger.error({ err, chatAiId }, 'markChatAiReportSent: unexpected error');
  }
}

/**
 * Fetch a single chat_ai row by id.
 * @param {string} chatAiId
 * @returns {Promise<object|null>}
 */
export async function getChatAiById(chatAiId, db = supabase) {
  try {
    const { data, error } = await db
      .from('chat_ai')
      .select('*')
      .eq('id', chatAiId)
      .maybeSingle();

    if (error) {
      logger.error({ err: error, chatAiId }, 'getChatAiById: query failed');
      return null;
    }

    return data ?? null;
  } catch (err) {
    logger.error({ err, chatAiId }, 'getChatAiById: unexpected error');
    return null;
  }
}

/**
 * Return all active sessions from session_config.
 * Falls back to all rows if is_active column doesn't exist (schema mismatch guard).
 * @returns {Promise<Array<{session_id: string, display_name: string, phone_number: string}>>}
 */
export async function getActiveSessions(db = supabase) {
  try {
    const { data, error } = await db
      .from('session_config')
      .select('session_id, display_name, phone_number')
      .eq('is_active', true)
      .order('created_at', { ascending: true });

    if (error) {
      // If is_active column is missing — fall back to all rows
      if (error.code === 'PGRST116' || error.message?.includes('is_active')) {
        logger.warn('getActiveSessions: is_active column not found — fetching all rows');
        const { data: all, error: allErr } = await db
          .from('session_config')
          .select('session_id, display_name, phone_number')
          .order('created_at', { ascending: true });
        if (allErr) {
          logger.error({ err: allErr }, 'getActiveSessions: fallback query also failed');
          return [];
        }
        return all ?? [];
      }
      logger.error({ err: error }, 'getActiveSessions: query failed');
      return [];
    }

    return data ?? [];
  } catch (err) {
    logger.error({ err }, 'getActiveSessions: unexpected error');
    return [];
  }
}
