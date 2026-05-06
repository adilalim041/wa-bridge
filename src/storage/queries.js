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
    // ack_status: outgoing → 0 (pending), incoming → NULL.
    // Without explicit 0 the row would be NULL and `.lt('ack_status', x)` in the
    // delivery-receipt handler skips NULL rows (PG: NULL < n = NULL ≠ TRUE),
    // freezing every new outgoing on a single ✓ forever.
    ack_status: data.fromMe ? 0 : null,
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

export async function upsertChatTags(remoteJid, { tags, tagConfirmed } = {}, db = supabase) {
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
    const { error } = await db
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

    // Bump chats.last_message_at so calls move chat to top of list, mirroring
    // message ingestion behaviour. Use offered_at to preserve actual event time.
    // Only bump on the 'offer' event — that is the canonical moment the call arrived.
    // Subsequent status transitions (accept / reject / terminate) must NOT override
    // a real message that came after the call was offered.
    // Non-transactional: if this update fails we log a warning and continue — a
    // missed bump only affects sidebar sort order, not data integrity.
    if (offeredAt) {
      try {
        const { error: bumpError } = await supabase
          .from('chats')
          .update({ last_message_at: offeredAt })
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid)
          .lt('last_message_at', offeredAt); // only update if call is newer

        if (bumpError) {
          logger.warn(
            { err: bumpError, callId, sessionId, remoteJid },
            'upsertCall: failed to bump chats.last_message_at (non-fatal)'
          );
        }
      } catch (bumpErr) {
        logger.warn(
          { err: bumpErr, callId, sessionId, remoteJid },
          'upsertCall: unexpected error bumping chats.last_message_at (non-fatal)'
        );
      }
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
//   - '__service__' for x-api-key / worker paths (no req.user — passed explicitly from handler)
//   - req.user.id (Supabase auth UID) for dashboard JWT requests (passed explicitly from handler)
// userId is passed explicitly from the handler, NOT inferred via identity check (db === supabase).
// This prevents cross-tenant cache bleed: service_role rows must NOT be served to
// a lower-privilege user client that goes through RLS.
// Pattern mirrors analyticsCache fix (commit acb5943, audit HIGH #4).
const chatsCache = new Map(); // `${sessionId}::${userId}` -> { data, timestamp }
const CHATS_CACHE_TTL = 10_000; // 10 seconds

export async function getChatsWithLastMessage(sessionId, db = supabase, userId = null) {
  // W1.1 Phase 2: thread dbClient through all Supabase calls
  // W1.1 Phase 3 P-2: userId passed explicitly from handler — no db===supabase identity check
  const cacheUserId = userId ?? '__service__';
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
        .map((row) => {
          // Determine whether the most recent event was a call or a message.
          // SQL function returns last_call_offered_at (null when no calls yet).
          // We compare timestamps so the freshest event wins for display.
          const msgTs = row.last_timestamp ? new Date(row.last_timestamp).getTime() : 0;
          const callTs = row.last_call_offered_at ? new Date(row.last_call_offered_at).getTime() : 0;
          const callIsNewer = callTs > msgTs;

          return {
            remoteJid: row.remote_jid,
            chatType: row.chat_type,
            displayName: row.display_name,
            participantCount: row.participant_count,
            phoneNumber: row.phone_number,
            isMuted: row.is_muted,
            mutedUntil: row.muted_until || null,
            tags: row.tags || [],
            tagConfirmed: row.tag_confirmed || false,
            // If call is newer — show call metadata; otherwise show last message as before.
            lastMessage: callIsNewer ? null : (row.last_message_body || null),
            lastMessageType: callIsNewer ? 'call' : (row.last_message_type || 'text'),
            lastTimestamp: callIsNewer ? row.last_call_offered_at : row.last_timestamp,
            fromMe: callIsNewer ? Boolean(row.last_call_from_me) : row.last_from_me,
            pushName: callIsNewer ? null : (row.last_push_name || null),
            sender: callIsNewer ? null : (row.last_sender || null),
            mediaUrl: callIsNewer ? null : (row.last_media_url || null),
            mediaType: callIsNewer ? null : (row.last_media_type || null),
            fileName: callIsNewer ? null : (row.last_file_name || null),
            unreadCount: Number(row.unread_count) || 0,
            hasCrmContact: Boolean(row.crm_first_name),
            crmName: row.crm_first_name
              ? `${row.crm_first_name}${row.crm_last_name ? ` ${row.crm_last_name}` : ''}`
              : null,
            crmRole: row.crm_role || null,
            crmAvatarUrl: row.crm_avatar_url || null,
            // Call-specific fields (null when last event was a message)
            lastCallMissed: callIsNewer ? Boolean(row.last_call_missed) : null,
            lastCallDurationSec: callIsNewer ? (row.last_call_duration_sec ?? null) : null,
            // ACK status of last outgoing message (null when call is newest or msg is incoming).
            // Frontend renders delivery ticks only when fromMe=true and lastAckStatus !== null.
            lastAckStatus: callIsNewer ? null : (row.last_ack_status ?? null),
          };
        });
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

    // Fetch latest call per jid in a single query using order + in().
    // Supabase/PostgREST doesn't expose DISTINCT ON directly, so we fetch all calls
    // for these jids ordered by offered_at DESC and collapse in JS.
    // For up to 2000 chats with typically <5 calls each this is fast enough.
    const [{ data: crmContacts }, { data: latestCallsRaw }] = await Promise.all([
      db
        .from('contacts_crm')
        .select('remote_jid, first_name, last_name, role, avatar_url')
        .eq('session_id', sessionId)
        .in('remote_jid', jids),
      db
        .from('calls')
        .select('remote_jid, offered_at, missed, from_me, duration_sec')
        .eq('session_id', sessionId)
        .in('remote_jid', jids)
        .order('offered_at', { ascending: false })
        .limit(jids.length * 3), // at most 3 calls per jid is plenty for latest-call lookup
    ]);

    const crmMap = new Map();
    for (const c of crmContacts ?? []) {
      crmMap.set(c.remote_jid, c);
    }

    // Collapse: first occurrence per remote_jid is the most recent (ordered DESC above)
    const latestCallMap = new Map();
    for (const call of latestCallsRaw ?? []) {
      if (!latestCallMap.has(call.remote_jid)) {
        latestCallMap.set(call.remote_jid, call);
      }
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
              .select('body, message_type, from_me, push_name, timestamp, sender, media_url, media_type, file_name, ack_status')
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

          // Determine whether the most recent event was a call or a message.
          const latestCall = latestCallMap.get(chat.remote_jid) || null;
          const msgTs = lastMessage?.timestamp ? new Date(lastMessage.timestamp).getTime() : 0;
          const callTs = latestCall?.offered_at ? new Date(latestCall.offered_at).getTime() : 0;
          const callIsNewer = callTs > msgTs;

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
            lastMessage: callIsNewer ? null : (lastMessage?.body || null),
            lastMessageType: callIsNewer ? 'call' : (lastMessage?.message_type || 'text'),
            lastTimestamp: callIsNewer
              ? latestCall.offered_at
              : (lastMessage?.timestamp || chat.last_message_at),
            fromMe: callIsNewer ? Boolean(latestCall.from_me) : (lastMessage?.from_me || false),
            pushName: callIsNewer ? null : (lastMessage?.push_name || null),
            sender: callIsNewer ? null : (lastMessage?.sender || null),
            mediaUrl: callIsNewer ? null : (lastMessage?.media_url || null),
            mediaType: callIsNewer ? null : (lastMessage?.media_type || null),
            fileName: callIsNewer ? null : (lastMessage?.file_name || null),
            unreadCount: count || 0,
            hasCrmContact: Boolean(crmContact),
            crmName,
            crmRole: crmContact?.role || null,
            crmAvatarUrl: crmContact?.avatar_url || null,
            // Call-specific fields (null when last event was a message)
            lastCallMissed: callIsNewer ? Boolean(latestCall.missed) : null,
            lastCallDurationSec: callIsNewer ? (latestCall.duration_sec ?? null) : null,
            // ACK status of last outgoing message (null when call is newest or msg is incoming).
            lastAckStatus: callIsNewer ? null : (lastMessage?.from_me ? (lastMessage.ack_status ?? null) : null),
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

// ============================================================================
// tenant_settings helpers
// ============================================================================

/**
 * Fetch tenant settings for a given userId.
 * Returns null if no row exists yet (caller handles default seeding).
 *
 * @param {string} userId  - Supabase auth.users UUID
 * @param {object} db      - Supabase client (userClient for RLS, supabase for service)
 * @returns {Promise<{roles: string[], cities: string[], tags: string[], lead_sources: string[], refusal_reasons: string[], task_types: string[], company_profile: object} | null>}
 */
export async function getTenantSettings(userId, db = supabase) {
  try {
    const { data, error } = await db
      .from('tenant_settings')
      .select('roles, cities, tags, lead_sources, refusal_reasons, task_types, company_profile')
      .eq('user_id', userId)
      .maybeSingle();

    if (error) {
      logger.error({ err: error, userId }, 'getTenantSettings: query failed');
      return null;
    }

    return data; // null when no row
  } catch (err) {
    logger.error({ err, userId }, 'getTenantSettings: unexpected error');
    return null;
  }
}

/**
 * Upsert tenant settings (INSERT on first call, UPDATE on subsequent calls).
 * Only provided fields are merged — omitted fields retain their DB values.
 *
 * @param {string} userId
 * @param {{ roles?: string[], cities?: string[], tags?: string[], lead_sources?: string[], refusal_reasons?: string[], task_types?: string[], company_profile?: object }} payload
 * @param {object} db
 * @returns {Promise<{roles: string[], cities: string[], tags: string[], lead_sources: string[], refusal_reasons: string[], task_types: string[], company_profile: object} | null>}
 */
export async function upsertTenantSettings(userId, payload, db = supabase) {
  try {
    const { data, error } = await db
      .from('tenant_settings')
      .upsert(
        { user_id: userId, ...payload },
        { onConflict: 'user_id', ignoreDuplicates: false }
      )
      .select('roles, cities, tags, lead_sources, refusal_reasons, task_types, company_profile')
      .single();

    if (error) {
      logger.error({ err: error, userId }, 'upsertTenantSettings: upsert failed');
      return null;
    }

    return data;
  } catch (err) {
    logger.error({ err, userId }, 'upsertTenantSettings: unexpected error');
    return null;
  }
}

// ============================================================================
// funnel_stages helpers
// ============================================================================

/**
 * Fetch all funnel stages for a tenant, ordered by sort_order asc.
 *
 * @param {string} userId
 * @param {object} db
 * @returns {Promise<Array<{id: string, name: string, color: string, sort_order: number, is_final: boolean}>>}
 */
export async function getFunnelStages(userId, db = supabase) {
  try {
    const { data, error } = await db
      .from('funnel_stages')
      .select('id, name, color, sort_order, is_final, created_at, updated_at')
      .eq('user_id', userId)
      .order('sort_order', { ascending: true });

    if (error) {
      logger.error({ err: error, userId }, 'getFunnelStages: query failed');
      return [];
    }

    return data ?? [];
  } catch (err) {
    logger.error({ err, userId }, 'getFunnelStages: unexpected error');
    return [];
  }
}

/**
 * Insert a new funnel stage for a tenant.
 * sort_order is auto-set to (max existing + 1) to append at the end.
 *
 * @param {string} userId
 * @param {{ name: string, color?: string, is_final?: boolean }} payload
 * @param {object} db
 * @returns {Promise<{id: string, name: string, color: string, sort_order: number, is_final: boolean} | null>}
 */
export async function createFunnelStage(userId, payload, db = supabase) {
  try {
    // Determine next sort_order
    const { data: existing } = await db
      .from('funnel_stages')
      .select('sort_order')
      .eq('user_id', userId)
      .order('sort_order', { ascending: false })
      .limit(1);

    const nextOrder = existing && existing.length > 0
      ? (existing[0].sort_order + 1)
      : 0;

    const { data, error } = await db
      .from('funnel_stages')
      .insert({
        user_id: userId,
        name: payload.name,
        color: payload.color ?? '#3b82f6',
        is_final: payload.is_final ?? false,
        sort_order: nextOrder,
      })
      .select('id, name, color, sort_order, is_final, created_at, updated_at')
      .single();

    if (error) {
      logger.error({ err: error, userId }, 'createFunnelStage: insert failed');
      return null;
    }

    return data;
  } catch (err) {
    logger.error({ err, userId }, 'createFunnelStage: unexpected error');
    return null;
  }
}

/**
 * Update a funnel stage. Only the calling tenant's own stages can be modified
 * (the WHERE clause includes user_id so RLS is doubly enforced).
 *
 * @param {string} id      - Stage UUID
 * @param {string} userId
 * @param {{ name?: string, color?: string, is_final?: boolean }} payload
 * @param {object} db
 * @returns {Promise<{id: string, name: string, color: string, sort_order: number, is_final: boolean} | null>}
 */
export async function updateFunnelStage(id, userId, payload, db = supabase) {
  try {
    const { data, error } = await db
      .from('funnel_stages')
      .update(payload)
      .eq('id', id)
      .eq('user_id', userId)
      .select('id, name, color, sort_order, is_final, created_at, updated_at')
      .single();

    if (error) {
      logger.error({ err: error, id, userId }, 'updateFunnelStage: update failed');
      return null;
    }

    return data;
  } catch (err) {
    logger.error({ err, id, userId }, 'updateFunnelStage: unexpected error');
    return null;
  }
}

/**
 * Delete a funnel stage. Returns false (and does NOT delete) if any chat_ai
 * row currently has deal_stage matching this stage's name — prevents orphaned data.
 *
 * The collision check is done via the service-role client intentionally:
 * chat_ai rows may belong to any session; we need a full-table count.
 * The DELETE itself uses the caller's db (RLS-gated by user_id).
 *
 * @param {string} id
 * @param {string} userId
 * @param {object} db
 * @returns {Promise<{ deleted: boolean, conflict: boolean, conflictCount?: number }>}
 */
export async function deleteFunnelStage(id, userId, db = supabase) {
  try {
    // Fetch the stage name first (need it for the collision check)
    const { data: stage, error: stageErr } = await db
      .from('funnel_stages')
      .select('name')
      .eq('id', id)
      .eq('user_id', userId)
      .maybeSingle();

    if (stageErr) {
      logger.error({ err: stageErr, id }, 'deleteFunnelStage: fetch stage failed');
      return { deleted: false, conflict: false };
    }

    if (!stage) {
      // Not found or belongs to another tenant
      return { deleted: false, conflict: false };
    }

    // Check for active deals on this stage
    // SERVICE ROLE INTENTIONAL: collision check must see all sessions regardless
    // of which session the user happens to be viewing. We read count only, no data.
    const { count, error: countErr } = await supabase
      .from('chat_ai')
      .select('id', { count: 'exact', head: true })
      .eq('deal_stage', stage.name);

    if (countErr) {
      logger.error({ err: countErr, id }, 'deleteFunnelStage: collision check failed');
      return { deleted: false, conflict: false };
    }

    if (count > 0) {
      return { deleted: false, conflict: true, conflictCount: count };
    }

    // Safe to delete
    const { error: delErr } = await db
      .from('funnel_stages')
      .delete()
      .eq('id', id)
      .eq('user_id', userId);

    if (delErr) {
      logger.error({ err: delErr, id }, 'deleteFunnelStage: delete failed');
      return { deleted: false, conflict: false };
    }

    return { deleted: true, conflict: false };
  } catch (err) {
    logger.error({ err, id, userId }, 'deleteFunnelStage: unexpected error');
    return { deleted: false, conflict: false };
  }
}

/**
 * Batch update sort_order for a tenant's funnel stages.
 * Accepts an ordered array of IDs — positions are 0-indexed from the array.
 *
 * Each update is scoped to the tenant's user_id to prevent cross-tenant tampering.
 *
 * @param {string} userId
 * @param {string[]} orderIds  - Stage UUIDs in desired order (index = new sort_order)
 * @param {object} db
 * @returns {Promise<boolean>}  true on success
 */
export async function reorderFunnelStages(userId, orderIds, db = supabase) {
  try {
    await Promise.all(
      orderIds.map((stageId, index) =>
        db
          .from('funnel_stages')
          .update({ sort_order: index })
          .eq('id', stageId)
          .eq('user_id', userId)
      )
    );
    return true;
  } catch (err) {
    logger.error({ err, userId }, 'reorderFunnelStages: unexpected error');
    return false;
  }
}
