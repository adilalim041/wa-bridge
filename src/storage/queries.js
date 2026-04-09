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

export async function getMessages(sessionId, remoteJid, limit = 50, offset = 0) {
  try {
    let query = supabase
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
export async function getLinkedSessions(remoteJid) {
  try {
    const { data, error } = await supabase
      .from('chats')
      .select('session_id, display_name, last_message_at, tags')
      .eq('remote_jid', remoteJid);

    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to fetch linked sessions');
      return [];
    }

    if (!data?.length) return [];

    // Enrich with session display names
    const sessionIds = [...new Set(data.map((c) => c.session_id))];
    const { data: configs } = await supabase
      .from('session_config')
      .select('session_id, display_name')
      .in('session_id', sessionIds);

    const configMap = {};
    for (const c of configs || []) configMap[c.session_id] = c.display_name;

    return data.map((c) => ({
      sessionId: c.session_id,
      sessionName: configMap[c.session_id] || c.session_id,
      lastMessageAt: c.last_message_at,
      contactName: c.display_name,
      tags: c.tags || [],
    }));
  } catch (error) {
    logger.error({ err: error, remoteJid }, 'Unexpected error in getLinkedSessions');
    return [];
  }
}

// Get messages from ALL sessions for one contact, sorted chronologically
export async function getUnifiedMessages(remoteJid, limit = 50, offset = 0) {
  // Reject LID/group jids
  const digits = (remoteJid || '').replace(/@.*$/, '').replace(/\D/g, '');
  if (!remoteJid || digits.length > 13 || remoteJid.includes('@g.us') || remoteJid.includes('@lid')) {
    return [];
  }
  try {
    const { data, error } = await supabase
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

export async function getContacts(sessionId) {
  try {
    const { data, error } = await supabase
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
const chatsCache = new Map(); // sessionId -> { data, timestamp }
const CHATS_CACHE_TTL = 10_000; // 10 seconds

export async function getChatsWithLastMessage(sessionId) {
  // Check cache first
  const cached = chatsCache.get(sessionId);
  if (cached && Date.now() - cached.timestamp < CHATS_CACHE_TTL) {
    return cached.data;
  }

  const queryStart = Date.now();
  try {
    // Try optimized RPC first (single SQL query with LATERAL JOINs)
    const { data: rpcData, error: rpcError } = await supabase
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
      const elapsed = Date.now() - queryStart;
      if (elapsed > 2000) logger.warn({ sessionId, elapsed }, 'Slow getChatsWithLastMessage query');
      chatsCache.set(sessionId, { data: result, timestamp: Date.now() });
      return result;
    }

    // Fallback: RPC not available yet — use multi-query approach
    if (rpcError) {
      logger.warn({ err: rpcError.message }, 'RPC not available, using fallback queries');
    }

    const { data: chatList, error: chatError } = await supabase
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

    const { data: crmContacts } = await supabase
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
            supabase
              .from('messages')
              .select('body, message_type, from_me, push_name, timestamp, sender, media_url, media_type, file_name')
              .eq('session_id', sessionId)
              .eq('remote_jid', chat.remote_jid)
              .order('timestamp', { ascending: false })
              .limit(1),
            supabase
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

    return result;
  } catch (error) {
    logger.error({ err: error, sessionId }, 'Unexpected error while fetching chats');
    return [];
  }
}
