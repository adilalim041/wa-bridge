import { logger } from '../config.js';
import { supabase } from './supabase.js';

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

  logger.error({ messageId: data.messageId }, 'CRITICAL: Message lost after all retry attempts');
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

export async function getChatsWithLastMessage(sessionId) {
  try {
    // Try optimized RPC first (single SQL query with LATERAL JOINs)
    const { data: rpcData, error: rpcError } = await supabase
      .rpc('get_chats_with_last_message', {
        p_session_id: sessionId,
        p_limit: 300,
      });

    if (!rpcError && rpcData) {
      return rpcData.map((row) => ({
        remoteJid: row.remote_jid,
        chatType: row.chat_type,
        displayName: row.display_name,
        participantCount: row.participant_count,
        phoneNumber: row.phone_number,
        isMuted: row.is_muted,
        mutedUntil: row.muted_until || null,
        tags: row.tags || [],
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
      .limit(300);

    if (chatError) {
      logger.error({ err: chatError, sessionId }, 'Failed to fetch chats');
      return [];
    }

    if (!chatList?.length) return [];

    const jids = chatList.map((c) => c.remote_jid);

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

    for (let i = 0; i < chatList.length; i += BATCH_SIZE) {
      const batch = chatList.slice(i, i + BATCH_SIZE);
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
