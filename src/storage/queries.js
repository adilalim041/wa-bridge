import { logger } from '../config.js';
import { supabase } from './supabase.js';

export async function saveMessage(data) {
  try {
    const { error } = await supabase.from('messages').upsert(
      {
        message_id: data.messageId,
        session_id: data.sessionId,
        remote_jid: data.remoteJid,
        from_me: data.fromMe,
        body: data.body,
        message_type: data.messageType,
        push_name: data.pushName ?? null,
        timestamp: data.timestamp,
      },
      {
        onConflict: 'message_id',
        ignoreDuplicates: true,
      }
    );

    if (error) {
      logger.error({ err: error, messageId: data.messageId }, 'Failed to save message');
    }
  } catch (error) {
    logger.error({ err: error, messageId: data.messageId }, 'Unexpected error while saving message');
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

export async function getChats(sessionId) {
  try {
    const { data, error } = await supabase.rpc('get_chats', { p_session_id: sessionId });

    if (!error && data) {
      return data;
    }

    const { data: messages, error: fallbackError } = await supabase
      .from('messages')
      .select('*')
      .eq('session_id', sessionId)
      .order('timestamp', { ascending: false });

    if (fallbackError) {
      logger.error({ err: fallbackError, sessionId }, 'Failed to fetch chats');
      return [];
    }

    const chatMap = new Map();
    for (const message of messages ?? []) {
      if (!chatMap.has(message.remote_jid)) {
        chatMap.set(message.remote_jid, {
          remoteJid: message.remote_jid,
          lastMessage: message.body,
          lastMessageType: message.message_type,
          lastTimestamp: message.timestamp,
          pushName: message.push_name,
          fromMe: message.from_me,
        });
      }
    }

    return Array.from(chatMap.values());
  } catch (error) {
    logger.error({ err: error, sessionId }, 'Unexpected error while fetching chats');
    return [];
  }
}
