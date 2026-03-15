import { logger } from '../config.js';
import { supabase } from './supabase.js';

export async function saveMessage(data) {
  try {
    const { error } = await supabase.from('messages').upsert(
      {
        message_id: data.messageId,
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

export async function getMessages(remoteJid, limit = 50, offset = 0) {
  try {
    const { data, error } = await supabase
      .from('messages')
      .select('*')
      .eq('remote_jid', remoteJid)
      .order('timestamp', { ascending: false })
      .range(offset, offset + limit - 1);

    if (error) {
      logger.error({ err: error, remoteJid }, 'Failed to fetch messages');
      return [];
    }

    return data ?? [];
  } catch (error) {
    logger.error({ err: error, remoteJid }, 'Unexpected error while fetching messages');
    return [];
  }
}

export async function getContacts() {
  try {
    const { data, error } = await supabase
      .from('contacts')
      .select('*')
      .order('updated_at', { ascending: false });

    if (error) {
      logger.error({ err: error }, 'Failed to fetch contacts');
      return [];
    }

    return data ?? [];
  } catch (error) {
    logger.error({ err: error }, 'Unexpected error while fetching contacts');
    return [];
  }
}
