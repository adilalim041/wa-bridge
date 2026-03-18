import { logger } from '../config.js';
import { getContactName, saveContact, saveMessage, upsertChat } from '../storage/queries.js';
import { supabase } from '../storage/supabase.js';
import { processMedia } from './mediaHandler.js';

const SKIP_TYPES = [
  'protocolMessage',
  'senderKeyDistributionMessage',
  'messageContextInfo',
  'reactionMessage',
];

const hiddenChatsCache = new Map();
const hiddenCacheTimestamps = new Map();
const HIDDEN_CACHE_TTL = 5 * 60 * 1000;

async function isChatHidden(sessionId, remoteJid) {
  const cacheKey = `${sessionId}:${remoteJid}`;
  const cachedAt = hiddenCacheTimestamps.get(cacheKey);

  if (cachedAt && Date.now() - cachedAt < HIDDEN_CACHE_TTL) {
    return hiddenChatsCache.get(cacheKey) || false;
  }

  try {
    const { data } = await supabase
      .from('chats')
      .select('is_hidden')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .maybeSingle();

    const isHidden = data?.is_hidden || false;
    hiddenChatsCache.set(cacheKey, isHidden);
    hiddenCacheTimestamps.set(cacheKey, Date.now());
    return isHidden;
  } catch (error) {
    logger.error({ err: error, sessionId, remoteJid }, 'Failed to resolve hidden chat state');
    return false;
  }
}

export function invalidateHiddenCache(sessionId, remoteJid) {
  const cacheKey = `${sessionId}:${remoteJid}`;
  hiddenChatsCache.delete(cacheKey);
  hiddenCacheTimestamps.delete(cacheKey);
}

async function normalizeRemoteJid(remoteJid = '', sock = null, sessionId = '') {
  if (remoteJid.endsWith('@lid')) {
    const lid = remoteJid.replace('@lid', '');
    try {
      const { supabase } = await import('../storage/supabase.js');
      const key = `${sessionId}:lid-mapping:${lid}_reverse`;
      const { data } = await supabase
        .from('auth_state')
        .select('value')
        .eq('key', key)
        .single();
      if (data?.value) {
        return JSON.parse(data.value);
      }
    } catch (e) {}
    return lid;
  }
  return remoteJid
    .replace('@s.whatsapp.net', '')
    .replace('@g.us', '');
}

async function normalizeParticipant(participant = '', sock = null, sessionId = '') {
  return normalizeRemoteJid(participant, sock, sessionId);
}

function resolveContactName(sock, remoteJid) {
  if (!sock || !remoteJid) {
    return null;
  }

  const candidates = [
    remoteJid,
    `${remoteJid}@s.whatsapp.net`,
    `${remoteJid}@lid`,
  ];

  for (const candidate of candidates) {
    const contact =
      sock?.store?.contacts?.[candidate] ||
      sock?.contacts?.[candidate];

    const name = contact?.name || contact?.notify || contact?.verifiedName || null;
    if (name) {
      return name;
    }
  }

  return null;
}

function getMessageType(msg) {
  if (!msg) {
    return 'unknown';
  }

  if (msg.conversation || msg.extendedTextMessage) {
    return 'text';
  }

  if (msg.imageMessage) {
    return 'image';
  }

  if (msg.videoMessage) {
    return 'video';
  }

  if (msg.audioMessage) {
    return 'audio';
  }

  if (msg.documentMessage) {
    return 'document';
  }

  if (msg.stickerMessage) {
    return 'sticker';
  }

  if (msg.contactMessage || msg.contactsArrayMessage) {
    return 'contact';
  }

  if (msg.locationMessage || msg.liveLocationMessage) {
    return 'location';
  }

  return 'unknown';
}

function extractBody(msg) {
  if (!msg) {
    return null;
  }

  if (msg.conversation) {
    return msg.conversation;
  }

  if (msg.extendedTextMessage?.text) {
    return msg.extendedTextMessage.text;
  }

  if (msg.imageMessage?.caption) {
    return msg.imageMessage.caption;
  }

  if (msg.videoMessage?.caption) {
    return msg.videoMessage.caption;
  }

  if (msg.documentMessage?.caption) {
    return msg.documentMessage.caption;
  }

  if (msg.imageMessage) {
    return '[image]';
  }

  if (msg.videoMessage) {
    return '[video]';
  }

  if (msg.audioMessage) {
    return '[audio]';
  }

  if (msg.documentMessage) {
    return `[document: ${msg.documentMessage.fileName || 'file'}]`;
  }

  if (msg.stickerMessage) {
    return '[sticker]';
  }

  if (msg.contactMessage) {
    return `[contact: ${msg.contactMessage.displayName || 'unknown'}]`;
  }

  if (msg.contactsArrayMessage) {
    return '[contacts]';
  }

  if (msg.locationMessage) {
    return `[location: ${msg.locationMessage.degreesLatitude}, ${msg.locationMessage.degreesLongitude}]`;
  }

  if (msg.liveLocationMessage) {
    return '[live location]';
  }

  return null;
}

export async function handleMessage(message, sock, sessionId) {
  try {
    if (!message?.key?.remoteJid || message.key.remoteJid === 'status@broadcast') {
      return;
    }

    const msg = message.message;
    if (!msg) {
      return;
    }

    const msgType = Object.keys(msg)[0];
    if (SKIP_TYPES.includes(msgType)) {
      return;
    }

    const body = extractBody(msg);
    if (body === null) {
      return;
    }

    const rawRemoteJid = message.key.remoteJid;
    const remoteJid = await normalizeRemoteJid(rawRemoteJid, sock, sessionId);
    const fromMe = Boolean(message.key.fromMe);
    const timestampValue = Number(message.messageTimestamp ?? Math.floor(Date.now() / 1000));
    const timestamp = new Date(timestampValue * 1000).toISOString();
    const messageType = getMessageType(msg);
    const messageId = message.key.id;
    const isGroup = rawRemoteJid.endsWith('@g.us');
    const sender = isGroup ? await normalizeParticipant(message.key.participant || '', sock, sessionId) : null;

    if (await isChatHidden(sessionId, remoteJid)) {
      logger.debug({ sessionId, remoteJid, messageId }, 'Skipping hidden chat message');
      return null;
    }

    let pushName = message.pushName ?? resolveContactName(sock, isGroup ? sender : remoteJid);

    if (!pushName) {
      pushName = await getContactName(isGroup ? sender : remoteJid);
    }

    let chatType = 'personal';
    let chatDisplayName = null;
    let participantCount = null;
    let phoneNumber = null;

    if (isGroup) {
      chatType = 'group';

      try {
        const groupMeta = await sock.groupMetadata(rawRemoteJid);
        chatDisplayName = groupMeta?.subject || null;
        participantCount = groupMeta?.participants?.length || null;
      } catch (error) {
        logger.warn({ err: error, sessionId, remoteJid: rawRemoteJid }, 'Failed to fetch group metadata');
      }
    } else {
      chatType = 'personal';
      chatDisplayName =
        (fromMe ? resolveContactName(sock, remoteJid) : pushName) ||
        resolveContactName(sock, remoteJid) ||
        (await getContactName(remoteJid));
      phoneNumber = remoteJid;
    }

    let mediaUrl = null;
    let mediaFileType = null;
    let mediaFileName = null;
    if (['image', 'video', 'audio', 'document', 'sticker'].includes(messageType)) {
      const media = await processMedia(message, sessionId);
      if (media) {
        mediaUrl = media.url;
        mediaFileType = media.mediaType;
        mediaFileName = media.fileName;
      }
    }

    const payload = {
      messageId,
      remoteJid,
      fromMe,
      body,
      messageType,
      timestamp,
      pushName,
      sessionId,
      sender,
      chatType,
      mediaUrl,
      mediaType: mediaFileType || messageType,
      fileName: mediaFileName,
      displayName: chatDisplayName,
      participantCount,
      phoneNumber,
    };

    await saveMessage(payload);
    await upsertChat({
      remoteJid,
      sessionId,
      chatType,
      displayName: chatDisplayName,
      participantCount,
      phoneNumber,
    });

    if (isGroup) {
      if (sender && pushName) {
        await saveContact(sender, pushName);
      }
    } else {
      await saveContact(remoteJid, chatDisplayName || pushName);
    }

    const preview = body.length > 50 ? `${body.substring(0, 50)}...` : body;
    logger.info(`[${sessionId}] [${fromMe ? 'OUT' : 'IN'}] ${remoteJid}: ${preview}`);
    return payload;
  } catch (error) {
    logger.error({ err: error, sessionId, messageId: message?.key?.id }, 'Failed to handle message');
    return null;
  }
}
