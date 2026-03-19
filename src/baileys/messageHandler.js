import { logger } from '../config.js';
import { getOrCreateDialogSession } from '../ai/dialogSessions.js';
import { trackResponseTime } from '../ai/responseTracker.js';
import { enqueueForAI } from '../ai/queueManager.js';
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

// Cross-session mirroring: maps phone numbers to session IDs
// e.g. { "77014135151": "omoikiri-main", "77786137600": "almaty-alim" }
const phoneToSession = new Map();
let phoneRegistryLoaded = false;

export async function loadPhoneRegistry() {
  try {
    const { data } = await supabase
      .from('session_config')
      .select('session_id, phone_number')
      .not('phone_number', 'is', null);

    phoneToSession.clear();
    for (const row of data ?? []) {
      if (row.phone_number) {
        phoneToSession.set(row.phone_number, row.session_id);
        console.log(`[mirror] Registered: ${row.phone_number} → ${row.session_id}`);
      }
    }
    phoneRegistryLoaded = true;
  } catch (err) {
    logger.error({ err }, 'Failed to load phone registry for cross-session mirroring');
  }
}

export function registerSessionPhone(sessionId, phoneNumber) {
  if (phoneNumber) {
    phoneToSession.set(phoneNumber, sessionId);
    console.log(`[mirror] Registered: ${phoneNumber} → ${sessionId}`);
  }
}

export function getPhoneToSession() {
  return phoneToSession;
}

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

    let msg = message.message;
    if (!msg) {
      console.log(`[${sessionId}] SKIP: no message body, key=${message.key?.id}`);
      return;
    }

    // Unwrap deviceSentMessage — outgoing messages synced from the phone
    if (msg.deviceSentMessage?.message) {
      console.log(`[${sessionId}] Unwrapping deviceSentMessage: ${message.key?.id}`);
      msg = msg.deviceSentMessage.message;
    }

    // Unwrap ephemeralMessage
    if (msg.ephemeralMessage?.message) {
      console.log(`[${sessionId}] Unwrapping ephemeralMessage: ${message.key?.id}`);
      msg = msg.ephemeralMessage.message;
    }

    // Unwrap viewOnceMessage
    if (msg.viewOnceMessage?.message) {
      console.log(`[${sessionId}] Unwrapping viewOnceMessage: ${message.key?.id}`);
      msg = msg.viewOnceMessage.message;
    }

    // Unwrap viewOnceMessageV2
    if (msg.viewOnceMessageV2?.message) {
      msg = msg.viewOnceMessageV2.message;
    }

    // Unwrap documentWithCaptionMessage
    if (msg.documentWithCaptionMessage?.message) {
      msg = msg.documentWithCaptionMessage.message;
    }

    const msgType = Object.keys(msg)[0];
    if (SKIP_TYPES.includes(msgType)) {
      console.log(`[${sessionId}] SKIP type: ${msgType}, key=${message.key?.id}, jid=${message.key?.remoteJid}`);
      return;
    }

    const body = extractBody(msg);
    if (body === null) {
      console.log(`[${sessionId}] SKIP: null body, type=${msgType}, key=${message.key?.id}, jid=${message.key?.remoteJid}`);
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

    try {
      const dialogSessionId = await getOrCreateDialogSession(
        sessionId,
        remoteJid,
        payload.timestamp
      );

      if (dialogSessionId) {
        const { error: linkError } = await supabase
          .from('messages')
          .update({ dialog_session_id: dialogSessionId })
          .eq('message_id', payload.messageId)
          .eq('session_id', sessionId)
          .eq('remote_jid', remoteJid);

        if (linkError) {
          logger.error(
            { err: linkError, sessionId, remoteJid, messageId: payload.messageId, dialogSessionId },
            'Failed to link message to dialog session'
          );
        }

        await trackResponseTime(
          sessionId,
          remoteJid,
          dialogSessionId,
          payload.fromMe,
          payload.timestamp
        );

        await enqueueForAI(sessionId, remoteJid, payload.messageId, dialogSessionId);
      }
    } catch (error) {
      logger.error(
        { err: error, sessionId, remoteJid, messageId: payload.messageId },
        'AI pipeline failed after message save'
      );
    }

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

    // Cross-session mirroring: if remoteJid is also one of our sessions,
    // save a mirrored copy for that session (with flipped from_me)
    if (!isGroup && phoneToSession.size > 0) {
      const mirrorSessionId = phoneToSession.get(remoteJid);
      const myPhone = [...phoneToSession.entries()].find(([, sid]) => sid === sessionId)?.[0];

      if (mirrorSessionId && mirrorSessionId !== sessionId && myPhone) {
        try {
          const mirrorPayload = {
            messageId,
            sessionId: mirrorSessionId,
            remoteJid: myPhone,
            fromMe: !fromMe,
            body,
            messageType,
            pushName: fromMe ? (pushName || null) : pushName,
            sender: null,
            chatType: 'personal',
            mediaUrl: mediaUrl,
            mediaType: mediaFileType || messageType,
            fileName: mediaFileName,
            timestamp,
          };

          await saveMessage(mirrorPayload);
          await upsertChat({
            remoteJid: myPhone,
            sessionId: mirrorSessionId,
            chatType: 'personal',
            displayName: chatDisplayName || pushName,
            participantCount: null,
            phoneNumber: myPhone,
          });

          logger.info(
            `[${sessionId}] [MIRROR → ${mirrorSessionId}] ${myPhone}: ${preview}`
          );
        } catch (mirrorErr) {
          logger.error({ err: mirrorErr, sessionId, mirrorSessionId }, 'Cross-session mirror failed');
        }
      }
    }

    return payload;
  } catch (error) {
    logger.error({ err: error, sessionId, messageId: message?.key?.id }, 'Failed to handle message');
    return null;
  }
}
