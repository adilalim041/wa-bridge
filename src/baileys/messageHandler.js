import { logger } from '../config.js';
import { saveContact, saveMessage } from '../storage/queries.js';

const SKIP_TYPES = [
  'protocolMessage',
  'senderKeyDistributionMessage',
  'messageContextInfo',
  'reactionMessage',
];

function normalizeRemoteJid(remoteJid = '', sock = null) {
  if (remoteJid.endsWith('@lid') && sock) {
    try {
      const lidMap = sock.authState?.creds?.lidTagMap || {};
      for (const [realJid, lid] of Object.entries(lidMap)) {
        if (remoteJid.includes(lid)) {
          return realJid.replace('@s.whatsapp.net', '');
        }
      }
    } catch (e) {}
  }
  return remoteJid
    .replace('@s.whatsapp.net', '')
    .replace('@lid', '')
    .replace('@g.us', '');
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

    const remoteJid = normalizeRemoteJid(message.key.remoteJid, sock);
    const fromMe = Boolean(message.key.fromMe);
    const timestampValue = Number(message.messageTimestamp ?? Math.floor(Date.now() / 1000));
    const timestamp = new Date(timestampValue * 1000).toISOString();
    const pushName = message.pushName ?? sock?.contacts?.[message.key.remoteJid]?.name;
    const messageType = getMessageType(msg);
    const messageId = message.key.id;

    const payload = {
      messageId,
      remoteJid,
      fromMe,
      body,
      messageType,
      timestamp,
      pushName,
      sessionId,
    };

    await saveMessage(payload);

    await saveContact(remoteJid, pushName);

    const preview = body.length > 50 ? `${body.substring(0, 50)}...` : body;
    logger.info(`[${sessionId}] [${fromMe ? 'OUT' : 'IN'}] ${remoteJid}: ${preview}`);
    return payload;
  } catch (error) {
    logger.error({ err: error, sessionId, messageId: message?.key?.id }, 'Failed to handle message');
    return null;
  }
}
