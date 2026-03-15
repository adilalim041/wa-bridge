import { logger } from '../config.js';
import { saveContact, saveMessage } from '../storage/queries.js';

const SKIP_TYPES = [
  'protocolMessage',
  'senderKeyDistributionMessage',
  'messageContextInfo',
  'reactionMessage',
];

function normalizeRemoteJid(remoteJid = '') {
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

export async function handleMessage(message, sock) {
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

    const remoteJid = normalizeRemoteJid(message.key.remoteJid);
    const fromMe = Boolean(message.key.fromMe);
    const timestampValue = Number(message.messageTimestamp ?? Math.floor(Date.now() / 1000));
    const timestamp = new Date(timestampValue * 1000).toISOString();
    const pushName = message.pushName ?? sock?.contacts?.[message.key.remoteJid]?.name;
    const messageType = getMessageType(msg);
    const messageId = message.key.id;

    await saveMessage({
      messageId,
      remoteJid,
      fromMe,
      body,
      messageType,
      timestamp,
      pushName,
    });

    await saveContact(remoteJid, pushName);

    const preview = body.length > 50 ? `${body.substring(0, 50)}...` : body;
    logger.info(`[${fromMe ? 'OUT' : 'IN'}] ${remoteJid}: ${preview}`);
  } catch (error) {
    logger.error({ err: error, messageId: message?.key?.id }, 'Failed to handle message');
  }
}
