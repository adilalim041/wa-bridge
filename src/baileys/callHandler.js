import { logger } from '../config.js';
import { upsertCall, formatCallRow } from '../storage/queries.js';
import { emitCallEvent } from '../api/websocket.js';

/**
 * Normalize a WhatsApp JID to a bare phone number (or group ID).
 * Strips @s.whatsapp.net, @g.us, @lid, and device suffix (e.g. :5).
 */
function normalizeJid(raw) {
  if (!raw) return raw;
  return raw
    .replace(/@s\.whatsapp\.net$/i, '')
    .replace(/@g\.us$/i, '')
    .replace(/@lid$/i, '')
    .replace(/:\d+$/, '');
}

/**
 * Handle a single Baileys call event and persist it to Supabase.
 *
 * @param {string} sessionId
 * @param {object} event - Baileys call event object
 *   Fields: id, from, status, date, isVideo, isGroup, offline
 */
export async function handleCallEvent(sessionId, event) {
  try {
    const { id: callId, from, status, date, isVideo = false, isGroup = false, offline = false } = event;

    if (!callId || !from) {
      logger.warn({ sessionId, event }, 'call event missing id or from — skipping');
      return;
    }

    const remoteJid = normalizeJid(from);

    // Outgoing calls: Baileys marks offline=true for events we initiated
    const fromMe = Boolean(offline);

    // Convert Baileys date (may be Date object or Unix seconds) to ISO string
    const eventDate = date instanceof Date
      ? date.toISOString()
      : typeof date === 'number'
        ? new Date(date * 1000).toISOString()
        : new Date().toISOString();

    // Build the upsert payload based on status transition
    let payload = {
      callId,
      sessionId,
      remoteJid,
      fromMe,
      isVideo: Boolean(isVideo),
      isGroup: Boolean(isGroup),
      status,
      rawData: event,
    };

    switch (status) {
      case 'offer':
        payload.offeredAt = eventDate;
        payload.missed = false;
        break;

      case 'accept':
        payload.answeredAt = new Date().toISOString();
        break;

      case 'reject':
        payload.endedAt = new Date().toISOString();
        // Only mark as missed if incoming (not from us)
        payload.missed = !fromMe;
        break;

      case 'timeout':
        payload.endedAt = new Date().toISOString();
        payload.missed = !fromMe;
        break;

      case 'terminate': {
        const endedAt = new Date().toISOString();
        payload.endedAt = endedAt;

        // We need answered_at to compute duration — fetch existing row first
        // Duration is computed in the DB via answered_at; we pass endedAt only.
        // missed=true only if we never answered (no answered_at in DB yet).
        // The SQL UPSERT merges: if answered_at already set, duration = endedAt - answered_at.
        // We signal this by passing durationFlag=true and let queries.js build the right SQL.
        payload.terminateAt = endedAt; // queries.js will calculate duration on terminate
        break;
      }

      default:
        logger.debug({ sessionId, callId, status }, 'unknown call status — storing as-is');
    }

    const saved = await upsertCall(payload);

    logger.info(
      { sessionId, callId, remoteJid, status, fromMe, isVideo },
      'call event processed'
    );

    // Emit over WebSocket so dashboard can update in real-time.
    // IMPORTANT: frontend reads camelCase fields (remoteJid / fromMe / durationSec…),
    // so we MUST format the raw DB row the same way the REST endpoint does via
    // formatCallRow. Without this, the frontend's liveCall effect never matches
    // the open chat (remoteJid is undefined) and the bubble never renders.
    if (saved) {
      emitCallEvent(sessionId, formatCallRow(saved));
    }
  } catch (err) {
    logger.error({ err, sessionId, callId: event?.id, status: event?.status }, 'handleCallEvent failed');
  }
}
