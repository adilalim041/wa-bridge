import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';

const BASE_SESSION_GAP_MS = 4 * 60 * 60 * 1000;
const EXTENDED_SESSION_GAP_MS = 72 * 60 * 60 * 1000;
const EXTENDED_SESSION_GAP_BUSINESS_MINUTES = 8 * 60;
const LOCAL_TZ_OFFSET_MINUTES = 5 * 60;
const WORK_START_HOUR = 10;
const WORK_END_HOUR = 20;

function localParts(date) {
  const shifted = new Date(date.getTime() + LOCAL_TZ_OFFSET_MINUTES * 60 * 1000);
  return {
    y: shifted.getUTCFullYear(),
    m: shifted.getUTCMonth(),
    d: shifted.getUTCDate(),
  };
}

function utcFromLocal(y, m, d, hour) {
  return new Date(Date.UTC(y, m, d, hour, 0, 0, 0) - LOCAL_TZ_OFFSET_MINUTES * 60 * 1000);
}

export function businessMinutesBetween(start, end) {
  let cursor = new Date(start);
  const finish = new Date(end);
  if (!Number.isFinite(cursor.getTime()) || !Number.isFinite(finish.getTime()) || finish <= cursor) {
    return 0;
  }

  let total = 0;
  while (cursor < finish && total < 60 * 24 * 30) {
    const p = localParts(cursor);
    const dayStart = utcFromLocal(p.y, p.m, p.d, WORK_START_HOUR);
    const dayEnd = utcFromLocal(p.y, p.m, p.d, WORK_END_HOUR);

    if (cursor < dayStart) cursor = dayStart;
    if (cursor >= dayEnd) {
      cursor = utcFromLocal(p.y, p.m, p.d + 1, WORK_START_HOUR);
      continue;
    }

    const segmentEnd = finish < dayEnd ? finish : dayEnd;
    total += Math.max(0, Math.round((segmentEnd.getTime() - cursor.getTime()) / 60000));
    cursor = segmentEnd >= dayEnd
      ? utcFromLocal(p.y, p.m, p.d + 1, WORK_START_HOUR)
      : segmentEnd;
  }

  return total;
}

export function shouldReuseDialogSession(lastMessageAt, messageTimestamp) {
  const lastMsgTime = new Date(lastMessageAt);
  const msgTime = new Date(messageTimestamp);
  if (!Number.isFinite(lastMsgTime.getTime()) || !Number.isFinite(msgTime.getTime())) return false;

  const rawGap = msgTime.getTime() - lastMsgTime.getTime();
  if (rawGap <= 0) return true;
  if (rawGap < BASE_SESSION_GAP_MS) return true;
  if (rawGap > EXTENDED_SESSION_GAP_MS) return false;

  return businessMinutesBetween(lastMsgTime, msgTime) <= EXTENDED_SESSION_GAP_BUSINESS_MINUTES;
}

function shouldReuseOpenSession(openSession, messageTimestamp) {
  const lastMsgTime = new Date(openSession.last_message_at);
  const msgTime = new Date(messageTimestamp);
  if (!shouldReuseDialogSession(lastMsgTime, msgTime)) return false;

  const rawGap = msgTime.getTime() - lastMsgTime.getTime();
  if (rawGap < BASE_SESSION_GAP_MS) return true;

  // Extended overnight/weekend stitching is only for the tiny lead-start slice
  // (ad template, bot greeting, first real manager answer). Long-running chats
  // should still split normally to avoid month-long analysis sessions.
  return (openSession.message_count || 0) <= 5;
}

export async function getOrCreateDialogSession(sessionId, remoteJid, messageTimestamp) {
  try {
    const msgTime = new Date(messageTimestamp);

    const { data: openSession, error: findError } = await supabase
      .from('dialog_sessions')
      .select('*')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .eq('status', 'open')
      .order('last_message_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (findError) {
      logger.error({ err: findError, sessionId, remoteJid }, 'Failed to find dialog session');
    }

    if (openSession) {
      const lastMsgTime = new Date(openSession.last_message_at);

      if (shouldReuseOpenSession(openSession, msgTime)) {
        const nextLastMessageAt = msgTime > lastMsgTime ? msgTime : lastMsgTime;
        const { error: updateError } = await supabase
          .from('dialog_sessions')
          .update({
            last_message_at: nextLastMessageAt.toISOString(),
            message_count: (openSession.message_count || 0) + 1,
          })
          .eq('id', openSession.id);

        if (updateError) {
          logger.error({ err: updateError, sessionId, remoteJid }, 'Failed to update dialog session');
        }

        return openSession.id;
      }

      const { error: closeError } = await supabase
        .from('dialog_sessions')
        .update({ status: 'closed' })
        .eq('id', openSession.id);

      if (closeError) {
        logger.error({ err: closeError, sessionId, remoteJid }, 'Failed to close dialog session');
      }
    }

    const { data: newSession, error: createError } = await supabase
      .from('dialog_sessions')
      .insert({
        session_id: sessionId,
        remote_jid: remoteJid,
        started_at: msgTime.toISOString(),
        last_message_at: msgTime.toISOString(),
        message_count: 1,
        status: 'open',
      })
      .select('id')
      .single();

    if (createError) {
      // Race condition: another thread created a session between our check and insert
      // Re-query to find the one that was just created
      const { data: raceSession } = await supabase
        .from('dialog_sessions')
        .select('id')
        .eq('session_id', sessionId)
        .eq('remote_jid', remoteJid)
        .eq('status', 'open')
        .order('last_message_at', { ascending: false })
        .limit(1)
        .maybeSingle();

      if (raceSession?.id) {
        logger.info({ sessionId, remoteJid, id: raceSession.id }, 'Used existing dialog session after race');
        return raceSession.id;
      }

      logger.error({ err: createError, sessionId, remoteJid }, 'Failed to create dialog session');
      return null;
    }

    return newSession.id;
  } catch (error) {
    logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error in dialog session manager');
    return null;
  }
}
