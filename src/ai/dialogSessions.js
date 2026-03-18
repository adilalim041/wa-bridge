import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';

const SESSION_GAP_MS = 4 * 60 * 60 * 1000;

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
      const gap = msgTime.getTime() - lastMsgTime.getTime();

      if (gap < SESSION_GAP_MS) {
        const { error: updateError } = await supabase
          .from('dialog_sessions')
          .update({
            last_message_at: msgTime.toISOString(),
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
      logger.error({ err: createError, sessionId, remoteJid }, 'Failed to create dialog session');
      return null;
    }

    return newSession.id;
  } catch (error) {
    logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error in dialog session manager');
    return null;
  }
}
