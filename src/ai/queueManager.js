import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';

export async function enqueueForAI(sessionId, remoteJid, messageId, dialogSessionId) {
  try {
    const { error } = await supabase.from('ai_queue').insert({
      session_id: sessionId,
      remote_jid: remoteJid,
      message_id: messageId,
      dialog_session_id: dialogSessionId,
      status: 'pending',
    });

    if (error) {
      logger.error({ err: error, sessionId, remoteJid, messageId }, 'Failed to enqueue message for AI');
    }
  } catch (error) {
    logger.error({ err: error, sessionId, messageId }, 'Unexpected error enqueueing for AI');
  }
}
