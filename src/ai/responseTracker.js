import { logger } from '../config.js';
import { supabase } from '../storage/supabase.js';

export async function trackResponseTime(sessionId, remoteJid, dialogSessionId, fromMe, timestamp) {
  try {
    const msgTime = new Date(timestamp);

    if (!fromMe) {
      const { error } = await supabase.from('manager_analytics').insert({
        session_id: sessionId,
        remote_jid: remoteJid,
        dialog_session_id: dialogSessionId,
        customer_message_at: msgTime.toISOString(),
        manager_response_at: null,
        response_time_seconds: null,
      });

      if (error) {
        logger.error({ err: error, sessionId, remoteJid }, 'Failed to insert pending customer response');
      }

      return;
    }

    const { data: pending, error } = await supabase
      .from('manager_analytics')
      .select('id, customer_message_at')
      .eq('session_id', sessionId)
      .eq('remote_jid', remoteJid)
      .is('manager_response_at', null)
      .order('customer_message_at', { ascending: false })
      .limit(1)
      .maybeSingle();

    if (error) {
      logger.error({ err: error, sessionId, remoteJid }, 'Failed to find pending response');
      return;
    }

    if (!pending) {
      return;
    }

    const customerTime = new Date(pending.customer_message_at);
    const responseSeconds = Math.round((msgTime.getTime() - customerTime.getTime()) / 1000);

    const { error: updateError } = await supabase
      .from('manager_analytics')
      .update({
        manager_response_at: msgTime.toISOString(),
        response_time_seconds: Math.max(0, responseSeconds),
      })
      .eq('id', pending.id);

    if (updateError) {
      logger.error({ err: updateError, sessionId, remoteJid }, 'Failed to save manager response time');
    }
  } catch (error) {
    logger.error({ err: error, sessionId, remoteJid }, 'Unexpected error tracking response time');
  }
}
