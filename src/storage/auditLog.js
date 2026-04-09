import { logger } from '../config.js';
import { supabase } from './supabase.js';

/**
 * Fire-and-forget audit log entry. Never blocks the caller.
 * @param {string} action - What happened (e.g. 'session.connect', 'message.send', 'chat.hide')
 * @param {string} [sessionId] - Which WA session
 * @param {object} [details] - Additional context (JSON-serializable)
 */
export function logAudit(action, sessionId = null, details = null) {
  supabase
    .from('audit_log')
    .insert({
      action,
      session_id: sessionId,
      details: details ? JSON.stringify(details) : null,
    })
    .then(({ error }) => {
      if (error) {
        logger.debug({ err: error.message, action }, 'Audit log insert failed (non-critical)');
      }
    })
    .catch(() => {
      // Never fail — audit is best-effort
    });
}
