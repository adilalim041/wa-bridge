/**
 * routing.js
 *
 * Resolves which WhatsApp session should SEND a manager report PDF.
 *
 * Business rules:
 *   - Default sender = env REPORT_DEFAULT_SENDER_SESSION_ID
 *   - Fallback sender = env REPORT_FALLBACK_SENDER_SESSION_ID
 *   - If target == default → use fallback (can't send to yourself)
 *   - If target != default → use default
 *   - In all cases, the resolved sender MUST be present in activeSessions
 *
 * Throws Error with a human-readable message on any misconfiguration.
 * The caller maps these errors to HTTP 409 Conflict.
 */

/**
 * @param {string} targetSessionId
 *   The session_id of the manager who will RECEIVE the report.
 * @param {Array<{session_id: string}>} activeSessions
 *   List of currently active sessions (from session_config).
 * @returns {string} senderSessionId
 * @throws {Error} if env is not configured or sender is not active.
 */
export function resolveSender(targetSessionId, activeSessions) {
  const defaultSender = process.env.REPORT_DEFAULT_SENDER_SESSION_ID;
  const fallbackSender = process.env.REPORT_FALLBACK_SENDER_SESSION_ID;

  if (!defaultSender) {
    throw new Error(
      'REPORT_DEFAULT_SENDER_SESSION_ID not configured — add it to Railway env.'
    );
  }

  const activeSet = new Set(
    (activeSessions ?? []).map((s) => s.session_id ?? s)
  );

  // Determine which sender to use
  let chosenSender;

  if (targetSessionId === defaultSender) {
    // Can't send from default to itself — need fallback
    if (!fallbackSender) {
      throw new Error(
        `Target session "${targetSessionId}" is the default sender, but ` +
        'REPORT_FALLBACK_SENDER_SESSION_ID is not configured. ' +
        'Set it to a different active session to send reports to this manager.'
      );
    }
    chosenSender = fallbackSender;
  } else {
    chosenSender = defaultSender;
  }

  // Verify chosen sender is actually running
  if (!activeSet.has(chosenSender)) {
    throw new Error(
      `Sender session "${chosenSender}" is not in the active sessions list. ` +
      'Make sure the session is connected before sending reports.'
    );
  }

  return chosenSender;
}
