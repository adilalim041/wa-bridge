/**
 * heartbeat.js — publishes Omoikiri service health to ObsidianVault every 5 min.
 *
 * Writes to: system/status/omoikiri.json  (via GitHub Contents API)
 *
 * Details payload:
 *   uptime_h                 — process uptime in hours
 *   baileys_connected        — true if at least one Baileys session is connected
 *   last_message_processed_at — ISO string of last successful DB write, or null
 *   supabase_ok              — failover queue empty → Supabase is responding
 *   queue_depth              — pending messages in failover queue (0 = healthy)
 *
 * Design constraints:
 *   - NEVER logs VAULT_WRITE_TOKEN or any auth header
 *   - On failure: single warn log, no retry, no process crash
 *   - If VAULT_WRITE_TOKEN is missing: single warn at boot, no further noise
 *   - setInterval timer is .unref()'d so it does not prevent graceful shutdown
 */

import { logger } from './config.js';
import { writeHeartbeat } from './lib/vault_github.js';
import { getQueueStats, getLastMessageProcessedAt, getSupabaseOk } from './storage/queries.js';
import { sessionManager } from './baileys/sessionManager.js';

const INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

let _timer = null;
let _tokenMissingWarned = false;

/** Collect current details and push heartbeat to the vault. */
async function publishHeartbeat() {
  try {
    const allStates = sessionManager.getAllStates();
    const sessionValues = Object.values(allStates);
    const baileysConnected = sessionValues.some((s) => s.connected === true);

    const queueStats = getQueueStats();

    const details = {
      uptime_h: Math.round((process.uptime() / 3600) * 100) / 100,
      baileys_connected: baileysConnected,
      last_message_processed_at: getLastMessageProcessedAt(),
      supabase_ok: getSupabaseOk(),
      queue_depth: queueStats.size,
    };

    const status = baileysConnected && details.supabase_ok ? 'healthy' : 'degraded';

    await writeHeartbeat('omoikiri', status, details);

    logger.info(
      { status, baileysConnected, supabaseOk: details.supabase_ok, queueDepth: queueStats.size },
      'Vault heartbeat published'
    );
  } catch (err) {
    // Deliberately not retrying — next tick will try again in 5 min.
    // Log the error class/message only, never the token or auth headers.
    logger.warn(
      { err: { message: err.message, name: err.name } },
      'Vault heartbeat failed (non-critical — will retry next tick)'
    );
  }
}

/**
 * Start the heartbeat scheduler.
 * Safe to call multiple times — subsequent calls are no-ops.
 *
 * Fires one heartbeat immediately on boot so the vault is fresh right away,
 * then every 5 minutes.
 */
export function startVaultHeartbeat() {
  if (_timer !== null) return; // already running

  if (!process.env.VAULT_WRITE_TOKEN) {
    if (!_tokenMissingWarned) {
      _tokenMissingWarned = true;
      logger.warn('VAULT_WRITE_TOKEN not set — vault heartbeat disabled');
    }
    return;
  }

  // Fire immediately so the vault shows fresh data right after boot
  publishHeartbeat();

  _timer = setInterval(publishHeartbeat, INTERVAL_MS);

  // .unref() — timer must not prevent the process from exiting cleanly
  _timer.unref();

  logger.info('Vault heartbeat started (interval: 5 min)');
}

/**
 * Stop the heartbeat scheduler gracefully.
 * Called during process shutdown.
 */
export function stopVaultHeartbeat() {
  if (_timer !== null) {
    clearInterval(_timer);
    _timer = null;
  }
}
