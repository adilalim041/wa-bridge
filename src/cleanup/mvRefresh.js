import { logger } from '../config.js';
import { supabase as serviceClient } from '../storage/supabase.js';
import { captureException } from '../observability/sentry.js';

/**
 * Materialized view refresh — wired up 2026-05-03.
 *
 * Why this exists: Sales-CRM loads full sales / partner_contacts tables into
 * RAM for every cold-cache request. With 4300+ sales on Railway 512 MB the
 * risk of OOM-kill grows with each monthly import. mv_sales_monthly and
 * mv_partner_aggregates pre-aggregate the heaviest data in Postgres so future
 * endpoint migrations can query aggregates instead of row-scanning.
 *
 * The refresh itself is cheap (< 2s on current dataset) and uses CONCURRENTLY
 * so existing data stays queryable. We call it once per day at 04:30 Almaty
 * (30 min after the Cloudinary cleanup window at 04:00).
 *
 * Manual trigger: POST /admin/sales-crm/refresh-mvs  (x-api-key required)
 */

// Almaty = UTC+5
const MV_REFRESH_HOUR_ALMATY   = Number(process.env.MV_REFRESH_HOUR_ALMATY || 4);
const MV_REFRESH_MINUTE_ALMATY = Number(process.env.MV_REFRESH_MINUTE_ALMATY || 30);
const ALMATY_OFFSET_HOURS      = 5;

let refreshTimer = null;
let isRunning    = false;

/**
 * Returns milliseconds until the next scheduled run.
 * Schedules for HH:MM Almaty, fires on the next occurrence (today or tomorrow).
 */
function nextRunDelayMs() {
  const nowUtcMs  = Date.now();
  const nowAlmaty = new Date(nowUtcMs + ALMATY_OFFSET_HOURS * 3_600_000);

  const next = new Date(nowAlmaty);
  next.setUTCHours(MV_REFRESH_HOUR_ALMATY, MV_REFRESH_MINUTE_ALMATY, 0, 0);

  if (next <= nowAlmaty) {
    next.setUTCDate(next.getUTCDate() + 1);
  }

  return next.getTime() - nowAlmaty.getTime();
}

/**
 * Core refresh logic. Called by the cron tick AND by the manual endpoint.
 * Returns { ok, duration_ms, refreshed_at } or throws on error.
 */
export async function runMvRefresh() {
  const startedAt = Date.now();

  const { error } = await serviceClient.rpc('refresh_sales_mvs');
  if (error) {
    throw new Error(`refresh_sales_mvs RPC failed: ${error.message}`);
  }

  const duration_ms   = Date.now() - startedAt;
  const refreshed_at  = new Date().toISOString();

  logger.info(
    { duration_ms, refreshed_at },
    'mv_refresh_ok'
  );

  return { ok: true, duration_ms, refreshed_at };
}

async function tick() {
  if (isRunning) {
    logger.warn('mv_refresh_skipped_already_running');
    scheduleNext();
    return;
  }

  isRunning = true;
  try {
    await runMvRefresh();
  } catch (err) {
    captureException(err, { task: 'mv_refresh_scheduled' });
    logger.error({ err: err.message }, 'mv_refresh_unhandled_error');
  } finally {
    isRunning = false;
    scheduleNext();
  }
}

function scheduleNext() {
  const delay    = nextRunDelayMs();
  const nextDate = new Date(Date.now() + delay);

  if (refreshTimer) clearTimeout(refreshTimer);
  refreshTimer = setTimeout(tick, delay);

  logger.info(
    { nextRunAt: nextDate.toISOString(), hoursUntil: (delay / 3_600_000).toFixed(1) },
    'mv_refresh_scheduled'
  );
}

export function startMvRefreshScheduler() {
  logger.info(
    { hour: MV_REFRESH_HOUR_ALMATY, minute: MV_REFRESH_MINUTE_ALMATY },
    'mv_refresh_scheduler_starting'
  );
  scheduleNext();
}

export function stopMvRefreshScheduler() {
  if (refreshTimer) {
    clearTimeout(refreshTimer);
    refreshTimer = null;
  }
}
