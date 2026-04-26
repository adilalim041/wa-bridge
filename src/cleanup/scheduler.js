import { logger } from '../config.js';
import { runCloudinaryCleanup, getCleanupConfig } from './cloudinaryCleanup.js';
import { captureException } from '../observability/sentry.js';

/**
 * Cleanup scheduler.
 *
 * Runs cloudinary cleanup once per 24h, at CLOUDINARY_CLEANUP_HOUR Almaty time
 * (default 04:00 — low traffic window). The first run is delayed to the next
 * scheduled hour, so process restart doesn't trigger an immediate sweep.
 *
 * The scheduler always installs (cheap timer); the cleanup itself decides
 * whether to actually do anything based on CLOUDINARY_CLEANUP_MODE.
 */

const CLEANUP_HOUR_ALMATY = Number(process.env.CLOUDINARY_CLEANUP_HOUR || 4);
const ALMATY_OFFSET_HOURS = 5;

let cleanupTimer = null;
let isRunning = false;

function nextRunDelayMs() {
  const now = new Date();
  const nowUtcMs = now.getTime();
  const nowAlmaty = new Date(nowUtcMs + ALMATY_OFFSET_HOURS * 3600_000);

  const next = new Date(nowAlmaty);
  next.setUTCHours(CLEANUP_HOUR_ALMATY, 0, 0, 0);
  if (next <= nowAlmaty) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next.getTime() - nowAlmaty.getTime();
}

async function tick() {
  if (isRunning) {
    logger.warn('cloudinary_cleanup_skipped_already_running');
    scheduleNext();
    return;
  }
  isRunning = true;
  try {
    const summary = await runCloudinaryCleanup();
    if (summary.errors?.length) {
      logger.warn({ errors: summary.errors.slice(0, 5) }, 'cloudinary_cleanup_partial_errors');
    }
  } catch (err) {
    captureException(err, { task: 'cloudinary_cleanup_scheduled' });
    logger.error({ err: err.message }, 'cloudinary_cleanup_unhandled_error');
  } finally {
    isRunning = false;
    scheduleNext();
  }
}

function scheduleNext() {
  const delay = nextRunDelayMs();
  if (cleanupTimer) clearTimeout(cleanupTimer);
  cleanupTimer = setTimeout(tick, delay);
  const nextDate = new Date(Date.now() + delay);
  logger.info(
    { nextRunAt: nextDate.toISOString(), hoursUntil: (delay / 3600_000).toFixed(1) },
    'cloudinary_cleanup_scheduled'
  );
}

export function startCleanupScheduler() {
  const config = getCleanupConfig();
  logger.info(config, 'cloudinary_cleanup_scheduler_starting');
  scheduleNext();
}

export function stopCleanupScheduler() {
  if (cleanupTimer) {
    clearTimeout(cleanupTimer);
    cleanupTimer = null;
  }
}
