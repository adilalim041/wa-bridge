/**
 * Meta Ads delta sync scheduler.
 *
 * Runs syncDelta every META_SYNC_INTERVAL_HOURS (default 6) hours.
 * First run fires START_DELAY_MS (30s) after process start — so Railway
 * deployments don't wait 6h to confirm the cron is alive.
 *
 * Guards:
 *  - metaAdsConfig.enabled must be true (token present + config valid).
 *  - META_ADS_CRON_ENABLED !== 'false' (opt-out env flag).
 *  - Idempotent: calling startMetaAdsCron() twice is safe.
 *
 * Alert policy (Telegram):
 *  - Token expired (code 190) → critical alert, stops alerting further.
 *  - Any other thrown error    → warning alert.
 *  - Partial sync (errors in results but completed) → log only, no alert
 *    (rate-limit partials are expected ~1×/day for busy accounts).
 *  - Success                   → log only, no alert (noise).
 */

import { logger } from '../config.js';
import { captureException } from '../observability/sentry.js';
import { metaAdsConfig } from './config.js';
import { syncDelta } from './sync.js';
import { sendTelegramMessage } from '../notifications/telegramBot.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Delay before the very first run after process start (ms). */
const START_DELAY_MS = 30_000;

/** Meta error code that signals an invalid / expired token. */
const TOKEN_EXPIRED_CODE = 190;

// ---------------------------------------------------------------------------
// Module state
// ---------------------------------------------------------------------------

let scheduled = false;
let firstRunTimer = null;
let intervalTimer = null;

// ---------------------------------------------------------------------------
// Core tick
// ---------------------------------------------------------------------------

async function tick() {
  logger.info('meta-ads:cron: delta sync starting');

  let results;
  try {
    results = await syncDelta(metaAdsConfig.adAccountId);
  } catch (err) {
    // Unexpected throw from syncDelta itself (should not normally happen —
    // syncDelta catches internally — but defensive).
    captureException(err, { task: 'meta_ads_cron' });
    logger.error({ err }, 'meta-ads:cron: delta sync threw unexpectedly');

    if (err instanceof Error && Number(err.code) === TOKEN_EXPIRED_CODE) {
      await sendTelegramMessage(
        'Meta API token expired — Omoikiri ad sync stopped. Regenerate token in BM and update Railway secret META_SYSTEM_USER_TOKEN.'
      );
    } else {
      await sendTelegramMessage(
        `Meta cron sync failed (unexpected): ${err.message}`
      );
    }
    return;
  }

  // Aggregate errors from all result stages.
  const allErrors = results.flatMap((r) => r.errors ?? []);
  const totalRecords = results.reduce((sum, r) => sum + (r.recordsSynced ?? 0), 0);
  const hasTokenExpired = allErrors.some((e) => Number(e.code) === TOKEN_EXPIRED_CODE);
  const overallStatus = results.find((r) => r.status === 'error')
    ? 'error'
    : results.find((r) => r.status === 'partial')
    ? 'partial'
    : 'ok';

  logger.info(
    { status: overallStatus, totalRecords, errorCount: allErrors.length },
    'meta-ads:cron: delta sync completed'
  );

  // Token expired — critical alert.
  if (hasTokenExpired) {
    await sendTelegramMessage(
      'Meta API token expired — Omoikiri ad sync stopped. Regenerate token in BM and update Railway secret META_SYSTEM_USER_TOKEN.'
    );
    return;
  }

  // Hard error (not partial) — alert.
  if (overallStatus === 'error' && allErrors.length > 0) {
    const firstErr = allErrors[0];
    await sendTelegramMessage(
      `Meta cron sync failed: [${firstErr.code}] ${firstErr.message}`
    );
    return;
  }

  // Partial sync (rate-limit, etc.) — log only, no Telegram noise.
  // Success — log only, no Telegram noise.
}

// ---------------------------------------------------------------------------
// Scheduler helpers
// ---------------------------------------------------------------------------

function scheduleInterval() {
  const intervalMs = (metaAdsConfig.syncIntervalHours ?? 6) * 3_600_000;
  intervalTimer = setInterval(tick, intervalMs);

  const nextDate = new Date(Date.now() + intervalMs);
  logger.info(
    {
      intervalHours: metaAdsConfig.syncIntervalHours ?? 6,
      nextRunAt: nextDate.toISOString(),
    },
    'meta-ads:cron: interval scheduled'
  );
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Start the Meta Ads delta sync cron.
 *
 * Safe to call multiple times — subsequent calls are no-ops.
 * Should be called once from src/index.js bootstrap(), after sessions start.
 */
export function startMetaAdsCron() {
  if (!metaAdsConfig.enabled) {
    logger.info('meta-ads:cron: module dormant — cron not started (no META_SYSTEM_USER_TOKEN)');
    return;
  }

  if (process.env.META_ADS_CRON_ENABLED === 'false') {
    logger.info('meta-ads:cron: disabled via META_ADS_CRON_ENABLED=false');
    return;
  }

  if (scheduled) {
    logger.warn('meta-ads:cron: startMetaAdsCron() called twice — ignoring');
    return;
  }

  scheduled = true;

  // Fire first run shortly after startup so Railway logs confirm cron is alive
  // without waiting a full interval.
  firstRunTimer = setTimeout(() => {
    firstRunTimer = null;
    tick().catch((err) => {
      captureException(err, { task: 'meta_ads_cron_first_run' });
      logger.error({ err }, 'meta-ads:cron: first run failed');
    });
  }, START_DELAY_MS);

  // Then repeat every N hours.
  scheduleInterval();

  logger.info(
    {
      adAccountId: metaAdsConfig.adAccountId,
      intervalHours: metaAdsConfig.syncIntervalHours ?? 6,
      firstRunInSeconds: START_DELAY_MS / 1000,
    },
    'meta-ads:cron: started'
  );
}

/**
 * Stop the scheduler. Called from src/index.js shutdown().
 * Safe to call even if never started.
 */
export function stopMetaAdsCron() {
  if (firstRunTimer) {
    clearTimeout(firstRunTimer);
    firstRunTimer = null;
  }
  if (intervalTimer) {
    clearInterval(intervalTimer);
    intervalTimer = null;
  }
  scheduled = false;
}
