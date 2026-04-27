import * as Sentry from '@sentry/node';
import { logger } from '../config.js';

/**
 * Sentry wiring — wired up 2026-04-26.
 *
 * Public API stays the same as the previous pino-only stub
 * (captureException, expressErrorHandler, installGlobalHandlers) so callers
 * never had to change. When SENTRY_DSN is set in env, errors are forwarded to
 * Sentry in addition to pino. When DSN is absent, behaviour falls back to the
 * original stdout-only logging — useful in dev/CI without burning Sentry
 * quota.
 *
 * Design choices:
 *   - tracesSampleRate: 0 — performance monitoring costs more events; we only
 *     want errors. Can be raised later if we want APM.
 *   - sendDefaultPii: false — don't auto-collect IP/cookies/headers. We pass
 *     explicit ctx tags (reqId, userId, sessionId) so we know what we send.
 *   - Always log to pino too. Cheap insurance: Sentry could hit its 5K/mo
 *     free tier mid-incident, and we still want stdout history for the bits
 *     of Railway logs we have.
 *   - captureException is sync — Sentry's transport buffers + flushes async,
 *     so callers don't await.
 */

const SENTRY_DSN = process.env.SENTRY_DSN || '';
const SENTRY_ENABLED = Boolean(SENTRY_DSN);

if (SENTRY_ENABLED) {
  Sentry.init({
    dsn: SENTRY_DSN,
    environment: process.env.NODE_ENV || 'production',
    release: process.env.RAILWAY_GIT_COMMIT_SHA || process.env.GIT_COMMIT || undefined,
    tracesSampleRate: 0,
    sendDefaultPii: false,
  });
  logger.info({ env: process.env.NODE_ENV || 'production' }, 'Sentry initialized');
} else {
  logger.info('Sentry DSN not set — running without remote error reporting');
}

export function captureException(err, ctx = {}) {
  if (SENTRY_ENABLED) {
    Sentry.withScope((scope) => {
      for (const [k, v] of Object.entries(ctx)) {
        if (v == null) continue;
        const str = typeof v === 'string' ? v : String(v);
        scope.setTag(k, str.length > 200 ? str.slice(0, 200) : str);
      }
      Sentry.captureException(err instanceof Error ? err : new Error(String(err)));
    });
  }
  logger.error(
    {
      err: { message: err?.message, stack: err?.stack, code: err?.code },
      ...ctx,
    },
    'captured_exception'
  );
}

/**
 * Capture a known operational event (no Error stack needed).
 * Use for intentional system alerts (cascade ban, mass disconnect, etc.)
 * where the event itself is expected code-path but warrants visibility in Sentry.
 *
 * @param {string} message - Short human-readable event name (shows in Sentry issue title)
 * @param {{ level?: 'fatal'|'error'|'warning'|'info', tags?: object, extra?: object }} opts
 */
export function captureMessage(message, { level = 'error', tags = {}, extra = {} } = {}) {
  if (SENTRY_ENABLED) {
    Sentry.withScope((scope) => {
      scope.setLevel(level);
      for (const [k, v] of Object.entries(tags)) {
        if (v == null) continue;
        const str = typeof v === 'string' ? v : String(v);
        scope.setTag(k, str.length > 200 ? str.slice(0, 200) : str);
      }
      for (const [k, v] of Object.entries(extra)) {
        scope.setExtra(k, v);
      }
      Sentry.captureMessage(message, level);
    });
  }
  const pinoLevel = level === 'fatal' || level === 'error' ? 'error' : level === 'warning' ? 'warn' : 'info';
  logger[pinoLevel]({ ...tags, ...extra }, `sentry_message: ${message}`);
}

/**
 * Express error-handling middleware. Mount AFTER all routes.
 * Captures the error, then delegates to Express's default 500 response.
 */
export function expressErrorHandler(err, req, res, next) {
  captureException(err, {
    reqId: req.id,
    userId: req.user?.userId,
    method: req.method,
    path: req.path,
  });
  next(err);
}

/**
 * Global hooks for unhandled promise rejections + uncaught exceptions.
 * These are fatal-ish — we capture and let the process continue (Express will
 * return 500 for request-bound errors). If we ever see cascading unhandled
 * rejections, revisit to process.exit(1) instead.
 */
export function installGlobalHandlers() {
  process.on('unhandledRejection', (reason) => {
    captureException(reason instanceof Error ? reason : new Error(String(reason)), {
      kind: 'unhandledRejection',
    });
  });
  process.on('uncaughtException', (err) => {
    captureException(err, { kind: 'uncaughtException' });
  });
}

/**
 * Test-only helper. Exposed so we can wire a /test-sentry-error route that
 * proves the pipeline works end-to-end after env config. Not for prod use.
 */
export function isSentryEnabled() {
  return SENTRY_ENABLED;
}
