import { logger } from '../config.js';

/**
 * Sentry-ready stub (W3 observability, 2026-04-17).
 *
 * We haven't signed up for Sentry yet. Rather than sprinkle `if (sentry)`
 * checks across the codebase, we export a stable `captureException(err, ctx)`
 * API that currently just logs. When Adil signs up and provides SENTRY_DSN in
 * Railway env, replacing this file with a real @sentry/node wiring is a
 * two-line change — callers don't need to touch anything.
 *
 * Design choices:
 *   - No fallback "save to DB" — we don't want a logging failure to consume
 *     Supabase writes.
 *   - `ctx` is a plain object of tags/extras. Callers pass `{ sessionId,
 *     userId, operation, ... }` — these map 1:1 to Sentry tags later.
 *   - Errors that propagate to Express's default error handler go through
 *     `expressErrorHandler` below, which is safer than `app.use((err, req,
 *     res, next) => ...)` because it re-throws after capture — we still want
 *     Express to produce the 500 response.
 */

export function captureException(err, ctx = {}) {
  logger.error(
    {
      err: { message: err?.message, stack: err?.stack, code: err?.code },
      ...ctx,
    },
    'captured_exception'
  );
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
