import crypto from 'crypto';
import { logger } from '../../config.js';

/**
 * Request correlation middleware (W3 observability, 2026-04-17).
 *
 * Assigns every incoming HTTP request:
 *   - req.id   — a short UUID that's echoed in the `X-Request-Id` response header
 *   - req.log  — a pino child logger with {reqId, method, path} baked in
 *
 * Downstream middleware (auth, routes) can enrich `req.log` by re-assigning it
 * to a further child (e.g. `req.log = req.log.child({ userId: ... })`) after
 * identity is known. Every subsequent log line carries the correlation fields
 * automatically, so we can trace a single request across the whole codebase by
 * grepping Railway logs for its reqId.
 *
 * On response finish we emit one structured line with status + duration. That's
 * enough to build basic latency/error dashboards later without APM.
 *
 * Safety:
 *   - PII policy: we log method, path, status, duration, reqId, userId (when
 *     auth succeeded). We do NOT log query strings, request body, JWT tokens,
 *     or response body. Path can contain phone numbers (`/sessions/X/chats/77014...`)
 *     so downstream consumers must treat paths as potentially PII.
 *   - Header accepts inbound `X-Request-Id` so upstream proxies (Railway, CDNs)
 *     can thread their own ID through without us generating a new one.
 *   - All ID generation is wrapped in try/catch — if crypto.randomUUID ever
 *     throws (it doesn't in Node 18+, but defence in depth), the request still
 *     proceeds with the bare module logger.
 */
export function requestLogger(req, res, next) {
  let reqId;
  try {
    const incoming = req.headers['x-request-id'];
    // Accept incoming request IDs but cap length to prevent log injection /
    // abuse (some actors send 10KB request IDs to pollute log indices).
    reqId = (typeof incoming === 'string' && incoming.length > 0 && incoming.length <= 64)
      ? incoming
      : crypto.randomUUID();
  } catch {
    reqId = `fallback-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }

  req.id = reqId;
  req.log = logger.child({
    reqId,
    method: req.method,
    path: req.path,
  });

  res.setHeader('X-Request-Id', reqId);

  const startHrTime = process.hrtime.bigint();

  res.on('finish', () => {
    const durationMs = Number(process.hrtime.bigint() - startHrTime) / 1_000_000;
    const level = res.statusCode >= 500 ? 'error' : res.statusCode >= 400 ? 'warn' : 'info';

    req.log[level](
      {
        status: res.statusCode,
        durationMs: Math.round(durationMs),
        userId: req.user?.userId,
      },
      'http_request_completed'
    );
  });

  next();
}
