import { v2 as cloudinary } from 'cloudinary';
import { logger } from '../config.js';
import { captureException } from '../observability/sentry.js';

/**
 * Cloudinary retention cleanup — wired up 2026-04-26.
 *
 * Why this exists: WhatsApp media uploads grow unbounded into Cloudinary.
 * On Free plan (25 credits ≈ 25 GB) with 6 sessions × ~30-100 media/day,
 * the project hits quota in ~80 days. This module deletes media older than
 * CLOUDINARY_RETENTION_DAYS (default 90) from the `wa-bridge/` folder only,
 * so PDF reports under `crm/reports` and other tenants are never touched.
 *
 * Safety rails:
 *   - Hard-coded prefix `wa-bridge/` — cannot delete from other folders
 *     even if env mis-configured.
 *   - CLOUDINARY_CLEANUP_MODE defaults to `disabled`; setting it explicitly
 *     to `dry-run` or `delete` is required for the cron to do anything.
 *   - The /admin endpoint accepts an explicit `mode` override but defaults
 *     to dry-run when called without query.
 *   - Iterates by resource_type (image, video, raw) — Cloudinary's listing
 *     API is per-type, easy to forget.
 *
 * Limits:
 *   - delete_resources accepts max 100 public_ids per call.
 *   - We sleep 200ms between batches to stay well under Cloudinary's
 *     500 calls/hour rate limit on Free plan.
 */

const RETENTION_DAYS = Number(process.env.CLOUDINARY_RETENTION_DAYS) || 90;
const CLEANUP_MODE_ENV = (process.env.CLOUDINARY_CLEANUP_MODE || 'disabled').toLowerCase();
const TARGET_PREFIX = 'wa-bridge/';
const RESOURCE_TYPES = ['image', 'video', 'raw'];
const PAGE_SIZE = 500;
const DELETE_BATCH_SIZE = 100;
const INTER_BATCH_DELAY_MS = 200;

function isValidMode(mode) {
  return mode === 'disabled' || mode === 'dry-run' || mode === 'delete';
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function listOldResources(resourceType, cutoffIso) {
  const candidates = [];
  let nextCursor = undefined;
  let pages = 0;

  do {
    const params = {
      type: 'upload',
      resource_type: resourceType,
      prefix: TARGET_PREFIX,
      max_results: PAGE_SIZE,
    };
    if (nextCursor) params.next_cursor = nextCursor;

    const result = await cloudinary.api.resources(params);
    pages++;

    for (const r of result.resources || []) {
      if (r.created_at && r.created_at < cutoffIso) {
        candidates.push({
          public_id: r.public_id,
          resource_type: resourceType,
          bytes: r.bytes || 0,
          created_at: r.created_at,
        });
      }
    }
    nextCursor = result.next_cursor;
  } while (nextCursor);

  return { candidates, pages };
}

async function deleteBatch(publicIds, resourceType) {
  if (publicIds.length === 0) return { deleted: 0, errors: [] };
  try {
    const result = await cloudinary.api.delete_resources(publicIds, {
      resource_type: resourceType,
      type: 'upload',
    });
    const deleted = Object.values(result.deleted || {}).filter((v) => v === 'deleted').length;
    const notFound = Object.values(result.deleted || {}).filter((v) => v === 'not_found').length;
    return { deleted, notFound, raw: result };
  } catch (err) {
    return { deleted: 0, errors: [err.message || String(err)] };
  }
}

/**
 * Run the cleanup.
 *
 * @param {Object} options
 * @param {('disabled'|'dry-run'|'delete')} [options.mode] - explicit override; defaults to env CLOUDINARY_CLEANUP_MODE
 * @param {number} [options.retentionDays] - explicit override; defaults to env CLOUDINARY_RETENTION_DAYS
 * @param {boolean} [options.forceDryRun] - safety override: even if mode=delete, behave as dry-run
 *
 * @returns {Promise<{ok: boolean, mode: string, scanned: number, candidates: number, bytesFreed: number, deleted: number, notFound: number, errors: string[], samples: Array, durationMs: number}>}
 */
export async function runCloudinaryCleanup(options = {}) {
  const startedAt = Date.now();
  const mode = isValidMode(options.mode) ? options.mode : CLEANUP_MODE_ENV;
  const retentionDays = Number(options.retentionDays) > 0 ? Number(options.retentionDays) : RETENTION_DAYS;
  const effectiveMode = options.forceDryRun ? 'dry-run' : mode;

  if (effectiveMode === 'disabled') {
    logger.info({ mode, retentionDays }, 'cloudinary_cleanup_skipped_disabled');
    return {
      ok: true,
      mode: 'disabled',
      scanned: 0,
      candidates: 0,
      bytesFreed: 0,
      deleted: 0,
      notFound: 0,
      errors: [],
      samples: [],
      durationMs: Date.now() - startedAt,
    };
  }

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - retentionDays);
  const cutoffIso = cutoff.toISOString();

  logger.info({ mode: effectiveMode, retentionDays, cutoffIso, prefix: TARGET_PREFIX }, 'cloudinary_cleanup_starting');

  let scanned = 0;
  let candidatesCount = 0;
  let bytesFreed = 0;
  let deleted = 0;
  let notFound = 0;
  const errors = [];
  const samples = [];

  for (const resourceType of RESOURCE_TYPES) {
    let candidates;
    try {
      const result = await listOldResources(resourceType, cutoffIso);
      candidates = result.candidates;
      scanned += result.pages * PAGE_SIZE;
    } catch (err) {
      const msg = `list ${resourceType} failed: ${err.message}`;
      errors.push(msg);
      captureException(err, { stage: 'list', resourceType, prefix: TARGET_PREFIX });
      continue;
    }

    candidatesCount += candidates.length;
    for (const c of candidates.slice(0, 3)) {
      if (samples.length < 5) samples.push(c);
    }
    bytesFreed += candidates.reduce((acc, c) => acc + c.bytes, 0);

    if (effectiveMode === 'dry-run' || candidates.length === 0) {
      continue;
    }

    // mode === 'delete' → batch delete
    for (let i = 0; i < candidates.length; i += DELETE_BATCH_SIZE) {
      const batch = candidates.slice(i, i + DELETE_BATCH_SIZE);
      const publicIds = batch.map((c) => c.public_id);
      const result = await deleteBatch(publicIds, resourceType);
      deleted += result.deleted || 0;
      notFound += result.notFound || 0;
      if (result.errors?.length) {
        errors.push(...result.errors);
        captureException(new Error(`Cloudinary delete batch failed: ${result.errors[0]}`), {
          stage: 'delete',
          resourceType,
          batchSize: batch.length,
        });
      }
      if (i + DELETE_BATCH_SIZE < candidates.length) {
        await sleep(INTER_BATCH_DELAY_MS);
      }
    }
  }

  const durationMs = Date.now() - startedAt;
  const summary = {
    ok: errors.length === 0,
    mode: effectiveMode,
    scanned,
    candidates: candidatesCount,
    bytesFreed,
    deleted,
    notFound,
    errors,
    samples,
    durationMs,
    retentionDays,
    cutoffIso,
  };

  logger.info(summary, 'cloudinary_cleanup_finished');
  return summary;
}

export function getCleanupConfig() {
  return {
    mode: CLEANUP_MODE_ENV,
    retentionDays: RETENTION_DAYS,
    targetPrefix: TARGET_PREFIX,
  };
}
