import { createClient } from '@supabase/supabase-js';
import { config, logger } from '../config.js';

/**
 * W1 Task 1.1 — Phase 1: client split.
 *
 * Before this refactor every backend query went through a single `supabase`
 * client built with the service_role key, which bypasses RLS. Any authenticated
 * dashboard user effectively had root on the database via the REST API.
 *
 * The split:
 *  - `serviceClient`  — service_role. Bypasses RLS. Use for background workers,
 *                       Baileys ingestion, admin endpoints, `auth.getUser` checks.
 *  - `getUserClient`  — returns an ad-hoc client carrying the caller's Supabase
 *                       JWT. Every query goes through PostgREST *as that user*,
 *                       so RLS policies (Phase 3) will scope the reads.
 *
 * Phase 1 (this commit) only adds the plumbing — no existing import is changed,
 * `supabase` stays as an alias of `serviceClient`. Phase 2 will migrate
 * user-facing GET endpoints in routes.js to `req.userClient ?? serviceClient`.
 * Phase 3 will enable RLS table-by-table once endpoints are migrated.
 */

export const serviceClient = createClient(config.supabaseUrl, config.supabaseServiceRoleKey);

/**
 * Back-compat alias. DO NOT import in new code — prefer `serviceClient`
 * (for internal workers) or `getUserClient(jwt)` (for request-scoped reads).
 * Existing imports keep working untouched in Phase 1.
 */
export const supabase = serviceClient;

const ANON_KEY = config.supabaseKey;

if (!ANON_KEY) {
  logger.warn('SUPABASE_KEY (anon) not set — getUserClient() will fall back to serviceClient which defeats RLS');
}

/**
 * Build a Supabase client scoped to a specific user JWT.
 *
 * Every query issued through the returned client is authorised by PostgREST
 * using the caller's JWT, so RLS policies apply. When Phase 3 enables RLS,
 * this is what enforces tenant isolation.
 *
 * Contract:
 *  - `jwt` must already be verified (see src/api/jwtAuth.js). This function
 *    does NOT verify the signature again.
 *  - Returns `null` if the anon key is missing — callers should fall back to
 *    `serviceClient` and log a warning, since RLS cannot be enforced without it.
 *  - The returned client does not auto-refresh tokens — it's meant for the
 *    lifetime of one HTTP request.
 *
 * @param {string} jwt - verified Supabase user JWT
 * @returns {import('@supabase/supabase-js').SupabaseClient | null}
 */
export function getUserClient(jwt) {
  if (!ANON_KEY || !jwt) return null;
  return createClient(config.supabaseUrl, ANON_KEY, {
    global: { headers: { Authorization: `Bearer ${jwt}` } },
    auth: { persistSession: false, autoRefreshToken: false },
  });
}
