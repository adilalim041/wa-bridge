/**
 * Supabase JWT verification middleware helper.
 *
 * Strategy (fail-closed):
 *   1. Try JWKS (asymmetric, ES256 / RS256) — Supabase projects created after ~2022 use this.
 *      JWKS endpoint: ${SUPABASE_URL}/auth/v1/.well-known/jwks.json
 *      jose createRemoteJWKSet caches keys for 1h with single-flight fetch on cache miss.
 *   2. Fallback to HS256 via SUPABASE_JWT_SECRET — only activated when JWKS verification
 *      throws a key-not-found / key-mismatch error AND SUPABASE_JWT_SECRET is set.
 *      Use case: legacy Supabase projects still on symmetric JWT.
 *
 * This project (gehiqhnzbumtbvhncblj) is confirmed JWKS / ES256 as of 2026-04-17.
 * The HS256 fallback exists purely as a defensive layer for future forks / template clients.
 *
 * env vars consumed:
 *   SUPABASE_URL          — required for JWKS path (e.g. https://xxx.supabase.co)
 *   SUPABASE_JWT_SECRET   — optional, enables HS256 fallback
 */

import { createRemoteJWKSet, jwtVerify, createSecretKey } from 'jose';
import { logger } from '../config.js';

// ── JWKS setup ────────────────────────────────────────────────────────────────

const SUPABASE_URL = process.env.SUPABASE_URL;

// jwks will be null when SUPABASE_URL is not set — JWT auth path disabled.
let JWKS = null;
let JWKS_ISSUER = null;

if (SUPABASE_URL) {
    JWKS = createRemoteJWKSet(
        new URL(`${SUPABASE_URL}/auth/v1/.well-known/jwks.json`),
        {
            cacheMaxAge: 60 * 60 * 1000,   // 1 hour — Supabase rotates keys rarely
            cooldownDuration: 30_000,        // 30 s between re-fetches on cache miss
            timeoutDuration: 5_000,          // 5 s fetch timeout
        }
    );
    JWKS_ISSUER = `${SUPABASE_URL}/auth/v1`;
    logger.info('Supabase JWT auth enabled (JWKS)');
} else {
    logger.warn('SUPABASE_URL not set — Supabase JWT auth disabled, x-api-key only');
}

// ── HS256 fallback setup ──────────────────────────────────────────────────────

// Only materialised when the env var is present — no object creation overhead otherwise.
const HS256_SECRET = process.env.SUPABASE_JWT_SECRET
    ? createSecretKey(Buffer.from(process.env.SUPABASE_JWT_SECRET, 'utf-8'))
    : null;

if (HS256_SECRET) {
    logger.info('Supabase JWT HS256 fallback enabled (SUPABASE_JWT_SECRET)');
}

// Errors from jose that indicate the key itself is wrong (not an expired/invalid token).
// On these errors we should try the HS256 fallback, not immediately reject.
const KEY_MISMATCH_CODES = new Set([
    'ERR_JWKS_NO_MATCHING_KEY',
    'ERR_JWS_SIGNATURE_VERIFICATION_FAILED',
    'ERR_JOSE_ALG_NOT_ALLOWED',
]);

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Verify a raw JWT string from Supabase Auth.
 *
 * @param {string} token  — raw JWT (no "Bearer " prefix)
 * @returns {{ userId: string, email: string|undefined, role: string|undefined }}
 * @throws  Error with .message when verification fails
 */
export async function verifySupabaseJwt(token) {
    // ── Attempt 1: JWKS (asymmetric) ─────────────────────────────────────────
    if (JWKS) {
        try {
            const { payload } = await jwtVerify(token, JWKS, {
                issuer: JWKS_ISSUER,
                audience: 'authenticated',
            });
            return {
                userId: payload.sub,
                email: payload.email,
                role: payload.role,
            };
        } catch (err) {
            if (KEY_MISMATCH_CODES.has(err.code) && HS256_SECRET) {
                // Key doesn't match JWKS — fall through to HS256 attempt
                logger.debug({ errCode: err.code }, 'jwks key mismatch, trying hs256 fallback');
            } else {
                // Token itself is invalid (expired, bad audience, malformed, etc.)
                throw err;
            }
        }
    }

    // ── Attempt 2: HS256 symmetric fallback ──────────────────────────────────
    if (HS256_SECRET) {
        // issuer check is intentionally omitted for HS256 — some older Supabase projects
        // omit the iss claim. The secret is itself the authentication factor.
        const { payload } = await jwtVerify(token, HS256_SECRET, {
            algorithms: ['HS256'],
        });
        return {
            userId: payload.sub,
            email: payload.email,
            role: payload.role,
        };
    }

    // No JWKS and no secret — auth is disabled
    throw new Error('JWT verification unavailable: SUPABASE_URL and SUPABASE_JWT_SECRET both missing');
}

/**
 * Returns true when at least one JWT verification path is configured.
 * Used by server.js to decide whether to attempt JWT auth at all.
 */
export function isJwtAuthAvailable() {
    return JWKS !== null || HS256_SECRET !== null;
}
