/**
 * Meta Marketing API — module configuration
 *
 * Env-driven config + zod validation. The module is **dormant by default**:
 * if META_SYSTEM_USER_TOKEN is empty/missing, `metaAdsConfig.enabled === false`
 * and no calls to Meta will be made. This keeps the module safe to deploy
 * before secrets are populated in Railway.
 *
 * Phase 1 (current): single-tenant — Adil's own ad account from .env.
 * Phase 2 (future): multi-tenant — tokens come from `meta_ad_accounts` table,
 *   .env values act as a default fallback for ops/debug.
 *
 * Decisions log: ObsidianVault/research/library/backend-libs/meta-marketing-api.md
 */

import { z } from 'zod';

/** Strict format check for ad account id ("act_" + digits). */
const adAccountIdSchema = z
  .string()
  .regex(/^act_\d+$/, 'META_AD_ACCOUNT_ID must look like act_1234567890');

/** Loose check for token shape — Meta tokens are EAA-prefixed and ~200+ chars. */
const tokenSchema = z
  .string()
  .min(50, 'META_SYSTEM_USER_TOKEN looks too short')
  .startsWith('EAA', 'META_SYSTEM_USER_TOKEN must start with EAA');

/** Schema for ALL meta_* vars when the module is active. */
const activeSchema = z.object({
  META_APP_ID: z.string().min(1),
  META_APP_SECRET: z.string().min(1),
  META_SYSTEM_USER_TOKEN: tokenSchema,
  META_AD_ACCOUNT_ID: adAccountIdSchema,
  META_API_VERSION: z
    .string()
    .regex(/^v\d+\.\d+$/, 'META_API_VERSION must look like v22.0')
    .default('v22.0'),
  META_SYNC_INTERVAL_HOURS: z.coerce
    .number()
    .int()
    .min(1, 'META_SYNC_INTERVAL_HOURS must be >= 1 hour')
    .max(168, 'META_SYNC_INTERVAL_HOURS must be <= 168 hours (1 week)')
    .default(6),
  ENABLE_CREATIVE_VISION: z
    .enum(['true', 'false'])
    .default('false')
    .transform((v) => v === 'true'),
  ANTHROPIC_API_KEY_VISION: z.string().optional().default(''),
});

/**
 * Mask a Meta token for safe logging.
 * Returns first 3 + last 4 characters with asterisks in between.
 *
 * @example maskToken('EAAGm0PX4ZCpsBA...VZAW') === 'EAA***VZAW'
 */
export function maskToken(token) {
  if (!token || typeof token !== 'string' || token.length < 10) return '<empty>';
  return `${token.slice(0, 3)}***${token.slice(-4)}`;
}

/**
 * Resolve module config from process.env. Pure — no side effects beyond reading env.
 *
 * @returns {{
 *   enabled: boolean,
 *   reason?: string,
 *   appId?: string,
 *   appSecret?: string,
 *   systemUserToken?: string,
 *   adAccountId?: string,
 *   apiVersion?: string,
 *   syncIntervalHours?: number,
 *   enableCreativeVision?: boolean,
 *   anthropicVisionKey?: string,
 *   baseUrl?: string,
 * }}
 */
export function resolveMetaAdsConfig(env = process.env) {
  // Module is dormant when token is missing — explicit, not error.
  // This is the default state for every wa-bridge deploy that doesn't
  // need Meta integration (e.g. template forks for other clients).
  if (!env.META_SYSTEM_USER_TOKEN?.trim()) {
    return {
      enabled: false,
      reason: 'META_SYSTEM_USER_TOKEN not set — module dormant',
    };
  }

  const parsed = activeSchema.safeParse(env);
  if (!parsed.success) {
    // When token IS set but other vars are broken — fail loud.
    // This prevents silent half-configured deploys.
    const issues = parsed.error.issues
      .map((i) => `  ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Meta-ads config invalid:\n${issues}`);
  }

  const v = parsed.data;
  return {
    enabled: true,
    appId: v.META_APP_ID,
    appSecret: v.META_APP_SECRET,
    systemUserToken: v.META_SYSTEM_USER_TOKEN,
    adAccountId: v.META_AD_ACCOUNT_ID,
    apiVersion: v.META_API_VERSION,
    syncIntervalHours: v.META_SYNC_INTERVAL_HOURS,
    enableCreativeVision: v.ENABLE_CREATIVE_VISION,
    anthropicVisionKey: v.ANTHROPIC_API_KEY_VISION,
    baseUrl: `https://graph.facebook.com/${v.META_API_VERSION}`,
  };
}

/** Singleton resolved at import time. Re-import after env changes (tests only). */
export const metaAdsConfig = resolveMetaAdsConfig();
