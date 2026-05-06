/**
 * Meta Ads — REST API endpoints (iteration 3)
 *
 * Mounts 5 read-only endpoints consumed by wa-dashboard:
 *   GET /meta-ads/summary          — KPI totals for the period
 *   GET /meta-ads/campaigns         — paginated campaigns list with metrics
 *   GET /meta-ads/campaigns/:id     — single campaign + ad sets
 *   GET /meta-ads/ad-sets/:id       — single ad set + ads + creatives
 *   GET /meta-ads/sync-status       — last sync info + recent log
 *
 * Auth: handled by the global auth middleware in server.js (Bearer JWT or
 * x-api-key). No per-route auth needed — all paths under /meta-ads/ are
 * covered because the global middleware runs before setupRoutes.
 *
 * Money normalisation (CRITICAL — see meta-marketing-api.md risks):
 *   - DB stores ALL money in minor units (cents for USD).
 *   - `spend` from Meta insights was stored after ×100 conversion in sync.js.
 *   - `daily_budget` / `lifetime_budget` on campaigns/adsets come from Meta
 *     already in minor units → stored as-is.
 *   - On read: divide by 100, round to 2 decimals → major units (dollars).
 *
 * Aggregation rules (avoid double-counting):
 *   summary      → level='campaign'  (do NOT mix levels)
 *   campaigns    → level='campaign'
 *   adsets       → level='adset'     filtered by campaign
 *   ads          → level='ad'        filtered by adset
 *
 * CTR / CPM / CPC must be re-derived from SUM totals (not averaged).
 *   CTR = SUM(clicks) / SUM(impressions) × 100
 *   CPM = SUM(spend)  / SUM(impressions) × 1000  (both in same units → result in major)
 *   CPC = SUM(spend)  / SUM(clicks)
 *
 * Date handling:
 *   `from` / `to` are YYYY-MM-DD strings in the account's timezone (Asia/Omsk
 *   for Omoikiri). They are compared directly against meta_insights_daily.date_start
 *   which was already written in account timezone by sync.js. No UTC conversion.
 *
 * Smoke-test examples (run against local wa-bridge, replace <KEY> and host):
 *
 *   # 1. Summary — last 7 days (default)
 *   curl -s -H "x-api-key: <KEY>" http://localhost:3000/meta-ads/summary | jq .
 *
 *   # 2. Summary — custom range
 *   curl -s -H "x-api-key: <KEY>" \
 *     "http://localhost:3000/meta-ads/summary?from=2026-04-01&to=2026-04-30" | jq .
 *
 *   # 3. Campaigns — default (all, sorted by spend desc)
 *   curl -s -H "x-api-key: <KEY>" \
 *     "http://localhost:3000/meta-ads/campaigns?limit=10" | jq '.items[0]'
 *
 *   # 4. Single campaign (replace <UUID> with id from step 3)
 *   curl -s -H "x-api-key: <KEY>" \
 *     "http://localhost:3000/meta-ads/campaigns/<UUID>" | jq '.adSets | length'
 *
 *   # 5. Single ad set (replace <UUID> with adSets[0].id from step 4)
 *   curl -s -H "x-api-key: <KEY>" \
 *     "http://localhost:3000/meta-ads/ad-sets/<UUID>" | jq '.ads | length'
 *
 *   # 6. Sync status
 *   curl -s -H "x-api-key: <KEY>" http://localhost:3000/meta-ads/sync-status | jq .
 */

import { Router } from 'express';
import { z } from 'zod';
import { serviceClient } from '../storage/supabase.js';
import { logger } from '../config.js';
import { syncSingleCampaign, syncCreativeDetails } from './sync.js';
import { parseTargeting, parsePlacements } from './parsers.js';

export const metaAdsRouter = Router();

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_DAYS_BACK = 7;
const MAX_LIMIT = 500;
const SYNC_LOG_RECENT = 5;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Convert minor units (integer cents) to major units (dollars), rounded to 2dp.
 * Returns null when input is null/undefined.
 */
function toMajor(minorUnits) {
  if (minorUnits == null) return null;
  return Math.round(Number(minorUnits)) / 100;
}

/**
 * Calculate derived metrics from raw SUM aggregates.
 * All spend/cpm/cpc returned in major units.
 * Safe division — returns 0 when denominator is 0.
 */
function deriveMetrics({ spendMinor, impressions, clicks }) {
  const spend = toMajor(spendMinor) ?? 0;
  const imp = Number(impressions) || 0;
  const clk = Number(clicks) || 0;

  const ctr = imp > 0 ? parseFloat(((clk / imp) * 100).toFixed(2)) : 0;
  // CPM = spend_major / impressions * 1000
  const cpm = imp > 0 ? parseFloat(((spend / imp) * 1000).toFixed(4)) : 0;
  const cpc = clk > 0 ? parseFloat((spend / clk).toFixed(4)) : 0;

  return { spend, impressions: imp, clicks: clk, ctr, cpm, cpc };
}

/**
 * Build default date range: (today - 7 days) → today as YYYY-MM-DD strings.
 * These are local calendar dates — matching how sync.js stores date_start.
 */
function defaultDateRange() {
  const now = new Date();
  const to = now.toISOString().slice(0, 10);
  const from = new Date(now.getTime() - (DEFAULT_DAYS_BACK - 1) * 86400000)
    .toISOString()
    .slice(0, 10);
  return { from, to };
}

// ---------------------------------------------------------------------------
// Zod schemas for query params
// ---------------------------------------------------------------------------

const dateString = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, 'Must be YYYY-MM-DD');

const periodSchema = z
  .object({
    from: dateString.optional(),
    to:   dateString.optional(),
    account: z.string().optional(),
  })
  .superRefine((data, ctx) => {
    if (data.from && data.to && data.from > data.to) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'from must be <= to',
        path: ['from'],
      });
    }
  });

const campaignsQuerySchema = periodSchema.extend({
  status: z.string().optional(),
  sort: z
    .enum(['spend_desc', 'spend_asc', 'name', 'ctr_desc', 'created_desc'])
    .optional()
    .default('spend_desc'),
  limit: z.coerce.number().int().min(1).max(MAX_LIMIT).optional().default(100),
  offset: z.coerce.number().int().min(0).optional().default(0),
});

// ---------------------------------------------------------------------------
// Account resolver — resolves ad account row from query param or first active
// ---------------------------------------------------------------------------

async function resolveAccount(accountParam) {
  let query = serviceClient
    .from('meta_ad_accounts')
    .select('id, meta_account_id, account_name, currency, timezone_name, last_sync_at, last_sync_status')
    .eq('is_active', true);

  if (accountParam) {
    // Allow either our uuid PK or the raw meta_account_id string
    if (accountParam.startsWith('act_')) {
      query = query.eq('meta_account_id', accountParam);
    } else {
      query = query.eq('id', accountParam);
    }
  }

  const { data, error } = await query.order('created_at', { ascending: true }).limit(1).maybeSingle();

  if (error) throw error;
  return data; // null when not found
}

/**
 * Aggregate insights for a set of object_ids at a given level.
 * Returns a map: objectId → { spendMinor, impressions, clicks, reach }
 */
async function fetchInsightsMap({ accountId, level, objectIds, from, to }) {
  if (!objectIds || objectIds.length === 0) return new Map();

  const { data, error } = await serviceClient
    .from('meta_insights_daily')
    .select('object_id, spend, impressions, clicks, reach')
    .eq('ad_account_id', accountId)
    .eq('level', level)
    .in('object_id', objectIds)
    .gte('date_start', from)
    .lte('date_start', to);

  if (error) throw error;

  // Aggregate per object_id (multiple rows per object = different dates)
  const map = new Map();
  for (const row of data || []) {
    const prev = map.get(row.object_id) || { spendMinor: 0, impressions: 0, clicks: 0, reach: 0 };
    map.set(row.object_id, {
      spendMinor:   prev.spendMinor   + Number(row.spend       || 0),
      impressions:  prev.impressions  + Number(row.impressions || 0),
      clicks:       prev.clicks       + Number(row.clicks      || 0),
      reach:        prev.reach        + Number(row.reach       || 0),
    });
  }

  return map;
}

/**
 * Format a campaign row (for list and detail endpoints).
 */
function formatCampaign(row, metrics) {
  return {
    id:               row.id,
    metaCampaignId:   row.meta_campaign_id,
    name:             row.name,
    status:           row.status,
    objective:        row.objective ?? null,
    dailyBudget:      toMajor(row.daily_budget),
    lifetimeBudget:   toMajor(row.lifetime_budget),
    createdTime:      row.created_time ?? null,
    metrics: {
      spend:       metrics.spend,
      impressions: metrics.impressions,
      clicks:      metrics.clicks,
      ctr:         metrics.ctr,
      cpm:         metrics.cpm,
      cpc:         metrics.cpc,
    },
  };
}

/**
 * Format an adset row.
 */
function formatAdSet(row, metrics) {
  return {
    id:               row.id,
    metaAdSetId:      row.meta_adset_id,
    name:             row.name,
    status:           row.status,
    dailyBudget:      toMajor(row.daily_budget),
    lifetimeBudget:   toMajor(row.lifetime_budget),
    optimizationGoal: row.optimization_goal ?? null,
    billingEvent:     row.billing_event ?? null,
    isAdvantagePlus:  row.is_advantage_plus ?? false,
    targeting:        row.targeting ?? null,
    placements:       row.placements ?? null,
    metrics: {
      spend:       metrics.spend,
      impressions: metrics.impressions,
      clicks:      metrics.clicks,
      ctr:         metrics.ctr,
      cpm:         metrics.cpm,
      cpc:         metrics.cpc,
    },
  };
}

// ---------------------------------------------------------------------------
// GET /meta-ads/summary
// ---------------------------------------------------------------------------

metaAdsRouter.get('/summary', async (req, res) => {
  const parsed = periodSchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query params', details: parsed.error.flatten().fieldErrors });
  }

  const { from: rawFrom, to: rawTo, account: accountParam } = parsed.data;
  const defaults = defaultDateRange();
  const from = rawFrom ?? defaults.from;
  const to   = rawTo   ?? defaults.to;

  try {
    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Aggregate over level='campaign' rows only (avoids double-counting)
    const { data: rows, error } = await serviceClient
      .from('meta_insights_daily')
      .select('spend, impressions, clicks, reach')
      .eq('ad_account_id', account.id)
      .eq('level', 'campaign')
      .gte('date_start', from)
      .lte('date_start', to);

    if (error) throw error;

    // Reduce to single totals
    let spendMinor = 0, impressions = 0, clicks = 0, reach = 0;
    for (const r of rows || []) {
      spendMinor   += Number(r.spend       || 0);
      impressions  += Number(r.impressions || 0);
      clicks       += Number(r.clicks      || 0);
      reach        += Number(r.reach       || 0);
    }

    const metrics = deriveMetrics({ spendMinor, impressions, clicks });

    // Last sync metadata
    let lastSync = null;
    if (account.last_sync_at) {
      const syncAt = new Date(account.last_sync_at);
      const ageMinutes = Math.round((Date.now() - syncAt.getTime()) / 60000);
      lastSync = {
        at:         account.last_sync_at,
        status:     account.last_sync_status ?? 'ok',
        ageMinutes,
      };
    }

    return res.json({
      account: {
        id:       account.meta_account_id,
        name:     account.account_name,
        currency: account.currency,
        timezone: account.timezone_name,
      },
      period: { from, to },
      totals: {
        spend:       metrics.spend,
        spendCents:  spendMinor,
        impressions: metrics.impressions,
        clicks:      metrics.clicks,
        reach,
        ctr:         metrics.ctr,
        cpm:         metrics.cpm,
        cpc:         metrics.cpc,
      },
      lastSync,
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message }, 'GET /meta-ads/summary failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/summary failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// GET /meta-ads/campaigns
// ---------------------------------------------------------------------------

metaAdsRouter.get('/campaigns', async (req, res) => {
  const parsed = campaignsQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query params', details: parsed.error.flatten().fieldErrors });
  }

  const { from: rawFrom, to: rawTo, account: accountParam, status, sort, limit, offset } = parsed.data;
  const defaults = defaultDateRange();
  const from = rawFrom ?? defaults.from;
  const to   = rawTo   ?? defaults.to;

  try {
    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Build campaigns query
    let campaignQuery = serviceClient
      .from('meta_campaigns')
      .select('id, meta_campaign_id, name, status, objective, daily_budget, lifetime_budget, created_time', { count: 'exact' })
      .eq('ad_account_id', account.id);

    // Status filter — comma-separated list e.g. "ACTIVE,PAUSED"
    if (status) {
      const statuses = status.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean);
      if (statuses.length > 0) {
        campaignQuery = campaignQuery.in('status', statuses);
      }
    }

    // Sorting — applied AFTER we join with insights; for spend/ctr sorts we
    // will sort in JS. For name/created we can sort in DB.
    const dbSort = sort === 'name'
      ? { col: 'name', asc: true }
      : sort === 'created_desc'
        ? { col: 'created_time', asc: false }
        : null; // spend/ctr sorts happen in JS after metrics join

    if (dbSort) {
      campaignQuery = campaignQuery.order(dbSort.col, { ascending: dbSort.asc });
    }

    // Fetch all campaigns for metric join (we need all to sort by spend)
    // Then apply offset/limit for pagination.
    // This is acceptable for 93 campaigns; if it grows to 10k+ revisit.
    const { data: campaigns, error: campaignError, count } = await campaignQuery;
    if (campaignError) throw campaignError;

    if (!campaigns || campaigns.length === 0) {
      return res.json({ items: [], total: count ?? 0, hasMore: false });
    }

    // Fetch insights for all campaigns in one query
    const metaCampaignIds = campaigns.map((c) => c.meta_campaign_id);
    const insightsMap = await fetchInsightsMap({
      accountId: account.id,
      level: 'campaign',
      objectIds: metaCampaignIds,
      from,
      to,
    });

    // Join metrics
    let items = campaigns.map((c) => {
      const raw = insightsMap.get(c.meta_campaign_id) || { spendMinor: 0, impressions: 0, clicks: 0, reach: 0 };
      return formatCampaign(c, deriveMetrics(raw));
    });

    // JS-side sort for spend/ctr
    if (sort === 'spend_desc') {
      items.sort((a, b) => b.metrics.spend - a.metrics.spend);
    } else if (sort === 'spend_asc') {
      items.sort((a, b) => a.metrics.spend - b.metrics.spend);
    } else if (sort === 'ctr_desc') {
      items.sort((a, b) => b.metrics.ctr - a.metrics.ctr);
    }

    const total = count ?? items.length;
    const paginated = items.slice(offset, offset + limit);

    return res.json({
      items:   paginated,
      total,
      hasMore: offset + paginated.length < total,
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message }, 'GET /meta-ads/campaigns failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/campaigns failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// GET /meta-ads/campaigns/:id
// ---------------------------------------------------------------------------

metaAdsRouter.get('/campaigns/:id', async (req, res) => {
  const parsed = periodSchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query params', details: parsed.error.flatten().fieldErrors });
  }

  const { from: rawFrom, to: rawTo, account: accountParam } = parsed.data;
  const defaults = defaultDateRange();
  const from = rawFrom ?? defaults.from;
  const to   = rawTo   ?? defaults.to;
  const { id } = req.params;

  try {
    // Validate id looks like a uuid (loose check — DB will reject anyway)
    if (!/^[0-9a-f-]{36}$/i.test(id)) {
      return res.status(400).json({ error: 'Invalid campaign id format' });
    }

    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Fetch campaign
    const { data: campaign, error: campaignError } = await serviceClient
      .from('meta_campaigns')
      .select('id, meta_campaign_id, name, status, objective, daily_budget, lifetime_budget, created_time')
      .eq('id', id)
      .eq('ad_account_id', account.id)
      .maybeSingle();

    if (campaignError) throw campaignError;
    if (!campaign) return res.status(404).json({ error: 'Campaign not found' });

    // Campaign-level metrics
    const campaignInsightsMap = await fetchInsightsMap({
      accountId: account.id,
      level: 'campaign',
      objectIds: [campaign.meta_campaign_id],
      from,
      to,
    });
    const campaignRaw = campaignInsightsMap.get(campaign.meta_campaign_id) || { spendMinor: 0, impressions: 0, clicks: 0, reach: 0 };

    // Fetch ad sets for this campaign
    const { data: adSets, error: adSetsError } = await serviceClient
      .from('meta_ad_sets')
      .select('id, meta_adset_id, name, status, daily_budget, lifetime_budget, optimization_goal, billing_event, is_advantage_plus, targeting, placements')
      .eq('campaign_id', id)
      .eq('ad_account_id', account.id)
      .order('name', { ascending: true });

    if (adSetsError) throw adSetsError;

    // Fetch adset-level insights in one call
    const metaAdSetIds = (adSets || []).map((s) => s.meta_adset_id);
    const adSetInsightsMap = await fetchInsightsMap({
      accountId: account.id,
      level: 'adset',
      objectIds: metaAdSetIds,
      from,
      to,
    });

    const formattedAdSets = (adSets || []).map((s) => {
      const raw = adSetInsightsMap.get(s.meta_adset_id) || { spendMinor: 0, impressions: 0, clicks: 0, reach: 0 };
      return formatAdSet(s, deriveMetrics(raw));
    });

    return res.json({
      campaign: formatCampaign(campaign, deriveMetrics(campaignRaw)),
      adSets:   formattedAdSets,
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message, campaignId: id }, 'GET /meta-ads/campaigns/:id failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/campaigns/:id failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// GET /meta-ads/ad-sets/:id
// ---------------------------------------------------------------------------

metaAdsRouter.get('/ad-sets/:id', async (req, res) => {
  const parsed = periodSchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query params', details: parsed.error.flatten().fieldErrors });
  }

  const { from: rawFrom, to: rawTo, account: accountParam } = parsed.data;
  const defaults = defaultDateRange();
  const from = rawFrom ?? defaults.from;
  const to   = rawTo   ?? defaults.to;
  const { id } = req.params;

  try {
    if (!/^[0-9a-f-]{36}$/i.test(id)) {
      return res.status(400).json({ error: 'Invalid ad set id format' });
    }

    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Fetch ad set
    const { data: adSet, error: adSetError } = await serviceClient
      .from('meta_ad_sets')
      .select('id, meta_adset_id, name, status, daily_budget, lifetime_budget, optimization_goal, billing_event, is_advantage_plus, targeting, placements')
      .eq('id', id)
      .eq('ad_account_id', account.id)
      .maybeSingle();

    if (adSetError) throw adSetError;
    if (!adSet) return res.status(404).json({ error: 'Ad set not found' });

    // Ad set level metrics
    const adSetInsightsMap = await fetchInsightsMap({
      accountId: account.id,
      level: 'adset',
      objectIds: [adSet.meta_adset_id],
      from,
      to,
    });
    const adSetRaw = adSetInsightsMap.get(adSet.meta_adset_id) || { spendMinor: 0, impressions: 0, clicks: 0, reach: 0 };

    // Fetch ads in this ad set, with their creative
    const { data: ads, error: adsError } = await serviceClient
      .from('meta_ads')
      .select(`
        id,
        meta_ad_id,
        name,
        status,
        creative_id,
        meta_creatives (
          id,
          meta_creative_id,
          title,
          body,
          cta_type,
          image_url,
          cached_image_url,
          thumbnail_url,
          video_id
        )
      `)
      .eq('ad_set_id', id)
      .eq('ad_account_id', account.id)
      .order('name', { ascending: true });

    if (adsError) throw adsError;

    // Fetch ad-level insights in one call
    const metaAdIds = (ads || []).map((a) => a.meta_ad_id);
    const adInsightsMap = await fetchInsightsMap({
      accountId: account.id,
      level: 'ad',
      objectIds: metaAdIds,
      from,
      to,
    });

    const formattedAds = (ads || []).map((a) => {
      const raw = adInsightsMap.get(a.meta_ad_id) || { spendMinor: 0, impressions: 0, clicks: 0, reach: 0 };
      const m = deriveMetrics(raw);
      const cr = a.meta_creatives;

      return {
        id:         a.id,
        metaAdId:   a.meta_ad_id,
        name:       a.name,
        status:     a.status,
        creative:   cr
          ? {
              id:              cr.id,
              metaCreativeId:  cr.meta_creative_id,
              title:           cr.title ?? null,
              body:            cr.body  ?? null,
              ctaType:         cr.cta_type ?? null,
              imageUrl:        cr.image_url ?? null,
              cachedImageUrl:  cr.cached_image_url ?? null,
              thumbnailUrl:    cr.thumbnail_url ?? null,
              videoId:         cr.video_id ?? null,
            }
          : null,
        metrics: {
          spend:       m.spend,
          impressions: m.impressions,
          clicks:      m.clicks,
          ctr:         m.ctr,
          cpm:         m.cpm,
          cpc:         m.cpc,
        },
      };
    });

    return res.json({
      adSet: formatAdSet(adSet, deriveMetrics(adSetRaw)),
      ads:   formattedAds,
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message, adSetId: id }, 'GET /meta-ads/ad-sets/:id failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/ad-sets/:id failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// GET /meta-ads/sync-status
// ---------------------------------------------------------------------------

metaAdsRouter.get('/sync-status', async (req, res) => {
  const parsed = z.object({ account: z.string().optional() }).safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query params' });
  }

  const { account: accountParam } = parsed.data;

  try {
    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Count total records in meta tables for this account
    const [campaignsCount, adSetsCount, adsCount, insightsCount, creativesCount] = await Promise.all([
      serviceClient.from('meta_campaigns').select('id', { count: 'exact', head: true }).eq('ad_account_id', account.id),
      serviceClient.from('meta_ad_sets').select('id', { count: 'exact', head: true }).eq('ad_account_id', account.id),
      serviceClient.from('meta_ads').select('id', { count: 'exact', head: true }).eq('ad_account_id', account.id),
      serviceClient.from('meta_insights_daily').select('id', { count: 'exact', head: true }).eq('ad_account_id', account.id),
      serviceClient.from('meta_creatives').select('id', { count: 'exact', head: true }).eq('ad_account_id', account.id),
    ]);

    const recordsTotal =
      (campaignsCount.count || 0) +
      (adSetsCount.count    || 0) +
      (adsCount.count       || 0) +
      (insightsCount.count  || 0) +
      (creativesCount.count || 0);

    // Recent sync log
    const { data: recentLog, error: logError } = await serviceClient
      .from('meta_sync_log')
      .select('sync_type, status, started_at, completed_at, records_synced')
      .eq('ad_account_id', account.id)
      .order('started_at', { ascending: false })
      .limit(SYNC_LOG_RECENT);

    if (logError) throw logError;

    // Last sync timing
    let lastSync = null;
    if (account.last_sync_at) {
      const syncAt = new Date(account.last_sync_at);
      const ageMinutes = Math.round((Date.now() - syncAt.getTime()) / 60000);
      lastSync = {
        at:           account.last_sync_at,
        status:       account.last_sync_status ?? 'ok',
        ageMinutes,
        recordsTotal,
      };
    }

    return res.json({
      account: account.meta_account_id,
      lastSync,
      recentLog: (recentLog || []).map((r) => ({
        syncType:      r.sync_type,
        status:        r.status,
        startedAt:     r.started_at,
        completedAt:   r.completed_at ?? null,
        recordsSynced: r.records_synced,
      })),
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message }, 'GET /meta-ads/sync-status failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/sync-status failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// GET /meta-ads/tree
// ---------------------------------------------------------------------------
// Главный endpoint для audit-view дашборда.
// Возвращает полное дерево: Кампании → AdSets → Ads → Creatives + метрики.
//
// Реализован через 4 SQL запроса + JS-сборка дерева (не N+1):
//   1. Campaigns для периода (через EXISTS на insights)
//   2. AdSets для этих кампаний
//   3. Ads для этих AdSets с creative join
//   4. Insights (campaign+adset+ad levels) за период
//
// Performance:
//   - Limit 50 кампаний — пагинация через ?offset=N
//   - Targeting/placements парсятся через parseTargeting()/parsePlacements()
//   - landing_url/whatsapp_phone берётся из meta_creatives если уже lazy-fetched
//
// Query params:
//   from          YYYY-MM-DD (default: today-7 days)
//   to            YYYY-MM-DD (default: today)
//   campaign_id   UUID — показать только эту кампанию (опционально)
//   account       act_XXX или UUID ad account
//   limit         кол-во кампаний (default: 50, max: 200)
//   offset        пагинация
// ---------------------------------------------------------------------------

metaAdsRouter.get('/tree', async (req, res) => {
  const treeQuerySchema = periodSchema.extend({
    campaign_id: z.string().uuid().optional(),
    limit: z.coerce.number().int().min(1).max(200).optional().default(50),
    offset: z.coerce.number().int().min(0).optional().default(0),
  });

  const parsed = treeQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: 'Invalid query params', details: parsed.error.flatten().fieldErrors });
  }

  const { from: rawFrom, to: rawTo, account: accountParam, campaign_id: singleCampaignId, limit, offset } = parsed.data;
  const defaults = defaultDateRange();
  const from = rawFrom ?? defaults.from;
  const to   = rawTo   ?? defaults.to;

  try {
    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // --- Query 1: Campaigns ---
    let campaignsQuery = serviceClient
      .from('meta_campaigns')
      .select('id, meta_campaign_id, name, status, effective_status, objective, daily_budget, lifetime_budget, created_time', { count: 'exact' })
      .eq('ad_account_id', account.id);

    if (singleCampaignId) {
      campaignsQuery = campaignsQuery.eq('id', singleCampaignId);
    }

    const { data: campaigns, error: campaignError, count: totalCampaigns } = await campaignsQuery
      .order('name', { ascending: true })
      .range(offset, offset + limit - 1);

    if (campaignError) throw campaignError;
    if (!campaigns || campaigns.length === 0) {
      return res.json({
        period: { from, to },
        total: 0,
        hasMore: false,
        campaigns: [],
      });
    }

    const campaignIds = campaigns.map((c) => c.id);
    const metaCampaignIds = campaigns.map((c) => c.meta_campaign_id);

    // --- Query 2: AdSets for these campaigns ---
    const { data: adSets, error: adSetsError } = await serviceClient
      .from('meta_ad_sets')
      .select('id, meta_adset_id, name, status, campaign_id, daily_budget, lifetime_budget, optimization_goal, billing_event, bid_strategy, is_advantage_plus, schedule_start, schedule_end, targeting, placements')
      .eq('ad_account_id', account.id)
      .in('campaign_id', campaignIds)
      .order('name', { ascending: true });

    if (adSetsError) throw adSetsError;

    const adSetIds = (adSets || []).map((a) => a.id);
    const metaAdSetIds = (adSets || []).map((a) => a.meta_adset_id);

    // --- Query 3: Ads + creatives for these adsets ---
    const { data: ads, error: adsError } = await serviceClient
      .from('meta_ads')
      .select(`
        id,
        meta_ad_id,
        name,
        status,
        ad_set_id,
        campaign_id,
        meta_creatives (
          id,
          meta_creative_id,
          title,
          body,
          cta_type,
          image_url,
          thumbnail_url,
          video_id,
          landing_url,
          whatsapp_phone,
          whatsapp_message_template
        )
      `)
      .eq('ad_account_id', account.id)
      .in('ad_set_id', adSetIds.length > 0 ? adSetIds : ['00000000-0000-0000-0000-000000000000'])
      .order('name', { ascending: true });

    if (adsError) throw adsError;

    const metaAdIds = (ads || []).map((a) => a.meta_ad_id);

    // --- Query 4: Insights for all three levels ---
    const allInsights = await Promise.all([
      fetchInsightsMap({ accountId: account.id, level: 'campaign', objectIds: metaCampaignIds, from, to }),
      fetchInsightsMap({ accountId: account.id, level: 'adset',    objectIds: metaAdSetIds,    from, to }),
      fetchInsightsMap({ accountId: account.id, level: 'ad',       objectIds: metaAdIds,       from, to }),
    ]);
    const [campaignInsights, adSetInsights, adInsights] = allInsights;

    // --- Helpers for active period ---
    async function getActivePeriod(metaId, level) {
      const { data } = await serviceClient
        .from('meta_insights_daily')
        .select('date_start')
        .eq('ad_account_id', account.id)
        .eq('level', level)
        .eq('object_id', metaId)
        .gt('spend', 0)
        .order('date_start', { ascending: true })
        .limit(1);
      const { data: last } = await serviceClient
        .from('meta_insights_daily')
        .select('date_start')
        .eq('ad_account_id', account.id)
        .eq('level', level)
        .eq('object_id', metaId)
        .gt('spend', 0)
        .order('date_start', { ascending: false })
        .limit(1);
      return {
        from: data?.[0]?.date_start ?? null,
        to: last?.[0]?.date_start ?? null,
      };
    }

    // --- Build tree ---
    // Group adsets by campaign_id
    const adSetsByCampaign = new Map();
    for (const as of adSets || []) {
      if (!adSetsByCampaign.has(as.campaign_id)) adSetsByCampaign.set(as.campaign_id, []);
      adSetsByCampaign.get(as.campaign_id).push(as);
    }

    // Group ads by ad_set_id
    const adsByAdSet = new Map();
    for (const ad of ads || []) {
      if (!adsByAdSet.has(ad.ad_set_id)) adsByAdSet.set(ad.ad_set_id, []);
      adsByAdSet.get(ad.ad_set_id).push(ad);
    }

    // Format metrics helper (extended with reach+frequency+linkClicks)
    function formatMetricsExtended(raw) {
      if (!raw) raw = { spendMinor: 0, impressions: 0, clicks: 0, reach: 0, linkClicks: 0 };
      const base = deriveMetrics(raw);
      return {
        ...base,
        spendCents: raw.spendMinor ?? 0,
        reach:      raw.reach ?? 0,
        linkClicks: raw.linkClicks ?? 0,
      };
    }

    const tree = campaigns.map((camp) => {
      const campInsightRaw = campaignInsights.get(camp.meta_campaign_id) ?? null;
      const campMetrics = formatMetricsExtended(campInsightRaw);

      const campAdSets = adSetsByCampaign.get(camp.id) ?? [];

      const formattedAdSets = campAdSets.map((as) => {
        const asInsightRaw = adSetInsights.get(as.meta_adset_id) ?? null;
        const asMetrics = formatMetricsExtended(asInsightRaw);

        const asAds = adsByAdSet.get(as.id) ?? [];

        const formattedAds = asAds.map((ad) => {
          const adInsightRaw = adInsights.get(ad.meta_ad_id) ?? null;
          const adMetrics = formatMetricsExtended(adInsightRaw);
          const cr = ad.meta_creatives;

          return {
            id:       ad.id,
            metaAdId: ad.meta_ad_id,
            name:     ad.name,
            status:   ad.status,
            creative: cr ? {
              id:                     cr.id,
              metaCreativeId:         cr.meta_creative_id,
              title:                  cr.title ?? null,
              body:                   cr.body ?? null,
              ctaType:                cr.cta_type ?? null,
              imageUrl:               cr.image_url ?? null,
              thumbnailUrl:           cr.thumbnail_url ?? null,
              videoId:                cr.video_id ?? null,
              landingUrl:             cr.landing_url ?? null,
              whatsappPhone:          cr.whatsapp_phone ?? null,
              whatsappMessageTemplate: cr.whatsapp_message_template ?? null,
            } : null,
            metrics: adMetrics,
          };
        });

        return {
          id:               as.id,
          metaAdSetId:      as.meta_adset_id,
          name:             as.name,
          status:           as.status,
          dailyBudget:      toMajor(as.daily_budget),
          lifetimeBudget:   toMajor(as.lifetime_budget),
          optimizationGoal: as.optimization_goal ?? null,
          billingEvent:     as.billing_event ?? null,
          bidStrategy:      as.bid_strategy ?? null,
          isAdvantagePlus:  as.is_advantage_plus ?? false,
          schedule: {
            start: as.schedule_start ?? null,
            end:   as.schedule_end ?? null,
          },
          // Parsed human-readable targeting / placements
          targeting:  parseTargeting(as.targeting),
          placements: parsePlacements(as.placements),
          metrics:    asMetrics,
          ads:        formattedAds,
        };
      });

      return {
        id:             camp.id,
        metaCampaignId: camp.meta_campaign_id,
        name:           camp.name,
        status:         camp.status,
        effectiveStatus: camp.effective_status ?? camp.status,
        objective:      camp.objective ?? null,
        dailyBudget:    toMajor(camp.daily_budget),
        lifetimeBudget: toMajor(camp.lifetime_budget),
        createdTime:    camp.created_time ?? null,
        metrics:        campMetrics,
        adSets:         formattedAdSets,
      };
    });

    return res.json({
      period: { from, to },
      account: {
        id:       account.meta_account_id,
        name:     account.account_name,
        currency: account.currency,
        timezone: account.timezone_name,
      },
      total:   totalCampaigns ?? campaigns.length,
      hasMore: offset + campaigns.length < (totalCampaigns ?? campaigns.length),
      campaigns: tree,
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message }, 'GET /meta-ads/tree failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/tree failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// POST /meta-ads/campaigns/:id/refresh
// ---------------------------------------------------------------------------
// Триггерит syncSingleCampaign для конкретной кампании.
// Используется кнопкой "Refresh now" в UI.
// :id — это наш UUID (из meta_campaigns.id), не Meta campaign ID.
// ---------------------------------------------------------------------------

metaAdsRouter.post('/campaigns/:id/refresh', async (req, res) => {
  const { id } = req.params;

  if (!/^[0-9a-f-]{36}$/i.test(id)) {
    return res.status(400).json({ error: 'Invalid campaign id format' });
  }

  try {
    const parsed = z.object({ account: z.string().optional() }).safeParse(req.query);
    const accountParam = parsed.success ? parsed.data.account : undefined;

    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Resolve Meta campaign ID от нашего UUID
    const { data: campaign, error: campError } = await serviceClient
      .from('meta_campaigns')
      .select('meta_campaign_id, name')
      .eq('id', id)
      .eq('ad_account_id', account.id)
      .maybeSingle();

    if (campError) throw campError;
    if (!campaign) return res.status(404).json({ error: 'Campaign not found' });

    const log = req.log ?? logger;
    log.info(
      { campaignId: id, metaCampaignId: campaign.meta_campaign_id },
      'POST /meta-ads/campaigns/:id/refresh triggered'
    );

    const summary = await syncSingleCampaign(campaign.meta_campaign_id, account.meta_account_id);

    return res.json({
      started:     summary.started,
      completed:   summary.completed,
      durationMs:  summary.durationMs,
      recordsSynced: summary.recordsSynced,
      errors:      summary.errors ?? [],
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message, campaignId: id }, 'POST /meta-ads/campaigns/:id/refresh failed')
      : logger.error({ err: err.message }, 'POST /meta-ads/campaigns/:id/refresh failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// ---------------------------------------------------------------------------
// GET /meta-ads/creatives/:id/details
// ---------------------------------------------------------------------------
// Lazy fetch деталей одного креатива: landing_url, whatsapp_phone, whatsapp_message_template.
// Если в БД уже есть landing_url — возвращает из кэша без API вызова.
// Если нет — дёргает Meta getCreative() + parseObjectStorySpec() + UPDATE.
// :id — наш UUID (из meta_creatives.id)
// ---------------------------------------------------------------------------

metaAdsRouter.get('/creatives/:id/details', async (req, res) => {
  const { id } = req.params;

  if (!/^[0-9a-f-]{36}$/i.test(id)) {
    return res.status(400).json({ error: 'Invalid creative id format' });
  }

  try {
    const parsed = z.object({ account: z.string().optional() }).safeParse(req.query);
    const accountParam = parsed.success ? parsed.data.account : undefined;

    const account = await resolveAccount(accountParam);
    if (!account) {
      return res.status(404).json({ error: 'Ad account not found' });
    }

    // Fetch creative from DB
    const { data: creative, error: creativeError } = await serviceClient
      .from('meta_creatives')
      .select('id, meta_creative_id, title, body, cta_type, image_url, thumbnail_url, video_id, landing_url, whatsapp_phone, whatsapp_message_template')
      .eq('id', id)
      .eq('ad_account_id', account.id)
      .maybeSingle();

    if (creativeError) throw creativeError;
    if (!creative) return res.status(404).json({ error: 'Creative not found' });

    let wasFetched = false;
    let cr = creative;

    // Lazy fetch если landing_url ещё не заполнен
    if (cr.landing_url === null) {
      const log = req.log ?? logger;
      log.info(
        { creativeId: id, metaCreativeId: cr.meta_creative_id },
        'GET /meta-ads/creatives/:id/details — lazy fetching from Meta'
      );

      try {
        const { fields } = await syncCreativeDetails(cr.meta_creative_id, account.id);

        // Перечитать обновлённые данные из БД
        const { data: updated } = await serviceClient
          .from('meta_creatives')
          .select('id, meta_creative_id, title, body, cta_type, image_url, thumbnail_url, video_id, landing_url, whatsapp_phone, whatsapp_message_template')
          .eq('id', id)
          .maybeSingle();

        if (updated) cr = updated;
        wasFetched = true;
      } catch (fetchErr) {
        // Lazy fetch не критичен — логируем и возвращаем что есть
        const log = req.log ?? logger;
        log.warn(
          { err: fetchErr.message, creativeId: id },
          'GET /meta-ads/creatives/:id/details — lazy fetch failed, returning cached'
        );
      }
    }

    return res.json({
      id:                       cr.id,
      metaCreativeId:           cr.meta_creative_id,
      title:                    cr.title ?? null,
      body:                     cr.body ?? null,
      ctaType:                  cr.cta_type ?? null,
      imageUrl:                 cr.image_url ?? null,
      thumbnailUrl:             cr.thumbnail_url ?? null,
      videoId:                  cr.video_id ?? null,
      landingUrl:               cr.landing_url ?? null,
      whatsappPhone:            cr.whatsapp_phone ?? null,
      whatsappMessageTemplate:  cr.whatsapp_message_template ?? null,
      wasFetched,
    });
  } catch (err) {
    req.log
      ? req.log.error({ err: err.message, creativeId: id }, 'GET /meta-ads/creatives/:id/details failed')
      : logger.error({ err: err.message }, 'GET /meta-ads/creatives/:id/details failed');
    return res.status(500).json({ error: 'Internal server error' });
  }
});
