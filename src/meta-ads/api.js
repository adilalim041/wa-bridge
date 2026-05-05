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
