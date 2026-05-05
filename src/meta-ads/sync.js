/**
 * Meta Marketing API — sync logic.
 *
 * Записывает реальные данные из Meta API в Supabase (таблицы meta_*).
 * Все функции идемпотентны — UPSERT, не INSERT. Безопасно запускать повторно.
 *
 * Ключевые решения (pre-flight 2026-05-04):
 *  - Lock через meta_sync_locks (heartbeat 10s, stale >30s — перезахватываем).
 *  - Audit trail в meta_sync_log (started → ok|partial|error).
 *  - spend/cpm/cpc из /insights — MAJOR units (доллары). Умножаем ×100 для MINOR (центы).
 *  - daily_budget/lifetime_budget из кампаний — уже MINOR units (без умножения).
 *  - CTR и frequency — обычные float-ы, не валюта.
 *  - При rate-limit errors (17/4/80004) — partial sync, продолжаем, не прерываем.
 *  - При token expired (190) — прерываем весь sync.
 *  - Cloudinary caching — отдельная итерация (cached_image_url остаётся NULL).
 *
 * Источник истины:
 *  ObsidianVault/research/library/backend-libs/meta-marketing-api.md
 *  wa-bridge/sql/migrations/0022_meta_ads.sql
 */

import os from 'os';
import pino from 'pino';
import { metaAdsClient, MetaApiError } from './client.js';
import { metaAdsConfig } from './config.js';
import { serviceClient } from '../storage/supabase.js';

const logger = pino({ level: process.env.LOG_LEVEL || 'info' }).child({
  module: 'meta-ads:sync',
});

// ---------------------------------------------------------------------------
// Константы
// ---------------------------------------------------------------------------

/** После скольких секунд без heartbeat считаем предыдущий процесс мёртвым. */
const LOCK_STALE_SECONDS = 30;

/** Интервал обновления heartbeat в мс. */
const HEARTBEAT_INTERVAL_MS = 10_000;

/**
 * Идентификатор текущего процесса для meta_sync_locks.locked_by.
 * Railway сервис имеет RAILWAY_SERVICE_NAME в env.
 */
const LOCKED_BY = process.env.RAILWAY_SERVICE_NAME
  ? `${process.env.RAILWAY_SERVICE_NAME}@${os.hostname()}`
  : `pid:${process.pid}@${os.hostname()}`;

/** Meta error codes — rate limit. Не прерываем sync, записываем partial. */
const RATE_LIMIT_CODES = new Set([17, 4, 80004]);

/** Meta error code — невалидный токен. Прерываем весь sync. */
const TOKEN_EXPIRED_CODE = 190;

// ---------------------------------------------------------------------------
// Вспомогательные типы
// ---------------------------------------------------------------------------

/**
 * @typedef {Object} SyncSummary
 * @property {string} syncType
 * @property {'ok'|'partial'|'error'} status
 * @property {number} recordsSynced
 * @property {Array<{code: string|number, message: string, objectId?: string}>} errors
 * @property {number} durationMs
 */

// ---------------------------------------------------------------------------
// Lock helpers
// ---------------------------------------------------------------------------

/**
 * Разрешить внутренний UUID ad_account'а по meta_account_id строке ("act_XXX").
 * Нужно для FK в sync_locks, sync_log и остальных таблицах.
 *
 * @param {string} adAccountId — "act_XXX"
 * @returns {Promise<string>} — UUID
 */
async function resolveAccountUuid(adAccountId) {
  const { data, error } = await serviceClient
    .from('meta_ad_accounts')
    .select('id')
    .eq('meta_account_id', adAccountId)
    .single();

  if (error || !data) {
    throw new Error(
      `meta_ad_accounts row not found for "${adAccountId}". ` +
        'Run syncAdAccount() first or ensure migration 0022 is applied.'
    );
  }
  return data.id;
}

/**
 * Захватить distributed lock. Если другой процесс держит lock и heartbeat свежий
 * (< LOCK_STALE_SECONDS назад) — возвращает false (bail).
 * Если heartbeat устарел — перезахватываем.
 *
 * @param {string} accountUuid — UUID из meta_ad_accounts.id
 * @returns {Promise<boolean>} — true если lock захвачен, false если занят живым процессом
 */
async function acquireLock(accountUuid) {
  const now = new Date();
  const staleThreshold = new Date(now.getTime() - LOCK_STALE_SECONDS * 1000).toISOString();

  // Пробуем вставить. ON CONFLICT — проверяем heartbeat.
  const { data: existing } = await serviceClient
    .from('meta_sync_locks')
    .select('locked_by, heartbeat_at')
    .eq('ad_account_id', accountUuid)
    .single();

  if (existing) {
    const heartbeatAge = existing.heartbeat_at;
    if (heartbeatAge > staleThreshold) {
      // Живой процесс держит lock
      logger.warn(
        { lockedBy: existing.locked_by, heartbeatAt: existing.heartbeat_at },
        'meta-ads:sync: another sync is running — bailing out'
      );
      return false;
    }
    // Предыдущий процесс умер — перезахватываем
    logger.info(
      { previousLockedBy: existing.locked_by, staleHeartbeat: existing.heartbeat_at },
      'meta-ads:sync: stale lock detected — reclaiming'
    );
    const { error: updateError } = await serviceClient
      .from('meta_sync_locks')
      .update({ locked_by: LOCKED_BY, locked_at: now.toISOString(), heartbeat_at: now.toISOString() })
      .eq('ad_account_id', accountUuid);
    if (updateError) throw new Error(`Failed to reclaim lock: ${updateError.message}`);
    return true;
  }

  // Нет строки — вставляем
  const { error: insertError } = await serviceClient.from('meta_sync_locks').insert({
    ad_account_id: accountUuid,
    locked_by: LOCKED_BY,
    locked_at: now.toISOString(),
    heartbeat_at: now.toISOString(),
  });

  if (insertError) {
    // Race: другой процесс вставил между нашим SELECT и INSERT
    logger.warn({ error: insertError.message }, 'meta-ads:sync: lock insert race — bailing out');
    return false;
  }

  return true;
}

/**
 * Запустить heartbeat interval. Обновляет heartbeat_at каждые HEARTBEAT_INTERVAL_MS.
 * Возвращает функцию-отменитель.
 *
 * @param {string} accountUuid
 * @returns {() => void} stop function
 */
function startHeartbeat(accountUuid) {
  const timer = setInterval(async () => {
    try {
      await serviceClient
        .from('meta_sync_locks')
        .update({ heartbeat_at: new Date().toISOString() })
        .eq('ad_account_id', accountUuid)
        .eq('locked_by', LOCKED_BY);
    } catch (err) {
      logger.warn({ err: err.message }, 'meta-ads:sync: heartbeat update failed');
    }
  }, HEARTBEAT_INTERVAL_MS);

  // unref — не держать процесс живым если это единственный timer
  if (timer.unref) timer.unref();

  return () => clearInterval(timer);
}

/**
 * Освободить lock.
 *
 * @param {string} accountUuid
 */
async function releaseLock(accountUuid) {
  try {
    await serviceClient
      .from('meta_sync_locks')
      .delete()
      .eq('ad_account_id', accountUuid)
      .eq('locked_by', LOCKED_BY);
  } catch (err) {
    logger.warn({ err: err.message }, 'meta-ads:sync: failed to release lock (non-fatal)');
  }
}

// ---------------------------------------------------------------------------
// Audit log helpers
// ---------------------------------------------------------------------------

/**
 * Вставить строку started в meta_sync_log.
 *
 * @param {string} accountUuid
 * @param {string} syncType
 * @returns {Promise<number>} — id строки
 */
async function logStarted(accountUuid, syncType) {
  const { data, error } = await serviceClient
    .from('meta_sync_log')
    .insert({
      ad_account_id: accountUuid,
      sync_type: syncType,
      status: 'started',
      started_at: new Date().toISOString(),
      records_synced: 0,
    })
    .select('id')
    .single();

  if (error) {
    logger.warn({ error: error.message, syncType }, 'meta-ads:sync: failed to insert sync_log row');
    return null;
  }
  return data.id;
}

/**
 * Обновить строку sync_log при успехе.
 *
 * @param {number|null} logId
 * @param {number} recordsSynced
 * @param {'ok'|'partial'} status
 */
async function logSuccess(logId, recordsSynced, status = 'ok') {
  if (!logId) return;
  await serviceClient
    .from('meta_sync_log')
    .update({
      status,
      completed_at: new Date().toISOString(),
      records_synced: recordsSynced,
    })
    .eq('id', logId);
}

/**
 * Обновить строку sync_log при ошибке.
 *
 * @param {number|null} logId
 * @param {Error|MetaApiError} err
 */
async function logError(logId, err) {
  if (!logId) return;
  const isMetaError = err instanceof MetaApiError;
  await serviceClient
    .from('meta_sync_log')
    .update({
      status: 'error',
      completed_at: new Date().toISOString(),
      error_code: isMetaError ? String(err.code) : 'INTERNAL',
      error_message: err.message,
      error_details: {
        stack: err.stack ?? null,
        ...(isMetaError ? err.toJSON() : {}),
      },
    })
    .eq('id', logId);
}

// ---------------------------------------------------------------------------
// Обёртка для выполнения sync-функций с lock + audit
// ---------------------------------------------------------------------------

/**
 * Обёртка: захватить lock, вставить log, выполнить fn, освободить.
 * Используется внутри syncFull/syncDelta. Отдельные функции (syncCampaigns и т.д.)
 * могут вызываться напрямую без lock если вызывающий (syncFull) уже держит lock.
 *
 * @param {string} accountUuid
 * @param {string} syncType
 * @param {() => Promise<{recordsSynced: number, errors: Array}>} fn
 * @returns {Promise<SyncSummary>}
 */
async function withLockAndLog(accountUuid, syncType, fn) {
  const startTime = Date.now();

  const locked = await acquireLock(accountUuid);
  if (!locked) {
    return {
      syncType,
      status: 'error',
      recordsSynced: 0,
      errors: [{ code: 'LOCK_BUSY', message: 'Another sync is running for this ad account' }],
      durationMs: Date.now() - startTime,
    };
  }

  const stopHeartbeat = startHeartbeat(accountUuid);
  const logId = await logStarted(accountUuid, syncType);

  try {
    const result = await fn();
    const status = result.errors.length > 0 ? 'partial' : 'ok';
    await logSuccess(logId, result.recordsSynced, status);

    // Обновить last_sync_at на meta_ad_accounts
    await serviceClient
      .from('meta_ad_accounts')
      .update({ last_sync_at: new Date().toISOString(), last_sync_status: status })
      .eq('id', accountUuid);

    return {
      syncType,
      status,
      recordsSynced: result.recordsSynced,
      errors: result.errors,
      durationMs: Date.now() - startTime,
    };
  } catch (err) {
    await logError(logId, err);
    return {
      syncType,
      status: 'error',
      recordsSynced: 0,
      errors: [{ code: err.code ?? 'INTERNAL', message: err.message, stack: err.stack }],
      durationMs: Date.now() - startTime,
    };
  } finally {
    stopHeartbeat();
    await releaseLock(accountUuid);
  }
}

// ---------------------------------------------------------------------------
// Утилиты для нормализации Meta-данных
// ---------------------------------------------------------------------------

/**
 * Нормализовать значение из insights (spend/cpm/cpc) в MINOR units.
 * Meta insights возвращает MAJOR units (доллары "12.34").
 * Мы храним MINOR units (центы → "1234").
 *
 * MAJOR units → ×100 для MINOR
 *
 * @param {string|number|null|undefined} value
 * @returns {number} — bigint-safe integer в MINOR units (центах)
 */
function toMinorUnits(value) {
  if (value == null || value === '') return 0;
  const num = parseFloat(value);
  if (isNaN(num)) return 0;
  // Умножаем на 100 и округляем (избегаем floating point drift: 12.34 * 100 = 1233.9999...)
  return Math.round(num * 100);
}

/**
 * Нормализовать float-поле (CTR, frequency) — не валюта, не умножаем.
 *
 * @param {string|number|null|undefined} value
 * @returns {number|null}
 */
function toFloat(value) {
  if (value == null || value === '') return null;
  const num = parseFloat(value);
  return isNaN(num) ? null : num;
}

/**
 * Нормализовать integer поле (impressions, clicks, reach).
 *
 * @param {string|number|null|undefined} value
 * @returns {number}
 */
function toInt(value) {
  if (value == null || value === '') return 0;
  const num = parseInt(value, 10);
  return isNaN(num) ? 0 : num;
}

/**
 * Преобразовать Meta actions array в плоский объект {action_type: count}.
 * Meta возвращает: [{action_type: 'lead', value: '12'}, ...]
 *
 * @param {Array|null|undefined} actions
 * @returns {object|null}
 */
function normalizeActions(actions) {
  if (!Array.isArray(actions) || actions.length === 0) return null;
  const result = {};
  for (const a of actions) {
    if (a.action_type) {
      result[a.action_type] = parseInt(a.value ?? '0', 10);
    }
  }
  return result;
}

/**
 * Собрать payload для колонки actions в meta_insights_daily.
 * Объединяет Meta actions array и inline_link_clicks в один плоский объект.
 * Возвращает null если нет данных.
 *
 * @param {object} row — строка из Meta insights
 * @returns {object|null}
 */
function buildActionsPayload(row) {
  const base = normalizeActions(row.actions) ?? {};
  if (row.inline_link_clicks != null) {
    base.inline_link_clicks = toInt(row.inline_link_clicks);
  }
  return Object.keys(base).length > 0 ? base : null;
}

/**
 * Проверить — является ли ошибка rate-limit (partial) или token-expired (fatal).
 * Возвращает: 'rate_limit' | 'token_expired' | 'other'
 *
 * @param {Error} err
 * @returns {'rate_limit'|'token_expired'|'other'}
 */
function classifyMetaError(err) {
  if (!(err instanceof MetaApiError)) return 'other';
  if (RATE_LIMIT_CODES.has(Number(err.code))) return 'rate_limit';
  if (Number(err.code) === TOKEN_EXPIRED_CODE) return 'token_expired';
  return 'other';
}

// ---------------------------------------------------------------------------
// Публичные sync-функции
// ---------------------------------------------------------------------------

/**
 * Синхронизировать запись рекламного кабинета (1 строка в meta_ad_accounts).
 * Если строки нет — создаёт. Если есть — обновляет поля кроме created_at и access_token.
 *
 * @param {string} [adAccountId] — "act_XXX", fallback на META_AD_ACCOUNT_ID из env
 * @returns {Promise<SyncSummary>}
 */
export async function syncAdAccount(adAccountId) {
  const startTime = Date.now();
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  if (!id) throw new Error('adAccountId is required — pass explicitly or set META_AD_ACCOUNT_ID');

  try {
    const account = await metaAdsClient.getAdAccount(id);

    const row = {
      meta_account_id: account.id ?? id,
      account_name: account.name ?? id,
      currency: account.currency ?? 'USD',
      timezone_name: account.timezone_name ?? 'UTC',
      // access_token: из env — вставляем при создании, не перетираем если уже есть
      access_token: metaAdsConfig.systemUserToken,
      is_active: true,
    };

    // UPSERT на meta_account_id (unique constraint)
    const { error } = await serviceClient
      .from('meta_ad_accounts')
      .upsert(row, { onConflict: 'meta_account_id', ignoreDuplicates: false });

    if (error) throw new Error(`Supabase upsert meta_ad_accounts failed: ${error.message}`);

    logger.info({ metaAccountId: id, name: account.name }, 'meta-ads:sync: syncAdAccount ok');

    return {
      syncType: 'ad_account',
      status: 'ok',
      recordsSynced: 1,
      errors: [],
      durationMs: Date.now() - startTime,
    };
  } catch (err) {
    logger.error({ err: err.message }, 'meta-ads:sync: syncAdAccount error');
    return {
      syncType: 'ad_account',
      status: 'error',
      recordsSynced: 0,
      errors: [{ code: err.code ?? 'INTERNAL', message: err.message }],
      durationMs: Date.now() - startTime,
    };
  }
}

/**
 * Синхронизировать кампании → UPSERT meta_campaigns.
 * Вызывается внутренне из syncFull (без lock) или напрямую из CLI (с lock).
 *
 * @param {string} adAccountId — "act_XXX"
 * @param {string} accountUuid — UUID из meta_ad_accounts.id
 * @param {object} [opts]
 * @param {number|null} [opts.logId] — id строки в sync_log для обновления
 * @returns {Promise<{recordsSynced: number, errors: Array}>}
 */
async function _syncCampaigns(adAccountId, accountUuid, opts = {}) {
  const errors = [];
  let recordsSynced = 0;

  // Default cap в client.js = 500 (защита UI/smoke от runaway). Для sync явно поднимаем.
  const campaigns = await metaAdsClient.listCampaigns(adAccountId, { maxRecords: 50000 });
  logger.info({ count: campaigns.length }, 'meta-ads:sync: campaigns fetched from Meta');

  if (campaigns.length === 0) return { recordsSynced: 0, errors };

  const rows = campaigns.map((c) => ({
    ad_account_id: accountUuid,
    meta_campaign_id: c.id,
    name: c.name,
    objective: c.objective ?? null,
    status: c.status,
    // daily_budget/lifetime_budget из campaign API — уже MINOR units (центы).
    // НЕ умножаем. Храним как есть.
    daily_budget: c.daily_budget != null ? parseInt(c.daily_budget, 10) : null,
    lifetime_budget: c.lifetime_budget != null ? parseInt(c.lifetime_budget, 10) : null,
    created_time: c.created_time ?? null,
    last_seen_at: new Date().toISOString(),
  }));

  const { error } = await serviceClient
    .from('meta_campaigns')
    .upsert(rows, { onConflict: 'ad_account_id,meta_campaign_id' });

  if (error) throw new Error(`Supabase upsert meta_campaigns failed: ${error.message}`);

  recordsSynced = rows.length;
  logger.info({ recordsSynced }, 'meta-ads:sync: meta_campaigns upserted');
  return { recordsSynced, errors };
}

/**
 * Публичная обёртка — syncCampaigns с lock + audit.
 *
 * @param {string} [adAccountId]
 * @returns {Promise<SyncSummary>}
 */
export async function syncCampaigns(adAccountId) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  const accountUuid = await resolveAccountUuid(id);
  return withLockAndLog(accountUuid, 'campaigns', () => _syncCampaigns(id, accountUuid));
}

/**
 * Синхронизировать ad sets → UPSERT meta_ad_sets.
 * Требует наличия кампаний в meta_campaigns (FK: campaign_id).
 *
 * @param {string} adAccountId
 * @param {string} accountUuid
 * @returns {Promise<{recordsSynced: number, errors: Array}>}
 */
async function _syncAdSets(adAccountId, accountUuid) {
  const errors = [];
  let recordsSynced = 0;

  // Загружаем существующие кампании для маппинга meta_campaign_id → uuid
  const { data: existingCampaigns, error: campaignError } = await serviceClient
    .from('meta_campaigns')
    .select('id, meta_campaign_id')
    .eq('ad_account_id', accountUuid);

  if (campaignError) throw new Error(`Failed to load campaigns for FK mapping: ${campaignError.message}`);

  const campaignMap = new Map(existingCampaigns.map((c) => [c.meta_campaign_id, c.id]));

  const adSets = await metaAdsClient.listAdSets(adAccountId, { maxRecords: 50000 });
  logger.info({ count: adSets.length }, 'meta-ads:sync: ad sets fetched from Meta');

  if (adSets.length === 0) return { recordsSynced: 0, errors };

  const rows = [];
  for (const a of adSets) {
    const campaignUuid = campaignMap.get(a.campaign_id);
    if (!campaignUuid) {
      errors.push({
        code: 'FK_MISSING',
        message: `Campaign ${a.campaign_id} not found in meta_campaigns — skipping adset ${a.id}`,
        objectId: a.id,
      });
      continue;
    }

    rows.push({
      ad_account_id: accountUuid,
      campaign_id: campaignUuid,
      meta_adset_id: a.id,
      name: a.name,
      status: a.status,
      daily_budget: a.daily_budget != null ? parseInt(a.daily_budget, 10) : null,
      lifetime_budget: a.lifetime_budget != null ? parseInt(a.lifetime_budget, 10) : null,
      optimization_goal: a.optimization_goal ?? null,
      billing_event: a.billing_event ?? null,
      bid_strategy: a.bid_strategy ?? null,
      // targeting — raw jsonb payload от Meta
      targeting: a.targeting ?? null,
      // placements — извлекаем из promoted_object или targeting.publisher_platforms
      placements: a.targeting?.publisher_platforms
        ? { publisher_platforms: a.targeting.publisher_platforms }
        : null,
      is_advantage_plus: a.is_dynamic_creative === true,
      schedule_start: a.schedule_start_time ?? null,
      schedule_end: a.schedule_end_time ?? null,
      last_seen_at: new Date().toISOString(),
    });
  }

  if (rows.length === 0) return { recordsSynced: 0, errors };

  const { error } = await serviceClient
    .from('meta_ad_sets')
    .upsert(rows, { onConflict: 'ad_account_id,meta_adset_id' });

  if (error) throw new Error(`Supabase upsert meta_ad_sets failed: ${error.message}`);

  recordsSynced = rows.length;
  logger.info({ recordsSynced, skipped: errors.length }, 'meta-ads:sync: meta_ad_sets upserted');
  return { recordsSynced, errors };
}

/**
 * Публичная обёртка.
 *
 * @param {string} [adAccountId]
 * @returns {Promise<SyncSummary>}
 */
export async function syncAdSets(adAccountId) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  const accountUuid = await resolveAccountUuid(id);
  return withLockAndLog(accountUuid, 'adsets', () => _syncAdSets(id, accountUuid));
}

/**
 * Синхронизировать creatives → UPSERT meta_creatives.
 * cached_image_url оставляем NULL — Cloudinary caching в следующей итерации.
 *
 * @param {string} adAccountId
 * @param {string} accountUuid
 * @returns {Promise<{recordsSynced: number, errors: Array}>}
 */
async function _syncCreatives(adAccountId, accountUuid) {
  const errors = [];
  let recordsSynced = 0;

  // Дропаем object_story_spec из bulk-fetch — даже на limit:25 Meta возвращает
  // error #100 "reduce the amount of data" из-за тяжёлого asset_feed_spec.
  // Полный spec можно fetch-нуть per-creative когда понадобится для UI (отложено).
  // Без object_story_spec: limit:100 спокойно работает.
  // См. gotcha в research/library/backend-libs/meta-marketing-api.md
  const creatives = await metaAdsClient.listCreatives(adAccountId, {
    maxRecords: 50000,
    limit: 100,
    fields:
      'id,name,title,body,call_to_action_type,image_url,thumbnail_url,video_id',
  });
  logger.info({ count: creatives.length }, 'meta-ads:sync: creatives fetched from Meta');

  if (creatives.length === 0) return { recordsSynced: 0, errors };

  // Загружаем существующие creatives чтобы не затирать cached_image_url
  const { data: existing } = await serviceClient
    .from('meta_creatives')
    .select('meta_creative_id, cached_image_url')
    .eq('ad_account_id', accountUuid);

  const existingCachedMap = new Map(
    (existing ?? []).map((r) => [r.meta_creative_id, r.cached_image_url])
  );

  // UPSERT-merge паттерн: поле включаем в row ТОЛЬКО если Meta его вернула.
  // Если поля нет в payload — Postgres сохранит существующее значение.
  // Это позволяет частичные sync-и (light vs full fields) без потери данных.
  const rows = creatives.map((c) => {
    const row = {
      ad_account_id: accountUuid,
      meta_creative_id: c.id,
      last_seen_at: new Date().toISOString(),
    };
    if (c.title !== undefined) row.title = c.title;
    if (c.body !== undefined) row.body = c.body;
    if (c.call_to_action_type !== undefined) row.cta_type = c.call_to_action_type;
    if (c.image_url !== undefined) row.image_url = c.image_url;
    if (c.thumbnail_url !== undefined) row.thumbnail_url = c.thumbnail_url;
    if (c.video_id !== undefined) row.video_id = c.video_id;
    if (c.object_story_spec !== undefined) row.object_story_spec = c.object_story_spec;
    // cached_image_url НИКОГДА не перезаписываем из API (это наш Cloudinary URL).
    // Если в БД уже есть — оставляем как есть, не включая в row.
    if (!existingCachedMap.has(c.id)) {
      row.cached_image_url = null; // только при первом INSERT
    }
    return row;
  });

  const { error } = await serviceClient
    .from('meta_creatives')
    .upsert(rows, { onConflict: 'ad_account_id,meta_creative_id' });

  if (error) throw new Error(`Supabase upsert meta_creatives failed: ${error.message}`);

  recordsSynced = rows.length;
  logger.info({ recordsSynced }, 'meta-ads:sync: meta_creatives upserted');
  return { recordsSynced, errors };
}

/**
 * Публичная обёртка.
 *
 * @param {string} [adAccountId]
 * @returns {Promise<SyncSummary>}
 */
export async function syncCreatives(adAccountId) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  const accountUuid = await resolveAccountUuid(id);
  return withLockAndLog(accountUuid, 'creatives', () => _syncCreatives(id, accountUuid));
}

/**
 * Синхронизировать ads → UPSERT meta_ads.
 * Требует наличия ad_sets и creatives (FK).
 *
 * @param {string} adAccountId
 * @param {string} accountUuid
 * @returns {Promise<{recordsSynced: number, errors: Array}>}
 */
async function _syncAds(adAccountId, accountUuid) {
  const errors = [];
  let recordsSynced = 0;

  // FK маппинги
  const { data: existingAdSets, error: adSetsError } = await serviceClient
    .from('meta_ad_sets')
    .select('id, meta_adset_id')
    .eq('ad_account_id', accountUuid);

  if (adSetsError) throw new Error(`Failed to load ad_sets for FK mapping: ${adSetsError.message}`);

  const { data: existingCampaigns, error: campaignsError } = await serviceClient
    .from('meta_campaigns')
    .select('id, meta_campaign_id')
    .eq('ad_account_id', accountUuid);

  if (campaignsError) throw new Error(`Failed to load campaigns for FK mapping: ${campaignsError.message}`);

  const { data: existingCreatives, error: creativesError } = await serviceClient
    .from('meta_creatives')
    .select('id, meta_creative_id')
    .eq('ad_account_id', accountUuid);

  if (creativesError) throw new Error(`Failed to load creatives for FK mapping: ${creativesError.message}`);

  const adSetMap = new Map((existingAdSets ?? []).map((a) => [a.meta_adset_id, a.id]));
  const campaignMap = new Map((existingCampaigns ?? []).map((c) => [c.meta_campaign_id, c.id]));
  const creativeMap = new Map((existingCreatives ?? []).map((c) => [c.meta_creative_id, c.id]));

  const ads = await metaAdsClient.listAds(adAccountId, { maxRecords: 50000 });
  logger.info({ count: ads.length }, 'meta-ads:sync: ads fetched from Meta');

  if (ads.length === 0) return { recordsSynced: 0, errors };

  const rows = [];
  for (const ad of ads) {
    const adSetUuid = adSetMap.get(ad.adset_id);
    const campaignUuid = campaignMap.get(ad.campaign_id);

    if (!adSetUuid) {
      errors.push({
        code: 'FK_MISSING',
        message: `AdSet ${ad.adset_id} not found — skipping ad ${ad.id}`,
        objectId: ad.id,
      });
      continue;
    }
    if (!campaignUuid) {
      errors.push({
        code: 'FK_MISSING',
        message: `Campaign ${ad.campaign_id} not found — skipping ad ${ad.id}`,
        objectId: ad.id,
      });
      continue;
    }

    // creative_id может быть null — объявление без привязанного creative (допустимо)
    const metaCreativeId = ad.creative?.id ?? null;
    const creativeUuid = metaCreativeId ? (creativeMap.get(metaCreativeId) ?? null) : null;

    rows.push({
      ad_account_id: accountUuid,
      campaign_id: campaignUuid,
      ad_set_id: adSetUuid,
      creative_id: creativeUuid,
      meta_ad_id: ad.id,
      name: ad.name,
      status: ad.status,
      created_time: ad.created_time ?? null,
      last_seen_at: new Date().toISOString(),
    });
  }

  if (rows.length === 0) return { recordsSynced: 0, errors };

  const { error } = await serviceClient
    .from('meta_ads')
    .upsert(rows, { onConflict: 'ad_account_id,meta_ad_id' });

  if (error) throw new Error(`Supabase upsert meta_ads failed: ${error.message}`);

  recordsSynced = rows.length;
  logger.info(
    { recordsSynced, skipped: errors.length },
    'meta-ads:sync: meta_ads upserted'
  );
  return { recordsSynced, errors };
}

/**
 * Публичная обёртка.
 *
 * @param {string} [adAccountId]
 * @returns {Promise<SyncSummary>}
 */
export async function syncAds(adAccountId) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  const accountUuid = await resolveAccountUuid(id);
  return withLockAndLog(accountUuid, 'ads', () => _syncAds(id, accountUuid));
}

/**
 * Перечисляет даты от since до until включительно.
 * @param {string} since "YYYY-MM-DD"
 * @param {string} until "YYYY-MM-DD"
 * @returns {string[]}
 */
function enumerateDates(since, until) {
  const result = [];
  const start = new Date(since + 'T00:00:00Z');
  const end = new Date(until + 'T00:00:00Z');
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    result.push(d.toISOString().slice(0, 10));
  }
  return result;
}

/**
 * Синхронизировать insights → UPSERT meta_insights_daily.
 *
 * Стратегия: ИТЕРАЦИЯ ПО ДНЯМ — для каждого дня делаем отдельный запрос
 * с since=until=date. Meta агрегирует за этот день и возвращает row(s)
 * со spend/impressions/clicks. date_start гарантированно = date.
 *
 * ПОЧЕМУ так: time_increments=1 ИГНОРИРУЕТСЯ Meta при level=campaign/adset/ad
 * (подтверждено _test_insights.mjs 2026-05-05). Возвращает 1 строку на объект
 * агрегированную за весь period — мы получали date_start = first day период
 * вместо daily breakdown.
 *
 * Стоимость: 30 дней × 3 levels = 90 API-вызовов на backfill (вместо 3).
 * В пределах rate limit (200/hour для dev tier).
 *
 * ВАЖНО: spend/cpm/cpc из /insights — MAJOR units (доллары).
 * В meta_insights_daily храним MINOR units (центы): умножаем ×100.
 *
 * @param {string} adAccountId — "act_XXX"
 * @param {object} [opts]
 * @param {number} [opts.days=30] — сколько дней назад
 * @param {string[]} [opts.levels=['campaign','adset','ad']] — уровни агрегации
 * @param {string} [opts.since] — явная дата "YYYY-MM-DD" (переопределяет days)
 * @param {string} [opts.until] — явная дата "YYYY-MM-DD"
 * @returns {Promise<{recordsSynced: number, errors: Array}>}
 */
async function _syncInsights(adAccountId, accountUuid, opts = {}) {
  const errors = [];
  let recordsSynced = 0;

  const days = opts.days ?? 30;
  const levels = opts.levels ?? ['campaign', 'adset', 'ad'];

  // Явные даты для воспроизводимости — не date_preset
  const until = opts.until ?? new Date().toISOString().slice(0, 10);
  const since =
    opts.since ??
    (() => {
      const d = new Date();
      d.setDate(d.getDate() - days);
      return d.toISOString().slice(0, 10);
    })();

  // Перечисляем все даты в окне — будем дёргать Meta по одной за раз.
  const dates = enumerateDates(since, until);
  logger.info(
    { since, until, days: dates.length, levels },
    'meta-ads:sync: fetching insights day-by-day (time_increments ignored by Meta)'
  );

  for (const level of levels) {
    let insightsRows = [];
    let daysSucceeded = 0;
    let daysFailed = 0;

    const levelIdField =
      level === 'campaign' ? 'campaign_id,campaign_name' :
      level === 'adset'    ? 'adset_id,adset_name' :
                             'ad_id,ad_name';

    // Каждый день — отдельный запрос с since=until=date.
    // Meta агрегирует за этот день, возвращает row на каждый объект который
    // delivered в этот день. date_start === date.
    for (const date of dates) {
      try {
        const dayRows = await metaAdsClient.getInsights(adAccountId, {
          level,
          since: date,
          until: date,
          fields: `date_start,impressions,clicks,inline_link_clicks,spend,reach,frequency,ctr,cpm,cpc,actions,${levelIdField}`,
          // НЕ передаём time_increments — Meta его игнорирует для object-levels.
          // single-day since=until даёт правильную агрегацию за этот день.
          maxRecords: 50000, // иначе default cap=500 порежет большие дни
        });
        insightsRows.push(...(dayRows ?? []));
        daysSucceeded++;
      } catch (err) {
        const errType = classifyMetaError(err);
        if (errType === 'token_expired') {
          throw err; // Fatal
        }
        // Rate limit или другая ошибка на конкретном дне — пропускаем,
        // продолжаем со следующим днём. Финальный status = 'partial'.
        daysFailed++;
        errors.push({
          code: err.code ?? 'INTERNAL',
          message: `Insights ${level} ${date}: ${err.message}`,
          objectId: `${adAccountId}/${level}/${date}`,
        });
        if (errType === 'rate_limit') {
          logger.warn(
            { level, date, errCode: err.code },
            'meta-ads:sync: rate limit on day — continuing'
          );
        }
      }
    }

    logger.info(
      { level, totalRows: insightsRows.length, daysSucceeded, daysFailed },
      'meta-ads:sync: insights collected for level'
    );

    if (insightsRows.length === 0) {
      logger.info({ level }, 'meta-ads:sync: no insights data for level');
      continue;
    }

    // Маппим objectId в зависимости от level
    const rows = insightsRows.map((row) => {
      let objectId;
      if (level === 'campaign') objectId = row.campaign_id;
      else if (level === 'adset') objectId = row.adset_id;
      else if (level === 'ad') objectId = row.ad_id;
      else objectId = row.campaign_id ?? row.adset_id ?? row.ad_id;

      return {
        ad_account_id: accountUuid,
        level,
        object_id: objectId ?? 'unknown',
        // date_start — дата в timezone кабинета, Meta возвращает её правильно
        date_start: row.date_start,
        impressions: toInt(row.impressions),
        clicks: toInt(row.clicks),
        // MAJOR units → ×100 для MINOR
        spend: toMinorUnits(row.spend),
        reach: toInt(row.reach),
        // frequency и ctr — обычные float-ы, не валюта
        frequency: toFloat(row.frequency),
        ctr: toFloat(row.ctr),
        // MAJOR units → ×100 для MINOR
        cpm: toMinorUnits(row.cpm),
        cpc: toMinorUnits(row.cpc),
        // actions: Meta actions array + inline_link_clicks добавляем сюда же
        // (inline_link_clicks — настоящие клики по ссылке, не engagement clicks)
        actions: buildActionsPayload(row),
        synced_at: new Date().toISOString(),
      };
    });

    // Батчи по 200 — PostgREST URL/body limit
    const BATCH_SIZE = 200;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const { error } = await serviceClient
        .from('meta_insights_daily')
        .upsert(batch, {
          onConflict: 'ad_account_id,level,object_id,date_start',
        });

      if (error) throw new Error(`Supabase upsert meta_insights_daily failed (level=${level}): ${error.message}`);
      recordsSynced += batch.length;
    }

    logger.info({ level, rows: rows.length }, 'meta-ads:sync: insights upserted for level');
  }

  return { recordsSynced, errors };
}

/**
 * Публичная обёртка.
 *
 * @param {string} [adAccountId]
 * @param {object} [opts]
 * @param {number} [opts.days=30]
 * @param {string[]} [opts.levels=['campaign','adset','ad']]
 * @param {string} [opts.since]
 * @param {string} [opts.until]
 * @returns {Promise<SyncSummary>}
 */
export async function syncInsights(adAccountId, opts = {}) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  const accountUuid = await resolveAccountUuid(id);
  return withLockAndLog(accountUuid, 'insights', () => _syncInsights(id, accountUuid, opts));
}

/**
 * Полный sync: AdAccount → Campaigns → AdSets → Creatives → Ads → Insights.
 * Порядок важен — FK зависимости.
 *
 * @param {string} [adAccountId]
 * @param {object} [opts]
 * @param {number} [opts.days=30] — глубина insights
 * @param {string[]} [opts.levels=['campaign','adset','ad']]
 * @param {string} [opts.since]
 * @param {string} [opts.until]
 * @returns {Promise<SyncSummary[]>} — массив summary по каждому шагу
 */
export async function syncFull(adAccountId, opts = {}) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  const startTime = Date.now();

  // Шаг 1: AdAccount (без lock — нет UUID пока)
  const accountResult = await syncAdAccount(id);
  if (accountResult.status === 'error') {
    return [accountResult];
  }

  const accountUuid = await resolveAccountUuid(id);
  const results = [accountResult];

  // Шаги 2-6 под одним lock — всю структуру синхронизируем атомарно
  const locked = await acquireLock(accountUuid);
  if (!locked) {
    return [
      ...results,
      {
        syncType: 'full',
        status: 'error',
        recordsSynced: 0,
        errors: [{ code: 'LOCK_BUSY', message: 'Another sync is running' }],
        durationMs: Date.now() - startTime,
      },
    ];
  }

  const stopHeartbeat = startHeartbeat(accountUuid);
  const logId = await logStarted(accountUuid, 'full');

  let totalRecords = accountResult.recordsSynced;
  let hasError = false;
  let hasFatalError = false;

  try {
    // Шаг 2: Campaigns
    const campaignsLogId = await logStarted(accountUuid, 'campaigns');
    let campaignsResult;
    try {
      campaignsResult = await _syncCampaigns(id, accountUuid);
      const campaignsStatus = campaignsResult.errors.length > 0 ? 'partial' : 'ok';
      await logSuccess(campaignsLogId, campaignsResult.recordsSynced, campaignsStatus);
    } catch (err) {
      await logError(campaignsLogId, err);
      campaignsResult = { recordsSynced: 0, errors: [{ code: err.code ?? 'INTERNAL', message: err.message }] };
      hasError = true;
    }
    results.push({
      syncType: 'campaigns',
      status: campaignsResult.errors.length > 0 ? (hasError ? 'error' : 'partial') : 'ok',
      recordsSynced: campaignsResult.recordsSynced,
      errors: campaignsResult.errors,
      durationMs: 0,
    });
    totalRecords += campaignsResult.recordsSynced;

    // Шаг 3: AdSets
    const adSetsLogId = await logStarted(accountUuid, 'adsets');
    let adSetsResult;
    try {
      adSetsResult = await _syncAdSets(id, accountUuid);
      const adSetsStatus = adSetsResult.errors.length > 0 ? 'partial' : 'ok';
      await logSuccess(adSetsLogId, adSetsResult.recordsSynced, adSetsStatus);
    } catch (err) {
      await logError(adSetsLogId, err);
      adSetsResult = { recordsSynced: 0, errors: [{ code: err.code ?? 'INTERNAL', message: err.message }] };
      hasError = true;
    }
    results.push({
      syncType: 'adsets',
      status: adSetsResult.errors.length > 0 ? (hasError ? 'error' : 'partial') : 'ok',
      recordsSynced: adSetsResult.recordsSynced,
      errors: adSetsResult.errors,
      durationMs: 0,
    });
    totalRecords += adSetsResult.recordsSynced;

    // Шаг 4: Creatives
    const creativesLogId = await logStarted(accountUuid, 'creatives');
    let creativesResult;
    try {
      creativesResult = await _syncCreatives(id, accountUuid);
      const creativesStatus = creativesResult.errors.length > 0 ? 'partial' : 'ok';
      await logSuccess(creativesLogId, creativesResult.recordsSynced, creativesStatus);
    } catch (err) {
      await logError(creativesLogId, err);
      creativesResult = { recordsSynced: 0, errors: [{ code: err.code ?? 'INTERNAL', message: err.message }] };
      hasError = true;
    }
    results.push({
      syncType: 'creatives',
      status: creativesResult.errors.length > 0 ? (hasError ? 'error' : 'partial') : 'ok',
      recordsSynced: creativesResult.recordsSynced,
      errors: creativesResult.errors,
      durationMs: 0,
    });
    totalRecords += creativesResult.recordsSynced;

    // Шаг 5: Ads
    const adsLogId = await logStarted(accountUuid, 'ads');
    let adsResult;
    try {
      adsResult = await _syncAds(id, accountUuid);
      const adsStatus = adsResult.errors.length > 0 ? 'partial' : 'ok';
      await logSuccess(adsLogId, adsResult.recordsSynced, adsStatus);
    } catch (err) {
      await logError(adsLogId, err);
      adsResult = { recordsSynced: 0, errors: [{ code: err.code ?? 'INTERNAL', message: err.message }] };
      hasError = true;
    }
    results.push({
      syncType: 'ads',
      status: adsResult.errors.length > 0 ? (hasError ? 'error' : 'partial') : 'ok',
      recordsSynced: adsResult.recordsSynced,
      errors: adsResult.errors,
      durationMs: 0,
    });
    totalRecords += adsResult.recordsSynced;

    // Шаг 6: Insights
    const insightsLogId = await logStarted(accountUuid, 'insights');
    let insightsResult;
    try {
      insightsResult = await _syncInsights(id, accountUuid, opts);
      const insightsStatus = insightsResult.errors.length > 0 ? 'partial' : 'ok';
      await logSuccess(insightsLogId, insightsResult.recordsSynced, insightsStatus);
    } catch (err) {
      await logError(insightsLogId, err);
      // Если token expired — это fatal
      if (err instanceof MetaApiError && Number(err.code) === TOKEN_EXPIRED_CODE) {
        hasFatalError = true;
        throw err;
      }
      insightsResult = { recordsSynced: 0, errors: [{ code: err.code ?? 'INTERNAL', message: err.message }] };
      hasError = true;
    }
    results.push({
      syncType: 'insights',
      status: insightsResult.errors.length > 0 ? (hasError ? 'error' : 'partial') : 'ok',
      recordsSynced: insightsResult.recordsSynced,
      errors: insightsResult.errors,
      durationMs: 0,
    });
    totalRecords += insightsResult.recordsSynced;

    const finalStatus = hasFatalError ? 'error' : hasError ? 'partial' : 'ok';
    await logSuccess(logId, totalRecords, finalStatus);
    await serviceClient
      .from('meta_ad_accounts')
      .update({ last_sync_at: new Date().toISOString(), last_sync_status: finalStatus })
      .eq('id', accountUuid);

    return results;
  } catch (err) {
    await logError(logId, err);
    results.push({
      syncType: 'full',
      status: 'error',
      recordsSynced: 0,
      errors: [{ code: err.code ?? 'INTERNAL', message: err.message }],
      durationMs: Date.now() - startTime,
    });
    return results;
  } finally {
    stopHeartbeat();
    await releaseLock(accountUuid);
  }
}

/**
 * Delta sync — обновить структуру + insights за последние 2 дня.
 * Не трогает исторические данные. Для регулярного 6h cron.
 *
 * @param {string} [adAccountId]
 * @returns {Promise<SyncSummary[]>}
 */
export async function syncDelta(adAccountId) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;

  // Delta: структура (campaigns/adsets/creatives/ads) + последние 2 дня insights
  const until = new Date().toISOString().slice(0, 10);
  const sinceDate = new Date();
  sinceDate.setDate(sinceDate.getDate() - 2);
  const since = sinceDate.toISOString().slice(0, 10);

  return syncFull(id, { since, until, levels: ['campaign', 'adset', 'ad'] });
}
