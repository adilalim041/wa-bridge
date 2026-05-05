/**
 * Meta Marketing API — тонкий fetch-клиент.
 *
 * Особенности:
 *  - Токен НИКОГДА не попадает в логи (URL/error payload пропускаются через maskToken).
 *  - Retry с exponential backoff на 429 / 5xx / Meta rate-limit error codes.
 *  - Semaphore: не более 3 одновременных запросов.
 *  - Auto-pagination: listCampaigns/listAdSets/listAds/listCreatives собирают все страницы
 *    до жёсткого предела 500 записей.
 *  - Rate-limit utilization: парсится x-business-use-case-usage, warn при >90%, info при >75%.
 *
 * Что НЕ реализовано здесь:
 *  - sync.js / cron — следующая итерация.
 *  - Write-операции (POST campaign, pause ad) — следующая итерация.
 *  - Creative caching в Cloudinary — отдельный шаг.
 *
 * Decisions log: ObsidianVault/research/library/backend-libs/meta-marketing-api.md
 */

import pino from 'pino';
import { metaAdsConfig, maskToken } from './config.js';

// Локальный logger — НЕ зависит от глобального src/config.js,
// чтобы модуль meta-ads был self-contained и smoke-тест не падал
// из-за невыставленных Supabase env'ов в локальном .env.
// В рантайме wa-bridge оба логгера пишут в stdout — pino merge-и в JSON.
const logger = pino({ level: process.env.LOG_LEVEL || 'info' }).child({
  module: 'meta-ads',
});

// ---------------------------------------------------------------------------
// Ошибки
// ---------------------------------------------------------------------------

/** Бросается когда modул dormant (META_SYSTEM_USER_TOKEN не задан). */
export class MetaAdsDisabledError extends Error {
  constructor() {
    super(
      'Meta Ads module is disabled — set META_SYSTEM_USER_TOKEN to activate. ' +
        'Reason: ' + (metaAdsConfig.reason ?? 'module not configured')
    );
    this.name = 'MetaAdsDisabledError';
    this.code = 'META_ADS_DISABLED';
  }
}

/**
 * Структурированная ошибка от Marketing API.
 * Никогда не содержит токен — ни в message, ни в toJSON().
 */
export class MetaApiError extends Error {
  /**
   * @param {object} params
   * @param {number|string} params.code       — Meta error.code
   * @param {number|string} [params.subcode]  — Meta error.error_subcode
   * @param {string}        params.message
   * @param {number}        params.httpStatus
   * @param {boolean}       params.isRetryable
   * @param {string}        params.requestPath — путь без токена
   */
  constructor({ code, subcode, message, httpStatus, isRetryable, requestPath }) {
    super(message);
    this.name = 'MetaApiError';
    this.code = code;
    this.subcode = subcode ?? null;
    this.httpStatus = httpStatus;
    this.isRetryable = isRetryable;
    this.requestPath = requestPath;
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      subcode: this.subcode,
      message: this.message,
      httpStatus: this.httpStatus,
      isRetryable: this.isRetryable,
      requestPath: this.requestPath,
      // Намеренно нет: stack (может содержать путь с токеном в некоторых рантаймах)
    };
  }
}

// ---------------------------------------------------------------------------
// Константы
// ---------------------------------------------------------------------------

/**
 * Meta error codes которые означают временную перегрузку — делаем retry.
 * 17    = User request limit reached
 * 4     = Application request limit reached
 * 80004 = There have been too many calls...
 * 100   = "Please reduce the amount of data you're asking for, then retry"
 *         (overload, чаще всего на тяжёлых fields типа object_story_spec)
 *         Подтверждено на Omoikiri 2026-05-05.
 * 1     = "An unknown error occurred" — часто транзиентный
 * 2     = "Service temporarily unavailable"
 */
const RATE_LIMIT_CODES = new Set([17, 4, 80004, 100, 1, 2]);

/**
 * Meta error codes означающие невалидный токен — НЕ делаем retry,
 * сразу пробрасываем чтобы можно было алертить.
 * 190 = Invalid OAuth 2.0 Access Token
 */
const TOKEN_INVALID_CODES = new Set([190]);

/** HTTP-статусы на которые делаем retry. */
const RETRYABLE_HTTP_STATUSES = new Set([429, 500, 502, 503, 504]);

/** Жёсткий предел записей за один list-вызов (защита от runaway pagination). */
const MAX_RECORDS_PER_CALL = 500;

/** Базовая задержка для первого retry в мс. */
const BASE_RETRY_DELAY_MS = 1000;

/** Максимальное число retry-попыток. */
const MAX_RETRIES = 3;

/** Максимальное число одновременных запросов к Meta API. */
const MAX_CONCURRENT = 3;

// ---------------------------------------------------------------------------
// Semaphore (простой счётчик + очередь промисов)
// ---------------------------------------------------------------------------

class Semaphore {
  constructor(max) {
    this._max = max;
    this._count = 0;
    this._queue = [];
  }

  /** Захватить слот. Возвращает промис, резолвящийся когда слот свободен. */
  acquire() {
    if (this._count < this._max) {
      this._count++;
      return Promise.resolve();
    }
    return new Promise((resolve) => this._queue.push(resolve));
  }

  /** Освободить слот. */
  release() {
    this._count--;
    const next = this._queue.shift();
    if (next) {
      this._count++;
      next();
    }
  }
}

const semaphore = new Semaphore(MAX_CONCURRENT);

// ---------------------------------------------------------------------------
// Вспомогательные функции
// ---------------------------------------------------------------------------

/**
 * Экспоненциальная задержка с ±20% jitter.
 * attempt=0 → ~1s, attempt=1 → ~2s, attempt=2 → ~4s.
 */
function retryDelayMs(attempt) {
  const base = BASE_RETRY_DELAY_MS * Math.pow(2, attempt);
  const jitter = base * 0.2 * (Math.random() * 2 - 1); // ±20%
  return Math.round(base + jitter);
}

/** sleep helper */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Парсить x-business-use-case-usage header и логировать утилизацию.
 * Header — JSON-объект вида:
 *   {"123456": [{"type":"ads_management","call_count":5,"total_cputime":10,...}]}
 */
function parseAndLogUsageHeader(headerValue, path) {
  if (!headerValue) return;
  try {
    const usage = JSON.parse(headerValue);
    for (const [, entries] of Object.entries(usage)) {
      if (!Array.isArray(entries)) continue;
      for (const entry of entries) {
        const utilization = Math.max(
          entry.call_count ?? 0,
          entry.total_cputime ?? 0,
          entry.total_time ?? 0
        );
        if (utilization > 90) {
          logger.warn(
            { path, utilization, usageEntry: entry },
            'meta-ads: rate limit utilization >90% — consider slowing down sync'
          );
        } else if (utilization > 75) {
          logger.info(
            { path, utilization, usageEntry: entry },
            'meta-ads: rate limit utilization >75%'
          );
        }
      }
    }
  } catch {
    // Не критично — просто не логируем утилизацию
  }
}

// ---------------------------------------------------------------------------
// Ядро: request()
// ---------------------------------------------------------------------------

/**
 * Приватная функция для всех запросов к Graph API.
 * Токен добавляется в params ВНУТРИ функции и НИКОГДА не логируется.
 *
 * @param {string} path     — путь, например "/act_XXX/campaigns"
 * @param {object} params   — query params БЕЗ access_token
 * @param {object} [opts]
 * @param {string} [opts.method='GET']
 * @param {string} [opts.token]         — переопределить токен (для multi-tenant)
 * @returns {Promise<any>}
 */
async function request(path, params = {}, opts = {}) {
  const method = (opts.method ?? 'GET').toUpperCase();
  const token = opts.token ?? metaAdsConfig.systemUserToken;
  const baseUrl = opts.baseUrl ?? metaAdsConfig.baseUrl;

  // Логируем path + params, но БЕЗ токена
  const safeParams = { ...params };
  const logCtx = { path, params: safeParams, method };

  let lastError;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      const delay = retryDelayMs(attempt - 1);
      logger.info({ ...logCtx, attempt, delayMs: delay }, 'meta-ads: retrying request');
      await sleep(delay);
    }

    await semaphore.acquire();
    let res;
    try {
      const url = new URL(`${baseUrl}${path}`);

      if (method === 'GET') {
        for (const [k, v] of Object.entries(params)) {
          if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
        }
        // Токен добавляем последним — не в логируемых params
        url.searchParams.set('access_token', token);

        res = await fetch(url.toString(), { method: 'GET' });
      } else {
        // POST: params в body (form-encoded), токен в URL
        url.searchParams.set('access_token', token);
        const body = new URLSearchParams();
        for (const [k, v] of Object.entries(params)) {
          if (v !== undefined && v !== null) body.set(k, String(v));
        }
        res = await fetch(url.toString(), {
          method,
          body,
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        });
      }
    } finally {
      semaphore.release();
    }

    // Мониторинг rate-limit utilization
    parseAndLogUsageHeader(res.headers.get('x-business-use-case-usage'), path);

    // Парсим тело
    let body;
    const contentType = res.headers.get('content-type') ?? '';
    if (contentType.includes('application/json')) {
      body = await res.json();
    } else {
      const text = await res.text();
      body = { _raw: text };
    }

    // Meta возвращает ошибки с HTTP 200 тоже — проверяем тело
    const metaError = body?.error;
    if (metaError) {
      const errorCode = Number(metaError.code ?? 0);
      const isTokenInvalid = TOKEN_INVALID_CODES.has(errorCode);
      const isRateLimit = RATE_LIMIT_CODES.has(errorCode);
      const isRetryable = isRateLimit && !isTokenInvalid;

      logger.error(
        {
          path,
          httpStatus: res.status,
          errorCode,
          subcode: metaError.error_subcode,
          tokenMasked: maskToken(token),
          isRetryable,
        },
        `meta-ads: API error — ${metaError.message}`
      );

      const err = new MetaApiError({
        code: errorCode,
        subcode: metaError.error_subcode,
        message: metaError.message ?? 'Unknown Meta API error',
        httpStatus: res.status,
        isRetryable,
        requestPath: path,
      });

      if (!isRetryable || attempt >= MAX_RETRIES) throw err;
      lastError = err;
      continue;
    }

    // HTTP ошибки (без Meta error body)
    if (!res.ok) {
      const isRetryable = RETRYABLE_HTTP_STATUSES.has(res.status);
      logger.error(
        {
          path,
          httpStatus: res.status,
          tokenMasked: maskToken(token),
          isRetryable,
        },
        `meta-ads: HTTP error ${res.status}`
      );

      const err = new MetaApiError({
        code: `HTTP_${res.status}`,
        message: `HTTP ${res.status} from Meta API`,
        httpStatus: res.status,
        isRetryable,
        requestPath: path,
      });

      if (!isRetryable || attempt >= MAX_RETRIES) throw err;
      lastError = err;
      continue;
    }

    // Успех
    return body;
  }

  // Вышли из цикла исчерпав все попытки
  throw lastError;
}

// ---------------------------------------------------------------------------
// Auto-pagination helper
// ---------------------------------------------------------------------------

/**
 * Собирает все страницы cursor-based пагинации Graph API.
 * Останавливается когда нет `paging.next` ИЛИ достигнут MAX_RECORDS_PER_CALL.
 *
 * @param {string} path
 * @param {object} params
 * @param {object} [opts]
 * @returns {Promise<Array>}
 */
async function collectAllPages(path, params = {}, opts = {}) {
  const allItems = [];
  let after = params.after ?? null;
  // maxRecords — мягкий предел "верни не больше N записей всего".
  // Default = MAX_RECORDS_PER_CALL (500) — защита для smoke/UI вызовов от runaway.
  // Sync-вызовы явно передают высокий maxRecords (десятки тысяч) чтобы забрать всё.
  const maxRecords = opts.maxRecords ?? MAX_RECORDS_PER_CALL;
  // Страничный размер. Если caller просит мало (maxRecords<100) — экономим запросы.
  const pageLimit = Math.min(params.limit ?? 100, 100, maxRecords);

  while (true) {
    const pageParams = { ...params, limit: pageLimit };
    if (after) pageParams.after = after;

    const res = await request(path, pageParams, opts);
    const items = res.data ?? [];
    allItems.push(...items);

    // Достигли запрошенного maxRecords — стоп
    if (allItems.length >= maxRecords) {
      if (allItems.length >= MAX_RECORDS_PER_CALL) {
        logger.warn(
          { path, count: allItems.length },
          `meta-ads: hit MAX_RECORDS_PER_CALL (${MAX_RECORDS_PER_CALL}), stopping pagination`
        );
      }
      break;
    }

    // Нет следующей страницы
    if (!res.paging?.next) break;
    after = res.paging?.cursors?.after;
    if (!after) break;
  }

  // Срез — пользователь просил maxRecords, не отдаём больше
  return allItems.slice(0, maxRecords);
}

// ---------------------------------------------------------------------------
// Клиент — публичные методы
// ---------------------------------------------------------------------------

function assertEnabled() {
  if (!metaAdsConfig.enabled) throw new MetaAdsDisabledError();
}

/**
 * Разрешить ad_account_id: переданный аргумент ИЛИ дефолт из config.
 */
function resolveAccountId(adAccountId) {
  const id = adAccountId ?? metaAdsConfig.adAccountId;
  if (!id) {
    throw new Error(
      'adAccountId is required — pass explicitly or set META_AD_ACCOUNT_ID in env'
    );
  }
  return id;
}

export const metaAdsClient = {
  // ------------------------------------------------------------------
  // /me — проверить токен и получить id/name системного юзера
  // ------------------------------------------------------------------
  async getMe() {
    assertEnabled();
    return request('/me', { fields: 'id,name' });
  },

  // ------------------------------------------------------------------
  // /act_XXX — детали рекламного кабинета
  // ------------------------------------------------------------------
  async getAdAccount(adAccountId) {
    assertEnabled();
    const id = resolveAccountId(adAccountId);
    return request(`/${id}`, {
      fields: 'id,name,currency,timezone_name,account_status,balance',
    });
  },

  // ------------------------------------------------------------------
  // /act_XXX/campaigns
  // ------------------------------------------------------------------
  /**
   * @param {string} [adAccountId]
   * @param {{ limit?: number, after?: string, fields?: string }} [opts]
   * @returns {Promise<Array>}
   */
  async listCampaigns(adAccountId, opts = {}) {
    assertEnabled();
    const id = resolveAccountId(adAccountId);
    const fields =
      opts.fields ??
      'id,name,objective,status,daily_budget,lifetime_budget,created_time';
    return collectAllPages(
      `/${id}/campaigns`,
      {
        fields,
        limit: opts.limit ?? 100,
        ...(opts.after ? { after: opts.after } : {}),
      },
      { maxRecords: opts.maxRecords }
    );
  },

  // ------------------------------------------------------------------
  // /act_XXX/adsets
  // ------------------------------------------------------------------
  /**
   * @param {string} [adAccountId]
   * @param {{ limit?: number, after?: string, fields?: string }} [opts]
   * @returns {Promise<Array>}
   */
  async listAdSets(adAccountId, opts = {}) {
    assertEnabled();
    const id = resolveAccountId(adAccountId);
    const fields =
      opts.fields ??
      'id,name,campaign_id,status,daily_budget,lifetime_budget,' +
        'optimization_goal,billing_event,bid_strategy,targeting,promoted_object,' +
        'is_dynamic_creative,schedule_start_time,schedule_end_time';
    return collectAllPages(
      `/${id}/adsets`,
      {
        fields,
        limit: opts.limit ?? 100,
        ...(opts.after ? { after: opts.after } : {}),
      },
      { maxRecords: opts.maxRecords }
    );
  },

  // ------------------------------------------------------------------
  // /act_XXX/ads
  // ------------------------------------------------------------------
  /**
   * @param {string} [adAccountId]
   * @param {{ limit?: number, after?: string, fields?: string }} [opts]
   * @returns {Promise<Array>}
   */
  async listAds(adAccountId, opts = {}) {
    assertEnabled();
    const id = resolveAccountId(adAccountId);
    const fields =
      opts.fields ?? 'id,name,adset_id,campaign_id,creative,status,created_time';
    return collectAllPages(
      `/${id}/ads`,
      {
        fields,
        limit: opts.limit ?? 100,
        ...(opts.after ? { after: opts.after } : {}),
      },
      { maxRecords: opts.maxRecords }
    );
  },

  // ------------------------------------------------------------------
  // /act_XXX/adcreatives
  // ------------------------------------------------------------------
  /**
   * @param {string} [adAccountId]
   * @param {{ limit?: number, after?: string, fields?: string }} [opts]
   * @returns {Promise<Array>}
   */
  async listCreatives(adAccountId, opts = {}) {
    assertEnabled();
    const id = resolveAccountId(adAccountId);
    const fields =
      opts.fields ??
      'id,name,title,body,call_to_action_type,image_url,thumbnail_url,' +
        'video_id,object_story_spec';
    return collectAllPages(
      `/${id}/adcreatives`,
      {
        fields,
        limit: opts.limit ?? 100,
        ...(opts.after ? { after: opts.after } : {}),
      },
      { maxRecords: opts.maxRecords }
    );
  },

  // ------------------------------------------------------------------
  // Insights
  // ------------------------------------------------------------------
  /**
   * Получить метрики для объекта (account / campaign / adset / ad).
   *
   * @param {string} objectId       — например "act_XXX" или campaign_id
   * @param {object} [opts]
   * @param {'account'|'campaign'|'adset'|'ad'} [opts.level='account']
   * @param {string}  [opts.date_preset]  — 'last_7d', 'last_30d', etc.
   * @param {string}  [opts.since]        — ISO date строка (альтернатива date_preset)
   * @param {string}  [opts.until]        — ISO date строка
   * @param {string}  [opts.fields]
   * @param {string}  [opts.breakdowns]
   * @param {number}  [opts.limit]
   * @returns {Promise<Array>}
   */
  async getInsights(objectId, opts = {}) {
    assertEnabled();
    if (!objectId) throw new Error('objectId is required for getInsights');

    const params = {
      level: opts.level ?? 'account',
      fields:
        opts.fields ??
        'impressions,clicks,spend,reach,frequency,ctr,cpm,cpc,actions',
      limit: opts.limit ?? 100,
    };

    if (opts.date_preset) {
      params.date_preset = opts.date_preset;
    } else if (opts.since && opts.until) {
      params.time_range = JSON.stringify({ since: opts.since, until: opts.until });
    }

    if (opts.breakdowns) params.breakdowns = opts.breakdowns;
    // time_increments=1 → daily breakdown (одна строка на день).
    // Без этого Meta агрегирует весь period в одну строку.
    if (opts.time_increments != null) params.time_increments = opts.time_increments;

    return collectAllPages(`/${objectId}/insights`, params);
  },

  // ------------------------------------------------------------------
  // Batch endpoint — до 50 sub-requests за раз
  // ------------------------------------------------------------------
  /**
   * Выполнить batch-запрос к /v{version}/.
   * https://developers.facebook.com/docs/graph-api/batch-requests
   *
   * @param {Array<{method: string, relative_url: string, body?: string}>} requests
   * @returns {Promise<Array>}
   */
  async batch(requests) {
    assertEnabled();
    if (!Array.isArray(requests) || requests.length === 0) {
      throw new Error('batch() requires a non-empty array of requests');
    }
    if (requests.length > 50) {
      throw new Error('batch() supports up to 50 sub-requests per call');
    }

    // Batch endpoint — POST на корень версии
    const batchPath = '/';
    const result = await request(
      batchPath,
      { batch: JSON.stringify(requests) },
      { method: 'POST' }
    );

    // Каждый элемент ответа содержит {code, headers, body (JSON string)}
    return (result ?? []).map((item) => {
      if (!item) return null;
      try {
        return {
          code: item.code,
          body: typeof item.body === 'string' ? JSON.parse(item.body) : item.body,
        };
      } catch {
        return { code: item.code, body: item.body };
      }
    });
  },
};
