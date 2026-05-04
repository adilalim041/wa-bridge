/**
 * Sales CRM — extras module (top-by-category, future light analytics).
 *
 * Выделено из salesCrm.js (5188 LOC + 4745 LOC уже raised в audit как critical-overgrown).
 * См. ObsidianVault/projects/omoikiri/audits/2026-05-04-pre-saas-audit.md (C-2).
 *
 * Содержит:
 *   - getTopByCategory(req, opts): топ-SKU внутри каждой категории отдельно.
 *     Adil 2026-05-04: «нужно чтобы был анализ по конкретным моделям моек,
 *     смесителей, измельчителей — что покупают чаще там».
 *
 * Использует те же helpers (loadAllParallel, addCityFilter, cache), что и salesCrm.js,
 * но без зависимости от тяжёлых функций — это light-weight модуль.
 */

import { supabase as serviceClient } from '../storage/supabase.js';

function pickClient(req) {
  return req?.userClient || serviceClient;
}

const CACHE_TTL_MS = 24 * 60 * 60 * 1000;
const _cache = new Map();
function cacheKey(name, req, paramsObj) {
  if (!req?.user?.userId) return null;
  return `${name}|${req.user.userId}|${JSON.stringify(paramsObj || {})}`;
}
function cacheGet(key) {
  if (!key) return null;
  const e = _cache.get(key);
  if (!e) return null;
  if (Date.now() - e.ts > CACHE_TTL_MS) { _cache.delete(key); return null; }
  return e.value;
}
function cacheSet(key, value) {
  if (!key) return;
  _cache.set(key, { value, ts: Date.now() });
}

// ── Filters helpers (mirror salesCrm.js minimal subset) ─────────────────────
function addCityFilter(filters, city) {
  if (!city || city === 'all') return filters;
  if (city === 'Алматы') return { ...filters, like: { ...(filters.like || {}), source_file: 'Алматы%' } };
  if (city === 'Астана') return { ...filters, notLike: { ...(filters.notLike || {}), source_file: 'Алматы%' } };
  return filters;
}

// ── Category labels — same map as frontend (kept here for autonomous use) ───
const CATEGORY_RU = {
  sink: 'Мойки', faucet: 'Смесители', disposer: 'Измельчители',
  water_filter: 'Фильтры', cartridge: 'Картриджи', dispenser: 'Дозаторы',
  roll_mat: 'Ролл-маты', accessory: 'Аксессуары', dryer: 'Сушилки',
  decoration: 'Декор', tap_switch: 'Кнопки', overflow: 'Перелив',
  disposer_arm: 'Сифоны', other: 'Прочее',
};

// Категории в которых смотрим top-SKU. Картриджи / аксессуары / прочее — не интересны
// (расходники, мелочёвка).
const TOP_BY_CATEGORY_INTERESTING = ['sink', 'faucet', 'disposer', 'water_filter', 'dispenser'];

// ── Базовая модель без цвета — same regex as salesCrm.js cross-sell ─────────
const COLOR_SUFFIX_RE = /\s+(LG|GM|IN|BL|BN|GB|WH|GR|GS|PA|SA|BE|DC|SS|AS|GBL|MBL|MAS|EBL|EAS|EAN)\s*$/i;
function baseModelName(raw) {
  if (!raw) return null;
  let s = String(raw).trim();
  s = s.replace(COLOR_SUFFIX_RE, '').trim();
  s = s.replace(/\s+/g, ' ');
  return s || null;
}

// ── loadAllParallel mini — copy from salesCrm.js for filters subset ─────────
async function loadAllParallel(sb, table, columns, filters = {}) {
  // Initial count + first page
  let cntQ = sb.from(table).select(columns, { count: 'exact', head: true });
  if (filters.eq)      for (const [k, v] of Object.entries(filters.eq))      cntQ = cntQ.eq(k, v);
  if (filters.gte)     for (const [k, v] of Object.entries(filters.gte))     cntQ = cntQ.gte(k, v);
  if (filters.lte)     for (const [k, v] of Object.entries(filters.lte))     cntQ = cntQ.lte(k, v);
  if (filters.like)    for (const [k, v] of Object.entries(filters.like))    cntQ = cntQ.like(k, v);
  if (filters.notLike) for (const [k, v] of Object.entries(filters.notLike)) cntQ = cntQ.not(k, 'like', v);
  if (filters.notNull) for (const k of filters.notNull)                       cntQ = cntQ.not(k, 'is', null);
  if (filters.isNull)  for (const k of filters.isNull)                        cntQ = cntQ.is(k, null);
  const { count, error } = await cntQ;
  if (error) throw new Error(`count ${table}: ${error.message}`);
  if (!count) return [];

  const PAGE = 1000;
  const pages = Math.ceil(count / PAGE);
  const requests = [];
  for (let p = 0; p < pages; p++) {
    let q = sb.from(table).select(columns).range(p * PAGE, (p + 1) * PAGE - 1);
    if (filters.eq)      for (const [k, v] of Object.entries(filters.eq))      q = q.eq(k, v);
    if (filters.gte)     for (const [k, v] of Object.entries(filters.gte))     q = q.gte(k, v);
    if (filters.lte)     for (const [k, v] of Object.entries(filters.lte))     q = q.lte(k, v);
    if (filters.like)    for (const [k, v] of Object.entries(filters.like))    q = q.like(k, v);
    if (filters.notLike) for (const [k, v] of Object.entries(filters.notLike)) q = q.not(k, 'like', v);
    if (filters.notNull) for (const k of filters.notNull)                       q = q.not(k, 'is', null);
    if (filters.isNull)  for (const k of filters.isNull)                        q = q.is(k, null);
    requests.push(q);
  }
  const results = await Promise.all(requests);
  return results.flatMap(r => r.data || []);
}

/**
 * getTopByCategory(req, opts) — top-N SKU внутри каждой категории.
 *
 * opts:
 *   - city: 'Алматы' | 'Астана' | 'all' (default 'all')
 *   - year: '2023'..'2026' | 'all' (default 'all')
 *   - top:  number (default 10)
 *
 * Возвращает:
 *   {
 *     city, year,
 *     by_category: {
 *       sink:        [{ name, base_name, qty_sold, revenue, orders, sample_sku }, ...],
 *       faucet:      [...],
 *       disposer:    [...],
 *       water_filter:[...],
 *       dispenser:   [...],
 *     },
 *     totals: { sink: { qty, revenue }, ... },
 *   }
 *
 * SKU группируются по «базовой модели без цвета» — Taki 74 LG / GM / IN
 * считаются одной моделью «Taki 74».
 */
export async function getTopByCategory(req, opts = {}) {
  const city = opts.city || 'all';
  const year = opts.year && /^\d{4}$/.test(String(opts.year)) ? String(opts.year) : 'all';
  const top = Math.min(50, Math.max(5, parseInt(opts.top, 10) || 10));

  const ck = cacheKey('top-by-category', req, { city, year, top });
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  let salesFilters = addCityFilter({}, city);
  if (year !== 'all') {
    salesFilters = {
      ...salesFilters,
      gte: { ...(salesFilters.gte || {}), sale_date: `${year}-01-01` },
      lte: { ...(salesFilters.lte || {}), sale_date: `${year}-12-31` },
    };
  }

  // 1. Sale_ids нужного city/year
  const sales = await loadAllParallel(sb, 'sales', 'id', salesFilters);
  const saleIds = sales.map(s => s.id);
  if (saleIds.length === 0) {
    const empty = { city, year, by_category: {}, totals: {}, total_orders: 0 };
    cacheSet(ck, empty);
    return empty;
  }

  // 2. Items по этим sale_ids — батчами по 200 (PostgREST .in() limit)
  // ТОЛЬКО для интересных категорий, чтобы не тащить лишнее.
  const batches = [];
  for (let i = 0; i < saleIds.length; i += 200) batches.push(saleIds.slice(i, i + 200));
  const itemResults = await Promise.all(
    batches.map(batch =>
      sb.from('sale_items')
        .select('sale_id, sku, raw_name, qty, amount, category')
        .in('sale_id', batch)
        .in('category', TOP_BY_CATEGORY_INTERESTING)
    )
  );
  const items = itemResults.flatMap(r => r.data || []);

  // 3. Агрегируем по (category, base_model)
  // Структура: byCatBase[category][baseLower] = { name, qty, revenue, orders:Set, sampleSku }
  const byCatBase = {};
  for (const it of items) {
    const cat = it.category;
    if (!TOP_BY_CATEGORY_INTERESTING.includes(cat)) continue;
    const base = baseModelName(it.raw_name);
    // Если raw_name пустой, fallback на sku — но для группировки используем sku as-is
    const key = base || it.sku;
    if (!key) continue;
    const keyLower = key.toLowerCase();
    if (!byCatBase[cat]) byCatBase[cat] = {};
    if (!byCatBase[cat][keyLower]) byCatBase[cat][keyLower] = {
      name: base || it.sku || '?',
      base_name: base,
      qty_sold: 0,
      revenue: 0,
      orders: new Set(),
      sample_sku: it.sku || null,
      // counts для разных цветов — для отображения «(LG×3, GM×5, IN×2)»
      color_breakdown: {},
    };
    const e = byCatBase[cat][keyLower];
    e.qty_sold += it.qty || 1;
    e.revenue += it.amount || 0;
    e.orders.add(it.sale_id);
    if (!e.sample_sku && it.sku) e.sample_sku = it.sku;
    // Track color suffix for breakdown (если есть)
    const m = (it.raw_name || '').match(/\s+([A-Z]{2,3})\s*$/);
    const color = m ? m[1].toUpperCase() : null;
    if (color) e.color_breakdown[color] = (e.color_breakdown[color] || 0) + (it.qty || 1);
  }

  // 4. Top-N в каждой категории по qty_sold
  const by_category = {};
  const totals = {};
  for (const cat of TOP_BY_CATEGORY_INTERESTING) {
    const arr = Object.values(byCatBase[cat] || {})
      .map(e => ({
        name: e.name,
        base_name: e.base_name,
        sample_sku: e.sample_sku,
        qty_sold: Math.round(e.qty_sold),
        revenue: Math.round(e.revenue),
        orders: e.orders.size,
        color_breakdown: e.color_breakdown,
      }))
      .sort((a, b) => b.qty_sold - a.qty_sold)
      .slice(0, top);
    by_category[cat] = arr;
    totals[cat] = {
      qty: arr.reduce((s, x) => s + x.qty_sold, 0),
      revenue: arr.reduce((s, x) => s + x.revenue, 0),
      label: CATEGORY_RU[cat] || cat,
    };
  }

  const result = {
    city,
    year,
    by_category,
    totals,
    total_orders: saleIds.length,
  };
  cacheSet(ck, result);
  return result;
}
