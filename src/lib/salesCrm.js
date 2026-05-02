/**
 * Sales CRM module — Omoikiri-only.
 *
 * Слой между REST endpoints / dashboard / AI tools и Supabase-данными
 * импортированных Excel-отчётов о продажах.
 *
 * Использует views из миграции 0015:
 *   - v_partner_full       — карточка партнёра (sales + chat aggregates)
 *   - v_partner_chat_link  — partner_contact ↔ WhatsApp session
 *   - v_followups_due      — followups готовые к действию
 *
 * Все функции принимают `req` (express request), чтобы автоматически выбрать:
 *   - req.userClient (JWT path, RLS-aware)  — для запросов из дашборда
 *   - serviceClient  (x-api-key path)       — для admin / cron / тестов
 *
 * NOTE: эта функциональность доступна ТОЛЬКО в Omoikiri sample bridge,
 * НЕ в template (миграции 0011/0012/0015 — Omoikiri-only).
 */

import { supabase as serviceClient } from '../storage/supabase.js';
import { z } from 'zod';

function pickClient(req) {
  return req?.userClient || serviceClient;
}

// ─── In-memory кэш с длинным TTL для тяжёлых аналитик-запросов ──────────────
//
// Аналитика по 2200+ заказам пересчитывается 1-3 секунды на холодный кэш.
// Источник данных — импорты Excel-файлов раз в месяц + редкие merge/agency-update
// из дашборда. Поэтому кэш живёт 24ч, а не 5 минут — нет смысла гонять 700+
// строк sales каждые 5 минут, если за это время ничего не изменилось.
//
// Cache-key включает userId (req.user?.userId), чтобы между разными
// авторизациями не было утечек.
//
// invalidateSalesCache() стирает кэш при write-операциях (merge,
// agency-update, followup-done). Это покрывает все мутации из UI.
// Для офлайн-импортов: рестарт bridge (Railway автодеплой) либо ручной
// POST /sales-crm/cache/invalidate.
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const _cache = new Map();
// SECURITY: x-api-key path использует serviceClient (RLS bypass).
// Кэшировать его результаты под общим ключом ОПАСНО — другой запрос
// под JWT user может получить service-level данные. Кэш ТОЛЬКО для JWT-юзеров.
function cacheKey(name, req, paramsObj) {
  if (!req?.user?.userId) return null; // service-role / no-auth → не кэшируем
  return `${name}|${req.user.userId}|${JSON.stringify(paramsObj || {})}`;
}
function cacheGet(key) {
  if (!key) return null;
  const e = _cache.get(key);
  if (!e) return null;
  if (Date.now() > e.expiresAt) { _cache.delete(key); return null; }
  return e.data;
}
function cacheSet(key, data) {
  if (!key) return; // service-role bypass — не сохраняем
  _cache.set(key, { data, expiresAt: Date.now() + CACHE_TTL_MS });
  // light cleanup чтобы Map не разрастался
  if (_cache.size > 200) {
    const cutoff = Date.now();
    for (const [k, v] of _cache) if (v.expiresAt < cutoff) _cache.delete(k);
  }
}
export function invalidateSalesCache() {
  _cache.clear();
}

// ─── Параллельная пагинация ─────────────────────────────────────────────────
// Вместо 7 последовательных range-запросов (medium 2200 sales = 2200/1000 = 3, items 6800/1000 = 7),
// делаем все запросы одновременно через Promise.all. Ускоряет загрузку 4-7x.
async function loadAllParallel(sb, table, selectFields, filters = {}) {
  // 1. Узнаём общее кол-во строк (head запрос)
  let cntQ = sb.from(table).select('*', { count: 'exact', head: true });
  if (filters.eq) for (const [k, v] of Object.entries(filters.eq)) cntQ = cntQ.eq(k, v);
  if (filters.notNull) for (const k of filters.notNull) cntQ = cntQ.not(k, 'is', null);
  if (filters.isNull) for (const k of filters.isNull) cntQ = cntQ.is(k, null);
  if (filters.gte) for (const [k, v] of Object.entries(filters.gte)) cntQ = cntQ.gte(k, v);
  if (filters.lte) for (const [k, v] of Object.entries(filters.lte)) cntQ = cntQ.lte(k, v);
  if (filters.like) for (const [k, v] of Object.entries(filters.like)) cntQ = cntQ.like(k, v);
  if (filters.notLike) for (const [k, v] of Object.entries(filters.notLike)) cntQ = cntQ.not(k, 'like', v);
  const { count } = await cntQ;
  const total = count || 0;
  if (total === 0) return [];

  const PAGE = 1000;
  const pages = Math.ceil(total / PAGE);
  const promises = [];
  for (let p = 0; p < pages; p++) {
    let q = sb.from(table).select(selectFields).range(p * PAGE, (p + 1) * PAGE - 1);
    if (filters.eq) for (const [k, v] of Object.entries(filters.eq)) q = q.eq(k, v);
    if (filters.notNull) for (const k of filters.notNull) q = q.not(k, 'is', null);
    if (filters.isNull) for (const k of filters.isNull) q = q.is(k, null);
    if (filters.gte) for (const [k, v] of Object.entries(filters.gte)) q = q.gte(k, v);
    if (filters.lte) for (const [k, v] of Object.entries(filters.lte)) q = q.lte(k, v);
    if (filters.like) for (const [k, v] of Object.entries(filters.like)) q = q.like(k, v);
    if (filters.notLike) for (const [k, v] of Object.entries(filters.notLike)) q = q.not(k, 'like', v);
    promises.push(q);
  }
  const results = await Promise.all(promises);
  return results.flatMap(r => r.data || []);
}

// UUID v4 — простая валидация (не строгая, но достаточная для SQL safety)
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isUuid(s) { return typeof s === 'string' && UUID_RE.test(s); }

// PostgREST .or() filter injection — экранируем символы которые ломают синтаксис.
// PostgREST или-фильтры разделяются запятыми и точкой, а скобки/звёздочки имеют особое значение в ilike.
// Удаляем всё что может изменить логику фильтра, оставляем только буквы/цифры/пробелы/дефис.
function safeFilterValue(s) {
  return String(s || '').replace(/[,()*\\%]/g, ' ').slice(0, 100).trim();
}

// ── City filter ──────────────────────────────────────────────────────────────
//
// Whitelist строго — только три значения. Zod enum для routes.js.
// applyCity() работает с PostgREST query-объектами и loadAllParallel-фильтрами.
//
// Использование с PostgREST:
//   q = applyCity(sb.from('sales').select('*'), city);
//
// Использование с loadAllParallel filters:
//   const filters = {};
//   if (city && city !== 'all') filters.eq = { ...filters.eq, city };
//   (или вызвать addCityFilter(filters, city))
//
export const CITY_ENUM = ['Алматы', 'Астана', 'all'];
export const citySchema = z.enum(['Алматы', 'Астана', 'all']).default('all');

// IMPORTANT (2026-05-02): "city" в аналитике = SHOP city (Астана/Алматы),
// а не delivery city (sales.city колонка может быть Шымкент/Атырау/др).
// Фильтр по shop city определяется через source_file префикс:
//   "Алматы — Январь 2024.xlsx"     → shop = Алматы
//   "Январь 2024.xlsx" / любое другое → shop = Астана
// Это исправляет 340 sales (Astana shop с доставкой в другой город),
// которые раньше выпадали из city='Астана' filter и потеря revenue 9.4M ₸/мес.

/** Применяет фильтр по SHOP city (Астана/Алматы) через source_file. */
function applyCity(query, city) {
  if (!city || city === 'all') return query;
  if (city === 'Алматы') return query.like('source_file', 'Алматы%');
  if (city === 'Астана') return query.not('source_file', 'like', 'Алматы%');
  return query;
}

/** Добавляет shop city в loadAllParallel filters (через source_file). */
function addCityFilter(filters, city) {
  if (!city || city === 'all') return filters;
  if (city === 'Алматы') return { ...filters, like: { ...(filters.like || {}), source_file: 'Алматы%' } };
  if (city === 'Астана') return { ...filters, notLike: { ...(filters.notLike || {}), source_file: 'Алматы%' } };
  return filters;
}

// ── Multi-city query parsing ──────────────────────────────────────────────────
//
// ?cities=Алматы,Астана  →  { mode: 'multi', cities: ['Алматы', 'Астана'] }
// ?cities=Алматы         →  { mode: 'single', cities: ['Алматы'] }
// ?city=Алматы           →  { mode: 'single', cities: ['Алматы'] }  (backward compat)
// (nothing)              →  { mode: 'all', cities: [] }
//
// Zod whitelist: только 'Алматы' и 'Астана'. 'all' — специальное значение, в cities[] не кладём.
//
const VALID_CITIES = new Set(['Алматы', 'Астана']);

/**
 * Парсит ?cities= (CSV) или fallback ?city= из query string.
 * Возвращает { ok, mode, cities, error }.
 *
 * mode:
 *   'all'    — фильтр не задан, данные по всем городам суммарно
 *   'single' — один конкретный город (использует старый path через opts.city)
 *   'multi'  — два города, нужен breakdown (разные цвета на графике)
 *
 * Используется routes.js как единая точка разбора query params.
 */
export function parseCitiesQuery(query) {
  // Новый параметр ?cities= имеет приоритет над ?city=
  const rawCities = query.cities;
  const rawCity = query.city;

  if (rawCities !== undefined && rawCities !== null && rawCities !== '') {
    const parts = String(rawCities)
      .split(',')
      .map(s => s.trim())
      .filter(Boolean);

    // Whitelist — отбрасываем невалидные значения
    const invalid = parts.filter(p => !VALID_CITIES.has(p) && p !== 'all');
    if (invalid.length > 0) {
      return { ok: false, mode: null, cities: [], error: `invalid cities: ${invalid.join(', ')}. Allowed: Алматы, Астана` };
    }

    // 'all' в списке → treated as no-filter
    const cities = parts.filter(p => p !== 'all');

    if (cities.length === 0) {
      return { ok: true, mode: 'all', cities: [] };
    }
    if (cities.length === 1) {
      return { ok: true, mode: 'single', cities };
    }
    // Deduplicate + sort for stable cache key
    const unique = [...new Set(cities)].sort();
    return { ok: true, mode: 'multi', cities: unique };
  }

  // Fallback: legacy ?city= (backward compat)
  if (rawCity !== undefined && rawCity !== null && rawCity !== '') {
    if (rawCity === 'all') return { ok: true, mode: 'all', cities: [] };
    if (VALID_CITIES.has(rawCity)) {
      return { ok: true, mode: 'single', cities: [rawCity] };
    }
    return { ok: false, mode: null, cities: [], error: 'city must be one of: Алматы, Астана, all' };
  }

  return { ok: true, mode: 'all', cities: [] };
}

/**
 * Запускает helperFn для каждого city в cities[], объединяет результаты.
 *
 * Format A (timeline / multi-year): каждый item в data массивах получает поле city.
 * Используется когда helperFn возвращает { data: [...] } или плоский массив.
 *
 * Конкретная функция-потребитель сама выбирает как мёрджить, потому что структура
 * у всех разная. withCityBreakdown — удобная обёртка для параллельного вызова:
 *
 *   const results = await withCityBreakdown(cities, async (c) => getSalesAnalytics(req, {...opts, city: c}));
 *   // results: { 'Алматы': {...}, 'Астана': {...} }
 *
 * Возвращает объект { byCity: { 'Алматы': result, 'Астана': result }, cities }.
 */
export async function withCityBreakdown(cities, helperFn) {
  const results = await Promise.all(cities.map(c => helperFn(c)));
  const byCity = {};
  for (let i = 0; i < cities.length; i++) {
    byCity[cities[i]] = results[i];
  }
  return { byCity, cities };
}

/**
 * Добавляет поле `city` к каждому item в массиве.
 * Используется для Format A (timeline items).
 */
export function tagItemsWithCity(items, city) {
  return (items || []).map(item => ({ ...item, city }));
}

// ── 1. listPartners ─────────────────────────────────────────────────────────
//
// Возвращает список партнёров с агрегатами. Поддерживает фильтры и поиск.
//
// filter:
//   'all'           — все с заказами (orders_count > 0)
//   'with_chat'     — только те у кого есть привязанный WhatsApp
//   'top_revenue'   — отсортированы по выручке desc
//   'no_phone'      — без телефона (рискованные для re-engage)
//   'recent'        — последняя продажа за 30 дней
//
// ─── Tier / Activity helpers ─────────────────────────────────────────────────
// Tier: Gold / Silver / Bronze — по total_purchases_amount из view v_partner_full.
// Activity: HOT / WARM / COLD — по last_purchase_date.
//
// Thresholds (₸):
//   Gold   >= 5 000 000
//   Silver  1 000 000 — 4 999 999
//   Bronze  > 0 (но < 1 000 000)
//
// Activity (дней с последней покупки):
//   HOT   < 30
//   WARM  30 — 90
//   COLD  > 90
//
const TIER_GOLD_THRESHOLD   = 5_000_000;
const TIER_SILVER_THRESHOLD = 1_000_000;

function computeTier(totalRevenue) {
  const rev = totalRevenue || 0;
  if (rev >= TIER_GOLD_THRESHOLD)   return 'Gold';
  if (rev >= TIER_SILVER_THRESHOLD) return 'Silver';
  return 'Bronze';
}

function computeActivity(lastPurchaseDate) {
  if (!lastPurchaseDate) return 'COLD';
  const daysSince = Math.floor((Date.now() - new Date(lastPurchaseDate).getTime()) / 86_400_000);
  if (daysSince < 30)  return 'HOT';
  if (daysSince <= 90) return 'WARM';
  return 'COLD';
}

// Парсим CSV tier / activity из query param.
// Принимает строку '«Gold,Silver»' → ['Gold','Silver']. Unknown значения отфильтровываются.
const VALID_TIERS      = new Set(['Gold', 'Silver', 'Bronze']);
const VALID_ACTIVITIES = new Set(['HOT', 'WARM', 'COLD']);

function parseCsvParam(raw, validSet) {
  if (!raw) return [];
  return String(raw).split(',').map(s => s.trim()).filter(s => validSet.has(s));
}

const listPartnersInput = z.object({
  filter: z.enum(['all', 'with_chat', 'top_revenue', 'no_phone', 'recent']).default('all'),
  q: z.string().max(200).optional().default(''),
  limit: z.number().int().min(1).max(500).default(100),
  offset: z.number().int().min(0).default(0),
  city: citySchema,
  // Tier и activity — CSV, валидация в коде (parseCsvParam)
  tier:     z.string().optional(),
  activity: z.string().optional(),
});

export async function listPartners(req, params = {}) {
  const args = listPartnersInput.parse(params);

  // Tier/activity фильтры — парсим заранее (для cache key + фильтрации)
  const tierFilter     = parseCsvParam(args.tier,     VALID_TIERS);
  const activityFilter = parseCsvParam(args.activity, VALID_ACTIVITIES);

  // Кэшируем по полному набору параметров — limit/offset включены, чтобы pagination работал
  const ck = cacheKey('list-partners', req, args);
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  let q = sb.from('v_partner_full').select('*');

  // Common: только с заказами (мусорные сироты должны быть удалены, но защитимся)
  q = q.gt('orders_count', 0);

  // NOTE: v_partner_full — view поверх partner_contacts + sales агрегатов.
  // city-фильтр на view не имеет смысла (у контакта может быть продажи из двух
  // городов). Фильтр хранится в cacheKey для cache isolation, но не применяется
  // к query самого view — это read-only список партнёров независимо от города.
  // Для city-разбивки используйте /sales-crm/analytics?city=...

  if (args.filter === 'with_chat') q = q.gt('total_messages', 0);
  if (args.filter === 'no_phone') q = q.is('primary_phone', null);
  if (args.filter === 'recent') {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    q = q.gte('last_purchase_date', d.toISOString().slice(0, 10));
  }

  if (args.q) {
    // Экранируем — PostgREST .or() уязвим к injection через запятые/скобки
    const s = safeFilterValue(args.q);
    if (s) q = q.or(`canonical_name.ilike.%${s}%,primary_phone.ilike.%${s}%`);
  }

  // Sort — всегда по revenue desc, tier/activity — post-filter (в JS, не SQL)
  q = q.order('total_revenue', { ascending: false });

  // Если есть tier/activity фильтр — нужно загрузить больше строк для in-memory фильтрации,
  // потому что PostgREST не знает про computed tier/activity.
  // Стратегия: если фильтр задан → грузим весь набор (до 1000 = max page PostgREST), фильтруем, пагинируем в JS.
  // Если фильтра нет → standard DB-side pagination (быстро для 100-200 партнёров).
  const hasTierActivityFilter = tierFilter.length > 0 || activityFilter.length > 0;

  if (!hasTierActivityFilter) {
    q = q.range(args.offset, args.offset + args.limit - 1);
  }
  // Иначе — не добавляем .range(), грузим всё (у нас < 500 уникальных партнёров)

  const { data, error } = await q;
  if (error) throw new Error(`listPartners: ${error.message}`);

  // Enrich each partner with tier + activity computed fields
  let enriched = (data || []).map(p => ({
    ...p,
    tier:     computeTier(p.total_revenue),
    activity: computeActivity(p.last_purchase_date),
  }));

  // Post-query фильтрация по tier / activity
  if (tierFilter.length > 0) {
    enriched = enriched.filter(p => tierFilter.includes(p.tier));
  }
  if (activityFilter.length > 0) {
    enriched = enriched.filter(p => activityFilter.includes(p.activity));
  }

  // JS-side pagination при наличии tier/activity фильтра
  const totalFiltered = enriched.length;
  if (hasTierActivityFilter) {
    enriched = enriched.slice(args.offset, args.offset + args.limit);
  }

  const result = { items: enriched, limit: args.limit, offset: args.offset, total: totalFiltered };
  cacheSet(ck, result);
  return result;
}

// ── 2. getPartnerCard ───────────────────────────────────────────────────────
//
// Полная карточка партнёра: основные поля + последние N заказов с позициями
// + связанные WhatsApp-сессии + последние 3 AI-анализа диалогов.
//
export async function getPartnerCard(req, contactId, { recentSales = 50, recentAi = 3 } = {}) {
  if (!isUuid(contactId)) throw new Error('invalid contact id');
  const sb = pickClient(req);

  // 1. Основная карточка из view
  const { data: card, error: ce } = await sb.from('v_partner_full').select('*').eq('id', contactId).maybeSingle();
  if (ce) throw new Error(`getPartnerCard: ${ce.message}`);
  if (!card) return null;

  // 2. Последние N заказов где контакт = customer ИЛИ partner
  // (через 2 запроса + merge, потому что .or() с join cross-condition сложен)
  const [{ data: asCust }, { data: asPart }] = await Promise.all([
    sb.from('sales').select('*').eq('customer_id', contactId).order('sale_date', { ascending: false }).limit(recentSales),
    sb.from('sales').select('*').eq('partner_id', contactId).order('sale_date', { ascending: false }).limit(recentSales),
  ]);
  const merged = [];
  for (const s of asCust || []) merged.push({ ...s, _role: 'customer' });
  for (const s of asPart || []) merged.push({ ...s, _role: 'partner' });
  merged.sort((a, b) => (b.sale_date || '').localeCompare(a.sale_date || ''));
  const recentSalesList = merged.slice(0, recentSales);

  // Подгружаем позиции для этих заказов
  if (recentSalesList.length > 0) {
    const ids = recentSalesList.map(s => s.id);
    const { data: items } = await sb.from('sale_items')
      .select('sale_id, position_idx, raw_name, sku, qty, price_per_unit, amount, category')
      .in('sale_id', ids);
    const byS = {};
    for (const it of items || []) (byS[it.sale_id] = byS[it.sale_id] || []).push(it);
    for (const s of recentSalesList) {
      s.items = (byS[s.id] || []).sort((a, b) => (a.position_idx || 0) - (b.position_idx || 0));
    }
  }

  // 3. Связанные WhatsApp-сессии
  const { data: chatLinks } = await sb.from('v_partner_chat_link')
    .select('session_id, remote_jid, message_count, last_message_at, first_message_at')
    .eq('contact_id', contactId)
    .order('last_message_at', { ascending: false });

  // 4. Последние AI-анализы диалогов (если есть messages → есть chat_ai)
  let recentAnalysis = [];
  if (chatLinks && chatLinks.length > 0) {
    const jids = [...new Set(chatLinks.map(l => l.remote_jid))];
    const { data: ai } = await sb.from('chat_ai')
      .select('id, analyzed_at, intent, lead_temperature, summary_ru, deal_stage, manager_issues, risk_flags, session_id, remote_jid')
      .in('remote_jid', jids)
      .order('analyzed_at', { ascending: false })
      .limit(recentAi);
    recentAnalysis = ai || [];
  }

  // 5. Pending followups
  const { data: followups } = await sb.from('v_followups_due')
    .select('followup_id, due_date, followup_type, note, urgency, related_sale_date, related_sale_total')
    .eq('contact_id', contactId)
    .order('due_date', { ascending: true });

  return {
    card,
    recent_sales: recentSalesList,
    chat_sessions: chatLinks || [],
    recent_ai_analysis: recentAnalysis,
    pending_followups: followups || [],
  };
}

// ── 3. getStudioCard ────────────────────────────────────────────────────────
//
// Карточка студии: метаданные + список партнёров + общая статистика заказов.
//
export async function getStudioCard(req, agencyId) {
  if (!isUuid(agencyId)) throw new Error('invalid agency id');
  const sb = pickClient(req);

  const [{ data: agency }, { data: partners }, { data: sales }] = await Promise.all([
    sb.from('agencies').select('*').eq('id', agencyId).maybeSingle(),
    sb.from('v_partner_full').select('*').eq('agency_id', agencyId).gt('orders_count', 0).order('total_revenue', { ascending: false }),
    sb.from('sales').select('id, sale_date, order_num, total_amount, customer_raw, partner_raw, manager, status_text, source_file')
      .eq('agency_id', agencyId).order('sale_date', { ascending: false }).limit(200),
  ]);
  if (!agency) return null;

  const totalRevenue = (partners || []).reduce((s, p) => s + (p.total_revenue || 0), 0);
  const totalOrders = (sales || []).length;

  return {
    agency,
    partners: partners || [],
    recent_sales: sales || [],
    aggregates: {
      partner_count: (partners || []).length,
      total_orders: totalOrders,
      total_revenue: totalRevenue,
    },
  };
}

// ── 4. getDueFollowups ──────────────────────────────────────────────────────
//
// Followups готовые к действию (overdue / this_week / this_month).
//
const dueFollowupsInput = z.object({
  window_days: z.number().int().min(1).max(365).default(30),
  limit: z.number().int().min(1).max(500).default(200),
});

export async function getDueFollowups(req, params = {}) {
  const args = dueFollowupsInput.parse(params);
  // NOTE: v_followups_due нет поля city — followups привязаны к partner_contact,
  // не к городу. city используется только для cache isolation.
  const city = params.city || 'all';
  const ck = cacheKey('followups-due', req, { ...args, city });
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() + args.window_days);
  const cutoffIso = cutoff.toISOString().slice(0, 10);

  const { data, error } = await sb.from('v_followups_due')
    .select('*')
    .lte('due_date', cutoffIso)
    .order('due_date', { ascending: true })
    .limit(args.limit);

  if (error) throw new Error(`getDueFollowups: ${error.message}`);

  // Группа по срочности
  const buckets = { overdue: [], this_week: [], this_month: [], later: [] };
  for (const f of data || []) {
    (buckets[f.urgency] || buckets.later).push(f);
  }
  const result = { ...buckets, total: (data || []).length };
  cacheSet(ck, result);
  return result;
}

// ── 5. markFollowupDone ─────────────────────────────────────────────────────
//
// Пометить followup выполненным. action: 'sent' | 'sold' | 'declined' | 'skipped'.
//
const markDoneInput = z.object({
  action: z.enum(['sent', 'sold', 'declined', 'skipped']).default('sent'),
  note: z.string().max(500).optional(),
});

export async function markFollowupDone(req, followupId, payload = {}) {
  if (!isUuid(followupId)) throw new Error('invalid followup id');
  const args = markDoneInput.parse(payload);
  const sb = pickClient(req);

  const completedAt = new Date().toISOString();
  // Сохраняем action в поле note (followups.note существующее поле).
  // Если уже было что-то в note — апендим.
  const { data: existing } = await sb.from('followups').select('note').eq('id', followupId).maybeSingle();
  const oldNote = existing?.note || '';
  const newNote = args.note
    ? `${oldNote}${oldNote ? '\n' : ''}[${completedAt.slice(0,10)}] ${args.action}: ${args.note}`
    : `${oldNote}${oldNote ? '\n' : ''}[${completedAt.slice(0,10)}] ${args.action}`;

  const { error } = await sb.from('followups')
    .update({ completed_at: completedAt, note: newNote })
    .eq('id', followupId);
  if (error) throw new Error(`markFollowupDone: ${error.message}`);
  invalidateSalesCache();
  return { ok: true, followup_id: followupId, completed_at: completedAt, action: args.action };
}

// ── 6. findPartnerByPhone (cross-link чат → партнёр) ────────────────────────
//
// По телефону (raw из jid или из dashboard) найти партнёра в sales-CRM.
// Возвращает компактную карточку для плашки в ChatView, или null если такого нет.
//
// Phone приходит в разных форматах: 77011234567, 77011234567@s.whatsapp.net,
// +77011234567, 7-701-123-45-67. Нормализуем до 11 digits, primary_phone в БД
// тоже хранится как 11 digits (7XXXXXXXXX).
//
function normalizePhoneForLookup(raw) {
  if (!raw) return null;
  const noJid = String(raw).split('@')[0];
  const digits = noJid.replace(/\D/g, '');
  if (digits.length < 10) return null;
  if (digits.length === 11 && digits.startsWith('8')) return '7' + digits.slice(1);
  if (digits.length === 11 && digits.startsWith('7')) return digits;
  if (digits.length === 10) return '7' + digits;
  return digits;
}

export async function findPartnerByPhone(req, phoneRaw) {
  const phone = normalizePhoneForLookup(phoneRaw);
  if (!phone) return null;
  const sb = pickClient(req);

  const { data, error } = await sb.from('v_partner_full')
    .select('id, canonical_name, primary_phone, agency_name, roles, orders_count, total_revenue, last_purchase_date')
    .eq('primary_phone', phone)
    .gt('orders_count', 0)
    .maybeSingle();

  if (error) throw new Error(`findPartnerByPhone: ${error.message}`);
  if (!data) return null;

  // Краткая сводка категорий из последних 20 заказов (для подсказки upsell-а)
  // Через 2 запроса (customer + partner role), так проще с PostgREST.
  const [{ data: salesAsCust }, { data: salesAsPart }] = await Promise.all([
    sb.from('sales').select('id').eq('customer_id', data.id).order('sale_date', { ascending: false }).limit(20),
    sb.from('sales').select('id').eq('partner_id', data.id).order('sale_date', { ascending: false }).limit(20),
  ]);
  const saleIds = [...new Set([...(salesAsCust || []), ...(salesAsPart || [])].map(s => s.id))];
  const cats = {};
  if (saleIds.length > 0) {
    const { data: recentItems } = await sb.from('sale_items')
      .select('category')
      .in('sale_id', saleIds);
    for (const it of recentItems || []) {
      const c = it.category || 'other';
      cats[c] = (cats[c] || 0) + 1;
    }
  }

  // Ближайший pending followup (например — картридж через месяц)
  const { data: nextFollowup } = await sb.from('v_followups_due')
    .select('followup_id, due_date, followup_type, urgency, note')
    .eq('contact_id', data.id)
    .order('due_date', { ascending: true })
    .limit(1)
    .maybeSingle();

  return {
    ...data,
    top_categories: Object.entries(cats).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([cat, n]) => ({ cat, n })),
    next_followup: nextFollowup || null,
  };
}

// ── 5b. mergePartners ──────────────────────────────────────────────────────
//
// Слить контакт source_id в target_id. Все sales/followups перейдут на target,
// aliases/phones/roles объединятся, source-запись удалится.
// Идемпотентно НЕ является — после merge source_id больше не существует.
//
const mergeInput = z.object({
  target_id: z.string().regex(UUID_RE),
});

// Атомарный merge через Postgres RPC `merge_partners` (миграция 0016).
// Раньше делали 5 sequential UPDATE через PostgREST без error-check'ов на 3
// из 5 — mid-flight failure оставлял половину переехавшими. RPC выполняется
// в одной транзакции, любой RAISE откатывает всё.
export async function mergePartners(req, sourceId, payload) {
  if (!isUuid(sourceId)) throw new Error('invalid source id');
  const args = mergeInput.parse(payload);
  if (args.target_id === sourceId) throw new Error('source and target are the same');
  const sb = pickClient(req);

  const { data, error } = await sb.rpc('merge_partners', {
    source_id: sourceId,
    target_id: args.target_id,
  });
  if (error) throw new Error(`mergePartners: ${error.message}`);

  invalidateSalesCache();
  return data; // { ok, merged_into, sales_customer_moved, sales_partner_moved, followups_moved }
}

// ── 5c. updatePartnerAgency ─────────────────────────────────────────────────
const agencyUpdateInput = z.object({
  agency_id: z.string().regex(UUID_RE).nullable(),
});

export async function updatePartnerAgency(req, contactId, payload) {
  if (!isUuid(contactId)) throw new Error('invalid contact id');
  const args = agencyUpdateInput.parse(payload);
  const sb = pickClient(req);

  const { error } = await sb.from('partner_contacts')
    .update({ agency_id: args.agency_id || null })
    .eq('id', contactId);
  if (error) throw new Error(`updatePartnerAgency: ${error.message}`);
  invalidateSalesCache();
  return { ok: true };
}

// ── 6b. listAgencies ────────────────────────────────────────────────────────
//
// Список всех студий с агрегатами по продажам. Используется в Studios page.
//
export async function listAgencies(req, opts = {}) {
  const city = opts.city || 'all';
  // Tier/activity фильтры для агентств (аналог listPartners)
  const tierFilter     = parseCsvParam(opts.tier,     VALID_TIERS);
  const activityFilter = parseCsvParam(opts.activity, VALID_ACTIVITIES);

  const ck = cacheKey('list-agencies', req, { city, tier: opts.tier, activity: opts.activity });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // Все agencies + sales-aggregates через 2 запроса
  // City-фильтр применяется к sales (агрегаты по выручке/заказам из нужного города)
  // Добавляем sale_date для вычисления last_purchase_date → activity
  let salesQ = sb.from('sales').select('agency_id, total_amount, sale_date').not('agency_id', 'is', null);
  salesQ = applyCity(salesQ, city);
  const [{ data: agencies }, { data: salesAgg }, { data: contactsAgg }] = await Promise.all([
    sb.from('agencies').select('id, canonical_name, city, notes'),
    salesQ,
    sb.from('partner_contacts').select('id, agency_id').not('agency_id', 'is', null),
  ]);

  const byAgency = {};
  for (const s of salesAgg || []) {
    if (!byAgency[s.agency_id]) byAgency[s.agency_id] = { orders: 0, revenue: 0, last_purchase_date: null };
    byAgency[s.agency_id].orders++;
    byAgency[s.agency_id].revenue += s.total_amount || 0;
    // Tracking last_purchase_date для activity
    if (s.sale_date) {
      if (!byAgency[s.agency_id].last_purchase_date || s.sale_date > byAgency[s.agency_id].last_purchase_date) {
        byAgency[s.agency_id].last_purchase_date = s.sale_date;
      }
    }
  }
  const contactsByAgency = {};
  for (const c of contactsAgg || []) {
    contactsByAgency[c.agency_id] = (contactsByAgency[c.agency_id] || 0) + 1;
  }

  let items = (agencies || []).map(a => {
    const agg = byAgency[a.id];
    const revenue = agg?.revenue || 0;
    const lastPurchaseDate = agg?.last_purchase_date || null;
    return {
      ...a,
      orders:             agg?.orders || 0,
      revenue,
      contacts:           contactsByAgency[a.id] || 0,
      last_purchase_date: lastPurchaseDate,
      // Computed tier + activity
      tier:               computeTier(revenue),
      activity:           computeActivity(lastPurchaseDate),
    };
  }).filter(a => a.orders > 0 || a.contacts > 0)
    .sort((a, b) => b.revenue - a.revenue);

  // Post-query фильтрация по tier / activity
  if (tierFilter.length > 0) {
    items = items.filter(a => tierFilter.includes(a.tier));
  }
  if (activityFilter.length > 0) {
    items = items.filter(a => activityFilter.includes(a.activity));
  }

  const result = { items };
  cacheSet(ck, result);
  return result;
}

// ── 7. getSalesAnalytics ────────────────────────────────────────────────────
//
// Расширенные агрегаты для дашборда «Аналитика продаж».
//
// Группа A (расширения 2026-04-30):
//   - timeline: выручка + средний чек по месяцам
//   - kpi с PoP/YoY дельтами (current_month vs previous_month / prev_year)
//   - movers: топ-5 best / worst (партнёры с наибольшим Δ за период)
//   - pareto: концентрация выручки (для графика Лоренца)
//   - top_studios/partners теперь со sparkline (12-мес миник по выручке)
//
// Принимает date_from / date_to для фильтрации; если не задано — всё время.
//
const analyticsInput = z.object({
  date_from: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  date_to:   z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  channel:   z.enum(['all', 'b2b', 'b2c']).default('all'),
  // YYYY-MM — целевой месяц для расчёта movers (this vs previous).
  // Если не указан — используется последний месяц с данными (default).
  compare_month: z.string().regex(/^\d{4}-\d{2}$/).optional(),
  city: citySchema,
});

// Helper: добавляем 1 месяц к YYYY-MM
function nextMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return m === 12 ? `${y + 1}-01` : `${y}-${String(m + 1).padStart(2, '0')}`;
}
function prevMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return m === 1 ? `${y - 1}-12` : `${y}-${String(m - 1).padStart(2, '0')}`;
}
function yoyMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return `${y - 1}-${String(m).padStart(2, '0')}`;
}
function pctDelta(curr, prev) {
  if (!prev) return null;
  return Math.round(((curr - prev) / prev) * 1000) / 10;
}

export async function getSalesAnalytics(req, params = {}) {
  const args = analyticsInput.parse(params);

  // Кэш проверяем ДО любой работы
  const ck = cacheKey('analytics', req, args);
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  // Параллельная пагинация для sales (с фильтрами)
  let filters = {};
  if (args.date_from) filters.gte = { sale_date: args.date_from };
  if (args.date_to) filters.lte = { sale_date: args.date_to };
  if (args.channel === 'b2b') filters.notNull = ['partner_id'];
  if (args.channel === 'b2c') filters.isNull = ['partner_id'];
  filters = addCityFilter(filters, args.city);
  const sales = await loadAllParallel(sb, 'sales',
    'id, sale_date, total_amount, agency_id, partner_id, customer_id',
    filters
  );

  // Bucket by month
  const monthly = {};
  let totalRevenue = 0;
  for (const s of sales) {
    if (!s.sale_date) continue;
    const m = s.sale_date.slice(0, 7);
    if (!monthly[m]) monthly[m] = { month: m, orders: 0, revenue: 0 };
    monthly[m].orders++;
    monthly[m].revenue += s.total_amount || 0;
    totalRevenue += s.total_amount || 0;
  }
  // Average check per month
  for (const m of Object.values(monthly)) {
    m.avg_check = m.orders > 0 ? Math.round(m.revenue / m.orders) : 0;
  }
  const timeline = Object.values(monthly).sort((a, b) => a.month.localeCompare(b.month));

  // 2. Top studios
  const byAgency = {};
  for (const s of sales) {
    if (!s.agency_id) continue;
    if (!byAgency[s.agency_id]) byAgency[s.agency_id] = { agency_id: s.agency_id, orders: 0, revenue: 0 };
    byAgency[s.agency_id].orders++;
    byAgency[s.agency_id].revenue += s.total_amount || 0;
  }
  const topAgenciesIds = Object.values(byAgency).sort((a, b) => b.revenue - a.revenue).slice(0, 15);
  const { data: agencies } = await sb.from('agencies').select('id, canonical_name, city').in('id', topAgenciesIds.map(a => a.agency_id));
  const aMap = new Map((agencies || []).map(a => [a.id, a]));
  const top_studios = topAgenciesIds.map(a => ({
    ...a,
    name: aMap.get(a.agency_id)?.canonical_name || '?',
    city: aMap.get(a.agency_id)?.city || null,
  }));

  // 3. Top partners (any role: customer or partner)
  const byContact = {};
  for (const s of sales) {
    for (const id of [s.customer_id, s.partner_id].filter(Boolean)) {
      if (!byContact[id]) byContact[id] = { contact_id: id, orders: 0, revenue: 0 };
      byContact[id].orders++;
      byContact[id].revenue += s.total_amount || 0;
    }
  }
  const topPartnersIds = Object.values(byContact).sort((a, b) => b.revenue - a.revenue).slice(0, 15);
  const { data: contacts } = await sb.from('partner_contacts')
    .select('id, canonical_name, primary_phone, agency_id')
    .in('id', topPartnersIds.map(p => p.contact_id));
  const cMap = new Map((contacts || []).map(c => [c.id, c]));
  const top_partners = topPartnersIds.map(p => ({
    ...p,
    name: cMap.get(p.contact_id)?.canonical_name || '?',
    phone: cMap.get(p.contact_id)?.primary_phone || null,
    agency_id: cMap.get(p.contact_id)?.agency_id || null,
  }));

  // 4. Categories — city-aware: если city задан, берём только items продаж этого города.
  // sale_items не имеют поля city, поэтому сначала получаем sale_ids нужного города,
  // потом items батчами по 500 (PostgREST .in() limit).
  let items;
  if (args.city && args.city !== 'all') {
    // Используем уже загруженные sales — они уже city-filtered (addCityFilter выше)
    const cityFilteredSaleIds = sales.map(s => s.id).filter(Boolean);
    if (cityFilteredSaleIds.length === 0) {
      items = [];
    } else {
      const batches = [];
      for (let i = 0; i < cityFilteredSaleIds.length; i += 500) {
        batches.push(cityFilteredSaleIds.slice(i, i + 500));
      }
      const batchResults = await Promise.all(
        batches.map(batch => sb.from('sale_items').select('category, sale_id, amount').in('sale_id', batch))
      );
      items = batchResults.flatMap(r => r.data || []);
    }
  } else {
    items = await loadAllParallel(sb, 'sale_items', 'category, sale_id, amount');
  }
  const cats = {};
  for (const it of items || []) {
    const c = it.category || 'other';
    if (!cats[c]) cats[c] = { category: c, count: 0, revenue: 0 };
    cats[c].count++;
    cats[c].revenue += it.amount || 0;
  }
  const categories = Object.values(cats).sort((a, b) => b.count - a.count);

  // 5. Segments (one-time vs repeat customers, filter buyers)
  const customerOrders = {};
  for (const s of sales) {
    const cid = s.customer_id;
    if (!cid) continue;
    customerOrders[cid] = (customerOrders[cid] || 0) + 1;
  }
  const oneTime = Object.values(customerOrders).filter(n => n === 1).length;
  const repeat = Object.values(customerOrders).filter(n => n > 1).length;

  // Filter customers (по items)
  const filterCustomerIds = new Set();
  for (const it of items || []) {
    if (it.category !== 'water_filter') continue;
    // sale_id → customer_id: ищем в sales
    const sale = sales.find(s => s.id === it.sale_id);
    if (sale?.customer_id) filterCustomerIds.add(sale.customer_id);
  }

  const segments = {
    one_time_customers: oneTime,
    repeat_customers: repeat,
    filter_buyers: filterCustomerIds.size,
  };

  // 6. KPI с PoP/YoY деltas
  // По умолчанию «текущий» месяц = последний месяц с заказами в timeline.
  // Если указан compare_month — используем его (для произвольного сравнения).
  const currMonth = args.compare_month || timeline[timeline.length - 1]?.month || null;
  const prevMo = currMonth ? prevMonth(currMonth) : null;
  const yoyMo = currMonth ? yoyMonth(currMonth) : null;
  const m_curr = currMonth ? monthly[currMonth] : null;
  const m_prev = prevMo ? monthly[prevMo] : null;
  const m_yoy  = yoyMo ? monthly[yoyMo] : null;

  const kpi = {
    total_orders: sales.length,
    total_revenue: totalRevenue,
    avg_check: sales.length > 0 ? Math.round(totalRevenue / sales.length) : 0,
    months_covered: timeline.length,
    current_month: currMonth,
    current_month_orders: m_curr?.orders || 0,
    current_month_revenue: m_curr?.revenue || 0,
    current_month_avg_check: m_curr?.avg_check || 0,
    pop_orders_pct: m_curr && m_prev ? pctDelta(m_curr.orders, m_prev.orders) : null,
    pop_revenue_pct: m_curr && m_prev ? pctDelta(m_curr.revenue, m_prev.revenue) : null,
    pop_avg_check_pct: m_curr && m_prev ? pctDelta(m_curr.avg_check, m_prev.avg_check) : null,
    yoy_orders_pct: m_curr && m_yoy ? pctDelta(m_curr.orders, m_yoy.orders) : null,
    yoy_revenue_pct: m_curr && m_yoy ? pctDelta(m_curr.revenue, m_yoy.revenue) : null,
  };

  // 7. Movers — top-5 best & worst партнёры по дельте текущий vs прошлый месяц.
  // Считаем выручку каждого партнёра в curr и prev, ранжируем.
  let movers_top = [], movers_bottom = [];
  if (currMonth && prevMo) {
    const byPartnerCurr = {}, byPartnerPrev = {};
    const cFrom = `${currMonth}-01`, cTo = nextMonth(currMonth) + '-01';
    const pFrom = `${prevMo}-01`, pTo = nextMonth(prevMo) + '-01';
    for (const s of sales) {
      const id = s.partner_id || s.customer_id;
      if (!id || !s.sale_date) continue;
      if (s.sale_date >= cFrom && s.sale_date < cTo) {
        byPartnerCurr[id] = (byPartnerCurr[id] || 0) + (s.total_amount || 0);
      } else if (s.sale_date >= pFrom && s.sale_date < pTo) {
        byPartnerPrev[id] = (byPartnerPrev[id] || 0) + (s.total_amount || 0);
      }
    }
    const allIds = new Set([...Object.keys(byPartnerCurr), ...Object.keys(byPartnerPrev)]);
    const movers = [...allIds].map(id => ({
      contact_id: id,
      curr_revenue: byPartnerCurr[id] || 0,
      prev_revenue: byPartnerPrev[id] || 0,
      delta: (byPartnerCurr[id] || 0) - (byPartnerPrev[id] || 0),
    })).filter(m => m.curr_revenue >= 100000 || m.prev_revenue >= 100000); // нерелевантную мелочь убираем

    // Нужны имена
    const moverIds = movers.map(m => m.contact_id);
    const { data: moverContacts } = moverIds.length > 0
      ? await sb.from('partner_contacts').select('id, canonical_name, primary_phone').in('id', moverIds)
      : { data: [] };
    const mcMap = new Map((moverContacts || []).map(c => [c.id, c]));
    for (const m of movers) {
      m.name = mcMap.get(m.contact_id)?.canonical_name || '?';
      m.phone = mcMap.get(m.contact_id)?.primary_phone || null;
    }

    movers_top = movers.filter(m => m.delta > 0).sort((a, b) => b.delta - a.delta).slice(0, 5);
    movers_bottom = movers.filter(m => m.delta < 0).sort((a, b) => a.delta - b.delta).slice(0, 5);
  }

  // 8. Pareto — % выручки от топ-N% партнёров (для curve).
  const allPartnerRevenues = Object.values(byContact).map(p => p.revenue).sort((a, b) => b - a);
  const totalPartnerRevenue = allPartnerRevenues.reduce((s, x) => s + x, 0);
  const pareto = [];
  let cumPct = 0;
  for (let i = 0; i < allPartnerRevenues.length; i++) {
    cumPct += allPartnerRevenues[i] / (totalPartnerRevenue || 1) * 100;
    const partnerPct = ((i + 1) / allPartnerRevenues.length) * 100;
    if (i % Math.max(1, Math.floor(allPartnerRevenues.length / 50)) === 0 || i === allPartnerRevenues.length - 1) {
      pareto.push({
        partner_pct: Math.round(partnerPct * 10) / 10,
        revenue_pct: Math.round(cumPct * 10) / 10,
      });
    }
  }

  // 9. Sparkline data для топ-15 партнёров и студий — 12-мес массив выручки.
  // Ограничиваем последними 12 месяцами от curr_month (или today).
  const sparkLastN = 12;
  const sparkMonths = [];
  if (currMonth) {
    let m = currMonth;
    for (let i = 0; i < sparkLastN; i++) {
      sparkMonths.unshift(m);
      m = prevMonth(m);
    }
  }
  const sparkByContact = {};
  const sparkByAgency = {};
  for (const s of sales) {
    if (!s.sale_date) continue;
    const m = s.sale_date.slice(0, 7);
    if (!sparkMonths.includes(m)) continue;
    for (const id of [s.customer_id, s.partner_id].filter(Boolean)) {
      if (!sparkByContact[id]) sparkByContact[id] = {};
      sparkByContact[id][m] = (sparkByContact[id][m] || 0) + (s.total_amount || 0);
    }
    if (s.agency_id) {
      if (!sparkByAgency[s.agency_id]) sparkByAgency[s.agency_id] = {};
      sparkByAgency[s.agency_id][m] = (sparkByAgency[s.agency_id][m] || 0) + (s.total_amount || 0);
    }
  }
  for (const p of top_partners) {
    p.sparkline = sparkMonths.map(m => sparkByContact[p.contact_id]?.[m] || 0);
  }
  for (const a of top_studios) {
    a.sparkline = sparkMonths.map(m => sparkByAgency[a.agency_id]?.[m] || 0);
  }

  // 10. B2B vs B2C breakdown (всегда, независимо от channel-фильтра — для KPI)
  // Считаем по фильтрованной выборке (sales уже отфильтрованы channel)
  let b2b_orders = 0, b2b_revenue = 0, b2c_orders = 0, b2c_revenue = 0;
  for (const s of sales) {
    if (s.partner_id) {
      b2b_orders++; b2b_revenue += s.total_amount || 0;
    } else {
      b2c_orders++; b2c_revenue += s.total_amount || 0;
    }
  }

  // 11. B2B/B2C по месяцам — stacked timeline
  const b2bMonthly = {};
  for (const s of sales) {
    if (!s.sale_date) continue;
    const m = s.sale_date.slice(0, 7);
    if (!b2bMonthly[m]) b2bMonthly[m] = { month: m, b2b_revenue: 0, b2c_revenue: 0, b2b_orders: 0, b2c_orders: 0 };
    if (s.partner_id) {
      b2bMonthly[m].b2b_revenue += s.total_amount || 0;
      b2bMonthly[m].b2b_orders++;
    } else {
      b2bMonthly[m].b2c_revenue += s.total_amount || 0;
      b2bMonthly[m].b2c_orders++;
    }
  }
  const b2b_timeline = Object.values(b2bMonthly).sort((a, b) => a.month.localeCompare(b.month));

  // Список доступных месяцев для UI-picker (последние 18 от сегодняшнего)
  const availableMonths = Object.keys(monthly).sort().reverse().slice(0, 18);

  // Multi-year overlay (legacy, для старого графика): для каждого месяца года —
  // точка на каждый год. Только revenue, без channel breakdown.
  // [{month_idx: 1, "2024": 21M, "2025": 22M, "2026": 33M}, ...]
  const yearMonthly = {};
  for (const s of sales) {
    if (!s.sale_date) continue;
    const y = s.sale_date.slice(0, 4);
    const m = parseInt(s.sale_date.slice(5, 7), 10);
    if (!yearMonthly[m]) yearMonthly[m] = { month: m };
    yearMonthly[m][y] = (yearMonthly[m][y] || 0) + (s.total_amount || 0);
  }
  const RU_MONTH = ['Янв','Фев','Мар','Апр','Май','Июн','Июл','Авг','Сен','Окт','Ноя','Дек'];
  const overlay_by_year = Array.from({ length: 12 }, (_, i) => ({
    month: RU_MONTH[i],
    month_idx: i + 1,
    ...(yearMonthly[i + 1] || {}),
  }));
  const yearsAvailable = [...new Set(sales.map(s => s.sale_date?.slice(0, 4)).filter(Boolean))].sort();

  // Расширенный multi-year breakdown: (year, month, channel) → revenue + orders + avg_check.
  // Wide-format для Recharts: каждая точка X (Jan-Dec) хранит все ключи "{year}__{channel}__{metric}".
  // Frontend делает multi-select по (year, channel, metric) и просто берёт нужные dataKey.
  //
  // ВАЖНО: используем sales БЕЗ channel-фильтра (только date_from/date_to + city),
  // иначе при args.channel='b2b' breakdown потеряет b2c данные. Это
  // независимый под-график со своим фильтром каналов.
  let salesForBreakdown = sales;
  if (args.channel === 'b2b' || args.channel === 'b2c') {
    let breakdownFilters = {};
    if (args.date_from) breakdownFilters.gte = { sale_date: args.date_from };
    if (args.date_to) breakdownFilters.lte = { sale_date: args.date_to };
    breakdownFilters = addCityFilter(breakdownFilters, args.city);
    salesForBreakdown = await loadAllParallel(sb, 'sales',
      'sale_date, total_amount, partner_id',
      breakdownFilters
    );
  }
  const yearChannelMonthly = {};
  for (const s of salesForBreakdown) {
    if (!s.sale_date) continue;
    const y = s.sale_date.slice(0, 4);
    const m = parseInt(s.sale_date.slice(5, 7), 10);
    const ch = s.partner_id ? 'b2b' : 'b2c';
    if (!yearChannelMonthly[m]) yearChannelMonthly[m] = {};
    const buckets = yearChannelMonthly[m];
    // 2 канала + 'all' (агрегатный)
    for (const channel of [ch, 'all']) {
      const key = `${y}__${channel}`;
      if (!buckets[key]) buckets[key] = { revenue: 0, orders: 0 };
      buckets[key].revenue += s.total_amount || 0;
      buckets[key].orders += 1;
    }
  }
  const multi_year_breakdown = Array.from({ length: 12 }, (_, i) => {
    const idx = i + 1;
    const row = { month_idx: idx, month: RU_MONTH[i] };
    const buckets = yearChannelMonthly[idx] || {};
    for (const [key, v] of Object.entries(buckets)) {
      const avgCheck = v.orders > 0 ? Math.round(v.revenue / v.orders) : 0;
      row[`${key}__revenue`] = v.revenue;
      row[`${key}__orders`] = v.orders;
      row[`${key}__avg_check`] = avgCheck;
    }
    return row;
  });

  const result = {
    timeline,
    top_studios, top_partners,
    categories,
    segments, kpi,
    movers_top, movers_bottom,
    movers_compare_month: currMonth,
    movers_prev_month: prevMo,
    available_months: availableMonths,
    pareto,
    spark_months: sparkMonths,
    channel_breakdown: {
      b2b_orders, b2b_revenue,
      b2c_orders, b2c_revenue,
      b2b_pct: (b2b_revenue + b2c_revenue) > 0
        ? Math.round((b2b_revenue / (b2b_revenue + b2c_revenue)) * 100) : 0,
    },
    b2b_timeline,
    overlay_by_year,
    years_available: yearsAvailable,
    multi_year_breakdown,
  };

  cacheSet(ck, result);
  return result;
}

// ── 7b. getSegmentation — RFM + cohorts + cities (Group B) ──────────────────
//
// RFM scoring: для каждого клиента (customer_id) считаются три балла 1-5:
//   R — Recency (дни с последней покупки → меньше = выше балл)
//   F — Frequency (кол-во заказов → больше = выше)
//   M — Monetary (общая сумма → больше = выше)
// Группы:
//   Champions   — R≥4, F≥4, M≥4
//   Loyal       — F≥4 (и не champions)
//   Big Spender — M≥4 (и не champions, не loyal)
//   At Risk     — R≤2 и F≥3 (раньше много, давно ничего)
//   New         — F=1, R≥4 (одна покупка, недавно)
//   Hibernating — R≤2 и F≤2 и M≤2
//   Casual      — остальные
//
// Cohort retention: для клиентов появившихся в каждый месяц 2024-2026,
// сколько вернулись в последующие N месяцев (table month-by-month).
//
// Cities: разбивка sales по городам (через sales.city).
//
export async function getSegmentation(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('segmentation', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  // 1. Все sales c customer_id — параллельная пагинация
  const salesFilters = addCityFilter({ notNull: ['customer_id'] }, city);
  const sales = await loadAllParallel(sb, 'sales',
    'customer_id, sale_date, total_amount, city, sale_items(category)',
    salesFilters
  );

  const today = new Date();
  const todayMs = today.getTime();

  // 2. Aggregate per customer
  const byCust = {};
  for (const s of sales) {
    if (!s.customer_id || !s.sale_date) continue;
    if (!byCust[s.customer_id]) byCust[s.customer_id] = {
      orders: 0, revenue: 0, last_date: null, first_date: null, has_filter: false,
    };
    const c = byCust[s.customer_id];
    c.orders++;
    c.revenue += s.total_amount || 0;
    if (!c.last_date || s.sale_date > c.last_date) c.last_date = s.sale_date;
    if (!c.first_date || s.sale_date < c.first_date) c.first_date = s.sale_date;
    if ((s.sale_items || []).some(i => i.category === 'water_filter')) c.has_filter = true;
  }

  // 3. RFM scoring (quintiles)
  const customers = Object.entries(byCust).map(([id, v]) => {
    const recency_days = v.last_date
      ? Math.floor((todayMs - new Date(v.last_date).getTime()) / (1000 * 60 * 60 * 24))
      : null;
    return { customer_id: id, ...v, recency_days };
  });
  // Quintile thresholds (manually for stability)
  const sortedByR = [...customers].sort((a, b) => (a.recency_days || 1e9) - (b.recency_days || 1e9));
  const sortedByF = [...customers].sort((a, b) => b.orders - a.orders);
  const sortedByM = [...customers].sort((a, b) => b.revenue - a.revenue);
  const Q = customers.length / 5;
  function rankOf(arr, id) {
    const idx = arr.findIndex(c => c.customer_id === id);
    return Math.min(5, Math.max(1, 5 - Math.floor(idx / Q)));
  }
  for (const c of customers) {
    c.r_score = rankOf(sortedByR, c.customer_id);
    c.f_score = rankOf(sortedByF, c.customer_id);
    c.m_score = rankOf(sortedByM, c.customer_id);
  }

  // 4. Segments
  function segmentOf(c) {
    const r = c.r_score, f = c.f_score, m = c.m_score;
    if (r >= 4 && f >= 4 && m >= 4) return 'champions';
    if (f >= 4) return 'loyal';
    if (m >= 4) return 'big_spender';
    if (r <= 2 && f >= 3) return 'at_risk';
    if (c.orders === 1 && r >= 4) return 'new';
    if (r <= 2 && f <= 2 && m <= 2) return 'hibernating';
    return 'casual';
  }
  for (const c of customers) c.segment = segmentOf(c);

  // 5. Bucket counts + revenue
  const segments = {};
  for (const c of customers) {
    if (!segments[c.segment]) segments[c.segment] = { name: c.segment, count: 0, revenue: 0 };
    segments[c.segment].count++;
    segments[c.segment].revenue += c.revenue;
  }
  const segmentList = Object.values(segments).sort((a, b) => b.revenue - a.revenue);

  // 6. Top customers per segment (для UI лент)
  const topPerSegment = {};
  for (const seg of Object.keys(segments)) {
    topPerSegment[seg] = customers
      .filter(c => c.segment === seg)
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 30)
      .map(c => ({ ...c, sale_items: undefined }));
  }
  // Add names + phone
  const allTopIds = [...new Set(Object.values(topPerSegment).flat().map(c => c.customer_id))];
  const { data: contacts } = allTopIds.length > 0
    ? await sb.from('partner_contacts').select('id, canonical_name, primary_phone, agency_id').in('id', allTopIds)
    : { data: [] };
  const cMap = new Map((contacts || []).map(c => [c.id, c]));
  for (const seg of Object.keys(topPerSegment)) {
    for (const c of topPerSegment[seg]) {
      const enrich = cMap.get(c.customer_id);
      c.name = enrich?.canonical_name || '?';
      c.phone = enrich?.primary_phone || null;
      c.agency_id = enrich?.agency_id || null;
    }
  }

  // 7. Cohort retention: for each cohort_month (when customer first bought),
  //    what % returned in month +1, +2, +3, ..., +6?
  const cohorts = {};
  for (const c of customers) {
    if (!c.first_date) continue;
    const cm = c.first_date.slice(0, 7);
    if (!cohorts[cm]) cohorts[cm] = { cohort_month: cm, size: 0, returns: {} };
    cohorts[cm].size++;
  }
  // For each sale, if it's not the first one — count return offset
  for (const s of sales) {
    if (!s.customer_id || !s.sale_date) continue;
    const c = byCust[s.customer_id];
    if (!c || s.sale_date === c.first_date) continue;
    const cm = c.first_date.slice(0, 7);
    const sm = s.sale_date.slice(0, 7);
    const offsetMo = monthDiff(cm, sm);
    if (offsetMo <= 0 || offsetMo > 12) continue;
    if (!cohorts[cm].returns[offsetMo]) cohorts[cm].returns[offsetMo] = new Set();
    cohorts[cm].returns[offsetMo].add(s.customer_id);
  }
  const cohortList = Object.values(cohorts)
    .filter(c => c.size >= 5) // фильтр шумных малых
    .sort((a, b) => a.cohort_month.localeCompare(b.cohort_month))
    .map(c => ({
      cohort_month: c.cohort_month,
      size: c.size,
      returns_pct: Object.fromEntries(
        Array.from({ length: 12 }, (_, i) => i + 1).map(off => [
          off,
          c.returns[off] ? Math.round((c.returns[off].size / c.size) * 100) : 0,
        ])
      ),
    }));

  // 8. Cities breakdown
  const cities = {};
  for (const s of sales) {
    const city = (s.city || '—').trim() || '—';
    if (!cities[city]) cities[city] = { city, orders: 0, revenue: 0, customers: new Set() };
    cities[city].orders++;
    cities[city].revenue += s.total_amount || 0;
    if (s.customer_id) cities[city].customers.add(s.customer_id);
  }
  const cityList = Object.values(cities)
    .map(c => ({ ...c, customers: c.customers.size }))
    .sort((a, b) => b.revenue - a.revenue);

  // 9. New vs repeat by month
  const monthlyNew = {};
  for (const c of customers) {
    if (!c.first_date) continue;
    const fm = c.first_date.slice(0, 7);
    if (!monthlyNew[fm]) monthlyNew[fm] = { month: fm, new_customers: 0, repeat_customers: 0 };
    monthlyNew[fm].new_customers++;
  }
  for (const s of sales) {
    if (!s.customer_id || !s.sale_date) continue;
    const c = byCust[s.customer_id];
    if (!c || s.sale_date === c.first_date) continue;
    const sm = s.sale_date.slice(0, 7);
    if (!monthlyNew[sm]) monthlyNew[sm] = { month: sm, new_customers: 0, repeat_customers: 0 };
    monthlyNew[sm].repeat_customers++;
  }
  const newRepeatTimeline = Object.values(monthlyNew).sort((a, b) => a.month.localeCompare(b.month));

  const result = {
    segments: segmentList,
    top_per_segment: topPerSegment,
    cohorts: cohortList,
    cities: cityList,
    new_repeat_timeline: newRepeatTimeline,
    total_customers: customers.length,
  };
  cacheSet(ck, result);
  return result;
}

function monthDiff(fromYM, toYM) {
  const [fy, fm] = fromYM.split('-').map(Number);
  const [ty, tm] = toYM.split('-').map(Number);
  return (ty - fy) * 12 + (tm - fm);
}

// ── 7c. getProductInsights — top SKUs + cross-sell + cartridge funnel (Group C) ─
//
// 1) top_skus: топ-50 SKU по количеству + сумме продаж (за всё время или период).
// 2) cross_sell: пары SKU которые часто берут вместе (для рекомендаций менеджерам).
// 3) cartridge_funnel: для каждого месяца — сколько фильтров продали → сколько
//    картриджей должно продаться через 6 мес → сколько реально продали (gap).
// 4) seasonality: heatmap "месяц × категория" — пиковые месяцы по каждой кат.
//
export async function getProductInsights(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('products', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // Все sale_items + sales — параллельно (значительно быстрее последовательной pagination)
  // При city-фильтре: загружаем только sales нужного города, потом берём только их items.
  const salesFilters = addCityFilter({}, city);
  const [items, sales] = await Promise.all([
    city === 'all'
      ? loadAllParallel(sb, 'sale_items', 'sale_id, sku, raw_name, qty, amount, category')
      : (async () => {
          // Сначала получаем sale_ids нужного города, потом items по ним
          const citySales = await loadAllParallel(sb, 'sales', 'id', salesFilters);
          const ids = citySales.map(s => s.id);
          if (ids.length === 0) return [];
          const batches = [];
          for (let i = 0; i < ids.length; i += 200) batches.push(ids.slice(i, i + 200));
          const results = await Promise.all(
            batches.map(batch =>
              sb.from('sale_items').select('sale_id, sku, raw_name, qty, amount, category').in('sale_id', batch)
            )
          );
          return results.flatMap(r => r.data || []);
        })(),
    loadAllParallel(sb, 'sales', 'id, sale_date, customer_id', salesFilters),
  ]);
  const salesById = new Map(sales.map(s => [s.id, s]));

  // 1. Top SKUs (агрегируем по SKU; если SKU отсутствует — по raw_name)
  const skuAgg = {};
  for (const it of items) {
    const key = it.sku || `noskus:${(it.raw_name || '').slice(0, 60)}`;
    if (!skuAgg[key]) skuAgg[key] = {
      sku: it.sku || null,
      name: it.raw_name || '?',
      category: it.category || 'other',
      qty_sold: 0,
      orders: new Set(),
      revenue: 0,
    };
    skuAgg[key].qty_sold += it.qty || 1;
    skuAgg[key].revenue += it.amount || 0;
    skuAgg[key].orders.add(it.sale_id);
  }
  const topSkus = Object.values(skuAgg)
    .map(s => ({ ...s, orders: s.orders.size }))
    .sort((a, b) => b.qty_sold - a.qty_sold)
    .slice(0, 50);

  // 2. Cross-sell pairs (SKU A + SKU B в одном заказе → counter)
  //
  // FIX 2026-05-02 (Adil feedback): исключаем «обвес» из cross-sell пар.
  // Adil видел «NA-01 + Измельчитель» 240 раз — NA-01 это отводная арматура для измельчителя,
  // очевидный обвес. Также картриджи и сменные модули — расходники, не интересные пары.
  //
  // CROSS_SELL_EXCLUDE_CATEGORIES: категории которые полностью исключаются из cross-sell.
  // CROSS_SELL_EXCLUDE_SKU_PREFIXES: SKU-префиксы которые исключаются (NA-01 = отв. арматура).
  // CROSS_SELL_EXCLUDE_NAME_PATTERNS: части raw_name которые сигнализируют о сменном элементе.
  //
  // ОСТАВЛЯЕМ только «основные» категории: sink, faucet, disposer, water_filter, dispenser.
  //
  const CROSS_SELL_EXCLUDE_CATEGORIES = new Set(['cartridge', 'accessory']);
  const CROSS_SELL_EXCLUDE_SKU_PREFIXES = ['NA-01', 'NA01'];
  const CROSS_SELL_EXCLUDE_NAME_PATTERNS = [
    'сменн', 'замен', 'replacement', 'картридж', 'картриджи',
    'модуль замены', 'v-complex', 'm-complex', 'pure drop замен',
  ];
  const CROSS_SELL_MAIN_CATEGORIES = new Set(['sink', 'faucet', 'disposer', 'water_filter', 'dispenser']);

  function isCrossSellExcluded(it) {
    if (!it.sku && !it.raw_name) return true; // no identity
    if (CROSS_SELL_EXCLUDE_CATEGORIES.has(it.category)) return true;
    const sku = (it.sku || '').toUpperCase();
    if (CROSS_SELL_EXCLUDE_SKU_PREFIXES.some(p => sku.startsWith(p.toUpperCase()))) return true;
    const name = (it.raw_name || '').toLowerCase();
    if (CROSS_SELL_EXCLUDE_NAME_PATTERNS.some(p => name.includes(p))) return true;
    // Если категория задана и не входит в основные — тоже exclude
    if (it.category && !CROSS_SELL_MAIN_CATEGORIES.has(it.category)) return true;
    return false;
  }

  const itemsBySale = {};
  for (const it of items) {
    if (!it.sku) continue;
    if (isCrossSellExcluded(it)) continue; // skip accessories/cartridges/replacements
    if (!itemsBySale[it.sale_id]) itemsBySale[it.sale_id] = new Set();
    itemsBySale[it.sale_id].add(it.sku);
  }
  const pairCount = {};
  for (const [saleId, skuSet] of Object.entries(itemsBySale)) {
    const arr = [...skuSet];
    if (arr.length < 2) continue;
    for (let i = 0; i < arr.length; i++) {
      for (let j = i + 1; j < arr.length; j++) {
        const key = arr[i] < arr[j] ? `${arr[i]}|${arr[j]}` : `${arr[j]}|${arr[i]}`;
        pairCount[key] = (pairCount[key] || 0) + 1;
      }
    }
  }
  const skuNames = new Map();
  for (const s of topSkus) if (s.sku) skuNames.set(s.sku, s.name);
  // Тоже — для всех skus используя last seen name
  for (const it of items) if (it.sku && !skuNames.has(it.sku)) skuNames.set(it.sku, it.raw_name || it.sku);

  const crossSell = Object.entries(pairCount)
    .filter(([_, c]) => c >= 3) // FIX 2026-05-02: снизили порог с 5 до 3 — после exclusion пар стало меньше
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30)
    .map(([key, count]) => {
      const [a, b] = key.split('|');
      return {
        sku_a: a, sku_b: b,
        name_a: skuNames.get(a) || a,
        name_b: skuNames.get(b) || b,
        count,
      };
    });

  // 3. Cartridge funnel: for each month, count water_filter sold → expected
  //    cartridges in (month + 6mo). Compare to actual cartridges sold that month.
  const filterByMonth = {};
  const cartridgeByMonth = {};
  for (const it of items) {
    const sale = salesById.get(it.sale_id);
    if (!sale?.sale_date) continue;
    const m = sale.sale_date.slice(0, 7);
    if (it.category === 'water_filter') {
      filterByMonth[m] = (filterByMonth[m] || 0) + (it.qty || 1);
    }
    if (it.category === 'cartridge') {
      cartridgeByMonth[m] = (cartridgeByMonth[m] || 0) + (it.qty || 1);
    }
  }
  const allMonths = [...new Set([...Object.keys(filterByMonth), ...Object.keys(cartridgeByMonth)])].sort();
  function addMonths(ym, n) {
    const [y, m] = ym.split('-').map(Number);
    const d = new Date(y, m - 1 + n, 1);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  }
  const cartridgeFunnel = allMonths.map(m => ({
    month: m,
    filters_sold: filterByMonth[m] || 0,
    expected_cartridges_at_plus6: filterByMonth[addMonths(m, -6)] || 0,
    actual_cartridges_sold: cartridgeByMonth[m] || 0,
    gap: (filterByMonth[addMonths(m, -6)] || 0) - (cartridgeByMonth[m] || 0),
  }));

  // 4. Seasonality (категория × месяц)
  const seasonality = {};
  for (const it of items) {
    const sale = salesById.get(it.sale_id);
    if (!sale?.sale_date) continue;
    const monthIdx = parseInt(sale.sale_date.slice(5, 7), 10); // 1-12
    const cat = it.category || 'other';
    if (!seasonality[cat]) seasonality[cat] = Array.from({ length: 12 }, () => 0);
    seasonality[cat][monthIdx - 1] += it.qty || 1;
  }
  const seasonalityList = Object.entries(seasonality)
    .map(([category, monthly]) => ({ category, monthly }))
    .sort((a, b) => b.monthly.reduce((s, x) => s + x, 0) - a.monthly.reduce((s, x) => s + x, 0));

  // 5. Bundle stats
  const itemsCountBySale = {};
  for (const it of items) {
    itemsCountBySale[it.sale_id] = (itemsCountBySale[it.sale_id] || 0) + 1;
  }
  const bundleStats = {
    avg_items_per_sale: items.length / Math.max(1, sales.length),
    one_item_sales: Object.values(itemsCountBySale).filter(n => n === 1).length,
    multi_item_sales: Object.values(itemsCountBySale).filter(n => n > 1).length,
  };

  const result = {
    top_skus: topSkus,
    cross_sell: crossSell,
    cartridge_funnel: cartridgeFunnel,
    seasonality: seasonalityList,
    bundle_stats: bundleStats,
  };
  cacheSet(ck, result);
  return result;
}

// ── 6c. getPartnerJourney(contactId) — единая хронология событий клиента ────
//
// Собирает все события клиента/партнёра в timeline:
//   - sale          — он что-то купил
//   - chat_in       — пришло сообщение в WhatsApp от него
//   - chat_out      — менеджер ответил
//   - chat_ai       — система проанализировала диалог
//   - followup_due  — должно было быть напоминание (для ретроспективы)
//   - followup_done — followup отмечен выполненным
//
// Сортировка по timestamp DESC (свежее сверху). Limit 200 событий.
// Используется в карточке партнёра (Customer Journey).
//
export async function getPartnerJourney(req, contactId) {
  if (!isUuid(contactId)) throw new Error('invalid contact id');
  const sb = pickClient(req);

  // 1. Контакт + linked_chat_jids
  const { data: contact } = await sb.from('partner_contacts')
    .select('id, canonical_name, primary_phone, linked_chat_jids, agency_id')
    .eq('id', contactId)
    .maybeSingle();
  if (!contact) return { events: [] };

  const jids = contact.linked_chat_jids || [];

  // 2. Параллельно: sales / messages / chat_ai / followups
  const [salesAsCust, salesAsPart, messages, chatAi, followups] = await Promise.all([
    sb.from('sales').select('id, sale_date, total_amount, customer_raw, partner_raw, source_file, order_num, manager').eq('customer_id', contactId).order('sale_date', { ascending: false }).limit(50),
    sb.from('sales').select('id, sale_date, total_amount, customer_raw, partner_raw, source_file, order_num, manager').eq('partner_id', contactId).order('sale_date', { ascending: false }).limit(50),
    jids.length > 0
      ? sb.from('messages').select('id, session_id, remote_jid, body, from_me, timestamp, message_type').in('remote_jid', jids).order('timestamp', { ascending: false }).limit(80)
      : Promise.resolve({ data: [] }),
    jids.length > 0
      ? sb.from('chat_ai').select('id, analyzed_at, intent, lead_temperature, deal_stage, summary_ru, manager_issues, risk_flags, session_id, remote_jid').in('remote_jid', jids).order('analyzed_at', { ascending: false }).limit(20)
      : Promise.resolve({ data: [] }),
    sb.from('followups').select('id, due_date, type, note, completed_at, related_sale_id').eq('contact_id', contactId).order('due_date', { ascending: false }).limit(40),
  ]);

  // 3. Преобразуем в единый формат event'ов
  const events = [];

  for (const s of (salesAsCust.data || [])) {
    events.push({
      type: 'sale',
      role: 'customer',
      ts: s.sale_date ? `${s.sale_date}T12:00:00Z` : null,
      sale_id: s.id,
      title: `Купил на ${(s.total_amount || 0).toLocaleString('ru-RU')} ₸`,
      meta: { order_num: s.order_num, source_file: s.source_file, manager: s.manager, partner_raw: s.partner_raw, customer_raw: s.customer_raw },
    });
  }
  for (const s of (salesAsPart.data || [])) {
    events.push({
      type: 'sale',
      role: 'partner',
      ts: s.sale_date ? `${s.sale_date}T12:00:00Z` : null,
      sale_id: s.id,
      title: `Привёл клиента на ${(s.total_amount || 0).toLocaleString('ru-RU')} ₸`,
      meta: { order_num: s.order_num, source_file: s.source_file, manager: s.manager, customer_raw: s.customer_raw, partner_raw: s.partner_raw },
    });
  }
  for (const m of (messages.data || [])) {
    events.push({
      type: m.from_me ? 'chat_out' : 'chat_in',
      ts: m.timestamp,
      message_id: m.id,
      title: m.from_me ? 'Менеджер написал' : 'Клиент написал',
      meta: { session_id: m.session_id, remote_jid: m.remote_jid, body: (m.body || '').slice(0, 240), message_type: m.message_type },
    });
  }
  for (const a of (chatAi.data || [])) {
    events.push({
      type: 'chat_ai',
      ts: a.analyzed_at,
      ai_id: a.id,
      title: `AI-анализ: ${a.intent || '—'} · ${a.lead_temperature || '—'} · ${a.deal_stage || '—'}`,
      meta: { summary: a.summary_ru, issues: a.manager_issues, risks: a.risk_flags, session_id: a.session_id, remote_jid: a.remote_jid },
    });
  }
  for (const f of (followups.data || [])) {
    events.push({
      type: f.completed_at ? 'followup_done' : 'followup_due',
      ts: f.completed_at || (f.due_date ? `${f.due_date}T09:00:00Z` : null),
      followup_id: f.id,
      title: f.completed_at ? `Followup выполнен: ${f.type}` : `Запланировано напоминание: ${f.type}`,
      meta: { note: f.note, due_date: f.due_date, related_sale_id: f.related_sale_id },
    });
  }

  // Sort by timestamp DESC, drop events without ts
  events.sort((a, b) => (b.ts || '').localeCompare(a.ts || ''));

  return {
    contact: {
      id: contact.id,
      canonical_name: contact.canonical_name,
      primary_phone: contact.primary_phone,
      has_chat: jids.length > 0,
    },
    events: events.slice(0, 200),
    counts: {
      sales: (salesAsCust.data?.length || 0) + (salesAsPart.data?.length || 0),
      messages: messages.data?.length || 0,
      chat_ai: chatAi.data?.length || 0,
      followups: followups.data?.length || 0,
    },
  };
}

// ── 7c-extra. getCategoryDrilldown(category) — детальный анализ одной категории ─
//
// При клике на категорию в Продуктах — открывается drill-down:
//   - Динамика продаж этой категории по месяцам (qty + revenue)
//   - Топ SKU внутри категории (с qty, orders, revenue)
//   - Топ клиентов покупавших эту категорию
//   - Топ-партнёров приведших клиентов на эту категорию
//
const drilldownInput = z.object({
  category: z.string().min(1).max(50),
  city: citySchema,
});

export async function getCategoryDrilldown(req, params) {
  const args = drilldownInput.parse(params);
  const ck = cacheKey('category', req, args);
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // Загружаем все sale_items этой категории + их sales
  let items = [];
  let off = 0;
  while (true) {
    const { data } = await sb.from('sale_items')
      .select('sale_id, sku, raw_name, qty, amount, category')
      .eq('category', args.category)
      .range(off, off + 999);
    if (!data || data.length === 0) break;
    items.push(...data);
    if (data.length < 1000) break;
    off += 1000;
  }

  if (items.length === 0) {
    return {
      category: args.category,
      total_qty: 0, total_revenue: 0, total_orders: 0,
      timeline: [], top_skus: [], top_customers: [], top_partners: [],
    };
  }

  const saleIds = [...new Set(items.map(it => it.sale_id))];
  // Грузим связанные sales батчами. City-фильтр применяется здесь:
  // если city задан — добавляем .eq('city', city) и только совпавшие sales
  // попадают в salesById. Items связанных с «чужим» городом окажутся без sale
  // и отфильтруются в timeline/top-sections через `if (!sale?.sale_date) continue`.
  const sales = [];
  for (let i = 0; i < saleIds.length; i += 200) {
    const batch = saleIds.slice(i, i + 200);
    let q = sb.from('sales')
      .select('id, sale_date, total_amount, customer_id, partner_id, agency_id')
      .in('id', batch);
    q = applyCity(q, args.city);
    const { data } = await q;
    sales.push(...(data || []));
  }
  const salesById = new Map(sales.map(s => [s.id, s]));

  // 1. Timeline по месяцам
  const monthly = {};
  for (const it of items) {
    const sale = salesById.get(it.sale_id);
    if (!sale?.sale_date) continue;
    const m = sale.sale_date.slice(0, 7);
    if (!monthly[m]) monthly[m] = { month: m, qty: 0, revenue: 0, orders: new Set() };
    monthly[m].qty += it.qty || 1;
    monthly[m].revenue += it.amount || 0;
    monthly[m].orders.add(it.sale_id);
  }
  const timeline = Object.values(monthly)
    .map(m => ({ ...m, orders: m.orders.size }))
    .sort((a, b) => a.month.localeCompare(b.month));

  // 2. Top SKUs
  const skuAgg = {};
  for (const it of items) {
    const key = it.sku || `noskus:${(it.raw_name || '').slice(0, 60)}`;
    if (!skuAgg[key]) skuAgg[key] = {
      sku: it.sku || null, name: it.raw_name || '?',
      qty: 0, revenue: 0, orders: new Set(),
    };
    skuAgg[key].qty += it.qty || 1;
    skuAgg[key].revenue += it.amount || 0;
    skuAgg[key].orders.add(it.sale_id);
  }
  const topSkus = Object.values(skuAgg)
    .map(s => ({ ...s, orders: s.orders.size }))
    .sort((a, b) => b.qty - a.qty)
    .slice(0, 30);

  // 3. Top customers (по revenue этой категории)
  const custAgg = {};
  for (const it of items) {
    const sale = salesById.get(it.sale_id);
    if (!sale?.customer_id) continue;
    if (!custAgg[sale.customer_id]) custAgg[sale.customer_id] = { qty: 0, revenue: 0, orders: 0 };
    custAgg[sale.customer_id].qty += it.qty || 1;
    custAgg[sale.customer_id].revenue += it.amount || 0;
    custAgg[sale.customer_id].orders++;
  }
  const topCustEntries = Object.entries(custAgg)
    .sort((a, b) => b[1].revenue - a[1].revenue)
    .slice(0, 15);
  const custIds = topCustEntries.map(([id]) => id);
  const { data: custContacts } = custIds.length > 0
    ? await sb.from('partner_contacts').select('id, canonical_name, primary_phone').in('id', custIds)
    : { data: [] };
  const cMap = new Map((custContacts || []).map(c => [c.id, c]));
  const topCustomers = topCustEntries.map(([id, agg]) => ({
    customer_id: id,
    name: cMap.get(id)?.canonical_name || '?',
    phone: cMap.get(id)?.primary_phone || null,
    ...agg,
  }));

  // 4. Top partners
  const partAgg = {};
  for (const it of items) {
    const sale = salesById.get(it.sale_id);
    if (!sale?.partner_id) continue;
    if (!partAgg[sale.partner_id]) partAgg[sale.partner_id] = { qty: 0, revenue: 0, orders: 0 };
    partAgg[sale.partner_id].qty += it.qty || 1;
    partAgg[sale.partner_id].revenue += it.amount || 0;
    partAgg[sale.partner_id].orders++;
  }
  const topPartEntries = Object.entries(partAgg)
    .sort((a, b) => b[1].revenue - a[1].revenue)
    .slice(0, 15);
  const partIds = topPartEntries.map(([id]) => id);
  const { data: partContacts } = partIds.length > 0
    ? await sb.from('partner_contacts').select('id, canonical_name, primary_phone, agency_id').in('id', partIds)
    : { data: [] };
  const pMap = new Map((partContacts || []).map(p => [p.id, p]));
  const topPartners = topPartEntries.map(([id, agg]) => ({
    partner_id: id,
    name: pMap.get(id)?.canonical_name || '?',
    phone: pMap.get(id)?.primary_phone || null,
    agency_id: pMap.get(id)?.agency_id || null,
    ...agg,
  }));

  // 5. Группировка SKU по «семействам» — первое значимое слово в raw_name
  // (например, "Мойка Omi 53" / "Мойка Omi 76" → семейство "Omi")
  // Цель: внутри категории "Мойки" увидеть какие модельные ряды лидируют.
  function familyKey(name) {
    if (!name) return '?';
    // Убираем тип-префикс (Мойка/Смеситель/Дозатор/...) и берём первое латинское слово
    const cleaned = String(name).replace(/^(Мойка|Смеситель|Дозатор|Измельчитель|Ролл-мат|Сушка|Картридж|Декоративная|Перелив|Сменный|Отводная|Подставка|Точилка|Кнопка|Стакан)\s*/i, '');
    const m = cleaned.match(/[A-Za-z][A-Za-z0-9-]+/);
    return m ? m[0] : cleaned.split(/\s+/)[0] || '?';
  }
  const families = {};
  for (const it of items) {
    const fam = familyKey(it.raw_name);
    if (!families[fam]) families[fam] = { family: fam, qty: 0, revenue: 0, orders: new Set(), sample_names: new Set() };
    families[fam].qty += it.qty || 1;
    families[fam].revenue += it.amount || 0;
    families[fam].orders.add(it.sale_id);
    if (families[fam].sample_names.size < 5) families[fam].sample_names.add(it.raw_name);
  }
  const familiesList = Object.values(families)
    .map(f => ({ ...f, orders: f.orders.size, sample_names: [...f.sample_names] }))
    .sort((a, b) => b.qty - a.qty)
    .slice(0, 20);

  const result = {
    category: args.category,
    total_qty: items.reduce((s, x) => s + (x.qty || 1), 0),
    total_revenue: items.reduce((s, x) => s + (x.amount || 0), 0),
    total_orders: saleIds.length,
    timeline,
    top_skus: topSkus,
    top_customers: topCustomers,
    top_partners: topPartners,
    families: familiesList,
  };
  cacheSet(ck, result);
  return result;
}

// ── 7h. getForecast — прогноз выручки + картриджного пайплайна ──────────────
//
// Простой forecast по линейной регрессии последних 12 месяцев + сезонный
// adjustment (среднее по месяцу года из всех годов).
//
// Также — картриджный пайплайн: для каждого месяца вперёд (12 мес)
// считаем сколько фильтров было продано 6 мес назад → должны иметь столько
// followups готовых к отправке.
//
export async function getForecast(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('forecast', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // 1. Sales по месяцам — для линейного прогноза.
  // Загружаем id тоже — нужен для city-фильтра картриджного pipeline (section 5).
  //
  // FIX 2026-05-02 (Adil feedback): исключаем текущий НЕПОЛНЫЙ месяц из training data.
  // Например, 2 мая 2026: продаж мая ≈0 → регрессия думает «май = 0» → прогноз = 0.
  // Решение: фильтруем данные WHERE sale_date < current month start.
  // Прогноз НАЧИНАЕТСЯ с текущего месяца (future). Ретроспектива — только завершённые.
  const currentMonth = new Date().toISOString().slice(0, 7); // 'YYYY-MM'
  // Load all sales including current month data (for cartridge pipeline), then
  // filter in JS — current month is excluded only from regression training data.
  const salesFilters = addCityFilter({}, city);
  const sales = await loadAllParallel(sb, 'sales',
    'id, sale_date, total_amount',
    salesFilters
  );
  const monthlyRev = {};
  for (const s of sales) {
    if (!s.sale_date) continue;
    const m = s.sale_date.slice(0, 7);
    // Skip current month — it's incomplete, would skew regression to 0
    if (m >= currentMonth) continue;
    monthlyRev[m] = (monthlyRev[m] || 0) + (s.total_amount || 0);
  }
  const months = Object.keys(monthlyRev).sort();
  if (months.length < 6) {
    return { error: 'Недостаточно данных для прогноза (нужно минимум 6 мес)' };
  }

  // Helper: addMonths
  function addMonths(ym, k) {
    const [y, mo] = ym.split('-').map(Number);
    const d = new Date(y, mo - 1 + k, 1);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  }

  // 2. Weighted Moving Average baseline
  // Weights: last 3 months × 0.5, last 6 × 0.3, last 12 (or all) × 0.2
  // Если данных < 6 мес — already guarded above. Если < 12 — используем то что есть.
  const last3  = months.slice(-3);
  const last6  = months.slice(-6);
  const last12 = months.slice(-12);

  const avg3  = last3.reduce((s, m) => s + monthlyRev[m], 0) / last3.length;
  const avg6  = last6.reduce((s, m) => s + monthlyRev[m], 0) / last6.length;
  const avg12 = last12.reduce((s, m) => s + monthlyRev[m], 0) / last12.length;

  // При < 6 мес данных fallback на simple average; выше guard уже отсеял эти кейсы.
  // При < 12 мес: avg12 = avg по всем имеющимся (last12 = все months если < 12).
  const wmaBaseline = avg3 * 0.5 + avg6 * 0.3 + avg12 * 0.2;

  // 3. Сезонный коэффициент + year-over-year growth
  // seasonal[i] = среднее за этот месяц / общее среднее по году
  const seasonal = Array.from({ length: 12 }, () => ({ sum: 0, count: 0 }));
  // Группируем по году для YoY
  const revenueByYear = {};
  for (const m of months) {
    const monthIdx = parseInt(m.slice(5, 7)) - 1; // 0..11
    const year = m.slice(0, 4);
    seasonal[monthIdx].sum += monthlyRev[m];
    seasonal[monthIdx].count++;
    revenueByYear[year] = (revenueByYear[year] || 0) + monthlyRev[m];
  }

  // avgRev для seasonalMult — используем WMA baseline (стабильнее чем simple mean)
  const avgRev = wmaBaseline > 0 ? wmaBaseline : (avg12 || 1);
  const seasonalMult = seasonal.map(s =>
    s.count > 0 && avgRev > 0 ? (s.sum / s.count) / avgRev : 1
  );

  // YoY growth: сравниваем последний полный год с предыдущим
  const years = Object.keys(revenueByYear).sort();
  let yearOverYearGrowth = 0;
  if (years.length >= 2) {
    const prevYear = revenueByYear[years[years.length - 2]];
    const lastYear = revenueByYear[years[years.length - 1]];
    if (prevYear > 0) yearOverYearGrowth = (lastYear - prevYear) / prevYear;
  }

  // Для trend insights (аналог slope из WMA: recent vs older)
  const recentAvgForInsight = avg3;
  const olderAvg = last12.slice(0, 3).reduce((s, m) => s + monthlyRev[m], 0) / Math.min(3, last12.length);
  // Приближение slope для insights (₸/мес тренд из WMA: recent vs older)
  const slope = olderAvg > 0 ? (avg3 - olderAvg) / last12.length : 0;

  // 4. Forecast следующих 6 мес — Weighted MA × seasonal × (1 + YoY×0.5)
  // Защита от излишнего pessimism/overshoot:
  //   min(forecast) = baseline × 0.7
  //   max(forecast) = baseline × 2.0
  const lastMonth = months[months.length - 1];
  const forecastMin = wmaBaseline * 0.70;
  const forecastMax = wmaBaseline * 2.00;

  const forecastTimeline = [];
  // historical (last 12)
  for (const m of last12) {
    forecastTimeline.push({
      month: m, actual: monthlyRev[m], forecast: null,
    });
  }
  // future (next 6)
  for (let k = 1; k <= 6; k++) {
    const fm = addMonths(lastMonth, k);
    const monthIdx = parseInt(fm.slice(5, 7)) - 1;
    const raw = wmaBaseline * seasonalMult[monthIdx] * (1 + yearOverYearGrowth * 0.5);
    const clamped = Math.min(forecastMax, Math.max(forecastMin, raw));
    forecastTimeline.push({
      month: fm, actual: null, forecast: Math.round(clamped),
    });
  }

  // 5. Картриджный пайплайн на 12 мес вперёд (фильтры -6 мес назад)
  // Использую loadAllParallel чтобы не упереться в default 1000 rows.
  const filterByMonth = {};
  const cartridgeByMonth = {};
  // Для city-фильтра в картриджном pipeline: фильтруем по sale_ids из нужного города.
  // sale_items не имеют city — матчим через salesIdDate (уже загруженные выше).
  // Если city='all', грузим все items напрямую.
  const [filterItems, salesIdDate] = await Promise.all([
    city === 'all'
      ? (async () => {
          const a = await loadAllParallel(sb, 'sale_items', 'category, qty, sale_id', { eq: { category: 'water_filter' } });
          const b = await loadAllParallel(sb, 'sale_items', 'category, qty, sale_id', { eq: { category: 'cartridge' } });
          return [...a, ...b];
        })()
      : (async () => {
          // Используем уже загруженные city-sales для фильтрации items
          const saleIds = sales.map(s => s.id);
          if (saleIds.length === 0) return [];
          const batches = [];
          for (let i = 0; i < saleIds.length; i += 200) batches.push(saleIds.slice(i, i + 200));
          const results = await Promise.all(
            batches.map(batch =>
              sb.from('sale_items').select('category, qty, sale_id')
                .in('sale_id', batch)
                .in('category', ['water_filter', 'cartridge'])
            )
          );
          return results.flatMap(r => r.data || []);
        })(),
    loadAllParallel(sb, 'sales', 'id, sale_date', salesFilters),
  ]);
  const idToDate = new Map((salesIdDate || []).map(s => [s.id, s.sale_date]));
  for (const it of filterItems) {
    const date = idToDate.get(it.sale_id);
    if (!date) continue;
    const m = date.slice(0, 7);
    if (it.category === 'water_filter') filterByMonth[m] = (filterByMonth[m] || 0) + (it.qty || 1);
    else if (it.category === 'cartridge') cartridgeByMonth[m] = (cartridgeByMonth[m] || 0) + (it.qty || 1);
  }

  const cartridgePipeline = [];
  for (let k = 1; k <= 12; k++) {
    const targetMonth = addMonths(lastMonth, k);
    const sourceMonth = addMonths(targetMonth, -6); // фильтры за 6 мес до target
    cartridgePipeline.push({
      target_month: targetMonth,
      filters_sold_6mo_ago: filterByMonth[sourceMonth] || 0,
      expected_cartridges: filterByMonth[sourceMonth] || 0, // 1:1 предположение
    });
  }

  // 6. Авто-инсайты по тренду (без AI на бэке — простые правила)
  // recentAvgForInsight и olderAvg уже вычислены в шаге 2 (WMA baseline).
  const insights = [];

  // YoY growth insight
  if (yearOverYearGrowth > 0.05) {
    const yoyPct = Math.round(yearOverYearGrowth * 100);
    insights.push({
      kind: 'positive',
      title: `Рост год к году: +${yoyPct}%`,
      text: `Последний год в сравнении с предыдущим вырос на ${yoyPct}%. На 6 мес вперёд ожидается ~${Math.round(forecastTimeline.filter(t => t.forecast).reduce((s, t) => s + t.forecast, 0) / 1_000_000)}M ₸ выручки.`,
    });
  } else if (yearOverYearGrowth < -0.05) {
    const yoyDropPct = Math.round(Math.abs(yearOverYearGrowth) * 100);
    insights.push({
      kind: 'warning',
      title: `Снижение год к году: −${yoyDropPct}%`,
      text: `Последний год показал снижение на ${yoyDropPct}% относительно предыдущего. Стоит разобрать: cold-партнёров, упавшие категории, изменение в каналах продаж.`,
    });
  } else if (recentAvgForInsight > olderAvg * 1.1) {
    // Нет YoY данных или рост нейтральный, но последние 3 мес растут
    const growthPct = Math.round((recentAvgForInsight / olderAvg - 1) * 100);
    insights.push({
      kind: 'positive',
      title: `Ускорение: +${growthPct}% за квартал`,
      text: `Последние 3 месяца в среднем на ${growthPct}% выше первых 3 в окне анализа. На 6 мес вперёд ожидается ~${Math.round(forecastTimeline.filter(t => t.forecast).reduce((s, t) => s + t.forecast, 0) / 1_000_000)}M ₸ выручки.`,
    });
  }

  // Сезонные пики и провалы
  const peakIdx = seasonalMult.indexOf(Math.max(...seasonalMult));
  const lowIdx = seasonalMult.indexOf(Math.min(...seasonalMult));
  const RU_MONTH = ['январь','февраль','март','апрель','май','июнь','июль','август','сентябрь','октябрь','ноябрь','декабрь'];
  if (seasonalMult[peakIdx] > 1.15) {
    insights.push({
      kind: 'positive',
      title: `Пиковый месяц: ${RU_MONTH[peakIdx]}`,
      text: `Исторически в ${RU_MONTH[peakIdx]} выручка на ${Math.round((seasonalMult[peakIdx] - 1) * 100)}% выше среднего. Готовь склад и менеджеров заранее.`,
    });
  }
  if (seasonalMult[lowIdx] < 0.85) {
    insights.push({
      kind: 'warning',
      title: `Низкий месяц: ${RU_MONTH[lowIdx]}`,
      text: `В ${RU_MONTH[lowIdx]} выручка на ${Math.round((1 - seasonalMult[lowIdx]) * 100)}% ниже. Стоит запускать акции / промо.`,
    });
  }

  // Картриджный пайплайн
  const totalCartridges = cartridgePipeline.reduce((s, p) => s + p.expected_cartridges, 0);
  if (totalCartridges > 0) {
    insights.push({
      kind: 'action',
      title: `Картриджей на 12 мес: ${totalCartridges}`,
      text: `Это «программа-минимум» по replacement-циклу. Реальные продажи могут быть выше за счёт upsell. Планируй 1-2 followup-сессии в месяц.`,
    });
  } else {
    insights.push({
      kind: 'warning',
      title: 'Картриджный pipeline пуст',
      text: 'Похоже что фильтры не продавались за последние 6 месяцев. Стоит проверить категоризацию sale_items или подвинуть продажи фильтров.',
    });
  }

  // Сравнение recent vs older (дополнительный insight, если ещё не добавлен выше)
  if (olderAvg > 0 && recentAvgForInsight > 0 && yearOverYearGrowth === 0) {
    const ratio = recentAvgForInsight / olderAvg;
    if (ratio > 1.3) {
      insights.push({
        kind: 'positive',
        title: 'Точка роста: ускорение',
        text: `Последние 3 месяца в среднем на ${Math.round((ratio - 1) * 100)}% выше первых 3 — бизнес ускоряется. Закрепи через расширение партнёрской базы.`,
      });
    } else if (ratio < 0.8) {
      insights.push({
        kind: 'warning',
        title: 'Торможение',
        text: `Последние 3 месяца на ${Math.round((1 - ratio) * 100)}% ниже первых 3. Проверь cold-партнёров и динамику менеджеров.`,
      });
    }
  }

  const result = {
    timeline: forecastTimeline,
    next_6_months_revenue: forecastTimeline.filter(t => t.forecast !== null).reduce((s, t) => s + t.forecast, 0),
    trend_slope: Math.round(slope), // ₸/мес — приближение тренда (recent vs older WMA)
    seasonality: seasonalMult.map((m, i) => ({
      month_idx: i + 1,
      multiplier: Math.round(m * 100) / 100,
    })),
    cartridge_pipeline: cartridgePipeline,
    total_pipeline_units: cartridgePipeline.reduce((s, p) => s + p.expected_cartridges, 0),
    // Debug fields — помогают понять почему прогноз такой
    wma_baseline: Math.round(wmaBaseline),
    yoy_growth_pct: Math.round(yearOverYearGrowth * 100),
    forecast_bounds: { min: Math.round(forecastMin), max: Math.round(forecastMax) },
    insights,
  };
  cacheSet(ck, result);
  return result;
}

// ── 7g2. getCityDetail(city) — детальный анализ одного города ───────────────
//
// При клике на город в географии — открывается popup с:
//   - KPI: orders / customers / revenue / avg_check
//   - B2B vs B2C breakdown
//   - Топ-категории в этом городе
//   - Топ-клиентов / партнёров
//   - Динамика по месяцам
//
const cityDetailInput = z.object({
  city: z.string().min(1).max(100),
});

export async function getCityDetail(req, params) {
  const args = cityDetailInput.parse(params);
  const ck = cacheKey('city-detail', req, args);
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const sales = await loadAllParallel(sb, 'sales',
    'id, sale_date, total_amount, customer_id, partner_id, manager, partner_raw, customer_raw',
    { eq: { city: args.city } }
  );

  if (sales.length === 0) {
    return { city: args.city, total_orders: 0, total_revenue: 0, b2b_orders: 0, b2c_orders: 0, b2b_revenue: 0, b2c_revenue: 0, top_categories: [], top_customers: [], top_partners: [], timeline: [] };
  }

  const saleIds = sales.map(s => s.id);

  // Items для категорий
  const items = [];
  for (let i = 0; i < saleIds.length; i += 200) {
    const batch = saleIds.slice(i, i + 200);
    const { data } = await sb.from('sale_items').select('sale_id, category, qty, amount').in('sale_id', batch);
    items.push(...(data || []));
  }

  // Aggregations
  let b2bOrd = 0, b2cOrd = 0, b2bRev = 0, b2cRev = 0;
  const monthly = {};
  const custAgg = {}, partAgg = {};
  for (const s of sales) {
    if (s.partner_id) { b2bOrd++; b2bRev += s.total_amount || 0; }
    else { b2cOrd++; b2cRev += s.total_amount || 0; }
    if (s.sale_date) {
      const m = s.sale_date.slice(0, 7);
      if (!monthly[m]) monthly[m] = { month: m, orders: 0, revenue: 0 };
      monthly[m].orders++;
      monthly[m].revenue += s.total_amount || 0;
    }
    if (s.customer_id) {
      if (!custAgg[s.customer_id]) custAgg[s.customer_id] = { orders: 0, revenue: 0 };
      custAgg[s.customer_id].orders++;
      custAgg[s.customer_id].revenue += s.total_amount || 0;
    }
    if (s.partner_id) {
      if (!partAgg[s.partner_id]) partAgg[s.partner_id] = { orders: 0, revenue: 0 };
      partAgg[s.partner_id].orders++;
      partAgg[s.partner_id].revenue += s.total_amount || 0;
    }
  }

  const catAgg = {};
  for (const it of items) {
    const c = it.category || 'other';
    if (!catAgg[c]) catAgg[c] = { category: c, qty: 0, revenue: 0 };
    catAgg[c].qty += it.qty || 1;
    catAgg[c].revenue += it.amount || 0;
  }
  const topCategories = Object.values(catAgg).sort((a, b) => b.revenue - a.revenue);

  // Имена топ-клиентов/партнёров
  const topCustIds = Object.entries(custAgg).sort((a, b) => b[1].revenue - a[1].revenue).slice(0, 10);
  const topPartIds = Object.entries(partAgg).sort((a, b) => b[1].revenue - a[1].revenue).slice(0, 10);
  const allIds = [...new Set([...topCustIds.map(x => x[0]), ...topPartIds.map(x => x[0])])];
  const { data: contacts } = allIds.length > 0
    ? await sb.from('partner_contacts').select('id, canonical_name, primary_phone').in('id', allIds)
    : { data: [] };
  const cMap = new Map((contacts || []).map(c => [c.id, c]));

  const result = {
    city: args.city,
    total_orders: sales.length,
    total_revenue: sales.reduce((s, x) => s + (x.total_amount || 0), 0),
    avg_check: sales.length > 0 ? Math.round(sales.reduce((s, x) => s + (x.total_amount || 0), 0) / sales.length) : 0,
    unique_customers: Object.keys(custAgg).length,
    b2b_orders: b2bOrd,
    b2c_orders: b2cOrd,
    b2b_revenue: b2bRev,
    b2c_revenue: b2cRev,
    b2b_pct: (b2bRev + b2cRev) > 0 ? Math.round(b2bRev / (b2bRev + b2cRev) * 100) : 0,
    top_categories: topCategories,
    top_customers: topCustIds.map(([id, agg]) => ({
      contact_id: id,
      name: cMap.get(id)?.canonical_name || '?',
      phone: cMap.get(id)?.primary_phone || null,
      ...agg,
    })),
    top_partners: topPartIds.map(([id, agg]) => ({
      contact_id: id,
      name: cMap.get(id)?.canonical_name || '?',
      phone: cMap.get(id)?.primary_phone || null,
      ...agg,
    })),
    timeline: Object.values(monthly).sort((a, b) => a.month.localeCompare(b.month)),
  };
  cacheSet(ck, result);
  return result;
}

// ── 7g. getGeoStats — города + доставка (Group G) ───────────────────────────
//
// Группировка sales по городу:
// - revenue / orders / clients / avg_check
// - delivery KPI: % delivered / refused / pending (из delivery_status)
// - средние дни доставки (sale_date → delivery_date)
//
export async function getGeoStats(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('geo', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const geoFilters = addCityFilter({}, city);
  const sales = await loadAllParallel(sb, 'sales',
    'sale_date, total_amount, customer_id, city, delivery_status, delivery_date',
    geoFilters
  );

  const byCity = {};
  let totalDelivered = 0, totalRefused = 0, totalPending = 0, totalNoStatus = 0;
  let deliveryDaysSum = 0, deliveryDaysCount = 0;

  for (const s of sales) {
    const city = (s.city || '—').toString().trim() || '—';
    if (!byCity[city]) byCity[city] = {
      city, orders: 0, revenue: 0, customers: new Set(),
      delivered: 0, refused: 0, pending: 0, no_status: 0,
      delivery_days_sum: 0, delivery_days_count: 0,
    };
    const c = byCity[city];
    c.orders++;
    c.revenue += s.total_amount || 0;
    if (s.customer_id) c.customers.add(s.customer_id);

    if (s.delivery_status === 'delivered') { c.delivered++; totalDelivered++; }
    else if (s.delivery_status === 'refused') { c.refused++; totalRefused++; }
    else if (s.delivery_status === 'pending') { c.pending++; totalPending++; }
    else { c.no_status++; totalNoStatus++; }

    if (s.sale_date && s.delivery_date) {
      const days = Math.round(
        (new Date(s.delivery_date).getTime() - new Date(s.sale_date).getTime()) / (1000 * 60 * 60 * 24)
      );
      if (days >= 0 && days <= 90) {
        c.delivery_days_sum += days;
        c.delivery_days_count++;
        deliveryDaysSum += days;
        deliveryDaysCount++;
      }
    }
  }

  const cities = Object.values(byCity).map(c => ({
    city: c.city,
    orders: c.orders,
    revenue: c.revenue,
    customers: c.customers.size,
    avg_check: c.orders > 0 ? Math.round(c.revenue / c.orders) : 0,
    delivered: c.delivered,
    refused: c.refused,
    pending: c.pending,
    no_status: c.no_status,
    delivery_rate_pct: c.orders > 0 ? Math.round(c.delivered / c.orders * 100) : 0,
    avg_delivery_days: c.delivery_days_count > 0 ? Math.round(c.delivery_days_sum / c.delivery_days_count) : null,
  })).sort((a, b) => b.revenue - a.revenue);

  const total = sales.length;
  const result = {
    cities,
    delivery_kpi: {
      total_orders: total,
      delivered: totalDelivered,
      refused: totalRefused,
      pending: totalPending,
      no_status: totalNoStatus,
      delivered_pct: total > 0 ? Math.round(totalDelivered / total * 100) : 0,
      refused_pct: total > 0 ? Math.round(totalRefused / total * 100) : 0,
      avg_delivery_days: deliveryDaysCount > 0 ? Math.round(deliveryDaysSum / deliveryDaysCount) : null,
    },
  };
  cacheSet(ck, result);
  return result;
}

// ── 7f. getAutoInsights — автоматические инсайты (Group H) ──────────────────
//
// Автоматически вычисляемые «факты дня» для главной страницы:
// 1. Сравнение PoP/YoY текущего месяца → если ярко плохо/хорошо
// 2. Топ-партнёр месяца с динамикой
// 3. Концентрация выручки (если 80%/<20% — alert)
// 4. Картриджный gap (сколько followups в эту неделю)
// 5. Cold partners count
// 6. Резкий drop студии (если -30%+)
// 7. Лидер по новым клиентам месяца
// 8. Конверсия из chat_ai (если есть данные)
//
export async function getAutoInsights(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('auto-insights', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const insightsFilters = addCityFilter({}, city);
  const sales = await loadAllParallel(sb, 'sales',
    'id, sale_date, total_amount, partner_id, customer_id, agency_id',
    insightsFilters
  );

  const insights = [];
  const today = new Date();
  const todayStr = today.toISOString().slice(0, 10);

  // 1. Текущий месяц vs предыдущий
  const monthly = {};
  for (const s of sales) {
    if (!s.sale_date) continue;
    const m = s.sale_date.slice(0, 7);
    if (!monthly[m]) monthly[m] = { revenue: 0, orders: 0 };
    monthly[m].revenue += s.total_amount || 0;
    monthly[m].orders++;
  }
  const months = Object.keys(monthly).sort();
  const currM = months[months.length - 1];
  const prevM = months[months.length - 2];
  const yoyM = currM ? `${parseInt(currM.slice(0, 4)) - 1}-${currM.slice(5)}` : null;

  if (currM && prevM) {
    const dRev = monthly[currM].revenue - monthly[prevM].revenue;
    const dPct = monthly[prevM].revenue > 0 ? Math.round(dRev / monthly[prevM].revenue * 100) : 0;
    if (Math.abs(dPct) >= 15) {
      insights.push({
        kind: dPct > 0 ? 'positive' : 'negative',
        priority: Math.abs(dPct) >= 30 ? 1 : 2,
        title: `${currM}: выручка ${dPct > 0 ? 'выросла' : 'упала'} на ${Math.abs(dPct)}%`,
        text: `${monthly[currM].revenue.toLocaleString('ru-RU')} ₸ vs ${monthly[prevM].revenue.toLocaleString('ru-RU')} ₸ в ${prevM}.`,
      });
    }
  }
  if (currM && yoyM && monthly[yoyM]) {
    const dRev = monthly[currM].revenue - monthly[yoyM].revenue;
    const dPct = monthly[yoyM].revenue > 0 ? Math.round(dRev / monthly[yoyM].revenue * 100) : 0;
    if (Math.abs(dPct) >= 25) {
      insights.push({
        kind: dPct > 0 ? 'positive' : 'negative',
        priority: 2,
        title: `Год-к-году ${yoyM} → ${currM}: ${dPct > 0 ? '+' : ''}${dPct}%`,
        text: `${monthly[currM].revenue.toLocaleString('ru-RU')} vs ${monthly[yoyM].revenue.toLocaleString('ru-RU')} ₸.`,
      });
    }
  }

  // 2. Концентрация (Pareto)
  const byPartner = {};
  for (const s of sales) {
    const id = s.partner_id || s.customer_id;
    if (!id) continue;
    byPartner[id] = (byPartner[id] || 0) + (s.total_amount || 0);
  }
  const pSorted = Object.values(byPartner).sort((a, b) => b - a);
  const totalRev = pSorted.reduce((s, x) => s + x, 0);
  let cum = 0;
  let k80 = 0;
  for (let i = 0; i < pSorted.length; i++) {
    cum += pSorted[i];
    if (cum / totalRev >= 0.8) { k80 = i + 1; break; }
  }
  const k80pct = pSorted.length > 0 ? Math.round(k80 / pSorted.length * 100) : 0;
  if (k80pct < 20 && pSorted.length >= 30) {
    insights.push({
      kind: 'warning',
      priority: 1,
      title: `Высокая зависимость: 80% выручки от ${k80pct}% партнёров`,
      text: `Всего ${k80} топ-партнёров делают 80% выручки. Если уйдут — будет резкий провал. Стоит расширять базу.`,
    });
  } else if (k80pct >= 40) {
    insights.push({
      kind: 'positive',
      priority: 3,
      title: `Распределённая выручка: 80% делают ${k80pct}% партнёров`,
      text: `Бизнес устойчивый — нет критической зависимости от 5-10 топов.`,
    });
  }

  // 3. Cold partners
  const cutoff60 = new Date(today.getTime() - 60 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const partnerLastSale = {};
  const partnerTotalRev = {};
  for (const s of sales) {
    if (!s.partner_id || !s.sale_date) continue;
    if (!partnerLastSale[s.partner_id] || s.sale_date > partnerLastSale[s.partner_id]) {
      partnerLastSale[s.partner_id] = s.sale_date;
    }
    partnerTotalRev[s.partner_id] = (partnerTotalRev[s.partner_id] || 0) + (s.total_amount || 0);
  }
  const coldCount = Object.keys(partnerLastSale).filter(id =>
    partnerLastSale[id] < cutoff60 && (partnerTotalRev[id] || 0) >= 500000
  ).length;
  if (coldCount >= 5) {
    insights.push({
      kind: 'warning',
      priority: 2,
      title: `${coldCount} партнёров не приводили клиентов >60 дней`,
      text: `Каждый из них исторически принёс ≥500K ₸. Открой «Аналитика → Инсайты → Холодные партнёры» для списка.`,
    });
  }

  // 4. Картриджный gap (followups due соседние недели)
  const next14 = new Date(today.getTime() + 14 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { count: dueFollowups } = await sb.from('followups')
    .select('*', { count: 'exact', head: true })
    .is('completed_at', null)
    .lte('due_date', next14);
  if ((dueFollowups || 0) >= 10) {
    insights.push({
      kind: 'action',
      priority: 1,
      title: `${dueFollowups} напоминаний на следующие 2 недели`,
      text: `Откройте «Партнёры» → ленту followups, или используйте виджет на главной странице.`,
    });
  }

  // 5. Топ-движение текущего месяца
  if (currM && prevM) {
    const cFrom = `${currM}-01`, cTo = nextMonth(currM) + '-01';
    const pFrom = `${prevM}-01`, pTo = nextMonth(prevM) + '-01';
    const byCurr = {}, byPrev = {};
    for (const s of sales) {
      const id = s.partner_id || s.customer_id;
      if (!id) continue;
      if (s.sale_date >= cFrom && s.sale_date < cTo) byCurr[id] = (byCurr[id] || 0) + (s.total_amount || 0);
      else if (s.sale_date >= pFrom && s.sale_date < pTo) byPrev[id] = (byPrev[id] || 0) + (s.total_amount || 0);
    }
    const movers = Object.keys({ ...byCurr, ...byPrev }).map(id => ({
      id, curr: byCurr[id] || 0, prev: byPrev[id] || 0, delta: (byCurr[id] || 0) - (byPrev[id] || 0),
    }));
    const topUp = movers.sort((a, b) => b.delta - a.delta)[0];
    const topDown = movers.sort((a, b) => a.delta - b.delta)[0];
    if (topUp && topUp.delta >= 500000) {
      const { data: c } = await sb.from('partner_contacts').select('canonical_name').eq('id', topUp.id).maybeSingle();
      insights.push({
        kind: 'positive',
        priority: 3,
        title: `★ Звезда месяца: ${c?.canonical_name || '?'}`,
        text: `Принёс +${topUp.delta.toLocaleString('ru-RU')} ₸ к ${prevM} (${topUp.prev.toLocaleString('ru-RU')} → ${topUp.curr.toLocaleString('ru-RU')}).`,
      });
    }
    if (topDown && topDown.delta <= -500000) {
      const { data: c } = await sb.from('partner_contacts').select('canonical_name').eq('id', topDown.id).maybeSingle();
      insights.push({
        kind: 'warning',
        priority: 2,
        title: `Просел: ${c?.canonical_name || '?'}`,
        text: `Принёс на ${(-topDown.delta).toLocaleString('ru-RU')} ₸ меньше чем в ${prevM}. Стоит написать.`,
      });
    }
  }

  // 6. Картриджный gap по фактическим данным (последние 6 мес)
  const start6m = new Date(today.getTime() - 6 * 31 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const { data: recentItems } = await sb.from('sale_items')
    .select('category, qty, sale_id')
    .eq('category', 'water_filter')
    .order('id', { ascending: false })
    .limit(5000);
  if (recentItems && recentItems.length > 0) {
    insights.push({
      kind: 'action',
      priority: 2,
      title: `База re-engage: ${recentItems.length} проданных фильтров`,
      text: `Картриджи нужно менять каждые 6 мес. Проверь «Аналитика → Продукты → Картриджная воронка».`,
    });
  }

  // Sort by priority
  insights.sort((a, b) => a.priority - b.priority);

  const result = { insights, generated_at: new Date().toISOString(), period: currM };
  cacheSet(ck, result);
  return result;
}

// ── 7e2. getSalesWithChats — продажи которые имеют WhatsApp-диалоги ─────────
//
// Adil-овая фича: показать список продаж + связанные WhatsApp-диалоги
// (читать как покупка прошла). Базовая фильтрация — только sales где
// у contact (customer or partner) есть linked_chat_jids.
//
// FIX 2026-05-02 (Adil feedback): убрали N+1 loop на messages/chat_ai.
// Старый код делал отдельный запрос к messages + chat_ai для каждой sale —
// при 50 sales = 100+ sequential DB roundtrips (~10-15 сек).
// Новый: batch fetch всех JIDs страницы одним запросом, группируем в памяти.
// Также убрали `.slice(0, 500)` на linkedIds — теперь батчим через loadAllParallel-style batches.
// Добавлен 1h cache (данные меняются редко, достаточно свежести для воронки).
//
export async function getSalesWithChats(req, params = {}) {
  const sb = pickClient(req);
  const limit = Math.min(parseInt(params.limit) || 100, 500);
  const offset = Math.max(parseInt(params.offset) || 0, 0);

  // Cache: 1h TTL (linked_chat_jids меняются только при новом WA-диалоге)
  const ck = cacheKey('sales-with-chats', req, { limit, offset });
  const cached = cacheGet(ck);
  if (cached) return cached;

  // 1. Все contacts с jids (loadAllParallel — нет row limit)
  const linked = await loadAllParallel(sb, 'partner_contacts',
    'id, canonical_name, primary_phone, linked_chat_jids',
    { notNull: ['linked_chat_jids'] }
  );
  const contactsMap = new Map();
  for (const c of linked) {
    if ((c.linked_chat_jids || []).length > 0) {
      contactsMap.set(c.id, c);
    }
  }
  const linkedIds = [...contactsMap.keys()];

  if (linkedIds.length === 0) {
    return { items: [], total: 0 };
  }

  // 2. Sales где customer_id или partner_id ∈ linkedIds — батчами по 500 UUID
  // (PostgREST IN() имеет практический лимит URL длины; 500 UUID ~= 18KB URL — safe)
  const batchSize = 500;
  const salesByIdMap = new Map();

  async function fetchSalesForIds(col, ids) {
    for (let i = 0; i < ids.length; i += batchSize) {
      const batch = ids.slice(i, i + batchSize);
      const { data } = await sb.from('sales')
        .select('id, sale_date, total_amount, source_file, order_num, customer_id, partner_id, customer_raw, partner_raw, manager')
        .in(col, batch)
        .order('sale_date', { ascending: false })
        .limit(limit + offset + 200); // slight over-fetch for merge dedup
      for (const s of data || []) if (!salesByIdMap.has(s.id)) salesByIdMap.set(s.id, s);
    }
  }

  await Promise.all([
    fetchSalesForIds('customer_id', linkedIds),
    fetchSalesForIds('partner_id', linkedIds),
  ]);

  const sortedSales = [...salesByIdMap.values()]
    .sort((a, b) => (b.sale_date || '').localeCompare(a.sale_date || ''))
    .slice(offset, offset + limit);

  // 3. Collect all JIDs for the current page — single batch query for messages + chat_ai
  //    instead of N+1 per-sale queries.
  const allJids = new Set();
  const saleJidsMap = new Map(); // sale_id → { jids, contact }
  for (const s of sortedSales) {
    const linkedContact = (s.customer_id && contactsMap.get(s.customer_id))
      || (s.partner_id && contactsMap.get(s.partner_id));
    const jids = linkedContact?.linked_chat_jids || [];
    saleJidsMap.set(s.id, { jids, linkedContact });
    for (const j of jids) allJids.add(j);
  }

  const jidsArr = [...allJids];

  // Batch fetch messages + chat_ai for all JIDs on this page (2 queries total, not N*2)
  let msgsByJid = {}; // jid → [msg, ...]
  let aiByJid = {};   // jid → chat_ai record (most recent)

  if (jidsArr.length > 0) {
    // Split into batches of 100 JIDs (PostgREST IN safety)
    const JID_BATCH = 100;
    const msgBatches = [];
    const aiBatches = [];
    for (let i = 0; i < jidsArr.length; i += JID_BATCH) {
      const jBatch = jidsArr.slice(i, i + JID_BATCH);
      msgBatches.push(
        sb.from('messages')
          .select('id, remote_jid, body, from_me, timestamp, session_id')
          .in('remote_jid', jBatch)
          .order('timestamp', { ascending: false })
          .limit(200) // top-200 messages across all JIDs in batch
      );
      aiBatches.push(
        sb.from('chat_ai')
          .select('id, remote_jid, intent, lead_temperature, deal_stage, summary_ru, manager_issues, risk_flags, analyzed_at')
          .in('remote_jid', jBatch)
          .order('analyzed_at', { ascending: false })
          .limit(JID_BATCH) // latest one per JID is enough
      );
    }

    const [msgResults, aiResults] = await Promise.all([
      Promise.all(msgBatches),
      Promise.all(aiBatches),
    ]);

    for (const { data } of msgResults) {
      for (const m of data || []) {
        if (!msgsByJid[m.remote_jid]) msgsByJid[m.remote_jid] = [];
        msgsByJid[m.remote_jid].push(m);
      }
    }
    for (const { data } of aiResults) {
      for (const a of data || []) {
        // keep only most recent per JID (already ordered by analyzed_at DESC)
        if (!aiByJid[a.remote_jid]) aiByJid[a.remote_jid] = a;
      }
    }
  }

  // 4. Assemble result — pure in-memory join, no more DB calls per sale
  const result = [];
  for (const s of sortedSales) {
    const { jids, linkedContact } = saleJidsMap.get(s.id) || { jids: [], linkedContact: null };

    // Collect messages from all jids for this sale, filter to ±14/+7 day window
    let recentMessages = [];
    let chatAi = null;

    if (jids.length > 0 && s.sale_date) {
      const saleTs = new Date(s.sale_date).getTime();
      const fromTs = saleTs - 14 * 24 * 3600 * 1000;
      const toTs   = saleTs +  7 * 24 * 3600 * 1000;

      for (const jid of jids) {
        for (const m of (msgsByJid[jid] || [])) {
          const mts = new Date(m.timestamp).getTime();
          if (mts >= fromTs && mts <= toTs) recentMessages.push(m);
        }
        // Pick most recent chat_ai within window
        const a = aiByJid[jid];
        if (a && !chatAi) {
          const ats = new Date(a.analyzed_at).getTime();
          if (ats >= fromTs && ats <= toTs) chatAi = a;
        }
      }

      recentMessages.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    }

    result.push({
      sale_id: s.id,
      sale_date: s.sale_date,
      total_amount: s.total_amount,
      source_file: s.source_file,
      order_num: s.order_num,
      customer_raw: s.customer_raw,
      partner_raw: s.partner_raw,
      manager: s.manager,
      contact: linkedContact ? {
        id: linkedContact.id,
        name: linkedContact.canonical_name,
        phone: linkedContact.primary_phone,
        jids,
      } : null,
      messages_in_window: recentMessages.length,
      messages_sample: recentMessages.slice(0, 6).map(m => ({
        id: m.id, body: (m.body || '').slice(0, 200),
        from_me: m.from_me, ts: m.timestamp, session_id: m.session_id,
      })),
      chat_ai: chatAi,
    });
  }

  const out = { items: result, total: salesByIdMap.size };
  // Cache 1h — воронка не требует real-time свежести (24h стандартный TTL избыточен)
  if (ck) {
    _cache.set(ck, { data: out, expiresAt: Date.now() + 60 * 60 * 1000 });
  }
  return out;
}

// ── 7e. getLeadFunnel — lead→sale conversion (Group E) ──────────────────────
//
// Соединяет chat_ai (там есть intent / lead_temperature / deal_stage)
// с sales через jid → partner_contact_id → продажи этого контакта.
//
// Lead = строка в chat_ai (диалог с потенциальным клиентом).
// Считаем converted = была ли продажа этому контакту в окне ±30 дней от analyzed_at.
//
// Группируем:
// - by_intent: price_inquiry / consultation / complaint / collaboration / small_talk
// - by_temperature: hot / warm / cold / dead
// - by_deal_stage: first_contact → consultation → ... → completed/refused
// - by_source: instagram / altyn_agash / unknown
// - by_session: per manager session
//
export async function getLeadFunnel(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('lead-funnel', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // 1. Все chat_ai с jid (limit 5000 last)
  const { data: aiData } = await sb.from('chat_ai')
    .select('id, analyzed_at, intent, lead_temperature, deal_stage, lead_source, session_id, remote_jid, customer_type, dialog_session_id')
    .order('analyzed_at', { ascending: false })
    .limit(5000);
  const ai = aiData || [];

  // 2. Все partner_contacts с jid (для матча)
  const contacts = await loadAllParallel(sb, 'partner_contacts',
    'id, primary_phone, linked_chat_jids',
    { notNull: ['linked_chat_jids'] }
  );
  const contactByJid = new Map();
  for (const c of contacts) {
    for (const j of c.linked_chat_jids || []) contactByJid.set(j, c.id);
  }

  // 3. Все sales — для проверки конверсии (с city-фильтром)
  const funnelSalesFilters = addCityFilter({}, city);
  const sales = await loadAllParallel(sb, 'sales',
    'id, sale_date, customer_id, partner_id, total_amount',
    funnelSalesFilters
  );
  const salesByContact = new Map();
  for (const s of sales) {
    for (const id of [s.customer_id, s.partner_id].filter(Boolean)) {
      if (!salesByContact.has(id)) salesByContact.set(id, []);
      salesByContact.get(id).push(s);
    }
  }

  // 4. Аннотируем каждый AI-record
  const leads = [];
  for (const a of ai) {
    const contactId = contactByJid.get(a.remote_jid);
    let converted = false;
    let convertedRevenue = 0;
    if (contactId && a.analyzed_at) {
      const aTime = new Date(a.analyzed_at).getTime();
      const wnd = 30 * 24 * 60 * 60 * 1000; // 30 дней
      for (const s of salesByContact.get(contactId) || []) {
        if (!s.sale_date) continue;
        const sTime = new Date(s.sale_date).getTime();
        if (Math.abs(sTime - aTime) <= wnd) {
          converted = true;
          convertedRevenue += s.total_amount || 0;
          break;
        }
      }
    }
    leads.push({
      ...a,
      contact_id: contactId,
      has_contact: Boolean(contactId),
      converted,
      converted_revenue: convertedRevenue,
    });
  }

  // 5. Bucket aggregations
  function bucket(field, label_field = field) {
    const buckets = {};
    for (const l of leads) {
      const v = l[field] || '—';
      if (!buckets[v]) buckets[v] = { name: v, total: 0, converted: 0, revenue: 0 };
      buckets[v].total++;
      if (l.converted) {
        buckets[v].converted++;
        buckets[v].revenue += l.converted_revenue;
      }
    }
    return Object.values(buckets)
      .map(b => ({ ...b, conversion_pct: b.total > 0 ? Math.round(b.converted / b.total * 100) : 0 }))
      .sort((a, b) => b.total - a.total);
  }

  // 6. Per session
  const bySession = bucket('session_id');
  // Per intent
  const byIntent = bucket('intent');
  // Per temperature
  const byTemp = bucket('lead_temperature');
  // Per deal stage
  const byStage = bucket('deal_stage');
  // Per source
  const bySource = bucket('lead_source');

  // 7. Funnel (deal_stage в порядке voronka)
  const stageOrder = ['first_contact', 'consultation', 'model_selection', 'price_negotiation', 'payment', 'delivery', 'completed', 'refused', 'needs_review'];
  const funnel = stageOrder.map(stage => {
    const b = byStage.find(x => x.name === stage);
    return { stage, total: b?.total || 0, converted: b?.converted || 0, conversion_pct: b?.conversion_pct || 0 };
  });

  // 8. Aggregate KPI
  const totalLeads = leads.length;
  const totalConverted = leads.filter(l => l.converted).length;
  const totalRevenue = leads.reduce((s, l) => s + (l.converted_revenue || 0), 0);
  const overallConversion = totalLeads > 0 ? Math.round(totalConverted / totalLeads * 100) : 0;

  const result = {
    kpi: {
      total_leads: totalLeads,
      converted: totalConverted,
      conversion_pct: overallConversion,
      total_revenue: totalRevenue,
      contacts_matched: leads.filter(l => l.has_contact).length,
    },
    by_intent: byIntent.slice(0, 10),
    by_temperature: byTemp.slice(0, 10),
    by_deal_stage: byStage.slice(0, 10),
    by_source: bySource.slice(0, 10),
    by_session: bySession.slice(0, 10),
    funnel,
  };
  cacheSet(ck, result);
  return result;
}

// ── 7d-extra. getManagerPerformance — performance review (Group F) ──────────
//
// Аналитика по менеджерам (поле sales.manager — текстовое имя):
// - leaderboard: total_revenue / total_orders / avg_check / first_purchase_date
// - timeline: revenue по месяцам по каждому менеджеру (multi-line)
// - segments_per_manager: разрез B2B/B2C
// - response_time (если есть в manager_analytics): средний secs до ответа
//
export async function getManagerPerformance(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('manager-perf', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const perfFilters = addCityFilter({}, city);
  const sales = await loadAllParallel(sb, 'sales',
    'id, sale_date, total_amount, manager, partner_id, customer_id',
    perfFilters
  );

  // ТОЛЬКО 4 активных менеджера. Все остальные имена (Сания, Алим, Асель, Саги, ...)
  // НЕ попадают в leaderboard. Их заказы остаются в БД, но не учитываются здесь.
  const ACTIVE_MANAGERS = ['Айтжан', 'Нурсултан', 'Мади', 'Ренат'];

  function normalizeManagerName(raw) {
    if (!raw) return null;
    const s = String(raw).trim();
    for (const m of ACTIVE_MANAGERS) {
      if (s.toLowerCase().includes(m.toLowerCase())) return m;
    }
    return null; // unknown — отбрасываем
  }

  // Расщепляем "Айтжан/Нурсултан" → ["Айтжан", "Нурсултан"]
  // Каждому достаётся 1/N доли заказа и выручки.
  function splitManagers(raw) {
    if (!raw) return [];
    return String(raw)
      .split(/[/\\,;+]/)
      .map(s => s.trim())
      .filter(Boolean)
      .map(normalizeManagerName)
      .filter(Boolean);
  }

  const byManager = {};
  for (const s of sales) {
    const managers = splitManagers(s.manager);
    if (managers.length === 0) continue;
    const share = 1 / managers.length;

    for (const m of managers) {
      if (!byManager[m]) byManager[m] = {
        name: m,
        orders: 0, revenue: 0, b2b_orders: 0, b2c_orders: 0,
        first_date: null, last_date: null, by_month: {},
        shared_orders: 0,
      };
      const x = byManager[m];
      x.orders += share;
      x.revenue += (s.total_amount || 0) * share;
      if (managers.length > 1) x.shared_orders++;
      if (s.partner_id) x.b2b_orders += share;
      else x.b2c_orders += share;
      if (s.sale_date) {
        if (!x.first_date || s.sale_date < x.first_date) x.first_date = s.sale_date;
        if (!x.last_date || s.sale_date > x.last_date) x.last_date = s.sale_date;
        const ym = s.sale_date.slice(0, 7);
        if (!x.by_month[ym]) x.by_month[ym] = { revenue: 0, orders: 0 };
        x.by_month[ym].revenue += (s.total_amount || 0) * share;
        x.by_month[ym].orders += share;
      }
    }
  }

  // Сборка timeline для multi-line chart (последние 12 мес)
  const allMonths = [...new Set(sales.map(s => s.sale_date?.slice(0, 7)).filter(Boolean))].sort();
  const last12 = allMonths.slice(-12);
  const timeline = last12.map(m => {
    const row = { month: m };
    for (const mname of Object.keys(byManager)) {
      row[mname] = byManager[mname].by_month[m]?.revenue || 0;
    }
    return row;
  });

  const leaderboard = Object.values(byManager)
    .filter(m => m.orders >= 1)
    .map(m => ({
      ...m,
      orders: Math.round(m.orders * 10) / 10, // округляем (могут быть half-orders из split)
      revenue: Math.round(m.revenue),
      b2b_orders: Math.round(m.b2b_orders * 10) / 10,
      b2c_orders: Math.round(m.b2c_orders * 10) / 10,
      avg_check: m.orders > 0 ? Math.round(m.revenue / m.orders) : 0,
      b2b_pct: m.orders > 0 ? Math.round(m.b2b_orders / m.orders * 100) : 0,
      sparkline: last12.map(ym => m.by_month[ym]?.revenue || 0),
    }))
    .sort((a, b) => b.revenue - a.revenue)
    .map(m => { delete m.by_month; return m; });

  // Response time (если manager_analytics существует — try-catch)
  let responseTimes = [];
  try {
    const { data } = await sb.from('manager_analytics')
      .select('manager_session_id, response_time_seconds, customer_message_at')
      .not('response_time_seconds', 'is', null)
      .order('customer_message_at', { ascending: false })
      .limit(10000);
    if (data && data.length > 0) {
      const bySession = {};
      for (const r of data) {
        const sid = r.manager_session_id || '—';
        if (!bySession[sid]) bySession[sid] = { total: 0, count: 0 };
        bySession[sid].total += r.response_time_seconds || 0;
        bySession[sid].count++;
      }
      responseTimes = Object.entries(bySession).map(([sid, v]) => ({
        session_id: sid,
        avg_response_seconds: v.count > 0 ? Math.round(v.total / v.count) : 0,
        sample_count: v.count,
      })).sort((a, b) => a.avg_response_seconds - b.avg_response_seconds);
    }
  } catch (e) {
    // manager_analytics может не существовать — это OK
  }

  const result = { leaderboard, timeline, response_times: responseTimes };
  cacheSet(ck, result);
  return result;
}

// ── 7d. getPartnerInsights — cold/rising/ROI (Group D) ──────────────────────
//
// Глубокий анализ партнёров (роль = 'partner', т.е. дизайнеры/посредники):
// 1) cold_partners: партнёры которые приводили клиентов раньше, но не за последние 60+ дней
// 2) rising_stars: партнёры в топ-30 по выручке за последние 90 дней,
//    которых не было в топ-30 ранее (растущие)
// 3) roi: парсим commission_text из заказов (часто текст "10%", "50000",
//    "дозатор в подарок") → грубая оценка стоимости каждого партнёра
// 4) studio_vs_independent: % выручки от партнёров со студией vs одиночек
// 5) journey_per_partner: для каждого топ-партнёра — список месяцев с активностью
//
export async function getPartnerInsights(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('partner-insights', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const today = new Date();
  const todayMs = today.getTime();
  const cold_threshold_ms = 60 * 24 * 60 * 60 * 1000; // 60 дней
  const cutoff_90 = new Date(todayMs - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const cutoff_180 = new Date(todayMs - 180 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  // 1. Все sales где партнёр заполнен — параллельная пагинация (с city-фильтром)
  const piFilters = addCityFilter({ notNull: ['partner_id'] }, city);
  const sales = await loadAllParallel(sb, 'sales',
    'id, partner_id, sale_date, total_amount, customer_id, agency_id, commission_text',
    piFilters
  );

  // 2. Aggregate per partner
  const byPartner = {};
  for (const s of sales) {
    if (!byPartner[s.partner_id]) byPartner[s.partner_id] = {
      partner_id: s.partner_id,
      orders: 0, revenue: 0, last_date: null, first_date: null,
      revenue_recent_90: 0, revenue_prev_90_180: 0,
      commission_total: 0, commission_pct_avg: 0, commission_text_count: 0,
      unique_customers: new Set(),
    };
    const p = byPartner[s.partner_id];
    p.orders++;
    p.revenue += s.total_amount || 0;
    if (!p.last_date || s.sale_date > p.last_date) p.last_date = s.sale_date;
    if (!p.first_date || s.sale_date < p.first_date) p.first_date = s.sale_date;
    if (s.sale_date >= cutoff_90) p.revenue_recent_90 += s.total_amount || 0;
    else if (s.sale_date >= cutoff_180) p.revenue_prev_90_180 += s.total_amount || 0;
    if (s.customer_id) p.unique_customers.add(s.customer_id);

    // Commission parser (грубо)
    const ct = (s.commission_text || '').toString();
    if (ct) {
      p.commission_text_count++;
      // Попробуем найти %
      const pctMatch = ct.match(/(\d{1,2})\s*%/);
      if (pctMatch) {
        const pct = parseInt(pctMatch[1]);
        if (pct > 0 && pct < 50) {
          p.commission_total += (s.total_amount || 0) * pct / 100;
          p.commission_pct_avg += pct;
        }
      } else {
        // Прямое число
        const numMatch = ct.match(/(\d{4,7})/);
        if (numMatch) p.commission_total += parseInt(numMatch[1]);
      }
    }
  }

  // Финализируем агрегаты
  const partners = Object.values(byPartner).map(p => ({
    ...p,
    unique_customers: p.unique_customers.size,
    commission_pct_avg: p.commission_text_count > 0
      ? Math.round(p.commission_pct_avg / p.commission_text_count)
      : null,
    days_since_last: p.last_date
      ? Math.floor((todayMs - new Date(p.last_date).getTime()) / (1000 * 60 * 60 * 24))
      : null,
    roi_ratio: p.commission_total > 0 && p.revenue > 0
      ? Math.round((p.revenue / p.commission_total) * 10) / 10
      : null,
  }));

  // Имена + студия — параллельная пагинация по 200 ID за раз
  // (PostgREST .in() имеет лимит на длину URL и default 1000 rows)
  const partnerIds = partners.map(p => p.partner_id);
  const cMap = new Map();
  if (partnerIds.length > 0) {
    const batches = [];
    for (let i = 0; i < partnerIds.length; i += 200) batches.push(partnerIds.slice(i, i + 200));
    const results = await Promise.all(
      batches.map(batch =>
        sb.from('partner_contacts').select('id, canonical_name, primary_phone, agency_id').in('id', batch)
      )
    );
    for (const r of results) {
      for (const c of (r.data || [])) cMap.set(c.id, c);
    }
  }

  const { data: agencies } = await sb.from('agencies').select('id, canonical_name');
  const aMap = new Map((agencies || []).map(a => [a.id, a.canonical_name]));

  for (const p of partners) {
    const c = cMap.get(p.partner_id);
    p.name = c?.canonical_name || '?';
    p.phone = c?.primary_phone || null;
    p.agency_id = c?.agency_id || null;
    p.agency_name = c?.agency_id ? aMap.get(c.agency_id) || null : null;
  }

  // 3. Cold partners (>=60 дней без активности, при revenue >= 500K за всё время)
  const cold_partners = partners
    .filter(p => p.days_since_last !== null && p.days_since_last >= 60 && p.revenue >= 500000)
    .sort((a, b) => b.revenue - a.revenue)
    .slice(0, 30);

  // 4. Rising stars: revenue_recent_90 >= 200K, при этом revenue_prev_90_180 < revenue_recent_90 / 2
  const rising_stars = partners
    .filter(p => p.revenue_recent_90 >= 200000 && p.revenue_prev_90_180 < p.revenue_recent_90 * 0.5)
    .sort((a, b) => b.revenue_recent_90 - a.revenue_recent_90)
    .slice(0, 20);

  // 5. ROI ranking — топ по revenue / commission
  const roi_ranking = partners
    .filter(p => p.commission_total > 0 && p.revenue >= 500000)
    .sort((a, b) => b.roi_ratio - a.roi_ratio)
    .slice(0, 20);

  // 6. Studio vs Independent
  let with_studio = { partners: 0, revenue: 0 };
  let independent = { partners: 0, revenue: 0 };
  for (const p of partners) {
    if (p.agency_id) {
      with_studio.partners++;
      with_studio.revenue += p.revenue;
    } else {
      independent.partners++;
      independent.revenue += p.revenue;
    }
  }

  const result = {
    cold_partners,
    rising_stars,
    roi_ranking,
    studio_vs_independent: { with_studio, independent },
    total_partners: partners.length,
  };
  cacheSet(ck, result);
  return result;
}

// ── 9. getCitiesSummary — сводка по городам (count + revenue) ───────────────
//
// Возвращает агрегаты отдельно для Алматы / Астаны / null-city (прочие).
// Используется в combined-view дашборда для переключателя города.
//
export async function getCitiesSummary(req) {
  const ck = cacheKey('cities-summary', req, {});
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const sales = await loadAllParallel(sb, 'sales', 'city, total_amount');

  const byCity = {};
  for (const s of sales) {
    const c = (s.city || '').trim() || null;
    const key = c || '—';
    if (!byCity[key]) byCity[key] = { city: key, orders: 0, revenue: 0 };
    byCity[key].orders++;
    byCity[key].revenue += s.total_amount || 0;
  }

  // Собираем итог и сводку по основным городам
  const allCities = Object.values(byCity).sort((a, b) => b.revenue - a.revenue);
  const total_orders = sales.length;
  const total_revenue = sales.reduce((s, x) => s + (x.total_amount || 0), 0);

  // Канонические города + прочие
  const KNOWN_CITIES = ['Алматы', 'Астана'];
  const summary = KNOWN_CITIES.map(city => ({
    city,
    orders: byCity[city]?.orders || 0,
    revenue: byCity[city]?.revenue || 0,
    revenue_pct: total_revenue > 0
      ? Math.round(((byCity[city]?.revenue || 0) / total_revenue) * 100)
      : 0,
  }));
  const other_revenue = allCities
    .filter(c => !KNOWN_CITIES.includes(c.city))
    .reduce((s, c) => s + c.revenue, 0);
  const other_orders = allCities
    .filter(c => !KNOWN_CITIES.includes(c.city))
    .reduce((s, c) => s + c.orders, 0);
  summary.push({
    city: '—',
    orders: other_orders,
    revenue: other_revenue,
    revenue_pct: total_revenue > 0 ? Math.round((other_revenue / total_revenue) * 100) : 0,
  });

  const result = {
    summary,
    all_cities: allCities,
    total_orders,
    total_revenue,
  };
  cacheSet(ck, result);
  return result;
}

// ── 8. searchPartner (для AI tools / quick lookup) ──────────────────────────
//
// По имени или телефону вернуть до 10 контактов с базовыми агрегатами.
// Используется когда AI спрашивает «кто такой Бибинур».
//
const searchInput = z.object({
  q: z.string().min(1).max(200),
  limit: z.number().int().min(1).max(50).default(10),
});

export async function searchPartner(req, params) {
  const args = searchInput.parse(params);
  const sb = pickClient(req);
  const s = safeFilterValue(args.q);
  if (!s) return { items: [] };

  const { data, error } = await sb.from('v_partner_full')
    .select('id, canonical_name, primary_phone, agency_name, orders_count, total_revenue, last_purchase_date, total_messages')
    .gt('orders_count', 0)
    .or(`canonical_name.ilike.%${s}%,primary_phone.ilike.%${s}%`)
    .order('total_revenue', { ascending: false })
    .limit(args.limit);

  if (error) throw new Error(`searchPartner: ${error.message}`);
  return { items: data || [] };
}

// ── 9. getMultiYearBreakdown — выделенный endpoint для MultiYearChart ─────────
//
// Возвращает wide-format данные (year × channel × metric) по месяцам года.
// Это то же самое что analytics.multi_year_breakdown, но:
//   a) отдельный endpoint с более коротким TTL на кэш (нет тяжёлых sub-queries)
//   b) поддерживает multi-city breakdown (?cities=Алматы,Астана)
//
// Format A (multi-city, mode='multi'):
//   data: [
//     { month_idx: 1, month: 'Янв', city: 'Алматы', '2025__b2b__revenue': 12M, ... },
//     { month_idx: 1, month: 'Янв', city: 'Астана', '2025__b2b__revenue': 8M, ... },
//     ...
//   ]
//   years_available: ['2023', '2024', '2025', '2026']
//   cities: ['Алматы', 'Астана']
//   mode: 'multi'
//
// Format B (single city или all, mode='single'|'all'):
//   data: [
//     { month_idx: 1, month: 'Янв', '2025__b2b__revenue': 12M, ... },
//     ...
//   ]  — без поля city (совместимо со старым getSalesAnalytics.multi_year_breakdown)
//   years_available: ['2023', '2024', '2025', '2026']
//   mode: 'single' | 'all'
//
const RU_MONTH_SHORT = ['Янв','Фев','Мар','Апр','Май','Июн','Июл','Авг','Сен','Окт','Ноя','Дек'];

/**
 * Вычисляет multi_year_breakdown для одного city (или 'all').
 * Используется как внутренний helper — вызывается из getMultiYearBreakdown.
 */
async function _computeMultiYearBreakdown(sb, opts = {}) {
  const { date_from, date_to, city = 'all' } = opts;

  let filters = {};
  if (date_from) filters.gte = { sale_date: date_from };
  if (date_to) filters.lte = { sale_date: date_to };
  filters = addCityFilter(filters, city);

  const sales = await loadAllParallel(sb, 'sales',
    'sale_date, total_amount, partner_id',
    filters
  );

  const yearChannelMonthly = {};
  for (const s of sales) {
    if (!s.sale_date) continue;
    const y = s.sale_date.slice(0, 4);
    const m = parseInt(s.sale_date.slice(5, 7), 10);
    const ch = s.partner_id ? 'b2b' : 'b2c';
    if (!yearChannelMonthly[m]) yearChannelMonthly[m] = {};
    const buckets = yearChannelMonthly[m];
    for (const channel of [ch, 'all']) {
      const key = `${y}__${channel}`;
      if (!buckets[key]) buckets[key] = { revenue: 0, orders: 0 };
      buckets[key].revenue += s.total_amount || 0;
      buckets[key].orders += 1;
    }
  }

  const data = Array.from({ length: 12 }, (_, i) => {
    const idx = i + 1;
    const row = { month_idx: idx, month: RU_MONTH_SHORT[i] };
    const buckets = yearChannelMonthly[idx] || {};
    for (const [key, v] of Object.entries(buckets)) {
      const avgCheck = v.orders > 0 ? Math.round(v.revenue / v.orders) : 0;
      row[`${key}__revenue`] = v.revenue;
      row[`${key}__orders`] = v.orders;
      row[`${key}__avg_check`] = avgCheck;
    }
    return row;
  });

  const yearsAvailable = [...new Set(sales.map(s => s.sale_date?.slice(0, 4)).filter(Boolean))].sort();
  return { data, years_available: yearsAvailable };
}

export async function getMultiYearBreakdown(req, opts = {}) {
  // opts может содержать:
  //   city     — single city или 'all' (legacy compat)
  //   cities   — string[] (два города) → multi-city breakdown
  //   date_from, date_to — период

  const { city = 'all', cities, date_from, date_to } = opts;
  const isMulti = Array.isArray(cities) && cities.length > 1;
  const isSingleFromCities = Array.isArray(cities) && cities.length === 1;

  // Resolve effective city for single-city path
  const effectiveCity = isSingleFromCities ? cities[0] : city;

  const ckParams = isMulti
    ? { cities: [...cities].sort(), date_from, date_to }
    : { city: effectiveCity, date_from, date_to };
  const ck = cacheKey('multi-year-breakdown', req, ckParams);
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  if (isMulti) {
    // Format A: данные для каждого города, items получают поле city
    const cityResults = await Promise.all(
      cities.map(c => _computeMultiYearBreakdown(sb, { date_from, date_to, city: c }))
    );

    // Мёрджим data массивы — каждая точка получает поле city
    const mergedData = [];
    for (let i = 0; i < cities.length; i++) {
      for (const item of cityResults[i].data) {
        mergedData.push({ ...item, city: cities[i] });
      }
    }
    // Сортировка: month_idx ASC, city ASC (для удобства frontend группировки)
    mergedData.sort((a, b) => a.month_idx - b.month_idx || a.city.localeCompare(b.city));

    const yearsAvailable = [...new Set(cityResults.flatMap(r => r.years_available))].sort();

    const result = {
      data: mergedData,
      years_available: yearsAvailable,
      cities,
      mode: 'multi',
    };
    cacheSet(ck, result);
    return result;
  }

  // Format B: single-city или all — обратная совместимость
  const { data, years_available } = await _computeMultiYearBreakdown(sb, {
    date_from,
    date_to,
    city: effectiveCity,
  });

  const result = {
    data,
    years_available,
    cities: effectiveCity !== 'all' ? [effectiveCity] : [],
    mode: effectiveCity !== 'all' ? 'single' : 'all',
  };
  cacheSet(ck, result);
  return result;
}

// ── Distribution helpers ─────────────────────────────────────────────────────
//
// Общая логика для pie-chart distribution endpoints:
//   1. Загружаем данные с city-фильтром
//   2. Aggregating by entity id in JS
//   3. Категоризируем: named TOP-N | other | single | unknown/no_entity
//
// TOP_N — количество named слайсов (остальные → "Прочие")
const DISTRIBUTION_TOP_N = 15;

/**
 * Вспомогательная функция: строим distribution-данные для pie chart.
 * @param {Array} slices - Массив { name, id, revenue, count, category }
 * @param {number} totalRevenue
 * @param {number} totalEntities - количество уникальных named entities
 */
function buildDistributionResult(slices, totalRevenue, totalEntities) {
  // Сортируем named первыми (по revenue), потом специальные категории
  const named = slices.filter(s => s.category === 'named').sort((a, b) => b.revenue - a.revenue);
  const special = slices.filter(s => s.category !== 'named');
  return {
    data: [...named, ...special],
    total_revenue: totalRevenue,
    total_entities: totalEntities,
  };
}

// ── getPartnersDistribution ──────────────────────────────────────────────────
//
// Pie-chart distribution партнёров по revenue.
//
// Категории:
//   named   — TOP-15 партнёров с более чем 1 заказом (отдельные slices)
//   other   — именованные партнёры за пределами TOP-15 (1 slice «Прочие партнёры»)
//   single  — партнёры ровно с 1 заказом (1 slice «Одноразовые»)
//   unknown — canonical_name LIKE 'Неизвестный%' (1 slice «Неизвестные»)
//
// city-фильтр через addCityFilter (source_file prefix, как везде).
// multi-city: при isMulti — withCityBreakdown, возвращает { byCity }.
//
export async function getPartnersDistribution(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('partners-distribution', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  // 1. Все sales где есть partner_id (B2B продажи), с city-фильтром
  const salesFilters = addCityFilter({ notNull: ['partner_id'] }, city);
  const sales = await loadAllParallel(sb, 'sales',
    'partner_id, total_amount',
    salesFilters
  );

  // 2. Aggregate по partner_id
  const byPartner = {};
  let totalRevenue = 0;
  for (const s of sales) {
    const pid = s.partner_id;
    if (!pid) continue;
    if (!byPartner[pid]) byPartner[pid] = { revenue: 0, count: 0 };
    byPartner[pid].revenue += s.total_amount || 0;
    byPartner[pid].count++;
    totalRevenue += s.total_amount || 0;
  }

  const partnerIds = Object.keys(byPartner);
  if (partnerIds.length === 0) {
    const empty = { data: [], total_revenue: 0, total_entities: 0 };
    cacheSet(ck, empty);
    return empty;
  }

  // 3. Обогащаем именами (батч по 500)
  const nameMap = {};
  for (let i = 0; i < partnerIds.length; i += 500) {
    const batch = partnerIds.slice(i, i + 500);
    const { data: contacts } = await sb.from('partner_contacts')
      .select('id, canonical_name')
      .in('id', batch);
    for (const c of contacts || []) nameMap[c.id] = c.canonical_name;
  }

  // 4. Классификация
  // unknown: canonical_name начинается с 'Неизвестный' или контакт не найден в nameMap
  // single:  ровно 1 заказ (НЕ unknown)
  // named:   > 1 заказа (НЕ unknown)

  const unknown = { name: 'Неизвестные', revenue: 0, count: 0, category: 'unknown', ids: [] };
  const singleBucket = { name: 'Одноразовые (1 заказ)', revenue: 0, count: 0, category: 'single', ids: [] };
  const namedList = [];

  for (const [pid, agg] of Object.entries(byPartner)) {
    const cname = nameMap[pid];
    const isUnknown = !cname || /^Неизвестн/i.test(cname);

    if (isUnknown) {
      unknown.revenue += agg.revenue;
      unknown.count += agg.count;
      unknown.ids.push(pid);
      continue;
    }
    if (agg.count === 1) {
      singleBucket.revenue += agg.revenue;
      singleBucket.count += agg.count;
      singleBucket.ids.push(pid);
      continue;
    }
    namedList.push({ name: cname, id: pid, revenue: agg.revenue, count: agg.count, category: 'named' });
  }

  // 5. Сортируем named по revenue, отрезаем TOP-N
  namedList.sort((a, b) => b.revenue - a.revenue);
  const topNamed = namedList.slice(0, DISTRIBUTION_TOP_N);
  const restNamed = namedList.slice(DISTRIBUTION_TOP_N);

  const slices = [...topNamed];

  if (restNamed.length > 0) {
    slices.push({
      name: `Прочие партнёры (${restNamed.length})`,
      revenue: restNamed.reduce((s, x) => s + x.revenue, 0),
      count: restNamed.reduce((s, x) => s + x.count, 0),
      category: 'other',
      ids: restNamed.map(x => x.id),
    });
  }
  if (singleBucket.ids.length > 0) slices.push(singleBucket);
  if (unknown.ids.length > 0) slices.push(unknown);

  const result = buildDistributionResult(slices, totalRevenue, partnerIds.length);
  cacheSet(ck, result);
  return result;
}

// ── getAgenciesDistribution ──────────────────────────────────────────────────
//
// Pie-chart distribution студий по revenue.
//
// Категории:
//   named     — TOP-15 студий с > 1 заказом
//   other     — студии за пределами TOP-15 (slice «Прочие студии»)
//   single    — студии с 1 заказом (slice «Прочие студии (1 заказ)»)
//   no_agency — заказы без студии (agency_id IS NULL) → slice «Без студии»
//
// ВАЖНО: agency_id в sales — ссылка на студию дизайнера, не клиента.
// Все sales (не только B2B) учитываются, если agency_id заполнен.
//
export async function getAgenciesDistribution(req, opts = {}) {
  const city = opts.city || 'all';
  const ck = cacheKey('agencies-distribution', req, { city });
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  // 1. Все sales с city-фильтром (включаем ALL — и с agency_id, и без).
  // Используем loadAllParallel чтобы обойти лимит PostgREST 1000 строк.
  const salesFilters = addCityFilter({}, city);
  const sales = await loadAllParallel(sb, 'sales',
    'agency_id, total_amount',
    salesFilters
  );

  // 2. Aggregate по agency_id (null = «Без студии»)
  const byAgency = {};
  let totalRevenue = 0;
  let noAgencyRevenue = 0;
  let noAgencyCount = 0;

  for (const s of sales) {
    totalRevenue += s.total_amount || 0;
    if (!s.agency_id) {
      noAgencyRevenue += s.total_amount || 0;
      noAgencyCount++;
      continue;
    }
    if (!byAgency[s.agency_id]) byAgency[s.agency_id] = { revenue: 0, count: 0 };
    byAgency[s.agency_id].revenue += s.total_amount || 0;
    byAgency[s.agency_id].count++;
  }

  const agencyIds = Object.keys(byAgency);

  // 3. Обогащаем именами студий
  const nameMap = {};
  if (agencyIds.length > 0) {
    for (let i = 0; i < agencyIds.length; i += 500) {
      const batch = agencyIds.slice(i, i + 500);
      const { data: agencies } = await sb.from('agencies')
        .select('id, canonical_name')
        .in('id', batch);
      for (const a of agencies || []) nameMap[a.id] = a.canonical_name;
    }
  }

  // 4. Классификация
  const singleBucket = { name: 'Прочие студии (1 заказ)', revenue: 0, count: 0, category: 'single', ids: [] };
  const namedList = [];

  for (const [aid, agg] of Object.entries(byAgency)) {
    const cname = nameMap[aid] || `Студия ${aid.slice(0, 8)}`;
    if (agg.count === 1) {
      singleBucket.revenue += agg.revenue;
      singleBucket.count += agg.count;
      singleBucket.ids.push(aid);
      continue;
    }
    namedList.push({ name: cname, id: aid, revenue: agg.revenue, count: agg.count, category: 'named' });
  }

  // 5. TOP-N + остаток
  namedList.sort((a, b) => b.revenue - a.revenue);
  const topNamed = namedList.slice(0, DISTRIBUTION_TOP_N);
  const restNamed = namedList.slice(DISTRIBUTION_TOP_N);

  const slices = [...topNamed];

  if (restNamed.length > 0) {
    slices.push({
      name: `Прочие студии (${restNamed.length})`,
      revenue: restNamed.reduce((s, x) => s + x.revenue, 0),
      count: restNamed.reduce((s, x) => s + x.count, 0),
      category: 'other',
      ids: restNamed.map(x => x.id),
    });
  }
  if (singleBucket.ids.length > 0) slices.push(singleBucket);
  if (noAgencyCount > 0) {
    slices.push({
      name: 'Без студии',
      revenue: noAgencyRevenue,
      count: noAgencyCount,
      category: 'no_agency',
    });
  }

  const result = buildDistributionResult(slices, totalRevenue, agencyIds.length);
  cacheSet(ck, result);
  return result;
}
