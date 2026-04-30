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

// ─── Простой in-memory кэш с TTL для тяжёлых аналитик-запросов ──────────────
//
// Аналитика по 2200+ заказам пересчитывается 1-3 секунды каждый раз.
// Кэшируем результаты на 60s (для одинаковых params), чтобы повторные запросы
// дашборда (refresh, переключение вкладок) шли мгновенно.
//
// Cache-key включает userId (req.user?.userId или '__service__'), чтобы под
// разными авторизациями не было утечек между tenants.
//
const CACHE_TTL_MS = 60 * 1000;
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
const listPartnersInput = z.object({
  filter: z.enum(['all', 'with_chat', 'top_revenue', 'no_phone', 'recent']).default('all'),
  q: z.string().max(200).optional().default(''),
  limit: z.number().int().min(1).max(500).default(100),
  offset: z.number().int().min(0).default(0),
});

export async function listPartners(req, params = {}) {
  const args = listPartnersInput.parse(params);
  // Кэшируем по полному набору параметров — limit/offset включены, чтобы pagination работал
  const ck = cacheKey('list-partners', req, args);
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  let q = sb.from('v_partner_full').select('*');

  // Common: только с заказами (мусорные сироты должны быть удалены, но защитимся)
  q = q.gt('orders_count', 0);

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

  // Sort
  q = q.order('total_revenue', { ascending: false });

  // Paginate
  q = q.range(args.offset, args.offset + args.limit - 1);

  const { data, error } = await q;
  if (error) throw new Error(`listPartners: ${error.message}`);
  const result = { items: data || [], limit: args.limit, offset: args.offset };
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
  return { ...buckets, total: (data || []).length };
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

export async function mergePartners(req, sourceId, payload) {
  if (!isUuid(sourceId)) throw new Error('invalid source id');
  const args = mergeInput.parse(payload);
  if (args.target_id === sourceId) throw new Error('source and target are the same');
  const sb = pickClient(req);

  const [{ data: source }, { data: target }] = await Promise.all([
    sb.from('partner_contacts').select('*').eq('id', sourceId).maybeSingle(),
    sb.from('partner_contacts').select('*').eq('id', args.target_id).maybeSingle(),
  ]);
  if (!source) throw new Error('source contact not found');
  if (!target) throw new Error('target contact not found');

  // Объединяем поля
  const mergedAliases = [...new Set([
    ...(target.aliases || []),
    ...(source.aliases || []),
    source.canonical_name,
  ].filter(Boolean))];
  const mergedRoles = [...new Set([...(target.roles || []), ...(source.roles || [])])];
  const mergedPhones = [...new Set([
    ...(target.phones || []),
    ...(source.phones || []),
    target.primary_phone, source.primary_phone,
  ].filter(Boolean))];
  const mergedJids = [...new Set([
    ...(target.linked_chat_jids || []),
    ...(source.linked_chat_jids || []),
  ])];

  const update = {
    aliases: mergedAliases,
    roles: mergedRoles,
    phones: mergedPhones,
    linked_chat_jids: mergedJids,
  };
  if (!target.primary_phone && source.primary_phone) update.primary_phone = source.primary_phone;
  if (!target.agency_id && source.agency_id) update.agency_id = source.agency_id;

  const { error: ut } = await sb.from('partner_contacts').update(update).eq('id', args.target_id);
  if (ut) throw new Error(`merge target update: ${ut.message}`);

  // Перепривязываем sales и followups
  await sb.from('sales').update({ customer_id: args.target_id }).eq('customer_id', sourceId);
  await sb.from('sales').update({ partner_id: args.target_id }).eq('partner_id', sourceId);
  await sb.from('followups').update({ contact_id: args.target_id }).eq('contact_id', sourceId);

  // Удаляем source
  const { error: ed } = await sb.from('partner_contacts').delete().eq('id', sourceId);
  if (ed) throw new Error(`merge delete source: ${ed.message}`);

  invalidateSalesCache(); // данные изменились — стираем кэш чтобы UI увидел свежее
  return { ok: true, merged_into: args.target_id };
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
export async function listAgencies(req) {
  const ck = cacheKey('list-agencies', req, {});
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // Все agencies + sales-aggregates через 2 запроса
  const [{ data: agencies }, { data: salesAgg }, { data: contactsAgg }] = await Promise.all([
    sb.from('agencies').select('id, canonical_name, city, notes'),
    sb.from('sales').select('agency_id, total_amount').not('agency_id', 'is', null),
    sb.from('partner_contacts').select('id, agency_id').not('agency_id', 'is', null),
  ]);

  const byAgency = {};
  for (const s of salesAgg || []) {
    if (!byAgency[s.agency_id]) byAgency[s.agency_id] = { orders: 0, revenue: 0 };
    byAgency[s.agency_id].orders++;
    byAgency[s.agency_id].revenue += s.total_amount || 0;
  }
  const contactsByAgency = {};
  for (const c of contactsAgg || []) {
    contactsByAgency[c.agency_id] = (contactsByAgency[c.agency_id] || 0) + 1;
  }

  const items = (agencies || []).map(a => ({
    ...a,
    orders: byAgency[a.id]?.orders || 0,
    revenue: byAgency[a.id]?.revenue || 0,
    contacts: contactsByAgency[a.id] || 0,
  })).filter(a => a.orders > 0 || a.contacts > 0)
    .sort((a, b) => b.revenue - a.revenue);

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
  const filters = {};
  if (args.date_from) filters.gte = { sale_date: args.date_from };
  if (args.date_to) filters.lte = { sale_date: args.date_to };
  if (args.channel === 'b2b') filters.notNull = ['partner_id'];
  if (args.channel === 'b2c') filters.isNull = ['partner_id'];
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

  // 4. Categories — берём все sale_items параллельно
  const items = await loadAllParallel(sb, 'sale_items', 'category, sale_id, amount');
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
export async function getSegmentation(req) {
  const ck = cacheKey('segmentation', req, {});
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  // 1. Все sales c customer_id — параллельная пагинация
  const sales = await loadAllParallel(sb, 'sales',
    'customer_id, sale_date, total_amount, city, sale_items(category)',
    { notNull: ['customer_id'] }
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
export async function getProductInsights(req) {
  const ck = cacheKey('products', req, {});
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  // Все sale_items + sales — параллельно (значительно быстрее последовательной pagination)
  const [items, sales] = await Promise.all([
    loadAllParallel(sb, 'sale_items', 'sale_id, sku, raw_name, qty, amount, category'),
    loadAllParallel(sb, 'sales', 'id, sale_date, customer_id'),
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
  const itemsBySale = {};
  for (const it of items) {
    if (!it.sku) continue;
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
    .filter(([_, c]) => c >= 5)
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
  // Грузим связанные sales батчами
  const sales = [];
  for (let i = 0; i < saleIds.length; i += 200) {
    const batch = saleIds.slice(i, i + 200);
    const { data } = await sb.from('sales')
      .select('id, sale_date, total_amount, customer_id, partner_id, agency_id')
      .in('id', batch);
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

  const result = {
    category: args.category,
    total_qty: items.reduce((s, x) => s + (x.qty || 1), 0),
    total_revenue: items.reduce((s, x) => s + (x.amount || 0), 0),
    total_orders: saleIds.length,
    timeline,
    top_skus: topSkus,
    top_customers: topCustomers,
    top_partners: topPartners,
  };
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
export async function getPartnerInsights(req) {
  const ck = cacheKey('partner-insights', req, {});
  const cached = cacheGet(ck);
  if (cached) return cached;
  const sb = pickClient(req);

  const today = new Date();
  const todayMs = today.getTime();
  const cold_threshold_ms = 60 * 24 * 60 * 60 * 1000; // 60 дней
  const cutoff_90 = new Date(todayMs - 90 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const cutoff_180 = new Date(todayMs - 180 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);

  // 1. Все sales где партнёр заполнен — параллельная пагинация
  const sales = await loadAllParallel(sb, 'sales',
    'id, partner_id, sale_date, total_amount, customer_id, agency_id, commission_text',
    { notNull: ['partner_id'] }
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

  // Имена + студия
  const partnerIds = partners.map(p => p.partner_id);
  const { data: contacts } = partnerIds.length > 0
    ? await sb.from('partner_contacts').select('id, canonical_name, primary_phone, agency_id').in('id', partnerIds)
    : { data: [] };
  const cMap = new Map((contacts || []).map(c => [c.id, c]));

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
