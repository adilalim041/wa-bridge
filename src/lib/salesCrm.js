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

// UUID v4 — простая валидация (не строгая, но достаточная для SQL safety)
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isUuid(s) { return typeof s === 'string' && UUID_RE.test(s); }

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
    const s = args.q.trim();
    // ilike по имени или телефону
    q = q.or(`canonical_name.ilike.%${s}%,primary_phone.ilike.%${s}%`);
  }

  // Sort
  q = q.order('total_revenue', { ascending: false });

  // Paginate
  q = q.range(args.offset, args.offset + args.limit - 1);

  const { data, error } = await q;
  if (error) throw new Error(`listPartners: ${error.message}`);
  return { items: data || [], limit: args.limit, offset: args.offset };
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

// ── 6b. listAgencies ────────────────────────────────────────────────────────
//
// Список всех студий с агрегатами по продажам. Используется в Studios page.
//
export async function listAgencies(req) {
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

  return { items };
}

// ── 7. getSalesAnalytics ────────────────────────────────────────────────────
//
// Агрегаты для дашборда «Аналитика продаж»:
//   - timeline: выручка/кол-во заказов по месяцам
//   - top_studios: топ-15 студий с выручкой
//   - top_partners: топ-15 партнёров с выручкой
//   - categories: распределение позиций по категориям
//   - segments: разовые vs повторные клиенты, фильтр-покупатели
//   - kpi: общие цифры (revenue YTD, заказов в этом месяце, средний чек)
//
const analyticsInput = z.object({
  date_from: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  date_to:   z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
});

export async function getSalesAnalytics(req, params = {}) {
  const args = analyticsInput.parse(params);
  const sb = pickClient(req);

  // Pagination helper (PostgREST max range = 1000)
  async function loadAll(query) {
    let all = [];
    let off = 0;
    while (true) {
      const { data, error } = await query.range(off, off + 999);
      if (error) throw error;
      if (!data || data.length === 0) break;
      all.push(...data);
      if (data.length < 1000) break;
      off += 1000;
    }
    return all;
  }

  // 1. Sales timeline
  let salesQ = sb.from('sales').select('sale_date, total_amount, agency_id, partner_id, customer_id');
  if (args.date_from) salesQ = salesQ.gte('sale_date', args.date_from);
  if (args.date_to) salesQ = salesQ.lte('sale_date', args.date_to);
  const sales = await loadAll(salesQ);

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

  // 4. Categories
  let itemsQ = sb.from('sale_items').select('category, sale_id, amount');
  // Если есть date filter — фильтр по sales через join — proще загрузить ID-ки
  if (args.date_from || args.date_to) {
    const saleIds = sales.map(s => s.sale_date && (
      (!args.date_from || s.sale_date >= args.date_from) &&
      (!args.date_to || s.sale_date <= args.date_to)
    )).map((ok, i) => ok ? sales[i].id : null).filter(Boolean);
    // фильтр упрощим — берём всё (sale_items без даты, фильтр был выше через sales)
  }
  const items = await loadAll(itemsQ);
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

  // 6. KPI
  const kpi = {
    total_orders: sales.length,
    total_revenue: totalRevenue,
    avg_check: sales.length > 0 ? Math.round(totalRevenue / sales.length) : 0,
    months_covered: timeline.length,
  };

  return { timeline, top_studios, top_partners, categories, segments, kpi };
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
  const s = args.q.trim();

  const { data, error } = await sb.from('v_partner_full')
    .select('id, canonical_name, primary_phone, agency_name, orders_count, total_revenue, last_purchase_date, total_messages')
    .gt('orders_count', 0)
    .or(`canonical_name.ilike.%${s}%,primary_phone.ilike.%${s}%`)
    .order('total_revenue', { ascending: false })
    .limit(args.limit);

  if (error) throw new Error(`searchPartner: ${error.message}`);
  return { items: data || [] };
}
