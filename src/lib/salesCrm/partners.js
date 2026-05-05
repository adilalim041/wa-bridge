/**
 * Sales CRM — partner-related operations.
 *
 * Extracted from src/lib/salesCrm.js as part of Phase 1 C-2 split (2026-05-05).
 *
 * Functions:
 *   - listPartners            — paginated/filtered list (tier, activity, q, etc.)
 *   - getPartnerCard          — full partner card (sales + chats + AI + followups)
 *   - getPartnerJourney       — unified timeline events (sales+messages+AI+followups)
 *   - getSimilarPartners      — top-N similar by Jaccard(categories) + tier/agency
 *   - mergePartners           — atomic merge via merge_partners RPC
 *   - bulkUpdatePartnerTags   — add/replace/remove tags across N partners
 *   - bulkUpdatePartnerAgency — set agency_id across N partners
 *   - bulkMergePartners       — merge N source_ids into a single target_id
 *
 * Re-exported from src/lib/salesCrm.js so external callers continue to use
 *   import * as salesCrm from '../lib/salesCrm.js'
 *   await salesCrm.listPartners(req, ...)
 * without changing import paths.
 *
 * Private helpers (pickClient, isUuid, computeTier, computeActivity, etc.) are
 * duplicated here from salesCrm.js to avoid a circular import (salesCrm.js
 * re-exports this file, so partners.js cannot import back from it). The
 * duplication is small (~30 LOC) and the helpers are pure / stable.
 */
import { z } from 'zod';
import { supabase as serviceClient } from '../../storage/supabase.js';
import { cacheKey, cacheGet, cacheSet, invalidateSalesCache } from './cache.js';

// ─── Private helpers (duplicated from salesCrm.js) ────────────────────────────
//
// Kept in sync with the originals in salesCrm.js. If you change behaviour here,
// change it there too. There is no automated check — Phase 2 of the refactor
// will consolidate these into a shared helpers module.

function pickClient(req) {
  return req?.userClient || serviceClient;
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isUuid(s) { return typeof s === 'string' && UUID_RE.test(s); }

// PostgREST .or() filter injection — экранируем символы которые ломают синтаксис.
function safeFilterValue(s) {
  return String(s || '').replace(/[,()*\\%]/g, ' ').slice(0, 100).trim();
}

// Tier / Activity helpers — total_revenue thresholds + days-since-last-purchase.
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
const VALID_TIERS      = new Set(['Gold', 'Silver', 'Bronze']);
const VALID_ACTIVITIES = new Set(['HOT', 'WARM', 'COLD']);

function parseCsvParam(raw, validSet) {
  if (!raw) return [];
  return String(raw).split(',').map(s => s.trim()).filter(s => validSet.has(s));
}

// city schema — single-city; multi-city ('all'+breakdown) handled by caller.
const citySchema = z.enum(['Алматы', 'Астана', 'all']).default('all');

// ─────────────────────────────────────────────────────────────────────────────
// 1. listPartners
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// 2. getPartnerCard
// ─────────────────────────────────────────────────────────────────────────────
//
// Полная карточка партнёра: основные поля + последние N заказов с позициями
// + связанные WhatsApp-сессии + последние 3 AI-анализа диалогов.

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

// ─────────────────────────────────────────────────────────────────────────────
// 3. getPartnerJourney
// ─────────────────────────────────────────────────────────────────────────────
//
// Customer Journey: единый timeline всех событий, связанных с контактом.
// События:
//   - sale            — заказ как customer/partner
//   - chat_in/out     — WA-сообщение
//   - chat_ai         — AI-анализ диалога
//   - followup_due    — ожидающий followup
//   - followup_done   — followup отмечен выполненным
//
// Сортировка по timestamp DESC (свежее сверху). Limit 200 событий.
// Используется в карточке партнёра (Customer Journey).

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

// ─────────────────────────────────────────────────────────────────────────────
// 4. mergePartners (single)
// ─────────────────────────────────────────────────────────────────────────────
//
// Слить контакт source_id в target_id. Все sales/followups перейдут на target,
// aliases/phones/roles объединятся, source-запись удалится.
// Атомарный merge через Postgres RPC `merge_partners` (миграция 0016).
// Раньше делали 5 sequential UPDATE через PostgREST без error-check'ов на 3
// из 5 — mid-flight failure оставлял половину переехавшими. RPC выполняется
// в одной транзакции, любой RAISE откатывает всё.

const mergeInput = z.object({
  target_id: z.string().regex(UUID_RE),
});

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

// ─────────────────────────────────────────────────────────────────────────────
// 5. Bulk operations
// ─────────────────────────────────────────────────────────────────────────────

const BULK_MAX_IDS = 100;

const bulkTagInput = z.object({
  partner_ids: z.array(z.string().uuid()).min(1).max(BULK_MAX_IDS),
  tags: z.array(z.string().min(1)).min(1),
  action: z.enum(['add', 'replace', 'remove']),
});

/**
 * Массовое обновление тегов partner_contacts.
 *
 * action="add"     — объединить с существующими тегами (union)
 * action="replace" — перезаписать теги
 * action="remove"  — убрать перечисленные теги из существующих
 *
 * Возвращает { updated: N, errors: [{ id, error }] }
 */
export async function bulkUpdatePartnerTags(req, payload) {
  const args = bulkTagInput.parse(payload);
  const sb = pickClient(req);

  const errors = [];
  let updated = 0;

  if (args.action === 'replace') {
    // Одна операция UPDATE для всего списка — самый дешёвый путь
    const { error, count } = await sb
      .from('partner_contacts')
      .update({ tags: args.tags })
      .in('id', args.partner_ids)
      .select('id', { count: 'exact', head: true });

    if (error) throw new Error(`bulkUpdatePartnerTags replace: ${error.message}`);
    updated = count ?? args.partner_ids.length;
  } else {
    // add / remove требуют read-merge-write per row (Supabase не поддерживает
    // array_append / array_remove в PostgREST patch без RPC).
    // Читаем текущие теги одним батчем, затем пишем каждый UPDATE.
    const { data: rows, error: readErr } = await sb
      .from('partner_contacts')
      .select('id, tags')
      .in('id', args.partner_ids);

    if (readErr) throw new Error(`bulkUpdatePartnerTags read: ${readErr.message}`);

    const newTagsSet = new Set(args.tags);

    await Promise.all((rows || []).map(async (row) => {
      const existing = Array.isArray(row.tags) ? row.tags : [];
      let merged;
      if (args.action === 'add') {
        merged = [...new Set([...existing, ...args.tags])];
      } else {
        // remove
        merged = existing.filter(t => !newTagsSet.has(t));
      }

      const { error: writeErr } = await sb
        .from('partner_contacts')
        .update({ tags: merged })
        .eq('id', row.id);

      if (writeErr) {
        errors.push({ id: row.id, error: writeErr.message });
      } else {
        updated++;
      }
    }));
  }

  invalidateSalesCache();
  return { updated, errors };
}

const bulkAgencyInput = z.object({
  partner_ids: z.array(z.string().uuid()).min(1).max(BULK_MAX_IDS),
  agency_id: z.string().uuid().nullable(),
});

/**
 * Массовое изменение agency_id у partner_contacts.
 * agency_id=null — открепить от студии.
 *
 * Возвращает { updated: N }
 */
export async function bulkUpdatePartnerAgency(req, payload) {
  const args = bulkAgencyInput.parse(payload);
  const sb = pickClient(req);

  const { error, count } = await sb
    .from('partner_contacts')
    .update({ agency_id: args.agency_id })
    .in('id', args.partner_ids)
    .select('id', { count: 'exact', head: true });

  if (error) throw new Error(`bulkUpdatePartnerAgency: ${error.message}`);

  invalidateSalesCache();
  return { updated: count ?? args.partner_ids.length };
}

const bulkMergeInput = z.object({
  source_ids: z.array(z.string().uuid()).min(1).max(BULK_MAX_IDS),
  target_id: z.string().uuid(),
});

/**
 * Массовый merge: каждый source_id → target_id через RPC merge_partners.
 *
 * Atomicity per-source: если упадёт на 3-м — первые 2 уже смержены.
 * Это намеренно (документировано) — частичный merge лучше чем откат всего.
 *
 * Возвращает { merged: N, failed: [{ source_id, error }] }
 */
export async function bulkMergePartners(req, payload) {
  const args = bulkMergeInput.parse(payload);
  if (args.source_ids.includes(args.target_id)) {
    throw new Error('target_id не должен присутствовать в source_ids');
  }

  const sb = pickClient(req);
  let merged = 0;
  const failed = [];

  // Последовательно — merge не идемпотентен, параллельность может вызвать
  // конфликт если source_ids содержат дубликаты или транзакции пересекаются.
  for (const sourceId of args.source_ids) {
    const { error } = await sb.rpc('merge_partners', {
      source_id: sourceId,
      target_id: args.target_id,
    });
    if (error) {
      failed.push({ source_id: sourceId, error: error.message });
    } else {
      merged++;
    }
  }

  if (merged > 0) invalidateSalesCache();
  return { merged, failed };
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. getSimilarPartners
// ─────────────────────────────────────────────────────────────────────────────
//
// top-N похожих партнёров по tier + категориям.
//
// Алгоритм:
//   1. Загружаем sales базового партнёра (partner_id = contactId) → distinct categories
//      через двухэтапный join (sales → sale_items).
//   2. Загружаем всех партнёров из v_partner_full с orders_count > 0.
//   3. Для каждого кандидата (≠ base) вычисляем similarity_score:
//        jaccard = |catBase ∩ catCand| / |catBase ∪ catCand|
//        +0.10 если same tier
//        +0.05 если same agency_id (оба не null)
//        +0.05 если same activity
//   4. Сортировка по score DESC, top-N.
//
// Cache 24h (стандартный CACHE_TTL_MS). RLS-aware через pickClient(req).

const similarPartnersInput = z.object({
  limit: z.coerce.number().int().min(1).max(20).default(5),
});

export async function getSimilarPartners(req, contactId, opts = {}) {
  if (!isUuid(contactId)) throw new Error('invalid contact id');
  const args = similarPartnersInput.parse(opts);

  const ck = cacheKey('similar-partners', req, { contactId, limit: args.limit });
  const cached = cacheGet(ck);
  if (cached) return cached;

  const sb = pickClient(req);

  // ── Step 1: категории базового партнёра (двухэтапный join) ────────────────
  // Шаг 1a: все sale_id где partner_id = contactId
  const { data: baseSales, error: bse } = await sb
    .from('sales')
    .select('id')
    .eq('partner_id', contactId);
  if (bse) throw new Error(`getSimilarPartners baseSales: ${bse.message}`);

  let baseCategorySet = new Set();

  if (baseSales && baseSales.length > 0) {
    const saleIds = baseSales.map(s => s.id);

    // Шаг 1b: батчами по 500 (PostgREST URL limit для .in())
    const BATCH = 500;
    const batches = [];
    for (let i = 0; i < saleIds.length; i += BATCH) {
      batches.push(saleIds.slice(i, i + BATCH));
    }
    const itemResults = await Promise.all(
      batches.map(ids =>
        sb.from('sale_items').select('category').in('sale_id', ids)
      )
    );
    for (const { data: items } of itemResults) {
      for (const it of items || []) {
        if (it.category) baseCategorySet.add(it.category);
      }
    }
  }

  const baseCats = [...baseCategorySet];

  // ── Step 2: базовая карточка партнёра (tier, activity, agency_id) ─────────
  const { data: baseCard, error: bce } = await sb
    .from('v_partner_full')
    .select('id, total_revenue, last_purchase_date, agency_id, total_purchases_amount, total_purchases_count, orders_count')
    .eq('id', contactId)
    .maybeSingle();
  if (bce) throw new Error(`getSimilarPartners baseCard: ${bce.message}`);
  if (!baseCard) return { similar: [], base_partner_categories: baseCats };

  const baseTier     = computeTier(baseCard.total_revenue);
  const baseActivity = computeActivity(baseCard.last_purchase_date);
  const baseAgency   = baseCard.agency_id || null;

  // ── Step 3: все партнёры с заказами ───────────────────────────────────────
  // У Omoikiri < 500 уникальных партнёров — safe для in-memory.
  const { data: allPartners, error: ape } = await sb
    .from('v_partner_full')
    .select('id, canonical_name, total_revenue, last_purchase_date, agency_id, total_purchases_amount, total_purchases_count, orders_count')
    .gt('orders_count', 0)
    .neq('id', contactId);
  if (ape) throw new Error(`getSimilarPartners allPartners: ${ape.message}`);

  const candidates = allPartners || [];

  // Batch-загрузка categories для всех кандидатов ───────────────────────────
  // Шаг 3a: все их sale_id
  const candidateIds = candidates.map(p => p.id);

  // Загружаем sale_id → partner_id для всех кандидатов батчами
  const BATCH = 500;
  const salesBatches = [];
  for (let i = 0; i < candidateIds.length; i += BATCH) {
    salesBatches.push(candidateIds.slice(i, i + BATCH));
  }
  const salesResults = await Promise.all(
    salesBatches.map(ids =>
      sb.from('sales').select('id, partner_id').in('partner_id', ids)
    )
  );

  // Map: partner_id → Set<sale_id>
  const partnerSaleIds = new Map(); // partner_id → sale_id[]
  for (const { data: rows } of salesResults) {
    for (const row of rows || []) {
      if (!partnerSaleIds.has(row.partner_id)) partnerSaleIds.set(row.partner_id, []);
      partnerSaleIds.get(row.partner_id).push(row.id);
    }
  }

  // Шаг 3b: загружаем категории для всех sale_ids кандидатов
  const allCandSaleIds = [...partnerSaleIds.values()].flat();

  // Map: sale_id → category[]
  const saleCategories = new Map();
  if (allCandSaleIds.length > 0) {
    const itemBatches = [];
    for (let i = 0; i < allCandSaleIds.length; i += BATCH) {
      itemBatches.push(allCandSaleIds.slice(i, i + BATCH));
    }
    const itemResults2 = await Promise.all(
      itemBatches.map(ids =>
        sb.from('sale_items').select('sale_id, category').in('sale_id', ids)
      )
    );
    for (const { data: items } of itemResults2) {
      for (const it of items || []) {
        if (!saleCategories.has(it.sale_id)) saleCategories.set(it.sale_id, []);
        if (it.category) saleCategories.get(it.sale_id).push(it.category);
      }
    }
  }

  // ── Step 4: вычисляем similarity_score для каждого кандидата ──────────────
  const scored = candidates.map(p => {
    const tier     = computeTier(p.total_revenue);
    const activity = computeActivity(p.last_purchase_date);
    const agency   = p.agency_id || null;

    // Собираем distinct categories кандидата
    const saleIds = partnerSaleIds.get(p.id) || [];
    const candCatSet = new Set();
    for (const sid of saleIds) {
      for (const cat of saleCategories.get(sid) || []) {
        candCatSet.add(cat);
      }
    }
    const candCats = [...candCatSet];

    // Жаккар: |A ∩ B| / |A ∪ B|
    let jaccard = 0;
    if (baseCats.length > 0 || candCats.length > 0) {
      const intersection = baseCats.filter(c => candCatSet.has(c)).length;
      const unionSize = new Set([...baseCats, ...candCats]).size;
      jaccard = unionSize > 0 ? intersection / unionSize : 0;
    }

    let score = jaccard;
    if (tier === baseTier)                         score += 0.10;
    if (agency && baseAgency && agency === baseAgency) score += 0.05;
    if (activity === baseActivity)                 score += 0.05;

    // Агрегированные покупки: берём из v_partner_full напрямую
    // (v_partner_full содержит total_purchases_amount / total_purchases_count
    //  либо total_revenue / orders_count — зависит от версии view)
    const totalAmount = p.total_purchases_amount ?? p.total_revenue ?? 0;
    const totalCount  = p.total_purchases_count  ?? p.orders_count  ?? 0;

    return {
      id:                     p.id,
      canonical_name:         p.canonical_name,
      tier,
      activity,
      agency_id:              agency,
      agency_name:            null, // enrich ниже если нужна
      total_purchases_amount: totalAmount,
      total_purchases_count:  totalCount,
      shared_categories:      baseCats.filter(c => candCatSet.has(c)),
      similarity_score:       Math.round(score * 1000) / 1000,
    };
  });

  // ── Step 5: sort + top-N ──────────────────────────────────────────────────
  scored.sort((a, b) => b.similarity_score - a.similarity_score);
  const topN = scored.slice(0, args.limit);

  // ── Step 6: enrich agency_name (batch) ───────────────────────────────────
  const agencyIds = [...new Set(topN.map(p => p.agency_id).filter(Boolean))];
  if (agencyIds.length > 0) {
    const { data: agencies } = await sb
      .from('agencies')
      .select('id, name')
      .in('id', agencyIds);
    const agencyMap = new Map((agencies || []).map(a => [a.id, a.name]));
    for (const p of topN) {
      if (p.agency_id) p.agency_name = agencyMap.get(p.agency_id) || null;
    }
  }

  const result = {
    similar: topN,
    base_partner_categories: baseCats,
  };

  cacheSet(ck, result);
  return result;
}
