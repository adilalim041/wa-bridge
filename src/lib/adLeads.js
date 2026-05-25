import { logger } from '../config.js';

const AD_SESSION_IDS = new Set([
  'almaty-rabochiy-reklama',
  'astana-renat-rabochiy-reklama',
]);

const DEFAULT_PATTERNS = [
  {
    key: 'grinder_interest',
    label: 'Измельчитель',
    campaignLabel: 'Рекламная заявка: измельчитель',
    needles: ['меня интересует измельчитель от бренда omoikiri', 'измельчитель от бренда omoikiri'],
  },
  {
    key: 'discount_sanitary',
    label: 'Сантехника по скидке',
    campaignLabel: 'Рекламная заявка: скидка',
    needles: ['хочу сантехнику от омойкири по скидке', 'сантехнику от omoikiri по скидке'],
  },
  {
    key: 'luxury_sanitary',
    label: 'Люксовая сантехника',
    campaignLabel: 'Рекламная заявка: люксовая сантехника',
    needles: ['впервые вижу, мне нужна люксовая сантехника', 'нужна люксовая сантехника'],
  },
  {
    key: 'brand_interest',
    label: 'Omoikiri',
    campaignLabel: 'Рекламная заявка: бренд',
    needles: ['бренда omoikiri', 'бренд omoikiri', 'омойкири'],
  },
];

const NON_BUSINESS_TAGS = new Set(['сотрудник', 'спам', 'личное', 'коллега']);
const ALMATY_OFFSET_MIN = 5 * 60;
const WORK_START_HOUR = 10;
const WORK_END_HOUR = 20;
const SLOW_MINUTES = 15;
const VERY_SLOW_MINUTES = 60;
const CONVERSION_WINDOW_DAYS = 45;
const EVENT_BUCKET_LIMIT = 100;
const CACHE_TTL_MS = 2 * 60 * 1000;

const _cache = new Map();

function cacheKey({ userId, sessionId, dateFrom, dateTo, days, limit }) {
  return [
    userId || '__service__',
    sessionId || '__ad_all__',
    dateFrom || '',
    dateTo || '',
    days || '',
    limit || '',
  ].join('|');
}

function cacheGet(key) {
  const entry = _cache.get(key);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    _cache.delete(key);
    return null;
  }
  return entry.data;
}

function cacheSet(key, data) {
  _cache.set(key, { data, expiresAt: Date.now() + CACHE_TTL_MS });
  if (_cache.size > 100) {
    const now = Date.now();
    for (const [k, v] of _cache) {
      if (v.expiresAt < now) _cache.delete(k);
    }
  }
}

export function invalidateAdLeadsCache() {
  _cache.clear();
}

function almatyDateString(date) {
  const shifted = new Date(date.getTime() + ALMATY_OFFSET_MIN * 60000);
  return shifted.toISOString().slice(0, 10);
}

function toAlmatyLabel(date) {
  const shifted = new Date(date.getTime() + ALMATY_OFFSET_MIN * 60000);
  return shifted.toISOString().slice(0, 16).replace('T', ' ');
}

function utcFromAlmatyDate(date, end = false) {
  const suffix = end ? 'T23:59:59+05:00' : 'T00:00:00+05:00';
  return new Date(`${date}${suffix}`).toISOString();
}

function resolveRange({ dateFrom, dateTo, days }) {
  if (dateFrom) {
    const safeFrom = /^\d{4}-\d{2}-\d{2}$/.test(String(dateFrom)) ? String(dateFrom) : null;
    const safeTo = /^\d{4}-\d{2}-\d{2}$/.test(String(dateTo || dateFrom))
      ? String(dateTo || dateFrom)
      : safeFrom;
    if (safeFrom) {
      return {
        dateFrom: safeFrom,
        dateTo: safeTo,
        rangeStart: utcFromAlmatyDate(safeFrom),
        rangeEnd: utcFromAlmatyDate(safeTo, true),
      };
    }
  }

  const safeDays = Math.min(Math.max(parseInt(days, 10) || 30, 1), 180);
  const nowAlmaty = new Date(Date.now() + ALMATY_OFFSET_MIN * 60000);
  const to = nowAlmaty.toISOString().slice(0, 10);
  const fromDate = new Date(nowAlmaty);
  fromDate.setUTCDate(fromDate.getUTCDate() - safeDays + 1);
  const from = fromDate.toISOString().slice(0, 10);
  return {
    dateFrom: from,
    dateTo: to,
    rangeStart: utcFromAlmatyDate(from),
    rangeEnd: utcFromAlmatyDate(to, true),
  };
}

function localParts(date) {
  const shifted = new Date(date.getTime() + ALMATY_OFFSET_MIN * 60000);
  return {
    y: shifted.getUTCFullYear(),
    m: shifted.getUTCMonth(),
    d: shifted.getUTCDate(),
    h: shifted.getUTCHours(),
  };
}

function utcFromLocal(y, m, d, h) {
  return new Date(Date.UTC(y, m, d, h, 0, 0) - ALMATY_OFFSET_MIN * 60000);
}

function businessMinutesBetween(startValue, endValue) {
  let cursor = new Date(startValue);
  const end = new Date(endValue);
  if (!Number.isFinite(cursor.getTime()) || !Number.isFinite(end.getTime()) || end <= cursor) return 0;

  let total = 0;
  while (cursor < end && total < 60 * 24 * 60) {
    const p = localParts(cursor);
    const dayStart = utcFromLocal(p.y, p.m, p.d, WORK_START_HOUR);
    const dayEnd = utcFromLocal(p.y, p.m, p.d, WORK_END_HOUR);

    if (cursor < dayStart) cursor = dayStart;
    if (cursor >= dayEnd) {
      cursor = utcFromLocal(p.y, p.m, p.d + 1, WORK_START_HOUR);
      continue;
    }

    const segmentEnd = end < dayEnd ? end : dayEnd;
    total += Math.max(0, Math.round((segmentEnd - cursor) / 60000));
    cursor = segmentEnd >= dayEnd ? utcFromLocal(p.y, p.m, p.d + 1, WORK_START_HOUR) : segmentEnd;
  }
  return total;
}

function normalizePhone(value = '') {
  const digits = String(value).replace(/\D/g, '');
  if (digits.length === 11 && digits.startsWith('8')) return `7${digits.slice(1)}`;
  if (digits.length === 10 && digits.startsWith('7')) return `7${digits}`;
  return digits.length >= 9 && digits.length <= 12 ? digits : null;
}

function phoneFromJid(jid) {
  if (!jid || jid.includes('-') || jid.includes('@lid')) return null;
  return normalizePhone(String(jid).split('@')[0]);
}

function textPreview(value = '', max = 160) {
  const clean = String(value || '').replace(/\s+/g, ' ').trim();
  return clean.length > max ? `${clean.slice(0, max)}...` : clean;
}

function matchPattern(body) {
  const text = String(body || '').toLowerCase();
  if (!text) return null;
  return DEFAULT_PATTERNS.find((pattern) => pattern.needles.some((needle) => text.includes(needle))) || null;
}

function productInterest(messages) {
  const text = messages.slice(0, 10).map((m) => m.body || '').join(' ').toLowerCase();
  const out = [];
  if (/измельч|grinder|диспоуз/.test(text)) out.push('Измельчитель');
  if (/мойк|раковин|kuh|bosen|sakaime|akita|yasugata|nagare|kata/.test(text)) out.push('Мойка');
  if (/смесител|кран|faucet|tap|букс/.test(text)) out.push('Смеситель');
  if (/дозатор|аксессуар|картридж|ручк/.test(text)) out.push('Аксессуары');
  return out.length ? out : ['Неясно'];
}

async function fetchAll(query, pageSize = 1000, maxRows = 50000) {
  const out = [];
  for (let from = 0; from < maxRows; from += pageSize) {
    const { data, error } = await query.range(from, from + pageSize - 1);
    if (error) throw error;
    out.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return out;
}

function summarize(events) {
  const responseMinutes = events
    .map((e) => e.firstResponseWorkMinutes)
    .filter((v) => Number.isFinite(v));
  const avg = responseMinutes.length
    ? Math.round(responseMinutes.reduce((a, b) => a + b, 0) / responseMinutes.length)
    : null;
  const sorted = [...responseMinutes].sort((a, b) => a - b);
  const percentile = (p) => {
    if (!sorted.length) return null;
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1));
    return sorted[idx];
  };

  return {
    total: events.length,
    responded: events.filter((e) => e.firstResponseAt).length,
    noResponse: events.filter((e) => !e.firstResponseAt).length,
    avgFirstResponseMinutes: avg,
    p50FirstResponseMinutes: percentile(0.5),
    p90FirstResponseMinutes: percentile(0.9),
    slowFirstResponse: events.filter((e) => Number.isFinite(e.firstResponseWorkMinutes) && e.firstResponseWorkMinutes > SLOW_MINUTES).length,
    verySlowFirstResponse: events.filter((e) => Number.isFinite(e.firstResponseWorkMinutes) && e.firstResponseWorkMinutes > VERY_SLOW_MINUTES).length,
    noFollowup: events.filter((e) => e.followupStatus === 'client_waiting').length,
    salesMatched: events.filter((e) => e.matchedSalesCount > 0).length,
    customerSalesMatched: events.filter((e) => (e.matchedSalesByRole?.customer || 0) > 0).length,
    partnerSalesMatched: events.filter((e) => (e.matchedSalesByRole?.partner || 0) > 0).length,
    revenue: events.reduce((sum, e) => sum + (Number(e.matchedRevenue) || 0), 0),
  };
}

function buildContactIndexes(contacts = []) {
  const byPhone = new Map();
  const byJid = new Map();
  for (const contact of contacts) {
    const phones = [contact.primary_phone, ...(Array.isArray(contact.phones) ? contact.phones : [])];
    for (const raw of phones) {
      const phone = normalizePhone(raw);
      if (phone && !byPhone.has(phone)) byPhone.set(phone, contact);
    }
    for (const jid of contact.linked_chat_jids || []) {
      if (jid && !byJid.has(jid)) byJid.set(jid, contact);
    }
  }
  return { byPhone, byJid };
}

function buildSalesIndex(sales = []) {
  const byContact = new Map();
  for (const sale of sales) {
    for (const [role, id] of [['customer', sale.customer_id], ['partner', sale.partner_id]]) {
      if (!id) continue;
      if (!byContact.has(id)) byContact.set(id, []);
      byContact.get(id).push({ ...sale, matchRole: role });
    }
  }
  return byContact;
}

function contactsForLead({ remoteJid, phone, byPhone, byJid }) {
  const matches = [];
  const seen = new Set();
  const add = (contact, method) => {
    if (!contact || seen.has(contact.id)) return;
    seen.add(contact.id);
    matches.push({ contact, method });
  };

  add(byJid.get(remoteJid), 'linked_chat_jid');
  if (phone) add(byPhone.get(phone), 'phone');

  return matches;
}

function salesForLead({ contactMatches, triggerAt, salesByContact }) {
  const triggerDate = almatyDateString(triggerAt);
  const windowEnd = new Date(triggerAt);
  windowEnd.setUTCDate(windowEnd.getUTCDate() + CONVERSION_WINDOW_DAYS);
  const windowEndDate = almatyDateString(windowEnd);
  const bySaleId = new Map();

  for (const { contact, method } of contactMatches) {
    for (const sale of salesByContact.get(contact.id) || []) {
      if (!sale.sale_date) continue;
      if (sale.sale_date < triggerDate || sale.sale_date > windowEndDate) continue;
      const existing = bySaleId.get(sale.id);
      const match = {
        ...sale,
        matchedContactId: contact.id,
        matchedContactName: contact.canonical_name || null,
        matchMethod: method,
        matchRole: sale.matchRole,
      };
      if (!existing || existing.matchRole !== 'customer') bySaleId.set(sale.id, match);
    }
  }

  return [...bySaleId.values()];
}

function deriveStatus(event) {
  if (!event.firstResponseAt) return 'no_response';
  if (event.followupStatus === 'client_waiting') return 'client_waiting';
  if (event.firstResponseWorkMinutes > VERY_SLOW_MINUTES) return 'very_slow';
  if (event.firstResponseWorkMinutes > SLOW_MINUTES) return 'slow';
  return 'ok';
}

export async function getAdLeadAnalytics({
  db,
  sessionId,
  dateFrom,
  dateTo,
  days = 30,
  limit = 20,
  userId = null,
}) {
  const safeSessionId = sessionId && sessionId !== '__all__' ? String(sessionId) : null;
  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 100);
  const range = resolveRange({ dateFrom, dateTo, days });
  const key = cacheKey({ userId, sessionId: safeSessionId, dateFrom: range.dateFrom, dateTo: range.dateTo, days, limit: safeLimit });
  const cached = cacheGet(key);
  if (cached) return cached;

  const sessions = safeSessionId ? [safeSessionId] : [...AD_SESSION_IDS];
  const adSessions = sessions.filter((sid) => AD_SESSION_IDS.has(sid));
  if (!adSessions.length) {
    const empty = {
      range: { dateFrom: range.dateFrom, dateTo: range.dateTo },
      summary: summarize([]),
      bySession: {},
      events: [],
      patterns: DEFAULT_PATTERNS.map(({ key: patternKey, label, campaignLabel }) => ({ key: patternKey, label, campaignLabel })),
    };
    cacheSet(key, empty);
    return empty;
  }

  const [messages, tagRows, sessionRows, contacts, sales] = await Promise.all([
    fetchAll(
      db.from('messages')
        .select('id, message_id, session_id, remote_jid, from_me, body, push_name, timestamp')
        .in('session_id', adSessions)
        .order('timestamp', { ascending: true }),
      1000,
      60000
    ),
    db.from('chat_tags').select('remote_jid, tags'),
    db.from('session_config').select('session_id, display_name').in('session_id', adSessions),
    fetchAll(db.from('partner_contacts').select('id, canonical_name, primary_phone, phones, roles, linked_chat_jids'), 1000, 30000),
    fetchAll(db.from('sales').select('id, customer_id, partner_id, sale_date, total_amount, order_num, city, customer_raw, partner_raw'), 1000, 40000),
  ]);

  const sessionNames = new Map((sessionRows.data || []).map((s) => [s.session_id, s.display_name || s.session_id]));
  const tagsByJid = new Map((tagRows.data || []).map((row) => [row.remote_jid, row.tags || []]));
  const { byPhone, byJid } = buildContactIndexes(contacts);
  const salesByContact = buildSalesIndex(sales);

  const messagesByChat = new Map();
  for (const message of messages) {
    const keyByChat = `${message.session_id}:::${message.remote_jid}`;
    if (!messagesByChat.has(keyByChat)) messagesByChat.set(keyByChat, []);
    messagesByChat.get(keyByChat).push(message);
  }

  const firstInboundIdByChat = new Map();
  for (const [keyByChat, rows] of messagesByChat) {
    const firstInbound = rows.find((m) => !m.from_me);
    if (firstInbound) firstInboundIdByChat.set(keyByChat, firstInbound.id);
  }

  const rangeStart = new Date(range.rangeStart);
  const rangeEnd = new Date(range.rangeEnd);
  const events = [];

  for (const [keyByChat, rows] of messagesByChat) {
    const [sid, remoteJid] = keyByChat.split(':::');
    const firstInboundId = firstInboundIdByChat.get(keyByChat);
    const tags = tagsByJid.get(remoteJid) || [];
    const isCleanBusiness = !tags.some((tag) => NON_BUSINESS_TAGS.has(String(tag).toLowerCase()));

    for (const message of rows) {
      if (message.from_me) continue;
      const triggerAt = new Date(message.timestamp);
      if (triggerAt < rangeStart || triggerAt > rangeEnd) continue;

      const pattern = matchPattern(message.body);
      const isFirstInbound = message.id === firstInboundId;
      if (!pattern && !isFirstInbound) continue;

      const firstResponse = rows.find((m) => m.from_me && new Date(m.timestamp) > triggerAt);
      const firstResponseWorkMinutes = firstResponse
        ? businessMinutesBetween(triggerAt, firstResponse.timestamp)
        : null;
      const lastMessage = rows[rows.length - 1];
      const lastMessageAt = new Date(lastMessage.timestamp);
      const clientWaiting = !lastMessage.from_me && (Date.now() - lastMessageAt.getTime()) > 24 * 60 * 60 * 1000;
      const phone = phoneFromJid(remoteJid);
      const contactMatches = contactsForLead({ remoteJid, phone, byPhone, byJid });
      const primaryMatch = contactMatches.find((m) => m.method === 'linked_chat_jid') || contactMatches[0];
      const contact = primaryMatch?.contact || null;
      const matchedSales = salesForLead({ contactMatches, triggerAt, salesByContact });
      const matchedRevenue = matchedSales.reduce((sum, sale) => sum + (Number(sale.total_amount) || 0), 0);
      const matchedSalesByRole = matchedSales.reduce((acc, sale) => {
        const role = sale.matchRole || 'unknown';
        acc[role] = (acc[role] || 0) + 1;
        return acc;
      }, {});
      const chatMessagesAfterTrigger = rows.filter((m) => new Date(m.timestamp) >= triggerAt);

      const event = {
        id: `${sid}:${message.message_id || message.id}`,
        sessionId: sid,
        sessionDisplayName: sessionNames.get(sid) || sid,
        remoteJid,
        phone,
        displayName: message.push_name || contact?.canonical_name || phone || remoteJid,
        triggerMessageId: message.message_id,
        triggerAt: triggerAt.toISOString(),
        triggerAtLabel: toAlmatyLabel(triggerAt),
        triggerBody: textPreview(message.body),
        triggerKind: pattern ? 'template' : 'ad_account_first_inbound',
        patternKey: pattern?.key || 'ad_account_first_inbound',
        campaignLabel: pattern?.campaignLabel || 'Рекламный WhatsApp-аккаунт',
        firstResponseAt: firstResponse?.timestamp || null,
        firstResponseWorkMinutes,
        followupStatus: clientWaiting ? 'client_waiting' : (firstResponse ? 'responded' : 'no_response'),
        lastMessageAt: lastMessage.timestamp,
        lastMessageFromMe: Boolean(lastMessage.from_me),
        messageCount: chatMessagesAfterTrigger.length,
        productInterest: productInterest(chatMessagesAfterTrigger),
        isCleanBusiness,
        tags,
        matchedContactId: contact?.id || null,
        matchedContactName: contact?.canonical_name || null,
        matchedContactMethod: primaryMatch?.method || null,
        matchedSalesCount: matchedSales.length,
        matchedSalesByRole,
        matchedRevenue,
      };
      event.status = deriveStatus(event);
      events.push(event);
    }
  }

  events.sort((a, b) => new Date(b.triggerAt) - new Date(a.triggerAt));

  const bySession = {};
  for (const sid of adSessions) {
    const sessionEvents = events.filter((e) => e.sessionId === sid);
    bySession[sid] = {
      sessionId: sid,
      displayName: sessionNames.get(sid) || sid,
      summary: summarize(sessionEvents),
    };
  }

  const productCounts = {};
  for (const event of events) {
    for (const product of event.productInterest || []) {
      productCounts[product] = (productCounts[product] || 0) + 1;
    }
  }

  const statusCounts = {};
  for (const event of events) {
    statusCounts[event.status] = (statusCounts[event.status] || 0) + 1;
  }

  const eventBuckets = {
    recent: events.slice(0, safeLimit),
    sales: events.filter((event) => event.matchedSalesCount > 0).slice(0, EVENT_BUCKET_LIMIT),
    slow: events.filter((event) => Number.isFinite(event.firstResponseWorkMinutes) && event.firstResponseWorkMinutes > SLOW_MINUTES).slice(0, EVENT_BUCKET_LIMIT),
    noFollowup: events.filter((event) => event.followupStatus === 'client_waiting').slice(0, EVENT_BUCKET_LIMIT),
    noResponse: events.filter((event) => !event.firstResponseAt).slice(0, EVENT_BUCKET_LIMIT),
  };

  const response = {
    range: { dateFrom: range.dateFrom, dateTo: range.dateTo },
    summary: {
      ...summarize(events),
      cleanBusiness: events.filter((e) => e.isCleanBusiness).length,
      dirtyOrTagged: events.filter((e) => !e.isCleanBusiness).length,
    },
    bySession,
    productCounts,
    statusCounts,
    events: eventBuckets.recent,
    eventBuckets,
    patterns: DEFAULT_PATTERNS.map(({ key: patternKey, label, campaignLabel }) => ({ key: patternKey, label, campaignLabel })),
    notes: {
      definition: 'Ad lead event = first inbound message in an ad WhatsApp account, or any inbound message matching a known ad template.',
      businessHours: '10:00-20:00 UTC+5',
      conversion: `Sales attribution matches WhatsApp chat to partner_contacts by linked_chat_jids or phone, then counts customer_id and partner_id sales within ${CONVERSION_WINDOW_DAYS} days after the ad lead.`,
    },
  };

  cacheSet(key, response);
  return response;
}

export async function refreshPersistedAdLeadEvents({ db }) {
  const { data, error } = await db.rpc('refresh_ad_lead_events');
  if (error) {
    logger.warn({ err: error }, 'refresh_ad_lead_events RPC failed');
    throw error;
  }
  invalidateAdLeadsCache();
  return data;
}
