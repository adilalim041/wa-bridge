import dotenv from 'dotenv';
import XLSX from 'xlsx';
import fs from 'node:fs';
import path from 'node:path';
import { createClient } from '@supabase/supabase-js';

dotenv.config();

const DOWNLOADS_DIR = 'C:/Users/User/Downloads';
const SAVE = process.argv.includes('--save');

function argValue(name) {
  const idx = process.argv.indexOf(name);
  return idx >= 0 ? process.argv[idx + 1] : null;
}

function argValues(name) {
  const values = [];
  for (let i = 0; i < process.argv.length; i += 1) {
    if (process.argv[i] === name && process.argv[i + 1]) values.push(process.argv[i + 1]);
  }
  return values;
}

const RU = {
  sales: '\u041f\u0440\u043e\u0434\u0430\u0436\u0438',
  actual: '\u0430\u043a\u0442\u0443\u0430\u043b\u044c\u043d\u0430\u044f',
  actualStem: '\u0430\u043a\u0442\u0443\u0430\u043b',
  astana: '\u0410\u0441\u0442\u0430\u043d\u0430',
  almaty: '\u0410\u043b\u043c\u0430\u0442\u044b',
  june: '\u0418\u044e\u043d\u044c',
  july: '\u0418\u044e\u043b\u044c',
  total: '\u0438\u0442\u043e\u0433\u043e',
  no: '\u2116',
};

const period = argValue('--month') || '2026-06';
const [periodYear, periodMonth] = period.split('-').map(Number);
const monthLabels = new Map([
  [6, RU.june],
  [7, RU.july],
]);
const monthLabel = monthLabels.get(periodMonth);
if (!/^\d{4}-\d{2}$/.test(period) || !monthLabel) {
  throw new Error(`Unsupported --month ${period}; supported months: 2026-06, 2026-07`);
}
const periodStart = `${period}-01`;
const periodEnd = new Date(Date.UTC(periodYear, periodMonth, 0)).toISOString().slice(0, 10);

const explicitFiles = argValues('--file');
const FILES = explicitFiles.length
  ? explicitFiles.map((file) => path.resolve(file))
  : fs.readdirSync(DOWNLOADS_DIR)
    .filter((name) => name.startsWith('Omoikiri') && name.endsWith('(2).xlsx'))
    .map((name) => path.join(DOWNLOADS_DIR, name))
    .sort();

if (FILES.length === 0) {
  throw new Error('No Omoikiri *(2).xlsx files found in Downloads');
}

const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !serviceRoleKey) {
  throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
}

const sb = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false },
});

function text(value) {
  if (value === null || value === undefined) return '';
  return String(value).replace(/\s+/g, ' ').trim();
}

function lowerKey(value) {
  return text(value).toLowerCase();
}

function numberValue(value) {
  const cleaned = text(value).replace(/[^0-9.,-]/g, '').replace(',', '.');
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseExcelDate(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  const s = text(value);
  const m = s.match(/^(\d{1,2})[/.](\d{1,2})[/.](\d{2,4})$/);
  if (!m) return null;
  let year = Number(m[3]);
  if (year < 100) year += 2000;
  return `${year}-${String(Number(m[1])).padStart(2, '0')}-${String(Number(m[2])).padStart(2, '0')}`;
}

function normalizePhone(raw) {
  const digits = text(raw).replace(/\D/g, '');
  if (!digits) return null;
  let d = digits;
  if (d.length === 11 && d.startsWith('8')) d = `7${d.slice(1)}`;
  if (d.length === 10) d = `7${d}`;
  if (d.length < 10) return null;
  return d;
}

function classifyCategory(name = '') {
  const s = lowerKey(name);
  if (s.includes('\u043c\u043e\u0439\u043a\u0430')) return 'sink';
  if (s.includes('\u0441\u043c\u0435\u0441\u0438\u0442\u0435\u043b')) return 'faucet';
  if (s.includes('\u0438\u0437\u043c\u0435\u043b\u044c\u0447\u0438\u0442\u0435\u043b')) return 'disposer';
  if (s.includes('\u0432\u043e\u0434\u043e\u043e\u0447\u0438\u0441\u0442\u0438\u0442\u0435\u043b') || s.includes('pure drop') || s.includes('\u0444\u0438\u043b\u044c\u0442\u0440')) return 'water_filter';
  if (s.includes('\u043c\u043e\u0434\u0443\u043b\u044c \u0441\u043c\u0435\u043d\u043d\u044b\u0439') || s.includes('\u043a\u0430\u0440\u0442\u0440\u0438\u0434\u0436')) return 'cartridge';
  if (s.includes('\u0434\u043e\u0437\u0430\u0442\u043e\u0440')) return 'dispenser';
  if (s.includes('\u0440\u043e\u043b\u043b-\u043c\u0430\u0442')) return 'roll_mat';
  if (s.includes('\u043a\u043e\u043b\u0430\u043d\u0434\u0435\u0440')) return 'colander';
  if (
    s.includes('\u0430\u0440\u043c\u0430\u0442\u0443\u0440') ||
    s.includes('\u043d\u0430\u043a\u043b\u0430\u0434\u043a') ||
    s.includes('\u043f\u0435\u0440\u0435\u043b\u0438\u0432') ||
    s.includes('\u0433\u043e\u0440\u043b\u043e\u0432\u0438\u043d') ||
    s.includes('\u043a\u043d\u043e\u043f\u043a') ||
    s.includes('\u0434\u0435\u0440\u0436\u0430\u0442\u0435\u043b') ||
    s.includes('\u0432\u0441\u0442\u0430\u0432\u043a') ||
    s.includes('\u0441\u0443\u0448\u043a')
  ) return 'accessory';
  return 'other';
}

function isSummaryRow(row) {
  const joined = row.map(text).filter(Boolean).join(' ').toLowerCase();
  return joined.includes('\u043e\u0431\u0449\u0430\u044f \u0441\u0443\u043c\u043c\u0430') || joined === RU.total;
}

function parseWorkbook(filePath) {
  const base = path.basename(filePath);
  const shop = base.includes(RU.astana) ? RU.astana : RU.almaty;
  const hasId = base.includes(RU.astana);
  // order_num restarts every month in the "current sales" sheet, so the month
  // must be part of source_file for the DB unique key (source_file, order_num).
  const sourceFile = `${shop} \u2014 ${monthLabel} ${periodYear} (${RU.sales} ${RU.actual}).xlsx`;
  const wb = XLSX.readFile(filePath, { cellDates: true, raw: false });
  const sheetName = wb.SheetNames.find((name) => (
    lowerKey(name).includes(lowerKey(RU.sales)) && lowerKey(name).includes(RU.actualStem)
  ));
  if (!sheetName) throw new Error(`Target sheet not found in ${base}`);

  const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], {
    header: 1,
    defval: '',
    raw: false,
  });
  const rawRows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], {
    header: 1,
    defval: '',
    raw: true,
  });

  const idx = hasId
    ? { date: 0, id: 1, sku: 2, name: 3, qty: 4, price: 5, amount: 6, status: 7, manager: 8, designer: 9, designerPhone: 10, agency: 11, customer: 12, customerPhone: 13, city: 14, address: 15, commission: 16, comment: 17 }
    : { date: 0, sku: 1, name: 2, qty: 3, price: 4, amount: 5, status: 6, manager: 7, designer: 8, designerPhone: 9, agency: 10, customer: 11, customerPhone: 12, city: 13, address: 14, commission: 15, comment: 16 };

  const orders = [];
  let current = null;
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    const rawRow = rawRows[r] || [];
    const saleDate = parseExcelDate(row[idx.date]);
    const orderCell = text(row[idx.name]);
    const excelId = hasId ? text(row[idx.id]) : '';
    const hasTotalMarker = lowerKey(row[idx.price]).includes(RU.total);
    const hasOrderNumber = orderCell.startsWith(RU.no) && /\d+/.test(orderCell);
    const isOrderRow = saleDate && hasTotalMarker && (hasOrderNumber || excelId);

    if (isOrderRow) {
      if (current) orders.push(current);
      const explicitOrder = hasOrderNumber ? (orderCell.match(/\d+/) || [''])[0] : '';
      current = {
        source_file: sourceFile,
        source_workbook: base,
        sheet_name: sheetName,
        shop,
        row_number: r + 1,
        sale_date: saleDate,
        excel_id: excelId,
        order_num: explicitOrder || `ID-${excelId || r + 1}`,
        total_amount: numberValue(row[idx.amount]) || 0,
        discount_note: text(row[idx.qty]),
        payment_method: text(row[idx.status]),
        manager: text(row[idx.manager]),
        partner_raw: text(row[idx.designer]),
        partner_phone: normalizePhone(rawRow[idx.designerPhone] || row[idx.designerPhone]),
        agency_raw: text(row[idx.agency]),
        customer_raw: text(row[idx.customer]),
        customer_phone: normalizePhone(rawRow[idx.customerPhone] || row[idx.customerPhone]),
        city: text(row[idx.city]),
        address: text(row[idx.address]),
        commission_text: text(row[idx.commission]),
        comment: text(row[idx.comment]),
        items: [],
        statuses: new Set(),
        notes: [],
      };
      continue;
    }

    if (!current || isSummaryRow(row)) continue;

    const explicitItemIndex = text(row[idx.date]);
    const sameAstanaOrder = hasId && text(rawRow[idx.id] || row[idx.id]) === current.excel_id;
    const itemSku = text(row[idx.sku]);
    const itemName = text(row[idx.name]);
    const itemAmount = numberValue(row[idx.amount]);
    const isItemRow = (/^\d+$/.test(explicitItemIndex) || sameAstanaOrder)
      && (itemSku || itemName)
      && itemAmount !== null;
    if (isItemRow) {
      const status = text(row[idx.status]);
      current.items.push({
        position_idx: /^\d+$/.test(explicitItemIndex)
          ? Number(explicitItemIndex)
          : current.items.length + 1,
        sku: itemSku || null,
        raw_name: itemName,
        qty: numberValue(row[idx.qty]),
        price_per_unit: numberValue(row[idx.price]),
        amount: itemAmount,
        category: classifyCategory(itemName),
        status,
      });
      if (status) current.statuses.add(status);
      const itemNote = hasId ? text(row[idx.manager]) : '';
      if (itemNote) current.notes.push(`Item ${current.items.length}: ${itemNote}`);
    } else {
      const note = row.map(text).filter(Boolean).filter((x) => !/^false$/i.test(x)).join(' | ');
      if (note) current.notes.push(note);
    }
  }
  if (current) orders.push(current);

  const mergedByExcelId = [];
  const astanaByExcelId = new Map();
  for (const order of orders) {
    if (!order.excel_id) {
      mergedByExcelId.push(order);
      continue;
    }
    const key = `${order.source_file}|${order.excel_id}`;
    const existing = astanaByExcelId.get(key);
    if (!existing) {
      astanaByExcelId.set(key, order);
      mergedByExcelId.push(order);
      continue;
    }

    existing.total_amount += Number(order.total_amount || 0);
    existing.items.push(...order.items.map((item) => ({
      ...item,
      position_idx: existing.items.length + item.position_idx,
    })));
    for (const status of order.statuses) existing.statuses.add(status);
    for (const field of [
      'partner_raw', 'partner_phone', 'agency_raw', 'customer_raw',
      'customer_phone', 'city', 'address', 'commission_text', 'comment',
    ]) {
      if (!existing[field] && order[field]) existing[field] = order[field];
    }
    existing.notes.push(
      `Merged Excel fragments: rows ${existing.row_number} and ${order.row_number}, Excel ID ${order.excel_id}`,
      ...order.notes,
    );
  }

  for (const order of mergedByExcelId) {
    order.status_text = [...order.statuses].join(', ') || null;
    const itemSum = order.items.reduce((sum, item) => sum + Number(item.amount || 0), 0);
    order.item_sum = itemSum;
    if (
      order.source_file === `${RU.astana} \u2014 ${RU.june} 2026 (${RU.sales} ${RU.actual}).xlsx` &&
      String(order.order_num) === '23'
    ) {
      const originalTotal = order.total_amount;
      order.total_amount = itemSum;
      order.notes.push(`Manual correction: Excel total ${originalTotal} included next order; imported by item sum ${itemSum}`);
    }
    if (
      order.source_file === `${RU.astana} \u2014 ${RU.june} 2026 (${RU.sales} ${RU.actual}).xlsx` &&
      String(order.order_num) === '41' &&
      Number(order.total_amount) === 522880 &&
      Number(itemSum) === 293000
    ) {
      const originalTotal = order.total_amount;
      order.total_amount = itemSum;
      order.notes.push(`Manual correction: Excel total ${originalTotal} included next order; imported by item sum ${itemSum}`);
    }
    if (
      order.source_file === `${RU.almaty} \u2014 ${RU.july} 2026 (${RU.sales} ${RU.actual}).xlsx` &&
      String(order.order_num) === '47' &&
      Number(order.total_amount) === 767760 &&
      Number(itemSum) === 270000
    ) {
      const originalTotal = order.total_amount;
      order.total_amount = itemSum;
      order.notes.push(`Manual correction: Excel total ${originalTotal} included order 48; imported by item sum ${itemSum}`);
    }
    order.item_total_delta = Number(order.total_amount || 0) - itemSum;
  }

  return mergedByExcelId.filter((order) => order.sale_date?.startsWith(period));
}

function disambiguateDuplicateOrderNums(orders) {
  const groups = new Map();
  for (const order of orders) {
    const key = `${order.source_file}|${order.order_num}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(order);
  }

  const renamed = [];
  for (const group of groups.values()) {
    if (group.length < 2) continue;
    for (const order of group) {
      const originalOrderNum = String(order.order_num);
      const suffix = order.excel_id || order.row_number;
      order.order_num = `${originalOrderNum}-${suffix}`;
      order.notes.push(`Manual key disambiguation: duplicate Excel order №${originalOrderNum}; imported as ${order.order_num}`);
      renamed.push({
        source_file: order.source_file,
        original_order_num: originalOrderNum,
        order_num: order.order_num,
        excel_id: order.excel_id || null,
        row: order.row_number,
      });
    }
  }
  return renamed;
}

function buildComment(order) {
  const parts = [];
  if (order.comment) parts.push(order.comment);
  if (order.discount_note) parts.push(`Excel note: ${order.discount_note}`);
  if (order.excel_id) parts.push(`Excel ID: ${order.excel_id}`);
  if (order.item_total_delta) parts.push(`Item total delta: ${order.item_total_delta}`);
  for (const note of order.notes) parts.push(note);
  return parts.length ? parts.join(' | ') : null;
}

function deliveryStatus(statusText) {
  const s = lowerKey(statusText);
  if (s.includes('\u0434\u043e\u0441\u0442\u0430\u0432\u043b\u0435\u043d')) return 'delivered';
  if (s.includes('\u043e\u0442\u043a\u0430\u0437') || s.includes('\u0432\u043e\u0437\u0432\u0440\u0430\u0442')) return 'refused';
  return 'pending';
}

async function fetchAll(table, columns) {
  const out = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await sb.from(table).select(columns).range(from, from + pageSize - 1);
    if (error) throw new Error(`${table}: ${error.message}`);
    out.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return out;
}

function indexContacts(contacts) {
  const byPhone = new Map();
  const byName = new Map();
  for (const contact of contacts) {
    for (const phone of [contact.primary_phone, ...(contact.phones || [])].filter(Boolean)) {
      byPhone.set(phone, contact);
    }
    const key = lowerKey(contact.canonical_name);
    if (key) {
      if (!byName.has(key)) byName.set(key, []);
      byName.get(key).push(contact);
    }
  }
  return { byPhone, byName };
}

async function loadContext() {
  const [contacts, agencies, sales] = await Promise.all([
    fetchAll('partner_contacts', 'id, canonical_name, aliases, primary_phone, phones, roles, agency_id, city, tags'),
    fetchAll('agencies', 'id, canonical_name, aliases, city'),
    fetchAll('sales', 'id, source_file, order_num, customer_id, partner_id, sale_date, total_amount, partner_raw, customer_raw'),
  ]);
  return {
    contacts,
    agencies,
    sales,
    contactIndex: indexContacts(contacts),
    agencyByName: new Map(agencies.map((agency) => [lowerKey(agency.canonical_name), agency])),
    existingSaleKeys: new Set(sales.map((sale) => `${sale.source_file}|${sale.order_num}`)),
    salesByKey: new Map(sales.map((sale) => [`${sale.source_file}|${sale.order_num}`, sale])),
  };
}

function mergeArray(existing = [], values = []) {
  return [...new Set([...(existing || []), ...values.filter(Boolean)])];
}

async function resolveAgency(name, city, context, touchedAgencies) {
  const clean = text(name);
  if (!clean) return null;
  const key = lowerKey(clean);
  const existing = context.agencyByName.get(key);
  if (existing) return existing.id;
  if (!SAVE) return `dry-agency:${key}`;

  const { data, error } = await sb.from('agencies')
    .insert({ canonical_name: clean, aliases: [clean], city: city || null })
    .select('id, canonical_name, aliases, city')
    .single();
  if (error) throw new Error(`insert agency ${clean}: ${error.message}`);
  context.agencies.push(data);
  context.agencyByName.set(key, data);
  touchedAgencies.add(data.id);
  return data.id;
}

async function resolveContact({ name, phone, role, city, agencyId }, context, touchedContacts) {
  const cleanName = text(name);
  const cleanPhone = phone || null;
  if (!cleanName && !cleanPhone) return null;

  let contact = cleanPhone ? context.contactIndex.byPhone.get(cleanPhone) : null;
  if (!contact && cleanName) {
    const candidates = context.contactIndex.byName.get(lowerKey(cleanName)) || [];
    if (candidates.length === 1) contact = candidates[0];
    if (candidates.length > 1 && cleanPhone) {
      contact = candidates.find((c) => (c.phones || []).includes(cleanPhone) || c.primary_phone === cleanPhone) || null;
    }
  }

  const canonicalName = cleanName || cleanPhone;
  if (!contact) {
    if (!SAVE) return `dry-contact:${role}:${canonicalName}:${cleanPhone || ''}`;
    const payload = {
      canonical_name: canonicalName,
      aliases: cleanName ? [cleanName] : [],
      primary_phone: cleanPhone,
      phones: cleanPhone ? [cleanPhone] : [],
      roles: [role],
      agency_id: role === 'partner' ? agencyId : null,
      city: city || null,
    };
    const { data, error } = await sb.from('partner_contacts')
      .insert(payload)
      .select('id, canonical_name, aliases, primary_phone, phones, roles, agency_id, city, tags')
      .single();
    if (error) throw new Error(`insert contact ${canonicalName}: ${error.message}`);
    context.contacts.push(data);
    context.contactIndex = indexContacts(context.contacts);
    touchedContacts.add(data.id);
    return data.id;
  }

  const next = {
    aliases: mergeArray(contact.aliases, cleanName && cleanName !== contact.canonical_name ? [cleanName] : []),
    phones: mergeArray(contact.phones, cleanPhone ? [cleanPhone] : []),
    roles: mergeArray(contact.roles, [role]),
    agency_id: role === 'partner' && agencyId ? agencyId : contact.agency_id,
    city: contact.city || city || null,
  };

  const changed = JSON.stringify(next.aliases) !== JSON.stringify(contact.aliases || [])
    || JSON.stringify(next.phones) !== JSON.stringify(contact.phones || [])
    || JSON.stringify(next.roles) !== JSON.stringify(contact.roles || [])
    || next.agency_id !== contact.agency_id
    || next.city !== contact.city;

  if (changed && SAVE) {
    const { error } = await sb.from('partner_contacts').update(next).eq('id', contact.id);
    if (error) throw new Error(`update contact ${contact.id}: ${error.message}`);
    Object.assign(contact, next);
    context.contactIndex = indexContacts(context.contacts);
  }
  touchedContacts.add(contact.id);
  return contact.id;
}

async function updateContactAggregates(contactIds) {
  if (!SAVE || contactIds.size === 0) return { updated: 0 };
  const sales = await fetchAll('sales', 'id, sale_date, total_amount, customer_id, partner_id');
  const stats = new Map();
  for (const sale of sales) {
    for (const id of [sale.customer_id, sale.partner_id].filter(Boolean)) {
      if (!contactIds.has(id)) continue;
      const current = stats.get(id) || { count: 0, amount: 0, first: null, last: null, saleIds: new Set() };
      if (!current.saleIds.has(sale.id)) {
        current.saleIds.add(sale.id);
        current.count += 1;
        current.amount += Number(sale.total_amount || 0);
        if (sale.sale_date) {
          if (!current.first || sale.sale_date < current.first) current.first = sale.sale_date;
          if (!current.last || sale.sale_date > current.last) current.last = sale.sale_date;
        }
      }
      stats.set(id, current);
    }
  }

  let updated = 0;
  for (const id of contactIds) {
    const s = stats.get(id) || { count: 0, amount: 0, first: null, last: null };
    const { error } = await sb.from('partner_contacts').update({
      total_purchases_count: s.count,
      total_purchases_amount: s.amount,
      first_purchase_date: s.first,
      last_purchase_date: s.last,
    }).eq('id', id);
    if (error) throw new Error(`aggregate update ${id}: ${error.message}`);
    updated += 1;
  }
  return { updated };
}

async function refreshViewsAndCache() {
  if (!SAVE) return { mv: 'dry-run', cache: 'dry-run' };
  const { error } = await sb.rpc('refresh_sales_mvs');
  const result = { mv: error ? `error: ${error.message}` : 'ok', cache: 'skipped' };

  const bridgeUrl = process.env.BRIDGE_URL;
  const apiKey = process.env.BRIDGE_API_KEY || process.env.API_KEY;
  if (bridgeUrl && apiKey) {
    try {
      const res = await fetch(`${bridgeUrl.replace(/\/$/, '')}/admin/sales-crm/cache/invalidate`, {
        method: 'POST',
        headers: { 'x-api-key': apiKey },
      });
      result.cache = res.ok ? 'ok' : `http ${res.status}`;
    } catch (err) {
      result.cache = `error: ${err.message}`;
    }
  }
  return result;
}

function saleItemPayload(saleId, order) {
  return order.items.map((item) => ({
    sale_id: saleId,
    position_idx: item.position_idx,
    sku: item.sku,
    raw_name: item.raw_name || null,
    qty: item.qty,
    price_per_unit: item.price_per_unit,
    amount: item.amount,
    category: item.category,
  }));
}

async function main() {
  const orders = FILES.flatMap(parseWorkbook).sort((a, b) => (
    a.sale_date.localeCompare(b.sale_date) || a.source_file.localeCompare(b.source_file) || String(a.order_num).localeCompare(String(b.order_num))
  ));
  const disambiguatedOrderNums = disambiguateDuplicateOrderNums(orders);
  const summaryBySource = {};
  const duplicateKeys = [];
  const internalKeys = new Set();
  for (const order of orders) {
    const key = `${order.source_file}|${order.order_num}`;
    if (internalKeys.has(key)) duplicateKeys.push(key);
    internalKeys.add(key);
    const s = summaryBySource[order.source_file] || { orders: 0, revenue: 0, item_sum: 0, b2b: 0, b2c: 0 };
    s.orders += 1;
    s.revenue += Number(order.total_amount || 0);
    s.item_sum += Number(order.item_sum || 0);
    if (order.partner_raw || order.partner_phone) s.b2b += 1;
    else s.b2c += 1;
    summaryBySource[order.source_file] = s;
  }

  const context = await loadContext();
  const existing = orders.filter((order) => context.existingSaleKeys.has(`${order.source_file}|${order.order_num}`));
  const existingChanged = existing
    .map((order) => {
      const sale = context.salesByKey.get(`${order.source_file}|${order.order_num}`);
      const dbTotal = Number(sale?.total_amount || 0);
      const excelTotal = Number(order.total_amount || 0);
      return {
        source_file: order.source_file,
        order_num: order.order_num,
        sale_date: order.sale_date,
        db_total: dbTotal,
        excel_total: excelTotal,
        delta: excelTotal - dbTotal,
      };
    })
    .filter((row) => Math.abs(row.delta) > 1);
  const discrepancies = orders
    .filter((order) => Math.abs(order.item_total_delta) > 1)
    .map((order) => ({
      source_file: order.source_file,
      order_num: order.order_num,
      row: order.row_number,
      total_amount: order.total_amount,
      item_sum: order.item_sum,
      delta: order.item_total_delta,
    }));

  const dryReport = {
    mode: SAVE ? 'save' : 'dry-run',
    files: FILES.map((file) => path.basename(file)),
    orders: orders.length,
    revenue: orders.reduce((sum, order) => sum + Number(order.total_amount || 0), 0),
    summaryBySource,
    disambiguatedOrderNums,
    duplicateKeys,
    existing_count: existing.length,
    existing_sample: existing.slice(0, 10).map((order) => ({ source_file: order.source_file, order_num: order.order_num })),
    existing_changed: existingChanged,
    discrepancies,
  };
  console.log(JSON.stringify(dryReport, null, 2));

  if (!SAVE) return;
  if (duplicateKeys.length) throw new Error(`Internal duplicate sale keys: ${duplicateKeys.join(', ')}`);

  const touchedContacts = new Set();
  const touchedAgencies = new Set();
  let insertedSales = 0;
  let skippedSales = 0;
  let updatedSales = 0;
  let insertedItems = 0;
  let rewrittenItems = 0;

  for (const order of orders) {
    const saleKey = `${order.source_file}|${order.order_num}`;
    const existingSale = context.salesByKey.get(saleKey);
    const existingTotalChanged = existingSale
      ? Math.abs(Number(existingSale.total_amount || 0) - Number(order.total_amount || 0)) > 1
      : false;
    const existingPartnerMissing = existingSale
      ? Boolean((order.partner_raw || order.partner_phone) && (!existingSale.partner_id || !existingSale.partner_raw))
      : false;
    if (existingSale && !existingTotalChanged && !existingPartnerMissing) {
      if (existingSale.customer_id) touchedContacts.add(existingSale.customer_id);
      if (existingSale.partner_id) touchedContacts.add(existingSale.partner_id);
      skippedSales += 1;
      continue;
    }

    const agencyId = await resolveAgency(order.agency_raw, order.city || order.shop, context, touchedAgencies);
    const partnerId = await resolveContact({
      name: order.partner_raw,
      phone: order.partner_phone,
      role: 'partner',
      city: order.city || order.shop,
      agencyId,
    }, context, touchedContacts);
    const customerId = await resolveContact({
      name: order.customer_raw,
      phone: order.customer_phone,
      role: 'customer',
      city: order.city || order.shop,
      agencyId: null,
    }, context, touchedContacts);

    const salePayload = {
      source_file: order.source_file,
      order_num: String(order.order_num),
      sale_date: order.sale_date,
      total_amount: Math.round(Number(order.total_amount || 0)),
      customer_id: customerId,
      partner_id: partnerId,
      agency_id: agencyId,
      customer_raw: order.customer_raw || null,
      partner_raw: order.partner_raw || null,
      manager: order.manager || null,
      payment_method: order.payment_method || null,
      status_text: order.status_text,
      city: order.city || null,
      address: order.address || null,
      comment: buildComment(order),
      commission_text: order.commission_text || null,
      delivery_status: deliveryStatus(order.status_text),
    };

    if (existingSale) {
      const { error: updateError } = await sb.from('sales')
        .update(salePayload)
        .eq('id', existingSale.id);
      if (updateError) throw new Error(`update sale ${saleKey}: ${updateError.message}`);

      const { error: deleteItemsError } = await sb.from('sale_items').delete().eq('sale_id', existingSale.id);
      if (deleteItemsError) throw new Error(`delete items ${saleKey}: ${deleteItemsError.message}`);

      const itemPayload = saleItemPayload(existingSale.id, order);
      if (itemPayload.length) {
        const { error: itemError } = await sb.from('sale_items').insert(itemPayload);
        if (itemError) throw new Error(`rewrite items ${saleKey}: ${itemError.message}`);
        rewrittenItems += itemPayload.length;
      }
      updatedSales += 1;
      continue;
    }

    const { data: sale, error: saleError } = await sb.from('sales')
      .insert(salePayload)
      .select('id')
      .single();
    if (saleError) throw new Error(`insert sale ${saleKey}: ${saleError.message}`);
    insertedSales += 1;
    context.existingSaleKeys.add(saleKey);

    const itemPayload = saleItemPayload(sale.id, order);
    if (itemPayload.length) {
      const { error: itemError } = await sb.from('sale_items').insert(itemPayload);
      if (itemError) throw new Error(`insert items ${saleKey}: ${itemError.message}`);
      insertedItems += itemPayload.length;
    }
  }

  const aggregateResult = await updateContactAggregates(touchedContacts);
  const refreshResult = await refreshViewsAndCache();

  const { data: periodSales, error: verifyError } = await sb.from('sales')
    .select('source_file, total_amount, partner_id, sale_date')
    .gte('sale_date', periodStart)
    .lte('sale_date', periodEnd);
  if (verifyError) throw new Error(`verify ${period} sales: ${verifyError.message}`);

  const verify = {};
  for (const sale of periodSales || []) {
    const s = verify[sale.source_file] || { orders: 0, revenue: 0, b2b: 0, b2c: 0 };
    s.orders += 1;
    s.revenue += Number(sale.total_amount || 0);
    if (sale.partner_id) s.b2b += 1;
    else s.b2c += 1;
    verify[sale.source_file] = s;
  }

  console.log(JSON.stringify({
    saved: {
      insertedSales,
      skippedSales,
      updatedSales,
      insertedItems,
      rewrittenItems,
      touchedContacts: touchedContacts.size,
      touchedAgencies: touchedAgencies.size,
      aggregateResult,
      refreshResult,
    },
    verifyPeriod: {
      period,
      sources: verify,
    },
  }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || err);
  process.exit(1);
});
