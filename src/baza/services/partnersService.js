import { supabase, queryOne, rpc } from '../database.js';
import { computeFlags } from '../utils/statuses.js';
import { computePartnerMetrics } from './partnerMetricsService.js';

function toYmd(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function shiftYears(date, deltaYears) {
  const next = new Date(date);
  next.setFullYear(next.getFullYear() + deltaYears);
  return next;
}

function parseStatusesParam(value) {
  if (!value) return [];
  const raw = Array.isArray(value) ? value : String(value).split(',');
  const allowed = new Set(['vip', 'active', 'sleeping']);
  return raw.map((s) => String(s).trim()).filter((s) => allowed.has(s));
}

function parseTierParam(value) {
  if (!value) return [];
  const raw = Array.isArray(value) ? value : String(value).split(',');
  const allowed = new Set(['Gold', 'Silver', 'Bronze']);
  return raw.map((s) => String(s).trim()).filter((s) => allowed.has(s));
}

function parseActivityParam(value) {
  if (!value) return [];
  const raw = Array.isArray(value) ? value : String(value).split(',');
  const allowed = new Set(['Active', 'At-risk', 'Sleeping']);
  return raw.map((s) => String(s).trim()).filter((s) => allowed.has(s));
}

function enrichPartner(row, asOf) {
  const totalRevenue = Number(row.total_revenue || 0);
  const ordersCount = Number(row.orders_count || 0);
  const orders2y = Number(row.orders_2y || 0);
  const revenue2y = Number(row.revenue_2y || 0);
  const flags = computeFlags(
    { lastOrderDate: row.last_order_date, ordersCount2y: orders2y, sum2y: revenue2y },
    { asOf }
  );

  const statuses = [];
  if (flags.isVip) statuses.push('vip');
  if (flags.isSleeping) statuses.push('sleeping');
  else statuses.push('active');

  let primaryStatus = 'active';
  if (flags.isSleeping) primaryStatus = 'sleeping';
  else if (flags.isVip) primaryStatus = 'vip';

  return {
    ...row,
    total_revenue: totalRevenue,
    orders_count: ordersCount,
    orders_2y: orders2y,
    revenue_2y: revenue2y,
    statuses,
    primary_status: primaryStatus,
    status: primaryStatus,
  };
}

export async function listPartners({ search, city, status, tier, activity }, { asOf = new Date() } = {}) {
  const twoYearsAgo = toYmd(shiftYears(asOf, -2));
  const statusesFilter = parseStatusesParam(status);
  const tierFilter = parseTierParam(tier);
  const activityFilter = parseActivityParam(activity);

  const rows = await rpc('baza_list_partners_with_metrics', {
    p_two_years_ago: twoYearsAgo,
    p_search: search || null,
    p_city: city || null,
  });

  const metrics = await computePartnerMetrics({ asOf });
  let items = rows.map((row) => {
    const enriched = enrichPartner(row, asOf);
    const m = metrics.byPartnerId.get(Number(enriched.id));
    return m ? { ...enriched, ...m } : enriched;
  });

  if (statusesFilter.length) {
    items = items.filter((partner) => statusesFilter.every((s) => partner.statuses.includes(s)));
  }
  if (tierFilter.length) {
    items = items.filter((partner) => tierFilter.includes(partner.tier));
  }
  if (activityFilter.length) {
    items = items.filter((partner) => activityFilter.includes(partner.activity_status));
  }

  return {
    items,
    meta: {
      revenueMin: items.length ? Math.min(...items.map((p) => Number(p.total_revenue || 0))) : 0,
      revenueMax: items.length ? Math.max(...items.map((p) => Number(p.total_revenue || 0))) : 0,
      ordersMin: items.length ? Math.min(...items.map((p) => Number(p.orders_count || 0))) : 0,
      ordersMax: items.length ? Math.max(...items.map((p) => Number(p.orders_count || 0))) : 0,
    },
  };
}

export async function listPartnerCities() {
  const { data, error } = await supabase
    .from('baza_partners')
    .select('city')
    .order('city');
  if (error) throw error;

  const unique = [...new Set(data.map((row) => row.city).filter(Boolean))];
  return unique;
}

export async function getPartnersStatusSummary({ asOf = new Date() } = {}) {
  const twoYearsAgo = toYmd(shiftYears(asOf, -2));

  const rows = await rpc('baza_partner_status_summary', {
    p_two_years_ago: twoYearsAgo,
  });

  const summaryMap = {
    vip: { status: 'vip', count: 0, total_revenue: 0 },
    active: { status: 'active', count: 0, total_revenue: 0 },
    sleeping: { status: 'sleeping', count: 0, total_revenue: 0 },
  };

  for (const row of rows) {
    const flags = computeFlags(
      { lastOrderDate: row.last_order_date, ordersCount2y: row.orders_2y, sum2y: row.revenue_2y },
      { asOf }
    );

    const statuses = [];
    if (flags.isVip) statuses.push('vip');
    if (flags.isSleeping) statuses.push('sleeping');
    else statuses.push('active');

    for (const s of statuses) {
      summaryMap[s].count += 1;
      summaryMap[s].total_revenue += Number(row.total_revenue || 0);
    }
  }

  return Object.values(summaryMap);
}

export async function getTopPartners({ asOf = new Date() } = {}) {
  const twoYearsAgo = toYmd(shiftYears(asOf, -2));
  const twelveMonthsAgo = toYmd(new Date(asOf.getTime() - 365 * 86400000));

  const rows = await rpc('baza_top_partners', {
    p_twelve_months_ago: twelveMonthsAgo,
    p_two_years_ago: twoYearsAgo,
  });

  const metrics = await computePartnerMetrics({ asOf });
  return rows.map((row) => {
    const enriched = enrichPartner(row, asOf);
    const m = metrics.byPartnerId.get(Number(enriched.id));
    return m ? { ...enriched, ...m } : enriched;
  });
}

export async function getPartnerDetails(id, { asOf = new Date() } = {}) {
  const partnerId = Number(id);

  const partner = await queryOne('baza_v_partners_with_studio', {
    filters: { id: partnerId },
  });
  if (!partner) return null;

  // Fetch all sales for this partner to compute stats in JS
  const { data: allSales, error: salesErr } = await supabase
    .from('baza_sales')
    .select('amount, date')
    .eq('partner_id', partnerId);
  if (salesErr) throw salesErr;

  // Compute aggregate stats from sales rows
  const totalOrders = allSales.length;
  const totalRevenue = allSales.reduce((sum, s) => sum + Number(s.amount || 0), 0);
  const avgOrder = totalOrders > 0 ? totalRevenue / totalOrders : 0;
  const dates = allSales.map((s) => s.date).filter(Boolean).sort();
  const firstOrderDate = dates[0] || null;
  const lastOrderDate = dates[dates.length - 1] || null;

  const stats = {
    total_orders: totalOrders,
    total_revenue: totalRevenue,
    avg_order: avgOrder,
    first_order_date: firstOrderDate,
    last_order_date: lastOrderDate,
  };

  // Monthly sales for last 12 months
  const twelveMonthsAgo = toYmd(new Date(asOf.getTime() - 365 * 86400000));
  const recentSales = allSales.filter((s) => s.date >= twelveMonthsAgo);
  const monthlyMap = {};
  for (const s of recentSales) {
    const month = String(s.date).substring(0, 7);
    monthlyMap[month] = (monthlyMap[month] || 0) + Number(s.amount || 0);
  }
  const monthlySales = Object.entries(monthlyMap)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, total]) => ({ month, total }));

  // 2-year stats
  const twoYearsAgo = toYmd(shiftYears(asOf, -2));
  const sales2y = allSales.filter((s) => s.date >= twoYearsAgo);
  const orders2y = sales2y.length;
  const revenue2y = sales2y.reduce((sum, s) => sum + Number(s.amount || 0), 0);

  const flags = computeFlags(
    { lastOrderDate: stats.last_order_date, ordersCount2y: orders2y, sum2y: revenue2y },
    { asOf }
  );
  const statuses = [];
  if (flags.isVip) statuses.push('vip');
  if (flags.isSleeping) statuses.push('sleeping');
  else statuses.push('active');

  let status = 'active';
  if (flags.isSleeping) status = 'sleeping';
  else if (flags.isVip) status = 'vip';

  const metrics = await computePartnerMetrics({ asOf });
  const metric = metrics.byPartnerId.get(partnerId) || null;
  return { ...partner, status, statuses, stats, monthlySales, ...(metric || {}) };
}

export async function listPartnerSales(id, { page = 1, limit = 10, sortBy = 'date', sortOrder = 'DESC' } = {}) {
  const partnerId = Number(id);
  const safePage = Math.max(1, Number(page) || 1);
  const safeLimit = Math.min(50, Math.max(1, Number(limit) || 10));
  const safeSortOrder = sortOrder === 'ASC' ? 'ASC' : 'DESC';
  const allowedSortFields = ['date', 'amount', 'product'];
  const safeSortField = allowedSortFields.includes(sortBy) ? sortBy : 'date';
  const offset = (safePage - 1) * safeLimit;

  // Count query (head: true returns only count, no data)
  const { count, error: countErr } = await supabase
    .from('baza_sales')
    .select('*', { count: 'exact', head: true })
    .eq('partner_id', partnerId);
  if (countErr) throw countErr;

  const total = Number(count || 0);

  // Data query with sorting and pagination
  const { data: items, error: dataErr } = await supabase
    .from('baza_sales')
    .select('*')
    .eq('partner_id', partnerId)
    .order(safeSortField, { ascending: safeSortOrder === 'ASC' })
    .range(offset, offset + safeLimit - 1);
  if (dataErr) throw dataErr;

  return {
    items: items || [],
    meta: {
      page: safePage,
      limit: safeLimit,
      total,
      totalPages: Math.ceil(total / safeLimit) || 1,
    },
  };
}
