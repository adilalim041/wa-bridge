import { supabase, rpc } from '../database.js';
import { computeFlags } from '../utils/statuses.js';

const monthShort = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function parseStatusesParam(value) {
  if (!value) return [];
  const raw = Array.isArray(value) ? value : String(value).split(',');
  const allowed = new Set(['vip', 'active', 'sleeping']);
  return raw.map((s) => String(s).trim()).filter((s) => allowed.has(s));
}

function toYmd(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function shiftMonths(date, deltaMonths) {
  const next = new Date(date);
  next.setMonth(next.getMonth() + deltaMonths);
  return next;
}

function shiftYears(date, deltaYears) {
  const next = new Date(date);
  next.setFullYear(next.getFullYear() + deltaYears);
  return next;
}

function toWeekStartYmd(dateStr) {
  const date = new Date(dateStr);
  if (Number.isNaN(date.getTime())) return null;
  const day = date.getDay();
  const diffToMonday = day === 0 ? -6 : 1 - day;
  const monday = new Date(date);
  monday.setDate(date.getDate() + diffToMonday);
  return toYmd(monday);
}

function dayDiffInclusive(from, to) {
  const a = new Date(from);
  const b = new Date(to);
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return null;
  const ms = Math.abs(b.getTime() - a.getTime());
  return Math.floor(ms / 86400000) + 1;
}

function resolveTrendGranularity(dateFrom, dateTo, salesRows) {
  let days = null;
  if (dateFrom && dateTo) {
    days = dayDiffInclusive(dateFrom, dateTo);
  } else if (salesRows.length > 1) {
    const sorted = [...salesRows].sort((a, b) => String(a.date).localeCompare(String(b.date)));
    days = dayDiffInclusive(sorted[0].date, sorted[sorted.length - 1].date);
  }

  if (days !== null && days <= 30) return 'day';
  if (days !== null && days >= 183) return 'month';
  return 'week';
}

function buildTrend(rows, granularity) {
  const map = new Map();

  for (const row of rows) {
    const amount = Number(row.amount || 0);
    if (!Number.isFinite(amount)) continue;

    let key = row.date;
    let label = row.date;

    if (granularity === 'month') {
      key = String(row.date).slice(0, 7);
      const [yyyy, mm] = key.split('-');
      label = `${monthShort[Number(mm) - 1]} ${String(yyyy).slice(2)}`;
    } else if (granularity === 'week') {
      const weekStart = toWeekStartYmd(row.date);
      if (!weekStart) continue;
      key = weekStart;
      const d = new Date(weekStart);
      label = `W ${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}`;
    } else {
      const d = new Date(row.date);
      if (Number.isNaN(d.getTime())) continue;
      label = `${String(d.getDate()).padStart(2, '0')}.${String(d.getMonth() + 1).padStart(2, '0')}`;
    }

    const current = map.get(key) || { bucket: key, label, total: 0 };
    current.total += amount;
    map.set(key, current);
  }

  return [...map.values()].sort((a, b) => String(a.bucket).localeCompare(String(b.bucket)));
}

async function getFilteredPartners({ city, statusesFilter, asOf }) {
  const twoYearsAgo = toYmd(shiftYears(asOf, -2));

  const rows = await rpc('baza_analytics_filtered_partners', {
    p_two_years_ago: twoYearsAgo,
    p_city: city || null,
  });

  const enriched = rows.map((row) => {
    const flags = computeFlags(
      { lastOrderDate: row.last_order_date, ordersCount2y: row.orders_2y, sum2y: row.revenue_2y },
      { asOf }
    );
    const statuses = [];
    if (flags.isVip) statuses.push('vip');
    if (flags.isSleeping) statuses.push('sleeping');
    else statuses.push('active');

    let status = 'active';
    if (flags.isSleeping) status = 'sleeping';
    else if (flags.isVip) status = 'vip';

    return { ...row, statuses, status };
  });

  return statusesFilter.length ? enriched.filter((p) => statusesFilter.every((s) => p.statuses.includes(s))) : enriched;
}

async function getSalesRows(partnerIds, dateFrom, dateTo) {
  if (!partnerIds.length) return [];

  let query = supabase
    .from('baza_v_sales_detail')
    .select('id, partner_id, date, amount, product, comment, city')
    .in('partner_id', partnerIds);

  if (dateFrom) query = query.gte('date', dateFrom);
  if (dateTo) query = query.lte('date', dateTo);
  query = query.order('date');

  const { data, error } = await query;
  if (error) throw error;
  return data || [];
}

function buildDashboardFromRows({ partners, salesRows, dateFrom, dateTo }) {
  const totalRevenue = salesRows.reduce((acc, row) => acc + Number(row.amount || 0), 0);
  const avgOrder = salesRows.length ? Math.round(totalRevenue / salesRows.length) : 0;
  const activePartners = partners.filter((p) => p.statuses.includes('active')).length;

  const trendGranularity = resolveTrendGranularity(dateFrom, dateTo, salesRows);
  const salesTrend = buildTrend(salesRows, trendGranularity);

  const cityMap = new Map();
  for (const row of salesRows) {
    const key = row.city || '-';
    cityMap.set(key, (cityMap.get(key) || 0) + Number(row.amount || 0));
  }
  const salesByCity = [...cityMap.entries()]
    .map(([city, total]) => ({ city, total }))
    .sort((a, b) => b.total - a.total);

  const partnerTotals = new Map();
  const partnerOrders = new Map();
  for (const row of salesRows) {
    partnerTotals.set(row.partner_id, (partnerTotals.get(row.partner_id) || 0) + Number(row.amount || 0));
    partnerOrders.set(row.partner_id, (partnerOrders.get(row.partner_id) || 0) + 1);
  }

  const statusMap = { vip: 0, active: 0, sleeping: 0 };
  for (const partner of partners) {
    const total = partnerTotals.get(partner.id) || 0;
    for (const status of partner.statuses) {
      statusMap[status] += total;
    }
  }
  const salesByStatus = Object.entries(statusMap)
    .map(([status, total]) => ({ status, total }))
    .filter((entry) => entry.total > 0);

  const topPartners = partners
    .map((partner) => ({
      id: partner.id,
      designer_name: partner.designer_name,
      city: partner.city,
      studio_name: partner.studio_name,
      status: partner.status,
      statuses: partner.statuses,
      total: partnerTotals.get(partner.id) || 0,
      orders: partnerOrders.get(partner.id) || 0,
    }))
    .filter((partner) => partner.orders > 0)
    .sort((a, b) => b.total - a.total)
    .slice(0, 10);

  const seasonalityMap = {};
  for (const row of salesRows) {
    const monthNum = Number(String(row.date).slice(5, 7));
    seasonalityMap[monthNum] = (seasonalityMap[monthNum] || 0) + Number(row.amount || 0);
  }
  const seasonality = Object.keys(seasonalityMap)
    .map((month) => ({ month_num: Number(month), total: seasonalityMap[month] }))
    .sort((a, b) => a.month_num - b.month_num);

  const salesByMonth = buildTrend(salesRows, 'month').map((row) => ({ month: row.bucket, total: row.total }));

  return {
    kpi: {
      totalPartners: partners.length,
      totalRevenue,
      avgOrder,
      activePartners,
    },
    trendGranularity,
    salesTrend,
    salesByMonth,
    salesByCity,
    salesByStatus,
    topPartners,
    seasonality,
  };
}

export async function getAnalyticsDashboard(
  { dateFrom, dateTo, from, to, city, status, granularity },
  { asOf = new Date() } = {}
) {
  const resolvedDateFrom = dateFrom || from;
  const resolvedDateTo = dateTo || to;
  const statusesFilter = parseStatusesParam(status);
  const partners = await getFilteredPartners({ city, statusesFilter, asOf });
  const partnerIds = partners.map((partner) => partner.id);
  const salesRows = await getSalesRows(partnerIds, resolvedDateFrom, resolvedDateTo);
  const dashboard = buildDashboardFromRows({
    partners,
    salesRows,
    dateFrom: resolvedDateFrom,
    dateTo: resolvedDateTo,
  });

  if (granularity && ['day', 'week', 'month'].includes(granularity)) {
    dashboard.trendGranularity = granularity;
    dashboard.salesTrend = buildTrend(salesRows, granularity);
  }

  return dashboard;
}

export async function getAnalyticsExportRows(
  { dateFrom, dateTo, from, to, city, status },
  { asOf = new Date() } = {}
) {
  const resolvedDateFrom = dateFrom || from;
  const resolvedDateTo = dateTo || to;
  const statusesFilter = parseStatusesParam(status);
  const partners = await getFilteredPartners({ city, statusesFilter, asOf });
  const partnerIds = partners.map((partner) => partner.id);
  const salesRows = await getSalesRows(partnerIds, resolvedDateFrom, resolvedDateTo);
  const partnerMap = new Map(partners.map((partner) => [partner.id, partner]));

  return salesRows
    .sort((a, b) => String(b.date).localeCompare(String(a.date)))
    .map((sale) => {
      const partner = partnerMap.get(sale.partner_id);
      return {
        studio: partner?.studio_name || '',
        designer: partner?.designer_name || '',
        city: partner?.city || '',
        statuses: (partner?.statuses || []).join(', '),
        date: sale.date,
        product: sale.product,
        amount: sale.amount,
        comment: sale.comment,
      };
    });
}
