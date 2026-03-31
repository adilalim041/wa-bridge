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

export async function listStudios() {
  const items = await rpc('baza_list_studios_with_revenue');
  return {
    items,
    meta: { total: items.length },
  };
}

export async function getStudioDetails(id) {
  const asOf = new Date();
  const twoYearsAgo = toYmd(shiftYears(asOf, -2));
  const studioId = Number(id);

  const studio = await queryOne('baza_studios', { filters: { id: studioId } });
  if (!studio) return null;

  const metrics = await computePartnerMetrics({ asOf });
  const rawPartners = await rpc('baza_studio_partners_with_metrics', {
    p_studio_id: studioId,
    p_two_years_ago: twoYearsAgo,
  });

  const partners = rawPartners.map((row) => {
    const enriched = enrichPartner(row, asOf);
    const metric = metrics.byPartnerId.get(Number(enriched.id));
    return metric ? { ...enriched, ...metric } : enriched;
  });

  // Compute stats from partners data
  const partnersCount = partners.length;
  const totalRevenue = partners.reduce((sum, p) => sum + Number(p.total_revenue || 0), 0);
  const totalOrders = partners.reduce((sum, p) => sum + Number(p.orders_count || 0), 0);
  const stats = { partners_count: partnersCount, total_revenue: totalRevenue, total_orders: totalOrders };

  // Studio sales from the view
  const { data: studioSales, error } = await supabase
    .from('baza_v_sales_detail')
    .select('id, date, designer_name, product, amount, comment')
    .eq('studio_id', studioId)
    .order('date', { ascending: false })
    .order('id', { ascending: false });
  if (error) throw error;

  return { ...studio, partners, stats, studioSales: studioSales || [] };
}
