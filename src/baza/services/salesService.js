import { supabase } from '../database.js';

export async function listSales({
  page = 1,
  limit = 20,
  search,
  dateFrom,
  dateTo,
  minAmount,
  maxAmount,
  sortBy = 'date',
  sortOrder = 'DESC',
}) {
  const safePage = Math.max(1, Number(page) || 1);
  const safeLimit = Math.min(100, Math.max(1, Number(limit) || 20));
  const offset = (safePage - 1) * safeLimit;

  const allowedSort = ['date', 'amount', 'designer_name'];
  const safeSortBy = allowedSort.includes(sortBy) ? sortBy : 'date';
  const ascending = sortOrder === 'ASC';

  // --- Build base filters (without amount) for min/max metadata ---
  function applyBaseFilters(query) {
    let q = query;
    if (search) {
      const term = `%${String(search).trim()}%`;
      q = q.or(`designer_name.ilike.${term},studio_name.ilike.${term}`);
    }
    if (dateFrom) q = q.gte('date', dateFrom);
    if (dateTo) q = q.lte('date', dateTo);
    return q;
  }

  function applyAmountFilters(query) {
    let q = query;
    if (minAmount !== undefined && minAmount !== '') {
      const value = Number(minAmount);
      if (Number.isFinite(value)) q = q.gte('amount', value);
    }
    if (maxAmount !== undefined && maxAmount !== '') {
      const value = Number(maxAmount);
      if (Number.isFinite(value)) q = q.lte('amount', value);
    }
    return q;
  }

  // Min/max metadata (base filters only, no amount filter)
  const metaQuery = applyBaseFilters(
    supabase.from('baza_v_sales_detail').select('amount')
  );
  const { data: metaRows, error: metaError } = await metaQuery;
  if (metaError) throw metaError;

  const amounts = (metaRows || []).map((r) => Number(r.amount || 0));
  const metaMinAmount = amounts.length ? Math.min(...amounts) : 0;
  const metaMaxAmount = amounts.length ? Math.max(...amounts) : 0;

  // Main query with all filters + pagination
  let mainQuery = applyAmountFilters(
    applyBaseFilters(
      supabase
        .from('baza_v_sales_detail')
        .select('id, date, amount, comment, partner_id, designer_name, studio_id, studio_name', { count: 'exact' })
    )
  );

  mainQuery = mainQuery
    .order(safeSortBy, { ascending })
    .order('id', { ascending: false })
    .range(offset, offset + safeLimit - 1);

  const { data: items, count, error: mainError } = await mainQuery;
  if (mainError) throw mainError;

  const total = count || 0;
  const totalPages = Math.ceil(total / safeLimit) || 1;

  return {
    items: items || [],
    meta: {
      page: safePage,
      limit: safeLimit,
      total,
      totalPages,
    },
    extra: {
      filtersMeta: {
        minAmount: metaMinAmount,
        maxAmount: metaMaxAmount,
      },
    },
  };
}
