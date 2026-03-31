import { rpc } from '../database.js';

const DAY_MS = 24 * 60 * 60 * 1000;

function toYmd(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function shiftDays(date, deltaDays) {
  return new Date(date.getTime() + deltaDays * DAY_MS);
}

function numberOrZero(value) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n : 0;
}

function normalizeMinMax(value, min, max) {
  const safeValue = numberOrZero(value);
  const safeMin = numberOrZero(min);
  const safeMax = numberOrZero(max);
  if (safeMax <= safeMin) return 0;
  return Math.max(0, Math.min(1, (safeValue - safeMin) / (safeMax - safeMin)));
}

function resolveActivityStatus(recencyDays) {
  if (recencyDays < 90) return 'Active';
  if (recencyDays < 120) return 'At-risk';
  return 'Sleeping';
}

function assignTiers(rows) {
  // Tier assignment by annual revenue percentile buckets.
  const sorted = [...rows].sort((a, b) => b.revenue_12m - a.revenue_12m);
  const total = sorted.length;
  if (!total) return new Map();

  const goldCount = Math.max(1, Math.ceil(total * 0.15));
  const silverCount = Math.ceil(total * 0.35);
  const tiers = new Map();

  sorted.forEach((row, index) => {
    if (index < goldCount) {
      tiers.set(row.partner_id, 'Gold');
      return;
    }
    if (index < goldCount + silverCount) {
      tiers.set(row.partner_id, 'Silver');
      return;
    }
    tiers.set(row.partner_id, 'Bronze');
  });

  return tiers;
}

export async function computePartnerMetrics({ asOf = new Date() } = {}) {
  const start12m = toYmd(shiftDays(asOf, -365));
  const start3m = toYmd(shiftDays(asOf, -90));
  const startPrev3m = toYmd(shiftDays(asOf, -180));

  const rawRows = await rpc('baza_partner_metrics', {
    p_start_12m: start12m,
    p_start_3m: start3m,
    p_start_prev_3m: startPrev3m,
  });

  const rows = rawRows.map((row) => {
    const revenue12m = numberOrZero(row.revenue_12m);
    const orders12m = numberOrZero(row.orders_12m);
    const revenue3m = numberOrZero(row.revenue_3m);
    const revenuePrev3m = numberOrZero(row.revenue_prev_3m);
    const avgCheck12m = orders12m > 0 ? revenue12m / orders12m : 0;

    const lastOrderDate = row.last_order_date || null;
    const recencyDays = lastOrderDate
      ? Math.max(0, Math.floor((asOf.getTime() - new Date(`${String(lastOrderDate).slice(0, 10)}T00:00:00Z`).getTime()) / DAY_MS))
      : 9999;

    let trendPct = 0;
    if (revenuePrev3m > 0) {
      trendPct = ((revenue3m - revenuePrev3m) / revenuePrev3m) * 100;
    } else if (revenue3m > 0) {
      trendPct = 100;
    }

    return {
      partner_id: Number(row.partner_id),
      last_order_date: lastOrderDate,
      recency_days: recencyDays,
      revenue_12m: revenue12m,
      orders_12m: orders12m,
      avg_check_12m: avgCheck12m,
      revenue_3m: revenue3m,
      revenue_prev_3m: revenuePrev3m,
      trend_pct: Number(trendPct.toFixed(2)),
      activity_status: resolveActivityStatus(recencyDays),
    };
  });

  const tiers = assignTiers(rows);
  const freqValues = rows.map((row) => row.orders_12m);
  const recencyValues = rows.map((row) => row.recency_days);
  const avgCheckValues = rows.map((row) => row.avg_check_12m);
  const trendValues = rows.map((row) => row.trend_pct);

  const freqMin = Math.min(...freqValues, 0);
  const freqMax = Math.max(...freqValues, 0);
  const recencyMin = Math.min(...recencyValues, 0);
  const recencyMax = Math.max(...recencyValues, 0);
  const avgMin = Math.min(...avgCheckValues, 0);
  const avgMax = Math.max(...avgCheckValues, 0);
  const trendMin = Math.min(...trendValues, 0);
  const trendMax = Math.max(...trendValues, 0);

  const items = rows.map((row) => {
    // Potential Score = 40% frequency + 30% recency + 20% avg check + 10% trend.
    const freqNorm = normalizeMinMax(row.orders_12m, freqMin, freqMax);
    const recencyNormRaw = normalizeMinMax(row.recency_days, recencyMin, recencyMax);
    const recencyNorm = 1 - recencyNormRaw;
    const avgNorm = normalizeMinMax(row.avg_check_12m, avgMin, avgMax);
    const trendNorm = normalizeMinMax(row.trend_pct, trendMin, trendMax);

    const score = (0.4 * freqNorm) + (0.3 * recencyNorm) + (0.2 * avgNorm) + (0.1 * trendNorm);

    return {
      ...row,
      tier: tiers.get(row.partner_id) || 'Bronze',
      potential_score: Math.round(score * 100),
    };
  });

  const byPartnerId = new Map(items.map((item) => [item.partner_id, item]));
  return { items, byPartnerId };
}
