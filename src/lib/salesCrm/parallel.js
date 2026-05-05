/**
 * Sales CRM — parallel pagination + city-breakdown helpers.
 *
 * Extracted from src/lib/salesCrm.js as part of Phase 1 C-2 split (2026-05-05).
 * Self-contained — no upstream salesCrm imports, so no circular-dep risk.
 *
 * loadAllParallel(): параллельная пагинация Supabase-запросов.
 * Вместо 7 последовательных range-запросов (medium 2200 sales = 2200/1000 = 3,
 * items 6800/1000 = 7), делаем все запросы одновременно через Promise.all.
 * Ускоряет загрузку 4-7x.
 *
 * withCityBreakdown(): хелпер для аналитик с city-параметром, чтобы
 * повторно не дублировать "если city == 'all' → byCity" логику.
 *
 * tagItemsWithCity(): добавляет поле city к каждому item в массиве
 * (используется для Format A — timeline items).
 */

export async function loadAllParallel(sb, table, selectFields, filters = {}) {
  // 1. Узнаём общее кол-во строк (head запрос)
  let cntQ = sb.from(table).select('*', { count: 'exact', head: true });
  if (filters.eq) for (const [k, v] of Object.entries(filters.eq)) cntQ = cntQ.eq(k, v);
  if (filters.notNull) for (const k of filters.notNull) cntQ = cntQ.not(k, 'is', null);
  if (filters.isNull) for (const k of filters.isNull) cntQ = cntQ.is(k, null);
  if (filters.gte) for (const [k, v] of Object.entries(filters.gte)) cntQ = cntQ.gte(k, v);
  if (filters.lte) for (const [k, v] of Object.entries(filters.lte)) cntQ = cntQ.lte(k, v);
  if (filters.like) for (const [k, v] of Object.entries(filters.like)) cntQ = cntQ.like(k, v);
  if (filters.notLike) for (const [k, v] of Object.entries(filters.notLike)) cntQ = cntQ.not(k, 'like', v);
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
    if (filters.like) for (const [k, v] of Object.entries(filters.like)) q = q.like(k, v);
    if (filters.notLike) for (const [k, v] of Object.entries(filters.notLike)) q = q.not(k, 'like', v);
    promises.push(q);
  }
  const results = await Promise.all(promises);
  return results.flatMap(r => r.data || []);
}

/**
 * Запускает helperFn(city) для каждого city параллельно и возвращает
 * объект { byCity: { 'Алматы': result, 'Астана': result }, cities }.
 */
export async function withCityBreakdown(cities, helperFn) {
  const results = await Promise.all(cities.map(c => helperFn(c)));
  const byCity = {};
  for (let i = 0; i < cities.length; i++) {
    byCity[cities[i]] = results[i];
  }
  return { byCity, cities };
}

/**
 * Добавляет поле `city` к каждому item в массиве.
 * Используется для Format A (timeline items).
 */
export function tagItemsWithCity(items, city) {
  return (items || []).map(item => ({ ...item, city }));
}
