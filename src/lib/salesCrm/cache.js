/**
 * Sales CRM — in-memory cache helpers.
 *
 * Extracted from src/lib/salesCrm.js as part of Phase 1 C-2 split (2026-05-05).
 * Self-contained — no upstream salesCrm imports, so no circular-dep risk.
 *
 * ─── Контекст ──────────────────────────────────────────────────────────────
 * Аналитика по 2200+ заказам пересчитывается 1-3 секунды на холодный кэш.
 * Источник данных — импорты Excel-файлов раз в месяц + редкие merge/agency-update
 * из дашборда. Поэтому кэш живёт 24ч, а не 5 минут — нет смысла гонять 700+
 * строк sales каждые 5 минут, если за это время ничего не изменилось.
 *
 * Cache-key включает userId (req.user?.userId), чтобы между разными
 * авторизациями не было утечек.
 *
 * invalidateSalesCache() стирает кэш при write-операциях (merge,
 * agency-update, followup-done). Это покрывает все мутации из UI.
 * Для офлайн-импортов: рестарт bridge (Railway автодеплой) либо ручной
 * POST /sales-crm/cache/invalidate.
 *
 * SECURITY: x-api-key path использует serviceClient (RLS bypass).
 * Кэшировать его результаты под общим ключом ОПАСНО — другой запрос
 * под JWT user может получить service-level данные. Кэш ТОЛЬКО для JWT-юзеров.
 */

export const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const _cache = new Map();

export function cacheKey(name, req, paramsObj) {
  if (!req?.user?.userId) return null; // service-role / no-auth → не кэшируем
  return `${name}|${req.user.userId}|${JSON.stringify(paramsObj || {})}`;
}

export function cacheGet(key) {
  if (!key) return null;
  const e = _cache.get(key);
  if (!e) return null;
  if (Date.now() > e.expiresAt) { _cache.delete(key); return null; }
  return e.data;
}

export function cacheSet(key, data) {
  if (!key) return; // service-role bypass — не сохраняем
  _cache.set(key, { data, expiresAt: Date.now() + CACHE_TTL_MS });
  // light cleanup чтобы Map не разрастался
  if (_cache.size > 200) {
    const cutoff = Date.now();
    for (const [k, v] of _cache) if (v.expiresAt < cutoff) _cache.delete(k);
  }
}

export function invalidateSalesCache() {
  _cache.clear();
}
