/**
 * Диагностика — какой вариант запроса insights даёт daily breakdown.
 *
 * Запуск:  node src/meta-ads/_test_insights.mjs
 */

import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const dotenv = require('dotenv');
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

const { metaAdsClient, metaAdsConfig } = await import('./index.js');

if (!metaAdsConfig.enabled) {
  console.error('Module disabled — META_SYSTEM_USER_TOKEN missing');
  process.exit(1);
}

const accountId = metaAdsConfig.adAccountId;

async function tryVariant(name, params) {
  console.log(`\n--- ${name} ---`);
  try {
    const rows = await metaAdsClient.getInsights(accountId, params);
    const dates = new Map();
    for (const r of rows) {
      dates.set(r.date_start, (dates.get(r.date_start) ?? 0) + 1);
    }
    console.log(`  rows: ${rows.length}, distinct date_start: ${dates.size}`);
    const sample = [...dates.entries()].slice(0, 5);
    for (const [d, c] of sample) console.log(`    ${d}: ${c}`);
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
  }
}

// V1 — текущий sync вариант (видим что НЕ работает)
await tryVariant('V1: since/until + time_increments=1', {
  level: 'campaign',
  since: '2026-05-01',
  until: '2026-05-05',
  fields: 'date_start,date_stop,spend',
  time_increments: 1,
});

// V2 — single day (since=until)
await tryVariant('V2: single day, no time_increments', {
  level: 'campaign',
  since: '2026-05-04',
  until: '2026-05-04',
  fields: 'date_start,date_stop,spend',
});

// V3 — time_increments=monthly (вариант от Meta)
await tryVariant('V3: time_increments=monthly', {
  level: 'campaign',
  since: '2026-04-01',
  until: '2026-05-05',
  fields: 'date_start,date_stop,spend',
  time_increments: 'monthly',
});

// V4 — date_preset=yesterday
await tryVariant('V4: date_preset=yesterday', {
  level: 'campaign',
  date_preset: 'yesterday',
  fields: 'date_start,date_stop,spend',
});

// V5 — time_increments=7 (недельный)
await tryVariant('V5: time_increments=7', {
  level: 'campaign',
  since: '2026-04-01',
  until: '2026-05-05',
  fields: 'date_start,date_stop,spend',
  time_increments: 7,
});

console.log('\n--- Verdict ---');
console.log('Если V2 (single day) даёт 1 row на кампанию с date_start=2026-05-04 →');
console.log('  → решение: sync итерирует по дням, делает N запросов с since=until=date');
