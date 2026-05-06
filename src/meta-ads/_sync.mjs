/**
 * Meta Marketing API — CLI для ручного запуска sync.
 *
 * Использование:
 *   node src/meta-ads/_sync.mjs --help
 *   node src/meta-ads/_sync.mjs --backfill
 *   node src/meta-ads/_sync.mjs --rebuild
 *   node src/meta-ads/_sync.mjs --delta
 *   node src/meta-ads/_sync.mjs --campaign=<meta_campaign_id>
 *   node src/meta-ads/_sync.mjs --insights-only --days=7
 *   node src/meta-ads/_sync.mjs --insights-only --since=2025-07-01 --until=2025-07-31
 *   node src/meta-ads/_sync.mjs --account=act_XXX
 *
 * Требования:
 *   .env с META_* и SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
 *
 * Запуск из корня wa-bridge/:
 *   node src/meta-ads/_sync.mjs --delta
 */

import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import path from 'path';

// Загружаем .env перед любым другим импортом
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const dotenv = require('dotenv');
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

// ---------------------------------------------------------------------------
// Импорты после загрузки env — порядок важен
// ---------------------------------------------------------------------------

// config.js не требует Supabase — импортируем первым для ранней проверки META_*
const { metaAdsConfig, maskToken } = await import('./config.js');

// Проверяем Supabase ДО импорта sync.js (который транзитивно требует config.js →
// бросает ошибку если SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY/API_KEY не заданы).
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error(`[${new Date().toLocaleTimeString('ru-RU', { hour12: false })}] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.`);
  console.error('Add these to .env before running sync:');
  console.error('  SUPABASE_URL=https://<ref>.supabase.co');
  console.error('  SUPABASE_SERVICE_ROLE_KEY=<service_role_key>');
  console.error('  SUPABASE_KEY=<anon_key>');
  console.error('  API_KEY=<your_api_key>');
  process.exit(1);
}

const {
  syncAdAccount,
  syncCampaigns,
  syncAdSets,
  syncAds,
  syncCreatives,
  syncInsights,
  syncFull,
  syncDelta,
  syncSingleCampaign,
} = await import('./sync.js');

// ---------------------------------------------------------------------------
// Хелперы форматирования
// ---------------------------------------------------------------------------

function ts() {
  return new Date().toLocaleTimeString('ru-RU', { hour12: false });
}

function statusIcon(status) {
  if (status === 'ok') return '✅';
  if (status === 'partial') return '⚠️ ';
  return '❌';
}

function fmtMs(ms) {
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function printHelp() {
  console.log(`
Meta Marketing API — sync CLI
Usage: node src/meta-ads/_sync.mjs [options]

Structural + insights sync:
  --delta                       Delta sync: только active кампании/adsets/ads + insights за 3 дня
                                  Оптимизировано для 6h cron — PAUSED/ARCHIVED не трогает
  --rebuild                     Полный sync: вся структура + insights за 30 дней (UPSERT, данные не удаляются)
  --backfill                    Алиас для --rebuild (обратная совместимость)
  --rebuild --days=N            Полный sync с N днями insights (рекомендуемый max: 90)
  --rebuild --since=YYYY-MM-DD --until=YYYY-MM-DD  Явный диапазон дат для глубокого backfill

Per-campaign:
  --campaign=<meta_campaign_id> Точечный refresh одной кампании (структура + 7 дней insights)
                                  Пример: --campaign=120213601234567890

Частичный sync:
  --insights-only               Только insights (без обновления структуры)
  --insights-only --days=N      Insights за N дней
  --insights-only --since=YYYY-MM-DD --until=YYYY-MM-DD  Явный диапазон
  --campaigns-only              Только кампании (без insights и остальной структуры)

Общие:
  --account=act_XXX             Переопределить ad account из .env
  --help                        Показать эту справку

Notes:
  - UPSERT идемпотентен — существующие данные не удаляются, обновляются
  - При rate limit — sync продолжается (status = partial)
  - При token expired — sync прерывается (status = error)
  - Все money-значения хранятся в MINOR units (центах)
  - effective_status фильтрует именно "что сейчас активно" (лучше чем status)

Examples:
  # Регулярный 6h cron (только активное, быстро):
  node src/meta-ads/_sync.mjs --delta

  # Первый запуск / пересинхронизация за 30 дней:
  node src/meta-ads/_sync.mjs --rebuild

  # Глубокий бэкфилл конкретного периода:
  node src/meta-ads/_sync.mjs --rebuild --since=2025-07-01 --until=2025-07-31

  # Обновить только одну кампанию (после клика "Refresh" в UI):
  node src/meta-ads/_sync.mjs --campaign=120213601234567890

  # Только метрики за последнюю неделю:
  node src/meta-ads/_sync.mjs --insights-only --days=7
`);
}

// ---------------------------------------------------------------------------
// Парсинг аргументов (без yargs/commander)
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = {
    help: false,
    backfill: false,    // --backfill алиас → rebuild
    rebuild: false,     // --rebuild
    delta: false,
    insightsOnly: false,
    campaignsOnly: false,
    campaign: null,     // --campaign=<id>
    days: 30,
    since: null,        // --since=YYYY-MM-DD
    until: null,        // --until=YYYY-MM-DD
    account: null,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--backfill') { args.backfill = true; args.rebuild = true; }
    else if (arg === '--rebuild') args.rebuild = true;
    else if (arg === '--delta') args.delta = true;
    else if (arg === '--insights-only') args.insightsOnly = true;
    else if (arg === '--campaigns-only') args.campaignsOnly = true;
    else if (arg.startsWith('--campaign=')) {
      args.campaign = arg.slice('--campaign='.length).trim();
    } else if (arg.startsWith('--days=')) {
      const n = parseInt(arg.slice('--days='.length), 10);
      if (!isNaN(n) && n > 0) args.days = n;
    } else if (arg.startsWith('--since=')) {
      const val = arg.slice('--since='.length).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(val)) args.since = val;
      else console.warn(`[${ts()}] Invalid --since date format: ${val} (expected YYYY-MM-DD)`);
    } else if (arg.startsWith('--until=')) {
      const val = arg.slice('--until='.length).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(val)) args.until = val;
      else console.warn(`[${ts()}] Invalid --until date format: ${val} (expected YYYY-MM-DD)`);
    } else if (arg.startsWith('--account=')) {
      args.account = arg.slice('--account='.length);
    }
  }

  // Валидация: --since и --until оба должны быть или ни одного
  if ((args.since && !args.until) || (!args.since && args.until)) {
    console.error(`[${ts()}] --since and --until must be used together`);
    process.exit(1);
  }

  // --since/--until переопределяют --days
  if (args.since && args.until) {
    const d1 = new Date(args.since);
    const d2 = new Date(args.until);
    if (d1 > d2) {
      console.error(`[${ts()}] --since must be <= --until`);
      process.exit(1);
    }
  }

  return args;
}

// ---------------------------------------------------------------------------
// Summary table printer
// ---------------------------------------------------------------------------

/**
 * @param {Array<{syncType: string, status: string, recordsSynced: number, errors: Array, durationMs: number}>} results
 */
function printSummaryTable(results) {
  console.log('\n' + '─'.repeat(70));
  console.log('SYNC SUMMARY');
  console.log('─'.repeat(70));

  const colW = { type: 22, status: 10, records: 14, duration: 12, errors: 10 };
  const header =
    'Type'.padEnd(colW.type) +
    'Status'.padEnd(colW.status) +
    'Records'.padStart(colW.records) +
    'Duration'.padStart(colW.duration) +
    'Errors'.padStart(colW.errors);

  console.log(header);
  console.log('─'.repeat(70));

  let totalRecords = 0;
  let anyError = false;

  for (const r of results) {
    const icon = statusIcon(r.status);
    const line =
      r.syncType.padEnd(colW.type) +
      `${icon} ${r.status}`.padEnd(colW.status) +
      String(r.recordsSynced).padStart(colW.records) +
      fmtMs(r.durationMs ?? 0).padStart(colW.duration) +
      String(r.errors?.length ?? 0).padStart(colW.errors);
    console.log(line);
    totalRecords += r.recordsSynced ?? 0;
    if (r.status === 'error') anyError = true;

    // Детали ошибок
    if (r.errors && r.errors.length > 0) {
      for (const e of r.errors.slice(0, 3)) {
        console.log(`  └─ [${e.code}] ${e.message.slice(0, 80)}`);
      }
      if (r.errors.length > 3) {
        console.log(`  └─ ... and ${r.errors.length - 3} more errors`);
      }
    }
  }

  console.log('─'.repeat(70));
  console.log(`Total records synced: ${totalRecords}`);
  console.log('─'.repeat(70));

  return anyError;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const argv = process.argv.slice(2);
  const args = parseArgs(argv);

  // --help или нет аргументов
  if (args.help || argv.length === 0) {
    printHelp();
    process.exit(argv.length === 0 ? 1 : 0);
  }

  // Проверка токена
  console.log(`[${ts()}] Meta sync CLI starting`);
  console.log(`[${ts()}] Token: ${maskToken(process.env.META_SYSTEM_USER_TOKEN ?? '')}`);

  if (!metaAdsConfig.enabled) {
    console.error(`[${ts()}] ❌ Meta Ads module is disabled.`);
    console.error(`[${ts()}] Set META_SYSTEM_USER_TOKEN in .env to activate.`);
    console.error(`[${ts()}] Reason: ${metaAdsConfig.reason}`);
    process.exit(1);
  }

  const adAccountId = args.account ?? metaAdsConfig.adAccountId;
  console.log(`[${ts()}] Ad account: ${adAccountId}`);
  console.log(`[${ts()}] Mode: ${argv.join(' ')}`);

  let results = [];
  let exitCode = 0;

  // ---------------------------------------------------------------------------
  // --campaigns-only
  // ---------------------------------------------------------------------------
  if (args.campaignsOnly) {
    console.log(`\n[${ts()}] syncCampaigns...`);
    const t = Date.now();

    const accountResult = await syncAdAccount(adAccountId);
    if (accountResult.status === 'error') {
      console.error(`[${ts()}] ❌ syncAdAccount failed: ${accountResult.errors[0]?.message}`);
      process.exit(1);
    }

    const r = await syncCampaigns(adAccountId);
    r.durationMs = Date.now() - t;
    results = [accountResult, r];
    console.log(
      `[${ts()}] syncCampaigns ${statusIcon(r.status)} ${r.recordsSynced} records (${fmtMs(r.durationMs)})`
    );
  }

  // ---------------------------------------------------------------------------
  // --insights-only
  // ---------------------------------------------------------------------------
  else if (args.insightsOnly) {
    let since, until;

    if (args.since && args.until) {
      since = args.since;
      until = args.until;
    } else {
      until = new Date().toISOString().slice(0, 10);
      const d = new Date();
      d.setDate(d.getDate() - args.days);
      since = d.toISOString().slice(0, 10);
    }

    console.log(`\n[${ts()}] syncInsights (${since} → ${until})...`);
    const t = Date.now();

    try {
      const r = await syncInsights(adAccountId, { since, until });
      r.durationMs = Date.now() - t;
      results = [r];
      console.log(
        `[${ts()}] syncInsights ${statusIcon(r.status)} ${r.recordsSynced} records (${fmtMs(r.durationMs)})`
      );
    } catch (err) {
      if (err.message.includes('meta_ad_accounts row not found')) {
        console.error(`[${ts()}] ❌ Run --rebuild first to create ad_account row.`);
        process.exit(1);
      }
      throw err;
    }
  }

  // ---------------------------------------------------------------------------
  // --campaign=<id> — точечный refresh одной кампании
  // ---------------------------------------------------------------------------
  else if (args.campaign) {
    const metaCampaignId = args.campaign;
    console.log(`\n[${ts()}] syncSingleCampaign (${metaCampaignId})...`);
    const t = Date.now();

    try {
      // Убедимся что ad_account row существует
      const accountResult = await syncAdAccount(adAccountId);
      if (accountResult.status === 'error') {
        console.error(`[${ts()}] ❌ syncAdAccount failed: ${accountResult.errors[0]?.message}`);
        process.exit(1);
      }

      const summary = await syncSingleCampaign(metaCampaignId, adAccountId);
      const totalMs = Date.now() - t;

      console.log(`\n[${ts()}] Campaign refresh complete:`);
      console.log(`  Campaign:  ${summary.recordsSynced.campaign}`);
      console.log(`  Ad Sets:   ${summary.recordsSynced.adSets}`);
      console.log(`  Ads:       ${summary.recordsSynced.ads}`);
      console.log(`  Insights:  ${summary.recordsSynced.insights}`);
      console.log(`  Duration:  ${fmtMs(summary.durationMs)}`);

      // Конвертируем в стандартный SyncSummary format для printSummaryTable
      const total =
        summary.recordsSynced.campaign +
        summary.recordsSynced.adSets +
        summary.recordsSynced.ads +
        summary.recordsSynced.insights;

      results = [
        accountResult,
        {
          syncType: 'single_campaign',
          status: summary.errors.length > 0 ? 'partial' : 'ok',
          recordsSynced: total,
          errors: summary.errors,
          durationMs: summary.durationMs,
        },
      ];
    } catch (err) {
      console.error(`[${ts()}] ❌ syncSingleCampaign failed: ${err.message}`);
      results = [
        {
          syncType: 'single_campaign',
          status: 'error',
          recordsSynced: 0,
          errors: [{ code: err.code ?? 'INTERNAL', message: err.message }],
          durationMs: Date.now() - t,
        },
      ];
    }
  }

  // ---------------------------------------------------------------------------
  // --delta — только active объекты + последние 3 дня
  // ---------------------------------------------------------------------------
  else if (args.delta) {
    console.log(`\n[${ts()}] syncDelta (active only + last 3 days insights)...`);
    const t = Date.now();
    results = await syncDelta(adAccountId);
    const totalMs = Date.now() - t;

    for (const r of results) {
      console.log(
        `[${ts()}] ${r.syncType.padEnd(24)} ${statusIcon(r.status)} ${r.recordsSynced} records`
      );
    }
    results.forEach((r) => {
      if (!r.durationMs) r.durationMs = Math.floor(totalMs / results.length);
    });
  }

  // ---------------------------------------------------------------------------
  // --rebuild / --backfill — полный sync
  // ---------------------------------------------------------------------------
  else if (args.rebuild) {
    let since, until;

    if (args.since && args.until) {
      since = args.since;
      until = args.until;
      console.log(`\n[${ts()}] syncFull — rebuild ${since} → ${until}...`);
    } else {
      until = new Date().toISOString().slice(0, 10);
      const d = new Date();
      d.setDate(d.getDate() - args.days);
      since = d.toISOString().slice(0, 10);
      console.log(`\n[${ts()}] syncFull — rebuild ${args.days} days (${since} → ${until})...`);
    }

    console.log(`[${ts()}] Note: existing rows will be UPSERTed (not deleted)`);

    const t = Date.now();
    results = await syncFull(adAccountId, { since, until });

    const totalMs = Date.now() - t;

    for (const r of results) {
      console.log(
        `[${ts()}] ${r.syncType.padEnd(24)} ${statusIcon(r.status)} ${r.recordsSynced} records`
      );
    }
    results.forEach((r) => {
      if (!r.durationMs) r.durationMs = Math.floor(totalMs / results.length);
    });
  } else {
    console.error(`[${ts()}] ❌ No valid command provided.`);
    printHelp();
    process.exit(1);
  }

  // ---------------------------------------------------------------------------
  // Summary table
  // ---------------------------------------------------------------------------
  const anyError = printSummaryTable(results);
  exitCode = anyError ? 1 : 0;

  console.log(`\n[${ts()}] Done. Exit code: ${exitCode}`);
  process.exit(exitCode);
}

main().catch((err) => {
  const msg = String(err?.message ?? err);
  const masked = msg.replace(/EAA[A-Za-z0-9+/]{10,}/g, (t) => `EAA***${t.slice(-4)}`);
  console.error(`[${ts()}] Unhandled error: ${masked}`);
  if (err.stack) {
    const maskedStack = err.stack.replace(/EAA[A-Za-z0-9+/]{10,}/g, (t) => `EAA***${t.slice(-4)}`);
    console.error(maskedStack);
  }
  process.exit(1);
});
