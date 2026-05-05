/**
 * Meta Marketing API — CLI для ручного запуска sync.
 *
 * Использование:
 *   node src/meta-ads/_sync.mjs --help
 *   node src/meta-ads/_sync.mjs --backfill
 *   node src/meta-ads/_sync.mjs --backfill --days=90
 *   node src/meta-ads/_sync.mjs --delta
 *   node src/meta-ads/_sync.mjs --insights-only --days=7
 *   node src/meta-ads/_sync.mjs --campaigns-only
 *   node src/meta-ads/_sync.mjs --account=act_XXX
 *
 * Требования:
 *   .env с META_* и SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
 *
 * Запуск из корня wa-bridge/:
 *   node src/meta-ads/_sync.mjs --backfill
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
// Если переменных нет — выдаём понятное сообщение вместо голого throw из config.js.
if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
  console.error(`[${new Date().toLocaleTimeString('ru-RU', { hour12: false })}] SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing.`);
  console.error('Add these to .env before running sync:');
  console.error('  SUPABASE_URL=https://<ref>.supabase.co');
  console.error('  SUPABASE_SERVICE_ROLE_KEY=<service_role_key>');
  console.error('  SUPABASE_KEY=<anon_key>');
  console.error('  API_KEY=<your_api_key>');
  process.exit(1);
}

const { syncAdAccount, syncCampaigns, syncAdSets, syncAds, syncCreatives, syncInsights, syncFull, syncDelta } =
  await import('./sync.js');

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

Options:
  --backfill              Full sync: структура + insights за 30 дней (default)
  --backfill --days=N     Full sync с N днями insights (рекомендуемый max: 90)
  --delta                 Delta sync: структура + insights за последние 2 дня
  --insights-only         Только insights (без обновления структуры)
  --insights-only --days=N  Только insights за N дней
  --campaigns-only        Только кампании (без insights и структуры)
  --account=act_XXX       Переопределить ad account из .env
  --help                  Показать эту справку

Notes:
  - UPSERT идемпотентен — существующие данные не удаляются, обновляются
  - При rate limit — sync продолжается (status = partial)
  - При token expired — sync прерывается (status = error)
  - Все money-значения хранятся в MINOR units (центах)

Examples:
  node src/meta-ads/_sync.mjs --backfill
  node src/meta-ads/_sync.mjs --backfill --days=90
  node src/meta-ads/_sync.mjs --delta
  node src/meta-ads/_sync.mjs --insights-only --days=7
`);
}

// ---------------------------------------------------------------------------
// Парсинг аргументов (без yargs/commander)
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  const args = {
    help: false,
    backfill: false,
    delta: false,
    insightsOnly: false,
    campaignsOnly: false,
    days: 30,
    account: null,
  };

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') args.help = true;
    else if (arg === '--backfill') args.backfill = true;
    else if (arg === '--delta') args.delta = true;
    else if (arg === '--insights-only') args.insightsOnly = true;
    else if (arg === '--campaigns-only') args.campaignsOnly = true;
    else if (arg.startsWith('--days=')) {
      const n = parseInt(arg.slice('--days='.length), 10);
      if (!isNaN(n) && n > 0) args.days = n;
    } else if (arg.startsWith('--account=')) {
      args.account = arg.slice('--account='.length);
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

  const colW = { type: 20, status: 10, records: 14, duration: 12, errors: 10 };
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

    // Ensure ad_account row exists first
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
    const since = (() => {
      const d = new Date();
      d.setDate(d.getDate() - args.days);
      return d.toISOString().slice(0, 10);
    })();
    const until = new Date().toISOString().slice(0, 10);

    console.log(`\n[${ts()}] syncInsights (${args.days} days: ${since} → ${until})...`);
    const t = Date.now();

    // Resolve account UUID — нужна запись в meta_ad_accounts
    // Пробуем без ensureAdAccount чтобы не мутировать. Если нет — подсказываем.
    try {
      const r = await syncInsights(adAccountId, { since, until });
      r.durationMs = Date.now() - t;
      results = [r];
      console.log(
        `[${ts()}] syncInsights ${statusIcon(r.status)} ${r.recordsSynced} records (${fmtMs(r.durationMs)})`
      );
    } catch (err) {
      if (err.message.includes('meta_ad_accounts row not found')) {
        console.error(`[${ts()}] ❌ Run --backfill first to create ad_account row.`);
        process.exit(1);
      }
      throw err;
    }
  }

  // ---------------------------------------------------------------------------
  // --delta
  // ---------------------------------------------------------------------------
  else if (args.delta) {
    console.log(`\n[${ts()}] syncDelta (last 2 days)...`);
    const t = Date.now();
    results = await syncDelta(adAccountId);
    const totalMs = Date.now() - t;

    // Напечатать прогресс
    for (const r of results) {
      console.log(
        `[${ts()}] ${r.syncType.padEnd(12)} ${statusIcon(r.status)} ${r.recordsSynced} records`
      );
    }
    results.forEach((r) => {
      if (!r.durationMs) r.durationMs = Math.floor(totalMs / results.length);
    });
  }

  // ---------------------------------------------------------------------------
  // --backfill (default)
  // ---------------------------------------------------------------------------
  else if (args.backfill) {
    const since = (() => {
      const d = new Date();
      d.setDate(d.getDate() - args.days);
      return d.toISOString().slice(0, 10);
    })();
    const until = new Date().toISOString().slice(0, 10);

    console.log(
      `\n[${ts()}] syncFull — backfill ${args.days} days (${since} → ${until})...`
    );

    // Предупреждение если данные уже есть
    // (проверяем через мета факт — если есть строки, это UPSERT, не удаление)
    console.log(`[${ts()}] Note: existing rows will be UPSERTed (not deleted)`);

    const t = Date.now();
    results = await syncFull(adAccountId, { since, until });

    const totalMs = Date.now() - t;

    // Прогресс по ходу уже логируется через pino в sync.js
    // Здесь выводим краткие итоги по шагам
    for (const r of results) {
      console.log(
        `[${ts()}] ${r.syncType.padEnd(12)} ${statusIcon(r.status)} ${r.recordsSynced} records`
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
  // Маскируем токен если вдруг попал в стек
  const masked = msg.replace(/EAA[A-Za-z0-9+/]{10,}/g, (t) => `EAA***${t.slice(-4)}`);
  console.error(`[${ts()}] Unhandled error: ${masked}`);
  if (err.stack) {
    const maskedStack = err.stack.replace(/EAA[A-Za-z0-9+/]{10,}/g, (t) => `EAA***${t.slice(-4)}`);
    console.error(maskedStack);
  }
  process.exit(1);
});
