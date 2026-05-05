/**
 * Meta Marketing API — smoke test (read-only).
 *
 * Делает реальные GET-запросы в Meta API. Ничего не пишет.
 * Токены нигде не печатаются — только маскированные версии.
 *
 * Запуск:
 *   node src/meta-ads/_smoke.mjs
 *
 * Требования:
 *   - .env с META_SYSTEM_USER_TOKEN и META_AD_ACCOUNT_ID
 *   - Локальный запуск из корня wa-bridge/
 */

import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import path from 'path';

// Загружаем .env перед любым другим импортом
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const dotenv = require('dotenv');
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

// Теперь импортируем модуль (он читает process.env при старте)
const { metaAdsConfig, maskToken, metaAdsClient, MetaApiError, MetaAdsDisabledError } =
  await import('./index.js');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ok(msg) {
  console.log(`✅ ${msg}`);
}

function warn(msg) {
  console.log(`⚠️  ${msg}`);
}

function fail(msg) {
  console.error(`❌ ${msg}`);
}

function section(title) {
  console.log(`\n─── ${title} ${'─'.repeat(Math.max(0, 50 - title.length))}`);
}

function formatCurrency(minorUnits, currency) {
  if (minorUnits == null) return 'n/a';
  return `${(Number(minorUnits) / 100).toFixed(2)} ${currency ?? ''}`.trim();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('Meta Marketing API — smoke test');
  console.log(`Token: ${maskToken(process.env.META_SYSTEM_USER_TOKEN ?? '')}`);
  console.log(`Ad account: ${process.env.META_AD_ACCOUNT_ID ?? '(not set)'}`);

  // Шаг 1: проверить включённость модуля
  if (!metaAdsConfig.enabled) {
    warn('Module is dormant — set META_SYSTEM_USER_TOKEN in .env to run full smoke.');
    warn(`Reason: ${metaAdsConfig.reason}`);
    process.exit(0);
  }

  ok(`Module active. API version: ${metaAdsConfig.apiVersion}`);

  let currency = 'USD';
  let exitCode = 0;

  // Шаг 2: /me
  section('GET /me');
  try {
    const me = await metaAdsClient.getMe();
    ok(`System user: ${me.name} (id=${me.id})`);
  } catch (err) {
    fail(`getMe() failed: ${formatError(err)}`);
    exitCode = 1;
  }

  // Шаг 3: /act_XXX (details)
  section('GET ad account');
  try {
    const account = await metaAdsClient.getAdAccount();
    currency = account.currency ?? 'USD';
    const balance = formatCurrency(account.balance, currency);
    ok(`Account: "${account.name}"`);
    ok(`Currency: ${currency}   Timezone: ${account.timezone_name}`);
    ok(`Balance: ${balance}`);
  } catch (err) {
    fail(`getAdAccount() failed: ${formatError(err)}`);
    exitCode = 1;
  }

  // Шаг 4: /campaigns
  section('GET campaigns');
  try {
    const campaigns = await metaAdsClient.listCampaigns();
    ok(`Total campaigns: ${campaigns.length}`);
    const preview = campaigns.slice(0, 3);
    for (const c of preview) {
      console.log(`   • [${c.status}] ${c.name}`);
    }
    if (campaigns.length > 3) {
      console.log(`   … and ${campaigns.length - 3} more`);
    }
  } catch (err) {
    fail(`listCampaigns() failed: ${formatError(err)}`);
    exitCode = 1;
  }

  // Шаг 5: /adcreatives (limit 5)
  section('GET creatives (limit 5)');
  try {
    const creatives = await metaAdsClient.listCreatives(undefined, { maxRecords: 5 });
    ok(`Total creatives fetched: ${creatives.length}`);
    const preview = creatives.slice(0, 3);
    for (const c of preview) {
      const body = c.body ? c.body.slice(0, 50) : '(no body)';
      const hasImage = c.image_url ? 'yes' : 'no';
      console.log(`   • id=${c.id}  image=${hasImage}  body="${body}"`);
    }
  } catch (err) {
    fail(`listCreatives() failed: ${formatError(err)}`);
    exitCode = 1;
  }

  // Шаг 6: insights last_7d
  section('GET insights — last_7d (account level)');
  try {
    const accountId = metaAdsConfig.adAccountId;
    const insights = await metaAdsClient.getInsights(accountId, {
      level: 'account',
      date_preset: 'last_7d',
      fields: 'impressions,clicks,spend,reach',
    });

    if (insights.length === 0) {
      warn('No insights data for last_7d (account may have no activity)');
    } else {
      // Meta может вернуть несколько строк при разбивке по дням
      // Суммируем для общей картины
      let totalImpressions = 0;
      let totalClicks = 0;
      let totalSpend = 0;
      for (const row of insights) {
        totalImpressions += Number(row.impressions ?? 0);
        totalClicks += Number(row.clicks ?? 0);
        totalSpend += Number(row.spend ?? 0); // в units currency (не minor!)
      }
      // Примечание: spend от insights возвращается в ОСНОВНЫХ единицах (не minor units),
      // поэтому НЕ делим на 100 здесь. minor units только в бюджетах кампаний.
      ok(`Impressions: ${totalImpressions.toLocaleString()}`);
      ok(`Clicks:      ${totalClicks.toLocaleString()}`);
      ok(`Spend:       ${totalSpend.toFixed(2)} ${currency}`);
    }
  } catch (err) {
    fail(`getInsights() failed: ${formatError(err)}`);
    exitCode = 1;
  }

  // Итог
  section('Result');
  if (exitCode === 0) {
    ok('All smoke checks passed.');
  } else {
    fail('Some checks failed — see above for details.');
  }

  process.exit(exitCode);
}

/**
 * Форматировать ошибку для вывода без токена.
 */
function formatError(err) {
  if (err instanceof MetaAdsDisabledError) {
    return `[MetaAdsDisabledError] ${err.message}`;
  }
  if (err instanceof MetaApiError) {
    return (
      `[MetaApiError] code=${err.code} subcode=${err.subcode} ` +
      `httpStatus=${err.httpStatus} isRetryable=${err.isRetryable} ` +
      `path=${err.requestPath} — ${err.message}`
    );
  }
  // Общая ошибка — убедиться что нет токена в message
  const msg = String(err?.message ?? err);
  const masked = msg.replace(/EAA[A-Za-z0-9+/]{10,}/g, (t) => maskToken(t));
  return masked;
}

main().catch((err) => {
  fail(`Unhandled error: ${formatError(err)}`);
  process.exit(1);
});
