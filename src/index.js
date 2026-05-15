import { startServer } from './api/server.js';
import { sessionManager } from './baileys/sessionManager.js';
import { logger } from './config.js';
import { stopWebSocket } from './api/websocket.js';
import { stopHealthMonitor, startSummaryScheduler, stopSummaryScheduler, startTelegramPolling, stopTelegramPolling, setSessionManagerRef } from './monitor.js';
import { stopVersionChecker } from './versionChecker.js';
import { startAIWorker, stopAIWorker } from './ai/aiWorker.js';
import { loadPhoneRegistry } from './baileys/messageHandler.js';
import { startNotificationChecker, stopNotificationChecker } from './notifications/notificationService.js';
import { clearStaleLocks } from './storage/sessionLock.js';
import { startVaultHeartbeat, stopVaultHeartbeat } from './heartbeat.js';
import { startCleanupScheduler, stopCleanupScheduler } from './cleanup/scheduler.js';
import { startMvRefreshScheduler, stopMvRefreshScheduler } from './cleanup/mvRefresh.js';
import { startMetaAdsCron, stopMetaAdsCron } from './meta-ads/cron.js';

let server;
let keepAliveTimer;
let phoneRefreshTimer;
let phoneRefreshRunning = false;

function startKeepAlive() {
  const INTERVAL = 4 * 60 * 1000; // every 4 minutes
  const selfUrl = process.env.RAILWAY_PUBLIC_DOMAIN
    ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}/health`
    : `http://localhost:${process.env.PORT || 3000}/health`;

  keepAliveTimer = setInterval(async () => {
    try {
      const res = await fetch(selfUrl);
      logger.debug(`Keep-alive ping: ${res.status}`);
    } catch (err) {
      logger.debug({ err: err.message }, 'Keep-alive ping failed (non-critical)');
    }
  }, INTERVAL);

  logger.info(`Keep-alive started: pinging ${selfUrl} every 4 min`);
}

async function bootstrap() {
  const serverState = startServer();
  server = serverState.server;
  setSessionManagerRef(sessionManager); // Enable zombie detection in monitor
  await loadPhoneRegistry();
  await clearStaleLocks(); // Clear locks from previous instance (Railway redeploy)
  await sessionManager.startAll();
  if (process.env.ANTHROPIC_API_KEY) {
    startAIWorker();
    logger.info('AI worker enabled (ANTHROPIC_API_KEY found)');
  } else {
    logger.info('AI worker disabled (no ANTHROPIC_API_KEY — using external Claude Code scheduled task)');
  }
  startNotificationChecker();
  startSummaryScheduler();
  startTelegramPolling();
  startKeepAlive();
  startVaultHeartbeat();
  startCleanupScheduler();
  // MV refresh cron DISABLED 2026-05-14 — Adil: продажи импортируются раз
  // в месяц, daily refresh жрал disk IO budget Supabase Nano (98% сожжено,
  // 522 Cloudflare timeouts). Manual refresh теперь только через
  // POST /admin/sales-crm/refresh-mvs (дёргать после import_sales.mjs).
  // startMvRefreshScheduler();
  startMetaAdsCron();

  // Refresh phone registry periodically to pick up new sessions. Keep this
  // conservative: during Supabase outages overlapping refreshes can pile up
  // and add noise to an already throttled DB.
  const phoneRefreshIntervalMs = Number(process.env.PHONE_REGISTRY_REFRESH_MS || 5 * 60_000);
  phoneRefreshTimer = setInterval(async () => {
    if (phoneRefreshRunning) {
      logger.debug('Phone registry refresh skipped: previous refresh still running');
      return;
    }

    phoneRefreshRunning = true;
    try {
      await loadPhoneRegistry();
    } catch (err) {
      logger.debug({ err: err.message }, 'Phone registry refresh failed');
    } finally {
      phoneRefreshRunning = false;
    }
  }, phoneRefreshIntervalMs);

  logger.info('WA Bridge multi-session started');
}

async function shutdown(signal) {
  logger.info(`Received ${signal}. Shutting down...`);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  if (phoneRefreshTimer) clearInterval(phoneRefreshTimer);
  stopAIWorker();
  stopNotificationChecker();
  stopHealthMonitor();
  stopSummaryScheduler();
  stopTelegramPolling();
  stopVersionChecker();
  stopWebSocket();
  stopVaultHeartbeat();
  stopCleanupScheduler();
  stopMvRefreshScheduler();
  stopMetaAdsCron();

  // Hard timeout: force exit after 15s if graceful shutdown hangs
  const forceExitTimer = setTimeout(() => {
    logger.error('Graceful shutdown timed out after 15s — forcing exit');
    process.exit(1);
  }, 15000);
  forceExitTimer.unref(); // Don't keep process alive just for this timer

  try {
    await sessionManager.stopAll();
  } catch (err) {
    logger.error({ err }, 'Error stopping sessions during shutdown');
  }

  if (server) {
    await new Promise((resolve) => {
      server.close(() => resolve());
      // If server doesn't close in 5s, continue anyway
      setTimeout(resolve, 5000);
    });
  }

  process.exit(0);
}

process.on('unhandledRejection', (reason, promise) => {
  logger.error({ err: reason, promise }, 'Unhandled promise rejection — NOT crashing');
});

process.on('uncaughtException', (error) => {
  logger.error({ err: error }, 'Uncaught exception — shutting down');
  process.exit(1);
});

process.on('SIGINT', () => {
  shutdown('SIGINT').catch((error) => {
    logger.error({ err: error }, 'Shutdown failed');
    process.exit(1);
  });
});

process.on('SIGTERM', () => {
  shutdown('SIGTERM').catch((error) => {
    logger.error({ err: error }, 'Shutdown failed');
    process.exit(1);
  });
});

bootstrap().catch((error) => {
  logger.error({ err: error }, 'Failed to start WA Bridge');
  process.exit(1);
});
