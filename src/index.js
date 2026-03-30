import { startServer } from './api/server.js';
import { sessionManager } from './baileys/sessionManager.js';
import { logger } from './config.js';
import { stopWebSocket } from './api/websocket.js';
import { stopHealthMonitor } from './monitor.js';
import { stopVersionChecker } from './versionChecker.js';
import { startAIWorker, stopAIWorker } from './ai/aiWorker.js';
import { loadPhoneRegistry } from './baileys/messageHandler.js';
import { startNotificationChecker, stopNotificationChecker } from './notifications/notificationService.js';

let server;
let keepAliveTimer;
let phoneRefreshTimer;

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
  await loadPhoneRegistry();
  await sessionManager.startAll();
  startAIWorker();
  startNotificationChecker();
  startKeepAlive();

  // Refresh phone registry every 60s to pick up new sessions
  phoneRefreshTimer = setInterval(async () => {
    try {
      await loadPhoneRegistry();
    } catch (err) {
      logger.debug({ err: err.message }, 'Phone registry refresh failed');
    }
  }, 60_000);

  logger.info('WA Bridge multi-session started');
}

async function shutdown(signal) {
  logger.info(`Received ${signal}. Shutting down...`);
  if (keepAliveTimer) clearInterval(keepAliveTimer);
  if (phoneRefreshTimer) clearInterval(phoneRefreshTimer);
  stopAIWorker();
  stopNotificationChecker();
  await sessionManager.stopAll();
  stopHealthMonitor();
  stopVersionChecker();
  stopWebSocket();

  const closeTasks = [];

  if (server) {
    closeTasks.push(
      new Promise((resolve) => {
        server.close(() => resolve());
      })
    );
  }

  await Promise.allSettled(closeTasks);
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
