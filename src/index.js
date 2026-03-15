import { startServer } from './api/server.js';
import { sessionManager } from './baileys/sessionManager.js';
import { logger } from './config.js';
import { stopWebSocket } from './api/websocket.js';
import { stopHealthMonitor } from './monitor.js';
import { stopVersionChecker } from './versionChecker.js';

let server;

async function bootstrap() {
  const serverState = startServer();
  server = serverState.server;
  await sessionManager.startAll();
  logger.info('WA Bridge multi-session started');
}

async function shutdown(signal) {
  logger.info(`Received ${signal}. Shutting down...`);
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
