import { startServer } from './api/server.js';
import { startConnection } from './baileys/connection.js';
import { config, logger } from './config.js';

let currentSock;
let server;
let setSock = () => {};

async function bootstrap() {
  const connection = await startConnection({
    onSocket: (sock) => {
      currentSock = sock;
      setSock(sock);
    },
  });
  currentSock = connection.sock;

  const serverState = startServer(currentSock);
  server = serverState.server;
  setSock = serverState.setSock;
  logger.info(`WA Bridge started on port ${config.port}`);
}

async function shutdown(signal) {
  logger.info(`Received ${signal}. Shutting down gracefully...`);

  const closeTasks = [];

  if (server) {
    closeTasks.push(
      new Promise((resolve) => {
        server.close(() => resolve());
      })
    );
  }

  if (currentSock?.ws?.readyState === 1) {
    closeTasks.push(
      Promise.resolve(currentSock.end?.(undefined)).catch((error) => {
        logger.warn({ err: error }, 'Failed to close WhatsApp session cleanly');
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
