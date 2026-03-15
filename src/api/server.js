import express from 'express';
import { config, logger } from '../config.js';
import { setupRoutes } from './routes.js';

export function startServer(initialSock) {
  const app = express();
  let currentSock = initialSock;

  app.use(express.json());
  setupRoutes(app, () => currentSock);

  const server = app.listen(config.port, () => {
    logger.info(`API server listening on port ${config.port}`);
  });

  return {
    server,
    setSock(sock) {
      currentSock = sock;
    },
  };
}
