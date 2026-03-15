import express from 'express';
import { createServer } from 'http';
import { config, logger } from '../config.js';
import { setupRoutes } from './routes.js';
import { setupWebSocket } from './websocket.js';

export function startServer() {
  const app = express();
  const httpServer = createServer(app);

  app.use(express.json());
  setupRoutes(app);
  setupWebSocket(httpServer);

  httpServer.listen(config.port, () => {
    logger.info(`API server listening on port ${config.port}`);
  });

  return {
    server: httpServer,
    app,
  };
}
