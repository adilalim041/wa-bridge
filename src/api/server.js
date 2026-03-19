import express from 'express';
import { createServer } from 'http';
import { config, logger } from '../config.js';
import { setupRoutes } from './routes.js';
import { setupWebSocket } from './websocket.js';

export function startServer() {
    const app = express();
    const httpServer = createServer(app);

    app.use(express.json({ limit: '1mb' }));

    const ALLOWED_ORIGINS = [
        'https://wa-dashboard-blond.vercel.app',
        'http://localhost:5173',
        'http://localhost:3000',
    ];

    app.use((req, res, next) => {
        const origin = req.headers.origin;
        if (origin && ALLOWED_ORIGINS.includes(origin)) {
            res.header('Access-Control-Allow-Origin', origin);
        }
        res.header('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
        res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
        res.header('Access-Control-Allow-Credentials', 'true');
        if (req.method === 'OPTIONS') return res.sendStatus(204);
        next();
    });
    
    setupRoutes(app);
    setupWebSocket(httpServer);

    httpServer.listen(config.port, () => {
        logger.info(`API server listening on port ${config.port}`);
    });

    return { server: httpServer, app };
}
