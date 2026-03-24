import express from 'express';
import { createServer } from 'http';
import rateLimit from 'express-rate-limit';
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

    // Rate limiting — 300 requests per 15 min per IP (dashboard use)
    // WhatsApp messages bypass this entirely (they come via Baileys, not HTTP)
    app.use(rateLimit({
        windowMs: 15 * 60 * 1000,
        max: 300,
        standardHeaders: true,
        legacyHeaders: false,
        skip: (req) => req.path === '/health' || req.path === '/ws',
    }));

    // Stricter limit for admin/destructive endpoints — 10 per 15 min
    app.use('/admin', rateLimit({
        windowMs: 15 * 60 * 1000,
        max: 10,
        message: { error: 'Too many admin requests, try again later' },
    }));

    // API key auth — always required, skip only health & ws
    const API_KEY = process.env.API_KEY;
    app.use((req, res, next) => {
        if (req.path === '/health' || req.path === '/ws') return next();
        const key = req.headers['x-api-key'] || req.query.apiKey;
        if (key !== API_KEY) {
            return res.status(401).json({ error: 'Invalid or missing API key' });
        }
        next();
    });
    logger.info('API key authentication enabled');

    setupRoutes(app);
    setupWebSocket(httpServer);

    httpServer.listen(config.port, () => {
        logger.info(`API server listening on port ${config.port}`);
    });

    return { server: httpServer, app };
}
