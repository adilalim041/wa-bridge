import crypto from 'crypto';
import express from 'express';
import helmet from 'helmet';
import { createServer } from 'http';
import rateLimit from 'express-rate-limit';
import { config, logger } from '../config.js';
import { setupRoutes } from './routes.js';
import { setupWebSocket } from './websocket.js';
import bazaRouter from '../baza/router.js';

export function startServer() {
    const app = express();
    const httpServer = createServer(app);

    // Security headers
    app.use(helmet({
        contentSecurityPolicy: false, // frontend handles this
        crossOriginEmbedderPolicy: false, // allow cross-origin resources
    }));

    app.use(express.json({ limit: '10mb' }));
    app.use(express.urlencoded({ extended: true, limit: '10mb' }));

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
        res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
        res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Api-Key');
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

    // AI chat rate limit — 20 per 15 min to protect Claude API credits
    app.use('/ai/chat', rateLimit({
        windowMs: 15 * 60 * 1000,
        max: 20,
        message: { error: 'AI chat rate limit reached. Try again in a few minutes.' },
    }));

    // API key auth — always required, skip only health & ws
    const API_KEY = process.env.API_KEY;
    app.use((req, res, next) => {
        if (req.path === '/health' || req.path === '/ws' || req.path.startsWith('/baza/')) return next();
        // QR page: allow if apiKey in query (for browser access)
        if (req.path.startsWith('/qr')) {
            const qrKey = req.query.apiKey;
            if (qrKey && API_KEY && qrKey.length === API_KEY.length && crypto.timingSafeEqual(Buffer.from(qrKey), Buffer.from(API_KEY))) {
                return next();
            }
        }
        const key = req.headers['x-api-key'];
        const isValid = key && API_KEY &&
            key.length === API_KEY.length &&
            crypto.timingSafeEqual(Buffer.from(key), Buffer.from(API_KEY));
        if (!isValid) {
            return res.status(401).json({ error: 'Invalid or missing API key' });
        }
        next();
    });
    logger.info('API key authentication enabled');

    // Mount BAZA CRM routes (handles its own auth via bazaAuth middleware)
    app.use('/baza/api', bazaRouter);

    setupRoutes(app);
    setupWebSocket(httpServer);

    httpServer.listen(config.port, () => {
        logger.info(`API server listening on port ${config.port}`);
    });

    return { server: httpServer, app };
}
