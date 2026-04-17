import crypto from 'crypto';
import express from 'express';
import helmet from 'helmet';
import { createServer } from 'http';
import rateLimit from 'express-rate-limit';
import { config, logger } from '../config.js';
import { setupRoutes } from './routes.js';
import { setupWebSocket } from './websocket.js';
import bazaRouter from '../baza/router.js';
import { verifySupabaseJwt, isJwtAuthAvailable } from './jwtAuth.js';
import { requestLogger } from './middleware/requestLogger.js';
import { expressErrorHandler, installGlobalHandlers } from '../observability/sentry.js';

export function startServer() {
    // W3 observability: catch unhandledRejection / uncaughtException globally
    // so they're captured with a stack rather than silently swallowed by Node.
    installGlobalHandlers();

    const app = express();
    const httpServer = createServer(app);

    // Request correlation middleware — MOUNTED FIRST so req.id/req.log exist
    // before any other middleware (including helmet, rate-limit, auth) can
    // crash and want to log. Every subsequent log line in the request chain
    // carries reqId via pino child logger.
    app.use(requestLogger);

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

    // ── Authentication middleware ──────────────────────────────────────────────
    // Phase A (Task 1.2): parallel x-api-key + Supabase JWT.
    // Either one is sufficient — Phase B will migrate the frontend to JWT,
    // then Phase C will remove x-api-key from user-facing endpoints.
    //
    // Priority:
    //   1. Authorization: Bearer <jwt>  → Supabase JWT verify (sets req.user)
    //   2. x-api-key header             → timingSafeEqual (legacy, no req.user)
    //   3. Both absent                  → 401 fail-closed
    //
    // Special cases:
    //   - /health, /ws, /baza/*  — always bypass (handled elsewhere)
    //   - /qr/*                  → allows ?apiKey= query param (browser QR scan)
    // ─────────────────────────────────────────────────────────────────────────
    const API_KEY = process.env.API_KEY;
    const JWT_AVAILABLE = isJwtAuthAvailable();

    app.use(async (req, res, next) => {
        // ── Public paths ──────────────────────────────────────────────────────
        if (req.path === '/health' || req.path === '/ws' || req.path.startsWith('/baza/')) {
            return next();
        }

        // ── QR page: allow ?apiKey= in query (browser can't send headers) ────
        if (req.path.startsWith('/qr')) {
            const qrKey = req.query.apiKey;
            if (qrKey && API_KEY && qrKey.length === API_KEY.length &&
                crypto.timingSafeEqual(Buffer.from(qrKey), Buffer.from(API_KEY))) {
                return next();
            }
            // Fall through — QR page also accepts Bearer JWT (e.g. dashboard webview)
        }

        // ── Attempt 1: Bearer JWT ─────────────────────────────────────────────
        const authHeader = req.headers.authorization;
        if (authHeader && authHeader.startsWith('Bearer ')) {
            if (!JWT_AVAILABLE) {
                // JWT configured on client but server has no SUPABASE_URL — fail loudly
                req.log.warn('jwt_auth_not_configured');
                return res.status(401).json({ error: 'JWT auth not configured on server' });
            }
            const token = authHeader.slice(7); // strip "Bearer "
            try {
                const user = await verifySupabaseJwt(token);
                req.user = user;
                // Enrich request logger with userId — every subsequent log line
                // for this request will carry it. Useful for per-user audit trails.
                req.log = req.log.child({ userId: user.userId });
                req.log.debug('jwt_auth_ok');
                return next();
            } catch (err) {
                // Never log the token itself
                req.log.warn({ err: err.message }, 'jwt_verify_failed');
                return res.status(401).json({ error: 'Invalid or expired JWT' });
            }
        }

        // ── Attempt 2: x-api-key header (legacy) ─────────────────────────────
        const key = req.headers['x-api-key'];
        const isValid = key && API_KEY &&
            key.length === API_KEY.length &&
            crypto.timingSafeEqual(Buffer.from(key), Buffer.from(API_KEY));
        if (isValid) {
            return next();
        }

        // ── Nothing matched → 401 ────────────────────────────────────────────
        return res.status(401).json({ error: 'Invalid or missing API key' });
    });

    logger.info(`Auth middleware: x-api-key enabled, JWT ${JWT_AVAILABLE ? 'enabled' : 'disabled (no SUPABASE_URL)'}`);

    // Mount BAZA CRM routes (handles its own auth via bazaAuth middleware)
    if (process.env.ENABLE_BAZA === 'true') {
        app.use('/baza/api', bazaRouter);
        logger.info('BAZA CRM mounted at /baza/api');
    } else {
        logger.info('BAZA CRM disabled (set ENABLE_BAZA=true to enable)');
    }

    setupRoutes(app);
    setupWebSocket(httpServer);

    // Error-capture middleware — MUST be last. Express delegates to it only
    // when a route/middleware calls next(err) or throws in an async handler.
    // Our handler logs the error with request context, then delegates to
    // Express's default 500 response (we don't override the response shape).
    app.use(expressErrorHandler);

    httpServer.listen(config.port, () => {
        logger.info(`API server listening on port ${config.port}`);
    });

    return { server: httpServer, app };
}
