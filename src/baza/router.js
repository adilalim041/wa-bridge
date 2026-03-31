import { Router } from 'express';
import bazaAuth from './middleware/bazaAuth.js';
import requireAdminForMutations from './middleware/requireAdminForMutations.js';
import partnersRouter from './routes/partners.js';
import analyticsRouter from './routes/analytics.js';
import importRouter from './routes/import.js';
import studiosRouter from './routes/studios.js';
import salesRouter from './routes/sales.js';
import meRouter from './routes/me.js';
import tasksRouter from './routes/tasks.js';

const router = Router();

// BAZA auth middleware (API key or Basic Auth)
router.use(bazaAuth);
router.use(requireAdminForMutations);

// Mount BAZA routes
router.use('/me', meRouter);
router.use('/partners', partnersRouter);
router.use('/analytics', analyticsRouter);
router.use('/import', importRouter);
router.use('/studios', studiosRouter);
router.use('/sales', salesRouter);
router.use('/tasks', tasksRouter);

export default router;
