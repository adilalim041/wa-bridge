import { Router } from 'express';
import multer from 'multer';
import { supabase, queryOne, updateRows, rpc } from '../database.js';
import taskEngine from '../services/taskEngine.js';
import { ok, okList, fail } from '../utils/apiResponse.js';

const router = Router();

// Use memoryStorage since Railway has ephemeral disk
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
});

async function ensureTaskExists(id) {
  return queryOne('baza_tasks', { filters: { id } });
}

function canMutateTask(task, user) {
  if (!task || !user) return false;
  if (user.role === 'admin') return true;
  if (user.role === 'manager' && task.manager_username === user.username) return true;
  return false;
}

router.get('/', async (req, res) => {
  try {
    const managerUsername = req.user?.role === 'manager' ? req.user.username : null;
    const items = await rpc('baza_tasks_list', { p_manager_username: managerUsername });
    return okList(res, items, { total: items.length });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/summary', async (req, res) => {
  try {
    let baseQuery = supabase
      .from('baza_tasks')
      .select('priority, type', { count: 'exact' })
      .eq('status', 'OPEN');

    if (req.user?.role === 'manager') {
      baseQuery = baseQuery.eq('manager_username', req.user.username);
    }

    const { data: openTasks, count: totalOpen, error } = await baseQuery;
    if (error) throw error;

    const byPriority = { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0 };
    const byType = {};

    for (const row of (openTasks || [])) {
      const pKey = String(row.priority || '').toUpperCase();
      if (pKey in byPriority) byPriority[pKey] += 1;

      const tKey = String(row.type || '').toUpperCase();
      byType[tKey] = (byType[tKey] || 0) + 1;
    }

    return ok(res, { item: { totalOpen: Number(totalOpen || 0), byPriority, byType } });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/:id/complete', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const task = await ensureTaskExists(id);
    if (!task) return fail(res, 'NOT_FOUND', 'Task not found', undefined, 404);

    if (!canMutateTask(task, req.user)) {
      return fail(res, 'FORBIDDEN', 'No access to this task', undefined, 403);
    }
    if (!task.proof_file) {
      return fail(res, 'VALIDATION_ERROR', 'Upload proof first');
    }

    const note = req.body?.note !== undefined ? String(req.body.note || '').trim() : task.note;
    const completedAt = new Date().toISOString();

    await updateRows(
      'baza_tasks',
      { status: 'CLOSED', note: note || null, completed_at: completedAt },
      { id },
    );

    const updated = await ensureTaskExists(id);
    return ok(res, { item: updated });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/:id/proof', upload.single('file'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    const task = await ensureTaskExists(id);
    if (!task) return fail(res, 'NOT_FOUND', 'Task not found', undefined, 404);

    if (!canMutateTask(task, req.user)) {
      return fail(res, 'FORBIDDEN', 'No access to this task', undefined, 403);
    }
    if (!req.file) {
      return fail(res, 'VALIDATION_ERROR', 'Proof file is required');
    }

    // TODO: migrate to Cloudinary — store proof file in cloud storage
    // For now, store a placeholder path since Railway disk is ephemeral
    const proofFile = `memory://${req.file.originalname}`;
    await updateRows('baza_tasks', { proof_file: proofFile }, { id });

    const updated = await ensureTaskExists(id);
    return ok(res, { item: updated });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/run', async (req, res) => {
  try {
    if (req.user?.role !== 'admin') {
      return fail(res, 'FORBIDDEN', 'Only admin can run task engine', undefined, 403);
    }
    if (process.env.BAZA_TASK_ENGINE_ENABLED !== 'true') {
      return fail(res, 'SERVICE_UNAVAILABLE', 'Task engine is disabled', undefined, 503);
    }
    const result = await taskEngine();
    return ok(res, { result });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

export default router;
