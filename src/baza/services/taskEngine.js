import { supabase, queryAll, queryOne, insertRow, updateRows } from '../database.js';
import { computePartnerMetrics } from './partnerMetricsService.js';

const DAY_MS = 24 * 60 * 60 * 1000;

// Matrix of task rules based on Tier x Activity.
const TASK_RULES = {
  Sleeping: {
    Gold: { type: 'VISIT', priority: 'HIGH' },
    Silver: { type: 'CALL', priority: 'HIGH' },
    Bronze: { type: 'CALL', priority: 'MEDIUM' },
  },
  'At-risk': {
    Gold: { type: 'CALL', priority: 'HIGH' },
    Silver: { type: 'CALL', priority: 'MEDIUM' },
    Bronze: { type: 'FOLLOW_UP', priority: 'LOW' },
  },
  Active: {
    Gold: { type: 'RELATIONSHIP', priority: 'LOW', intervalDays: 60 },
    Silver: { type: 'CHECK_IN', priority: 'LOW', intervalDays: 90 },
    Bronze: null,
  },
};

function normalizeYmd(value) {
  const str = String(value || '').trim();
  if (!str) return null;
  return str.slice(0, 10);
}

function daysSinceIso(iso, now = new Date()) {
  if (!iso) return Number.POSITIVE_INFINITY;
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return Number.POSITIVE_INFINITY;
  return Math.floor((now.getTime() - t) / DAY_MS);
}

async function getAssignableManagers() {
  const { data, error } = await supabase
    .from('baza_users')
    .select('username, role')
    .eq('active', true)
    .in('role', ['manager', 'admin'])
    .order('role')
    .order('username');
  if (error) throw error;
  // Managers first, then admins
  return (data || []).sort((a, b) => {
    if (a.role === 'manager' && b.role !== 'manager') return -1;
    if (a.role !== 'manager' && b.role === 'manager') return 1;
    return a.username.localeCompare(b.username);
  });
}

function resolveManagerUsername(partner, assignableManagers) {
  if (!assignableManagers.length) return null;
  const activeNames = new Set(assignableManagers.map((row) => row.username));

  if (partner.manager_username && activeNames.has(partner.manager_username)) {
    return partner.manager_username;
  }

  const stableIndex = Math.max(0, (Number(partner.id) || 1) - 1) % assignableManagers.length;
  return assignableManagers[stableIndex].username;
}

async function listOpenAutoTasks(partnerId) {
  const { data, error } = await supabase
    .from('baza_tasks')
    .select('id, manager_username, type, priority, status, created_at')
    .eq('designer_id', partnerId)
    .eq('status', 'OPEN')
    .eq('auto_generated', true);
  if (error) throw error;
  return data || [];
}

async function getLatestAutoTaskByType(partnerId, type) {
  const { data, error } = await supabase
    .from('baza_tasks')
    .select('id, status, created_at')
    .eq('designer_id', partnerId)
    .eq('auto_generated', true)
    .eq('type', type)
    .order('created_at', { ascending: false })
    .order('id', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data;
}

async function createTask({ partnerId, managerUsername, type, priority, note, nowIso }) {
  const row = await insertRow('baza_tasks', {
    designer_id: partnerId,
    manager_username: managerUsername,
    type,
    priority,
    status: 'OPEN',
    due_date: null,
    created_at: nowIso,
    completed_at: null,
    note,
    proof_file: null,
    auto_generated: true,
  });
  return row.id;
}

async function closeTaskAsSuperseded(taskId, nowIso, reason) {
  await updateRows('baza_tasks', {
    status: 'CLOSED',
    completed_at: nowIso,
    note: reason,
  }, { id: taskId });
}

function buildAutoNote(metrics) {
  const lastOrder = normalizeYmd(metrics.last_order_date) || 'нет заказов';
  return `Автозадача: ${metrics.activity_status}, Tier ${metrics.tier}, ${metrics.recency_days} дней с последнего заказа (${lastOrder})`;
}

function resolveRule(metrics) {
  return TASK_RULES?.[metrics.activity_status]?.[metrics.tier] ?? null;
}

export default async function taskEngine({ now = new Date() } = {}) {
  const metrics = await computePartnerMetrics({ asOf: now });
  const partners = await queryAll('baza_partners', { select: 'id, manager_username' });
  const partnerMap = new Map(partners.map((p) => [Number(p.id), p]));

  const assignableManagers = await getAssignableManagers();
  const nowIso = now.toISOString();

  let created = 0;
  let keptExisting = 0;
  let skippedNoManager = 0;
  let skippedBySchedule = 0;
  let reassignedManagers = 0;
  let closedSuperseded = 0;

  for (const m of metrics.items) {
    const partner = partnerMap.get(Number(m.partner_id));
    if (!partner) continue;

    const managerUsername = resolveManagerUsername(partner, assignableManagers);
    if (!managerUsername) {
      skippedNoManager += 1;
      continue;
    }

    if (managerUsername !== partner.manager_username) {
      await updateRows('baza_partners', { manager_username: managerUsername }, { id: partner.id });
      reassignedManagers += 1;
    }

    const rule = resolveRule(m);
    const openAutoTasks = await listOpenAutoTasks(partner.id);

    if (!rule) {
      for (const task of openAutoTasks) {
        await closeTaskAsSuperseded(task.id, nowIso, 'Автозадача закрыта: неактуальна по текущему Tier/Activity');
        closedSuperseded += 1;
      }
      continue;
    }

    const reusableTask = openAutoTasks.find((task) => task.type === rule.type && task.priority === rule.priority);

    if (reusableTask) {
      keptExisting += 1;
      if (reusableTask.manager_username !== managerUsername) {
        await updateRows('baza_tasks', { manager_username: managerUsername }, { id: reusableTask.id });
      }

      for (const task of openAutoTasks) {
        if (task.id === reusableTask.id) continue;
        await closeTaskAsSuperseded(task.id, nowIso, 'Автозадача закрыта: заменена более актуальной');
        closedSuperseded += 1;
      }
      continue;
    }

    if (rule.intervalDays) {
      const latestSameType = await getLatestAutoTaskByType(partner.id, rule.type);
      if (latestSameType && daysSinceIso(latestSameType.created_at, now) < rule.intervalDays) {
        skippedBySchedule += 1;
        continue;
      }
    }

    await createTask({
      partnerId: partner.id,
      managerUsername,
      type: rule.type,
      priority: rule.priority,
      note: buildAutoNote(m),
      nowIso,
    });
    created += 1;

    for (const task of openAutoTasks) {
      await closeTaskAsSuperseded(task.id, nowIso, 'Автозадача закрыта: заменена новой задачей по матрице');
      closedSuperseded += 1;
    }
  }

  return {
    checkedPartners: metrics.items.length,
    created,
    keptExisting,
    skippedNoManager,
    skippedBySchedule,
    reassignedManagers,
    closedSuperseded,
  };
}

export { TASK_RULES };
