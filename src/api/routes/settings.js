/**
 * Settings routes — tenant_settings, funnel_stages, managers, message_templates.
 *
 * Extracted from src/api/routes.js as part of Phase 1 C-2 split (2026-05-05).
 * Audit ref: ObsidianVault/projects/omoikiri/audits/2026-05-05-post-fixes-audit.md
 *
 * NO behavior changes — routes are mounted on the same Express router used by
 * setupRoutes(), so middleware/auth ordering is preserved exactly.
 *
 * Mount via mountSettingsRoutes(router, deps) inside setupRoutes() in routes.js.
 */
import { z } from 'zod';

// ---------------------------------------------------------------------------
// Zod schemas for tenant_settings + funnel_stages endpoints
// ---------------------------------------------------------------------------

const labelArray = z
  .array(z.string().min(1).max(30))
  .max(30);

// Longer label array for lead_sources / refusal_reasons / task_types
const labelArrayLong = z
  .array(z.string().min(1).max(60))
  .max(50);

// Showroom shape inside company_profile
const ShowroomSchema = z.object({
  city:    z.string().min(1).max(80),
  address: z.string().min(1).max(300),
  hours:   z.string().max(100).optional(),
});

// company_profile — all fields optional so partial updates work
const CompanyProfileSchema = z.object({
  name:          z.string().max(100).optional(),
  description:   z.string().max(2000).optional(),
  website:       z.string().url().max(300).optional().or(z.literal('')),
  phone:         z.string().max(40).optional(),
  email:         z.string().email().max(200).optional().or(z.literal('')),
  working_hours: z.string().max(200).optional(),
  showrooms:     z.array(ShowroomSchema).max(10).optional(),
}).strict();

const TenantSettingsPutSchema = z.object({
  roles:           labelArray.optional(),
  cities:          labelArray.optional(),
  tags:            labelArray.optional(),
  lead_sources:    labelArrayLong.optional(),
  refusal_reasons: labelArrayLong.optional(),
  task_types:      labelArrayLong.optional(),
  company_profile: CompanyProfileSchema.optional(),
}).refine(
  (body) =>
    body.roles !== undefined ||
    body.cities !== undefined ||
    body.tags !== undefined ||
    body.lead_sources !== undefined ||
    body.refusal_reasons !== undefined ||
    body.task_types !== undefined ||
    body.company_profile !== undefined,
  { message: 'At least one settings field is required' }
);

// ---------------------------------------------------------------------------
// Zod schemas for /settings/managers
// ---------------------------------------------------------------------------

const ManagerCreateSchema = z.object({
  name:        z.string().min(1).max(100),
  email:       z.string().email().max(200).optional().or(z.literal('')),
  phone:       z.string().max(40).optional(),
  session_ids: z.array(z.string().min(1).max(80)).max(20).optional(),
  notes:       z.string().max(1000).optional(),
});

const ManagerPatchSchema = z.object({
  name:        z.string().min(1).max(100).optional(),
  email:       z.string().email().max(200).optional().or(z.literal('')),
  phone:       z.string().max(40).optional(),
  session_ids: z.array(z.string().min(1).max(80)).max(20).optional(),
  is_active:   z.boolean().optional(),
  notes:       z.string().max(1000).optional(),
}).refine(
  (b) =>
    b.name !== undefined ||
    b.email !== undefined ||
    b.phone !== undefined ||
    b.session_ids !== undefined ||
    b.is_active !== undefined ||
    b.notes !== undefined,
  { message: 'At least one field is required' }
);

const ReorderSchema = z.object({
  ids: z.array(z.string().uuid()).min(1).max(200),
});

// ---------------------------------------------------------------------------
// Zod schemas for /settings/templates
// ---------------------------------------------------------------------------

const TemplateCreateSchema = z.object({
  title:    z.string().min(1).max(100),
  body:     z.string().min(1).max(4000),
  category: z.string().min(1).max(60).optional(),
});

const TemplatePatchSchema = z.object({
  title:    z.string().min(1).max(100).optional(),
  body:     z.string().min(1).max(4000).optional(),
  category: z.string().min(1).max(60).optional(),
}).refine(
  (b) => b.title !== undefined || b.body !== undefined || b.category !== undefined,
  { message: 'At least one field (title, body, category) is required' }
);

const hexColorField = z
  .string()
  .regex(/^#[0-9a-fA-F]{6}$/, 'color must be a 6-digit hex value, e.g. #3b82f6');

const FunnelStageCreateSchema = z.object({
  name:     z.string().min(1).max(40),
  color:    hexColorField.optional(),
  is_final: z.boolean().optional(),
});

const FunnelStagePatchSchema = z.object({
  name:     z.string().min(1).max(40).optional(),
  color:    hexColorField.optional(),
  is_final: z.boolean().optional(),
}).refine(
  (body) => body.name !== undefined || body.color !== undefined || body.is_final !== undefined,
  { message: 'At least one field (name, color, is_final) is required' }
);

const FunnelStageReorderSchema = z.object({
  order: z.array(z.string().uuid()).min(1).max(200),
});

/** Format Zod error issues into a human-readable message. */
function zodErrorMessage(err) {
  return err.issues.map((i) => `${i.path.join('.') || 'body'}: ${i.message}`).join('; ');
}

/**
 * Mounts all /settings/* and /funnel/stages/* endpoints onto the given Express router.
 *
 * @param {import('express').Router} router
 * @param {{
 *   supabase: any,
 *   logger: any,
 *   getTenantSettings: Function,
 *   upsertTenantSettings: Function,
 *   getFunnelStages: Function,
 *   createFunnelStage: Function,
 *   updateFunnelStage: Function,
 *   deleteFunnelStage: Function,
 *   reorderFunnelStages: Function,
 * }} deps
 */
export function mountSettingsRoutes(router, deps) {
  const {
    supabase,
    logger,
    getTenantSettings,
    upsertTenantSettings,
    getFunnelStages,
    createFunnelStage,
    updateFunnelStage,
    deleteFunnelStage,
    reorderFunnelStages,
  } = deps;

  // ==========================================================================
  // Settings: tenant_settings
  // ==========================================================================

  /**
   * GET /settings/tenant
   * Returns { roles, cities, tags } for the authenticated tenant.
   * If no row exists yet, returns the schema defaults and does NOT insert.
   */
  router.get('/settings/tenant', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      let settings = await getTenantSettings(userId, db);

      if (!settings) {
        // First visit — return schema defaults without writing to DB.
        // The first PUT will create the row.
        settings = {
          roles:           ['клиент', 'партнёр', 'менеджер', 'другое'],
          cities:          [],
          tags:            [],
          lead_sources:    [],
          refusal_reasons: [],
          task_types:      [],
          company_profile: {},
        };
      } else {
        // Back-fill new columns for rows created before 0008 migration
        settings.lead_sources    = settings.lead_sources    ?? [];
        settings.refusal_reasons = settings.refusal_reasons ?? [];
        settings.task_types      = settings.task_types      ?? [];
        settings.company_profile = settings.company_profile ?? {};
      }

      return res.json(settings);
    } catch (err) {
      logger.error({ err, userId }, 'GET /settings/tenant failed');
      return res.status(500).json({ error: 'Failed to fetch tenant settings' });
    }
  });

  /**
   * PUT /settings/tenant
   * Body: { roles?, cities?, tags? }  — partial update, merges with existing values.
   * Upserts the row (INSERT on first call, UPDATE on subsequent calls).
   */
  router.put('/settings/tenant', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = TenantSettingsPutSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const result = await upsertTenantSettings(userId, parsed.data, db);
      if (!result) {
        return res.status(500).json({ error: 'Failed to save tenant settings' });
      }
      return res.json(result);
    } catch (err) {
      logger.error({ err, userId }, 'PUT /settings/tenant failed');
      return res.status(500).json({ error: 'Failed to save tenant settings' });
    }
  });

  // ==========================================================================
  // Funnel stages
  // ==========================================================================

  /**
   * GET /funnel/stages
   * Returns ordered list of funnel stages for the tenant.
   * If no stages exist, seeds one default "Новый лид" stage and returns it.
   */
  router.get('/funnel/stages', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      let stages = await getFunnelStages(userId, db);

      if (stages.length === 0) {
        // Seed default stage for new tenants so UI is never blank
        const defaultStage = await createFunnelStage(
          userId,
          { name: 'Новый лид', color: '#3b82f6', is_final: false },
          db
        );
        stages = defaultStage ? [defaultStage] : [];
      }

      return res.json(stages);
    } catch (err) {
      logger.error({ err, userId }, 'GET /funnel/stages failed');
      return res.status(500).json({ error: 'Failed to fetch funnel stages' });
    }
  });

  /**
   * POST /funnel/stages
   * Body: { name, color?, is_final? }
   * Appends a new stage at the end (sort_order = max + 1).
   */
  router.post('/funnel/stages', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = FunnelStageCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const stage = await createFunnelStage(userId, parsed.data, db);
      if (!stage) {
        return res.status(500).json({ error: 'Failed to create funnel stage' });
      }
      return res.status(201).json(stage);
    } catch (err) {
      logger.error({ err, userId }, 'POST /funnel/stages failed');
      return res.status(500).json({ error: 'Failed to create funnel stage' });
    }
  });

  /**
   * PATCH /funnel/stages/:id
   * Body: { name?, color?, is_final? }  — partial update.
   */
  router.patch('/funnel/stages/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = FunnelStagePatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const stage = await updateFunnelStage(id, userId, parsed.data, db);
      if (!stage) {
        return res.status(404).json({ error: 'Stage not found or access denied' });
      }
      return res.json(stage);
    } catch (err) {
      logger.error({ err, userId, stageId: id }, 'PATCH /funnel/stages/:id failed');
      return res.status(500).json({ error: 'Failed to update funnel stage' });
    }
  });

  /**
   * DELETE /funnel/stages/:id
   * Returns 409 if any active chat_ai deal is on this stage.
   * Returns 404 if stage not found or belongs to another tenant.
   */
  router.delete('/funnel/stages/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const result = await deleteFunnelStage(id, userId, db);

      if (result.conflict) {
        return res.status(409).json({
          error: `Cannot delete: ${result.conflictCount} active deal(s) are on this stage`,
          conflictCount: result.conflictCount,
        });
      }

      if (!result.deleted) {
        return res.status(404).json({ error: 'Stage not found or access denied' });
      }

      return res.json({ deleted: true });
    } catch (err) {
      logger.error({ err, userId, stageId: id }, 'DELETE /funnel/stages/:id failed');
      return res.status(500).json({ error: 'Failed to delete funnel stage' });
    }
  });

  /**
   * POST /funnel/stages/reorder
   * Body: { order: [id1, id2, id3, ...] }
   * Batch-updates sort_order so each stage gets its position index.
   */
  router.post('/funnel/stages/reorder', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = FunnelStageReorderSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const ok = await reorderFunnelStages(userId, parsed.data.order, db);
      if (!ok) {
        return res.status(500).json({ error: 'Failed to reorder stages' });
      }
      return res.json({ reordered: parsed.data.order.length });
    } catch (err) {
      logger.error({ err, userId }, 'POST /funnel/stages/reorder failed');
      return res.status(500).json({ error: 'Failed to reorder stages' });
    }
  });

  // ==========================================================================
  // Settings: managers
  // ==========================================================================

  /**
   * GET /settings/managers
   * Returns list of managers for the authenticated tenant, sorted by sort_order ASC,
   * then created_at ASC.
   */
  router.get('/settings/managers', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const { data, error } = await db
        .from('managers')
        .select('*')
        .eq('user_id', userId)
        .order('sort_order', { ascending: true })
        .order('created_at', { ascending: true });

      if (error) throw error;
      return res.json(data ?? []);
    } catch (err) {
      logger.error({ err, userId }, 'GET /settings/managers failed');
      return res.status(500).json({ error: 'Failed to fetch managers' });
    }
  });

  /**
   * POST /settings/managers
   * Body: { name, email?, phone?, session_ids?, notes? }
   * Creates a new manager. sort_order is set to max+1 among existing rows.
   */
  router.post('/settings/managers', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ManagerCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      // Determine next sort_order
      const { data: maxRow } = await db
        .from('managers')
        .select('sort_order')
        .eq('user_id', userId)
        .order('sort_order', { ascending: false })
        .limit(1)
        .maybeSingle();

      const sortOrder = maxRow ? maxRow.sort_order + 1 : 0;

      const { data, error } = await db
        .from('managers')
        .insert({
          user_id:     userId,
          name:        parsed.data.name,
          email:       parsed.data.email || null,
          phone:       parsed.data.phone || null,
          session_ids: parsed.data.session_ids ?? [],
          notes:       parsed.data.notes || null,
          sort_order:  sortOrder,
        })
        .select()
        .single();

      if (error) throw error;
      return res.status(201).json(data);
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/managers failed');
      return res.status(500).json({ error: 'Failed to create manager' });
    }
  });

  /**
   * POST /settings/managers/reorder
   * Body: { ids: [uuid, ...] }
   * Atomically resets sort_order so each id gets its array-index position.
   * Must be registered BEFORE PATCH /:id to avoid Express matching "reorder" as :id.
   */
  router.post('/settings/managers/reorder', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ReorderSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      // Atomic batch update: each id gets sort_order = its index position.
      // RLS ensures only the tenant's own rows can be touched.
      await Promise.all(
        parsed.data.ids.map((id, idx) =>
          db
            .from('managers')
            .update({ sort_order: idx })
            .eq('id', id)
            .eq('user_id', userId)
        )
      );
      return res.json({ reordered: parsed.data.ids.length });
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/managers/reorder failed');
      return res.status(500).json({ error: 'Failed to reorder managers' });
    }
  });

  /**
   * PATCH /settings/managers/:id
   * Body: partial manager fields.
   * Returns 404 when the row doesn't exist or belongs to another tenant.
   */
  router.patch('/settings/managers/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ManagerPatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    // Build update payload from only the provided fields
    const patch = {};
    if (parsed.data.name        !== undefined) patch.name        = parsed.data.name;
    if (parsed.data.email       !== undefined) patch.email       = parsed.data.email || null;
    if (parsed.data.phone       !== undefined) patch.phone       = parsed.data.phone || null;
    if (parsed.data.session_ids !== undefined) patch.session_ids = parsed.data.session_ids;
    if (parsed.data.is_active   !== undefined) patch.is_active   = parsed.data.is_active;
    if (parsed.data.notes       !== undefined) patch.notes       = parsed.data.notes || null;

    try {
      const { data, error } = await db
        .from('managers')
        .update(patch)
        .eq('id', id)
        .eq('user_id', userId)
        .select()
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Manager not found or access denied' });
      }
      return res.json(data);
    } catch (err) {
      logger.error({ err, userId, managerId: id }, 'PATCH /settings/managers/:id failed');
      return res.status(500).json({ error: 'Failed to update manager' });
    }
  });

  /**
   * DELETE /settings/managers/:id
   */
  router.delete('/settings/managers/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const { data, error } = await db
        .from('managers')
        .delete()
        .eq('id', id)
        .eq('user_id', userId)
        .select('id')
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Manager not found or access denied' });
      }
      return res.json({ deleted: true });
    } catch (err) {
      logger.error({ err, userId, managerId: id }, 'DELETE /settings/managers/:id failed');
      return res.status(500).json({ error: 'Failed to delete manager' });
    }
  });

  // ==========================================================================
  // Settings: message_templates
  // ==========================================================================

  /**
   * GET /settings/templates
   * Query: ?category=<category>  (optional filter)
   * Returns templates sorted by sort_order ASC, created_at ASC.
   */
  router.get('/settings/templates', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { category } = req.query;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      let query = db
        .from('message_templates')
        .select('*')
        .eq('user_id', userId)
        .order('sort_order', { ascending: true })
        .order('created_at', { ascending: true });

      if (category && typeof category === 'string') {
        query = query.eq('category', category.trim());
      }

      const { data, error } = await query;
      if (error) throw error;
      return res.json(data ?? []);
    } catch (err) {
      logger.error({ err, userId }, 'GET /settings/templates failed');
      return res.status(500).json({ error: 'Failed to fetch templates' });
    }
  });

  /**
   * POST /settings/templates
   * Body: { title, body, category? }
   */
  router.post('/settings/templates', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = TemplateCreateSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      const { data: maxRow } = await db
        .from('message_templates')
        .select('sort_order')
        .eq('user_id', userId)
        .order('sort_order', { ascending: false })
        .limit(1)
        .maybeSingle();

      const sortOrder = maxRow ? maxRow.sort_order + 1 : 0;

      const { data, error } = await db
        .from('message_templates')
        .insert({
          user_id:    userId,
          title:      parsed.data.title,
          body:       parsed.data.body,
          category:   parsed.data.category ?? 'general',
          sort_order: sortOrder,
        })
        .select()
        .single();

      if (error) throw error;
      return res.status(201).json(data);
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/templates failed');
      return res.status(500).json({ error: 'Failed to create template' });
    }
  });

  /**
   * POST /settings/templates/reorder
   * Body: { ids: [uuid, ...] }
   * Must be registered BEFORE PATCH /:id.
   */
  router.post('/settings/templates/reorder', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = ReorderSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    try {
      await Promise.all(
        parsed.data.ids.map((id, idx) =>
          db
            .from('message_templates')
            .update({ sort_order: idx })
            .eq('id', id)
            .eq('user_id', userId)
        )
      );
      return res.json({ reordered: parsed.data.ids.length });
    } catch (err) {
      logger.error({ err, userId }, 'POST /settings/templates/reorder failed');
      return res.status(500).json({ error: 'Failed to reorder templates' });
    }
  });

  /**
   * PATCH /settings/templates/:id
   * Body: partial template fields.
   */
  router.patch('/settings/templates/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    const parsed = TemplatePatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: zodErrorMessage(parsed.error) });
    }

    const patch = {};
    if (parsed.data.title    !== undefined) patch.title    = parsed.data.title;
    if (parsed.data.body     !== undefined) patch.body     = parsed.data.body;
    if (parsed.data.category !== undefined) patch.category = parsed.data.category;

    try {
      const { data, error } = await db
        .from('message_templates')
        .update(patch)
        .eq('id', id)
        .eq('user_id', userId)
        .select()
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Template not found or access denied' });
      }
      return res.json(data);
    } catch (err) {
      logger.error({ err, userId, templateId: id }, 'PATCH /settings/templates/:id failed');
      return res.status(500).json({ error: 'Failed to update template' });
    }
  });

  /**
   * DELETE /settings/templates/:id
   */
  router.delete('/settings/templates/:id', async (req, res) => {
    const db = req.userClient ?? supabase;
    const userId = req.user?.userId;
    const { id } = req.params;

    if (!userId) {
      return res.status(401).json({ error: 'User identity required' });
    }

    try {
      const { data, error } = await db
        .from('message_templates')
        .delete()
        .eq('id', id)
        .eq('user_id', userId)
        .select('id')
        .maybeSingle();

      if (error) throw error;
      if (!data) {
        return res.status(404).json({ error: 'Template not found or access denied' });
      }
      return res.json({ deleted: true });
    } catch (err) {
      logger.error({ err, userId, templateId: id }, 'DELETE /settings/templates/:id failed');
      return res.status(500).json({ error: 'Failed to delete template' });
    }
  });
}
