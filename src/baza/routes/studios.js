import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { supabase, queryOne, insertRow, updateRows, deleteRows } from '../database.js';
import { listStudios, getStudioDetails } from '../services/studiosService.js';
import { okItem, okList, ok, fail } from '../utils/apiResponse.js';

const router = Router();

// Use memoryStorage since Railway has ephemeral disk
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = /jpeg|jpg|png|webp/;
    const ext = allowed.test(path.extname(file.originalname).toLowerCase());
    const mime = allowed.test(file.mimetype);
    cb(null, ext && mime);
  }
});

router.get('/', async (req, res) => {
  try {
    const result = await listStudios();
    return okList(res, result.items, result.meta);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/:id', async (req, res) => {
  try {
    const item = await getStudioDetails(req.params.id);
    if (!item) return fail(res, 'NOT_FOUND', 'Studio not found', undefined, 404);
    return okItem(res, item);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/', upload.single('logo'), async (req, res) => {
  try {
    const { name, city, description } = req.body;
    if (!name || !city) {
      return fail(res, 'VALIDATION_ERROR', 'Required fields: name, city');
    }

    // TODO: migrate to Cloudinary — file upload temporarily stores null
    const logo_url = null;

    const created = await insertRow('baza_studios', {
      name,
      city,
      logo_url,
      description: description || null,
    });

    return res.status(201).json({ item: created });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.put('/:id', upload.single('logo'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    const existing = await queryOne('baza_studios', { filters: { id } });
    if (!existing) return fail(res, 'NOT_FOUND', 'Studio not found', undefined, 404);

    const { name, city, description } = req.body;

    // TODO: migrate to Cloudinary — keep existing logo_url, ignore new uploads for now
    const logo_url = existing.logo_url;

    const [updated] = await updateRows(
      'baza_studios',
      {
        name: name || existing.name,
        city: city || existing.city,
        logo_url,
        description: description !== undefined ? (description || null) : existing.description,
      },
      { id },
    );

    return okItem(res, updated);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.delete('/:id', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const existing = await queryOne('baza_studios', { filters: { id } });
    if (!existing) return fail(res, 'NOT_FOUND', 'Studio not found', undefined, 404);

    await updateRows('baza_partners', { studio_id: null }, { studio_id: id });
    await deleteRows('baza_studios', { id });

    // TODO: migrate to Cloudinary — delete logo from cloud storage when implemented

    return ok(res, { success: true });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/:id/merge', async (req, res) => {
  try {
    const sourceStudioId = Number(req.params.id);
    const targetStudioId = Number(req.body?.targetStudioId);

    if (!Number.isInteger(targetStudioId) || targetStudioId <= 0) {
      return fail(res, 'VALIDATION_ERROR', 'targetStudioId is required');
    }
    if (sourceStudioId === targetStudioId) {
      return fail(res, 'VALIDATION_ERROR', 'Cannot merge studio with itself');
    }

    const source = await queryOne('baza_studios', { filters: { id: sourceStudioId } });
    const target = await queryOne('baza_studios', { filters: { id: targetStudioId } });
    if (!source) return fail(res, 'NOT_FOUND', 'Source studio not found', undefined, 404);
    if (!target) return fail(res, 'NOT_FOUND', 'Target studio not found', undefined, 404);

    const { count: movedPartners } = await supabase
      .from('baza_partners')
      .select('*', { count: 'exact', head: true })
      .eq('studio_id', sourceStudioId);

    // Count orders belonging to source studio partners (via join emulation)
    const sourcePartners = await supabase
      .from('baza_partners')
      .select('id')
      .eq('studio_id', sourceStudioId);
    const sourcePartnerIds = (sourcePartners.data || []).map((p) => p.id);

    let movedOrders = 0;
    if (sourcePartnerIds.length) {
      const { count } = await supabase
        .from('baza_sales')
        .select('*', { count: 'exact', head: true })
        .in('partner_id', sourcePartnerIds);
      movedOrders = Number(count || 0);
    }

    // Count orders for target studio before merge
    const targetPartnersBefore = await supabase
      .from('baza_partners')
      .select('id')
      .eq('studio_id', targetStudioId);
    const targetPartnerIdsBefore = (targetPartnersBefore.data || []).map((p) => p.id);

    let targetOrdersBefore = 0;
    if (targetPartnerIdsBefore.length) {
      const { count } = await supabase
        .from('baza_sales')
        .select('*', { count: 'exact', head: true })
        .in('partner_id', targetPartnerIdsBefore);
      targetOrdersBefore = Number(count || 0);
    }

    // Move partners from source to target studio
    await updateRows('baza_partners', { studio_id: targetStudioId }, { studio_id: sourceStudioId });
    await deleteRows('baza_studios', { id: sourceStudioId });

    // TODO: migrate to Cloudinary — delete source logo from cloud storage when implemented

    // Count orders for target studio after merge
    const targetPartnersAfter = await supabase
      .from('baza_partners')
      .select('id')
      .eq('studio_id', targetStudioId);
    const targetPartnerIdsAfter = (targetPartnersAfter.data || []).map((p) => p.id);

    let targetOrdersAfter = 0;
    if (targetPartnerIdsAfter.length) {
      const { count } = await supabase
        .from('baza_sales')
        .select('*', { count: 'exact', head: true })
        .in('partner_id', targetPartnerIdsAfter);
      targetOrdersAfter = Number(count || 0);
    }

    return okItem(res, {
      sourceStudioId,
      sourceStudioName: source.name,
      targetStudioId,
      targetStudioName: target.name,
      movedPartners: Number(movedPartners || 0),
      movedOrders,
      targetOrdersBefore,
      targetOrdersAfter,
    });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

export default router;
