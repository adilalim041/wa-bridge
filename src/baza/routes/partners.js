import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { supabase, queryOne, insertRow, updateRows, deleteRows } from '../database.js';
import {
  listPartners,
  listPartnerCities,
  getPartnersStatusSummary,
  getTopPartners,
  getPartnerDetails,
  listPartnerSales,
} from '../services/partnersService.js';
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
    const asOf = new Date();
    const result = await listPartners(req.query, { asOf });
    return okList(res, result.items, result.meta);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/cities', async (req, res) => {
  try {
    const items = await listPartnerCities();
    return okList(res, items, { total: items.length });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/status-summary', async (req, res) => {
  try {
    const asOf = new Date();
    const items = await getPartnersStatusSummary({ asOf });
    return okList(res, items, { total: items.length });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/top5', async (req, res) => {
  try {
    const asOf = new Date();
    const items = await getTopPartners({ asOf });
    return okList(res, items, { total: items.length });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/:id', async (req, res) => {
  try {
    const asOf = new Date();
    const partner = await getPartnerDetails(req.params.id, { asOf });
    if (!partner) {
      return fail(res, 'NOT_FOUND', 'Partner not found', undefined, 404);
    }
    return okItem(res, partner);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/', upload.single('photo'), async (req, res) => {
  try {
    const { designer_name, phone, email, city, address, inn, status, notes, studio_id } = req.body;

    if (!designer_name || !phone || !city) {
      return fail(res, 'VALIDATION_ERROR', 'Required fields: designer_name, phone, city');
    }

    // TODO: migrate to Cloudinary — file upload temporarily stores null
    const photo_url = null;

    const newPartner = await insertRow('baza_partners', {
      designer_name,
      phone,
      email: email || null,
      city,
      address: address || null,
      inn: inn || null,
      status: status || 'active',
      photo_url,
      notes: notes || null,
      studio_id: studio_id ? Number(studio_id) : null,
    });

    return res.status(201).json({ item: newPartner });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.put('/:id', upload.single('photo'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    const existing = await queryOne('baza_partners', { filters: { id } });
    if (!existing) return fail(res, 'NOT_FOUND', 'Partner not found', undefined, 404);

    const { designer_name, phone, email, city, address, inn, status, notes, studio_id } = req.body;

    // TODO: migrate to Cloudinary — keep existing photo_url, ignore new uploads for now
    const photo_url = existing.photo_url;

    const [updated] = await updateRows(
      'baza_partners',
      {
        designer_name: designer_name || existing.designer_name,
        phone: phone || existing.phone,
        email: email !== undefined ? (email || null) : existing.email,
        city: city || existing.city,
        address: address !== undefined ? (address || null) : existing.address,
        inn: inn !== undefined ? (inn || null) : existing.inn,
        status: status || existing.status,
        photo_url,
        notes: notes !== undefined ? (notes || null) : existing.notes,
        studio_id: studio_id !== undefined ? (studio_id ? Number(studio_id) : null) : existing.studio_id,
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
    const existing = await queryOne('baza_partners', { filters: { id } });
    if (!existing) return fail(res, 'NOT_FOUND', 'Partner not found', undefined, 404);

    // TODO: migrate to Cloudinary — delete photo from cloud storage when implemented

    await deleteRows('baza_sales', { partner_id: id });
    await deleteRows('baza_partners', { id });

    return ok(res, { success: true });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/:id/sales', async (req, res) => {
  try {
    const result = await listPartnerSales(req.params.id, req.query);
    return okList(res, result.items, result.meta);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/:id/sales', async (req, res) => {
  try {
    const partnerId = Number(req.params.id);
    const { date, product, amount, comment } = req.body;

    if (!date || !product || !amount) {
      return fail(res, 'VALIDATION_ERROR', 'Required fields: date, product, amount');
    }

    const sale = await insertRow('baza_sales', {
      partner_id: partnerId,
      date,
      product,
      amount: Number(amount),
      comment: comment || null,
    });

    return res.status(201).json({ item: sale });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.put('/:id/sales/:saleId', async (req, res) => {
  try {
    const partnerId = Number(req.params.id);
    const saleId = Number(req.params.saleId);
    const { date, product, amount, comment } = req.body;

    const sale = await queryOne('baza_sales', {
      filters: { id: saleId, partner_id: partnerId },
    });
    if (!sale) return fail(res, 'NOT_FOUND', 'Sale not found', undefined, 404);

    const [updated] = await updateRows(
      'baza_sales',
      {
        date: date || sale.date,
        product: product || sale.product,
        amount: amount !== undefined ? Number(amount) : sale.amount,
        comment: comment || null,
      },
      { id: saleId, partner_id: partnerId },
    );

    return okItem(res, updated);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.delete('/:id/sales/:saleId', async (req, res) => {
  try {
    const partnerId = Number(req.params.id);
    const saleId = Number(req.params.saleId);

    const sale = await queryOne('baza_sales', {
      select: 'id',
      filters: { id: saleId, partner_id: partnerId },
    });
    if (!sale) return fail(res, 'NOT_FOUND', 'Sale not found', undefined, 404);

    await deleteRows('baza_sales', { id: saleId, partner_id: partnerId });
    return ok(res, { success: true });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.put('/:id/notes', async (req, res) => {
  try {
    const id = Number(req.params.id);
    const { notes } = req.body;
    await updateRows('baza_partners', { notes: notes || null }, { id });
    return ok(res, { success: true });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/:id/merge', async (req, res) => {
  try {
    const sourcePartnerId = Number(req.params.id);
    const targetPartnerId = Number(req.body?.targetPartnerId);

    if (!Number.isInteger(targetPartnerId) || targetPartnerId <= 0) {
      return fail(res, 'VALIDATION_ERROR', 'targetPartnerId is required');
    }
    if (sourcePartnerId === targetPartnerId) {
      return fail(res, 'VALIDATION_ERROR', 'Cannot merge partner with itself');
    }

    const source = await queryOne('baza_partners', { filters: { id: sourcePartnerId } });
    const target = await queryOne('baza_partners', { filters: { id: targetPartnerId } });
    if (!source) return fail(res, 'NOT_FOUND', 'Source partner not found', undefined, 404);
    if (!target) return fail(res, 'NOT_FOUND', 'Target partner not found', undefined, 404);

    const { count: movedOrders } = await supabase
      .from('baza_sales')
      .select('*', { count: 'exact', head: true })
      .eq('partner_id', sourcePartnerId);

    await updateRows('baza_sales', { partner_id: targetPartnerId }, { partner_id: sourcePartnerId });

    const mergedNotes = [target.notes, source.notes]
      .map((v) => String(v || '').trim())
      .filter(Boolean)
      .join('\n\n');

    if (mergedNotes && mergedNotes !== String(target.notes || '')) {
      await updateRows('baza_partners', { notes: mergedNotes }, { id: targetPartnerId });
    }

    await deleteRows('baza_partners', { id: sourcePartnerId });

    // TODO: migrate to Cloudinary — delete source photo from cloud storage when implemented

    return okItem(res, {
      sourcePartnerId,
      sourceDesignerName: source.designer_name,
      targetPartnerId,
      targetDesignerName: target.designer_name,
      movedOrders: Number(movedOrders || 0),
    });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

export default router;
