import { createHash } from 'crypto';
import { Router } from 'express';
import multer from 'multer';
import XLSX from 'xlsx';
import { supabase, queryAll, queryOne, insertRow, updateRows } from '../database.js';
import { fail } from '../utils/apiResponse.js';
import { importConfirmPayloadSchema, importPreviewPayloadSchema } from '../validators/importSchemas.js';

const router = Router();
const uploadFile = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } });

const TEMPLATE_HEADERS = [
  '\u0421\u0442\u0443\u0434\u0438\u044f',
  '\u0414\u0438\u0437\u0430\u0439\u043D\u0435\u0440 (\u0424\u0418\u041E)',
  '\u0422\u0435\u043B\u0435\u0444\u043E\u043D',
  '\u0414\u0430\u0442\u0430 \u0437\u0430\u043A\u0430\u0437\u0430',
  '\u041A\u043E\u043C\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439',
  '\u0421\u0443\u043C\u043C\u0430',
  '\u0413\u043E\u0440\u043E\u0434',
];

const ERR_DESIGNER_REQUIRED = 'ERR_DESIGNER_REQUIRED';
const ERR_PHONE_REQUIRED = 'ERR_PHONE_REQUIRED';
const ERR_DATE_INVALID = 'ERR_DATE_INVALID';
const ERR_AMOUNT_INVALID = 'ERR_AMOUNT_INVALID';
const ERR_CITY_REQUIRED = 'ERR_CITY_REQUIRED';
const ERR_STUDIO_CONFLICT_CONFIRM = 'ERR_STUDIO_CONFLICT_CONFIRM';

const normalizeSpaces = (value) => String(value || '').replace(/\s+/g, ' ').trim();
const normalizeStudioKeyRaw = (value) => normalizeSpaces(value).toLowerCase();
const normalizeDesignerKey = (value) => normalizeSpaces(value).toLowerCase();
const extractDigits = (value) => String(value || '').replace(/\D/g, '');

const CYR_TO_LAT = {
  а: 'a', б: 'b', в: 'v', г: 'g', д: 'd', е: 'e', ё: 'e', ж: 'zh', з: 'z', и: 'i', й: 'i',
  к: 'k', л: 'l', м: 'm', н: 'n', о: 'o', п: 'p', р: 'r', с: 's', т: 't', у: 'u', ф: 'f',
  х: 'h', ц: 'c', ч: 'ch', ш: 'sh', щ: 'sch', ъ: '', ы: 'y', ь: '', э: 'e', ю: 'yu', я: 'ya',
};

const STUDIO_STOP_WORDS = new Set([
  'studio', 'studiia', 'studiya', 'studia', 'design', 'dizain', 'dizayn', 'dizajn',
  'interior', 'interiors', 'bureau', 'buro', 'atelier', 'group', 'company', 'co', 'llp',
  'too', 'ip', 'ooo', 'brand', 'home',
]);

function transliterateToLatin(value) {
  const input = String(value || '').toLowerCase();
  let out = '';
  for (const ch of input) {
    out += Object.prototype.hasOwnProperty.call(CYR_TO_LAT, ch) ? CYR_TO_LAT[ch] : ch;
  }
  return out;
}

function normalizeStudioToken(token) {
  return String(token || '')
    .toLowerCase()
    .replace(/y/g, 'i')
    .replace(/j/g, 'i')
    .replace(/w/g, 'v')
    .replace(/ph/g, 'f');
}

function buildStudioMatchKey(value) {
  const base = transliterateToLatin(normalizeSpaces(value))
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

  if (!base) return '';

  const tokens = base
    .split(' ')
    .map(normalizeStudioToken)
    .filter(Boolean)
    .filter((token) => !STUDIO_STOP_WORDS.has(token));

  if (!tokens.length) {
    return base.replace(/\s+/g, '');
  }

  return tokens.join(' ');
}

function sameStudioAlias(nameA, nameB) {
  const keyA = buildStudioMatchKey(nameA);
  const keyB = buildStudioMatchKey(nameB);
  if (keyA && keyB && keyA === keyB) return true;

  const rawA = normalizeStudioKeyRaw(nameA);
  const rawB = normalizeStudioKeyRaw(nameB);
  return rawA && rawB && rawA === rawB;
}

const getPhoneTail7 = (value) => {
  const digits = extractDigits(value);
  if (digits.length < 7) return '';
  return digits.slice(-7);
};

const normalizePhoneForStorage = (value) => {
  const digitsRaw = extractDigits(value);
  if (!digitsRaw) return '';

  let digits = digitsRaw;

  if (digits.length === 10) {
    digits = `7${digits}`;
  } else if (digits.length === 11 && digits.startsWith('8')) {
    digits = `7${digits.slice(1)}`;
  }

  if (digits.length > 11 && (digits.startsWith('7') || digits.startsWith('8'))) {
    digits = `7${digits.slice(-10)}`;
  }

  if (digits.length === 11 && digits.startsWith('7')) {
    return `+${digits}`;
  }

  if (digits.length >= 7) {
    return `+${digits}`;
  }

  return '';
};

const parseOrderDate = (value) => {
  if (!value && value !== 0) return '';

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }

  if (typeof value === 'number') {
    const parsed = XLSX.SSF.parse_date_code(value);
    if (parsed?.y && parsed?.m && parsed?.d) {
      const mm = String(parsed.m).padStart(2, '0');
      const dd = String(parsed.d).padStart(2, '0');
      return `${parsed.y}-${mm}-${dd}`;
    }
  }

  const str = normalizeSpaces(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;

  const ddmmyyyy = str.match(/^(\d{2})[./-](\d{2})[./-](\d{4})$/);
  if (ddmmyyyy) {
    const [, dd, mm, yyyy] = ddmmyyyy;
    return `${yyyy}-${mm}-${dd}`;
  }

  return '';
};

const parseAmount = (value) => {
  if (typeof value === 'number') return value;
  if (value === null || value === undefined) return NaN;

  const raw = String(value).trim();
  if (!raw) return NaN;

  const cleaned = raw.replace(/\s/g, '').replace(/,/g, '');
  const number = Number(cleaned);
  if (Number.isFinite(number)) return number;

  const alt = Number(raw.replace(/\s/g, '').replace(',', '.'));
  return alt;
};

const makeOrderImportKey = ({ partnerId, studioId, date, amount, city, comment }) => {
  const payload = [
    String(partnerId || ''),
    String(studioId || ''),
    String(date || ''),
    Number(amount || 0).toFixed(2),
    normalizeSpaces(city).toLowerCase(),
    normalizeSpaces(comment).toLowerCase(),
  ].join('|');

  return createHash('sha1').update(payload).digest('hex');
};

function buildStudioIndexes(studios) {
  const byMatch = new Map();
  const byRaw = new Map();
  const byId = new Map();

  for (const studio of studios) {
    const ref = {
      id: Number(studio.id),
      name: studio.name,
      city: studio.city || null,
      source: studio.source || 'existing',
      createdRow: studio.createdRow || null,
    };

    byId.set(ref.id, ref);
    byId.set(String(ref.id), ref);

    const matchKey = buildStudioMatchKey(ref.name);
    if (matchKey && !byMatch.has(matchKey)) {
      byMatch.set(matchKey, ref);
    }

    const rawKey = normalizeStudioKeyRaw(ref.name);
    if (rawKey && !byRaw.has(rawKey)) {
      byRaw.set(rawKey, ref);
    }
  }

  return { byMatch, byRaw, byId };
}

function findStudioInIndexes(studioName, indexes) {
  const matchKey = buildStudioMatchKey(studioName);
  if (matchKey && indexes.byMatch.has(matchKey)) {
    return indexes.byMatch.get(matchKey);
  }

  const rawKey = normalizeStudioKeyRaw(studioName);
  if (rawKey && indexes.byRaw.has(rawKey)) {
    return indexes.byRaw.get(rawKey);
  }

  return null;
}

const findStudioByName = async (studioName) => {
  if (!normalizeSpaces(studioName)) return null;
  const studios = await queryAll('baza_studios', { select: 'id, name, city' });
  const indexes = buildStudioIndexes(studios);
  return findStudioInIndexes(studioName, indexes);
};

const findPartnerInListByPhoneAndName = (partners, phoneOrRaw, designerName) => {
  const tail7 = getPhoneTail7(phoneOrRaw);
  if (!tail7) return null;

  const nameKey = normalizeDesignerKey(designerName);
  const candidates = partners.filter((partner) => getPhoneTail7(partner.phone) === tail7);
  if (!candidates.length) return null;

  const exactByName = candidates.find((partner) => normalizeDesignerKey(partner.designer_name) === nameKey);
  if (exactByName) return exactByName;

  if (nameKey) {
    const softByName = candidates.find((partner) => {
      const partnerName = normalizeDesignerKey(partner.designer_name);
      return partnerName.includes(nameKey) || nameKey.includes(partnerName);
    });
    if (softByName) return softByName;
  }

  return candidates[0];
};

const findPartnerByPhoneAndName = async (phoneOrRaw, designerName) => {
  const partners = await queryAll('baza_partners', {
    select: 'id, designer_name, phone, city, studio_id',
  });
  return findPartnerInListByPhoneAndName(partners, phoneOrRaw, designerName);
};

function mapRowsFromSheet(matrix) {
  const rows = matrix.slice(1).filter((row) => {
    if (!Array.isArray(row)) return false;
    return row.some((cell) => normalizeSpaces(cell) !== '');
  });

  return rows.map((row, idx) => {
    const rowNumber = idx + 2;
    const studioName = normalizeSpaces(row[0]);
    const designerName = normalizeSpaces(row[1]);
    const phoneRaw = row[2];
    const dateRaw = row[3];
    const commentRaw = row[4];
    const amountRaw = row[5];
    const cityRaw = row[6];

    const phone = normalizePhoneForStorage(phoneRaw);
    const phoneTail7 = getPhoneTail7(phoneRaw);
    const date = parseOrderDate(dateRaw);
    const amount = parseAmount(amountRaw);
    const city = normalizeSpaces(cityRaw);
    const comment = normalizeSpaces(commentRaw);

    const errors = [];
    if (!designerName) errors.push(ERR_DESIGNER_REQUIRED);
    if (!phoneTail7) errors.push(ERR_PHONE_REQUIRED);
    if (!date) errors.push(ERR_DATE_INVALID);
    if (!Number.isFinite(amount) || amount <= 0) errors.push(ERR_AMOUNT_INVALID);
    if (!city) errors.push(ERR_CITY_REQUIRED);

    return {
      rowNumber,
      valid: errors.length === 0,
      errors,
      data: {
        studio_name: studioName || null,
        designer_name: designerName,
        phone,
        phone_tail7: phoneTail7,
        date,
        comment: comment || null,
        amount: Number.isFinite(amount) ? amount : null,
        city,
      },
    };
  });
}

async function createPreviewResolution(rows) {
  const existingStudios = (await queryAll('baza_studios', { select: 'id, name, city' })).map(
    (studio) => ({
      ...studio,
      source: 'existing',
      createdRow: null,
    }),
  );
  const studioIndexes = buildStudioIndexes(existingStudios);

  const existingPartners = await queryAll('baza_partners', {
    select: 'id, designer_name, phone, city, studio_id',
  });

  const { data: importKeyRows } = await supabase
    .from('baza_sales')
    .select('import_key')
    .not('import_key', 'is', null);

  const existingImportKeys = new Set(
    (importKeyRows || [])
      .map((row) => String(row.import_key || '').trim())
      .filter(Boolean),
  );

  const virtualPartners = existingPartners.map((partner) => ({
    id: Number(partner.id),
    designer_name: partner.designer_name,
    phone: partner.phone,
    city: partner.city,
    studio_id: partner.studio_id == null ? null : Number(partner.studio_id),
    source: 'existing',
    createdRow: null,
  }));

  const previewCreatedOrderKeys = new Set();
  let newStudioSeq = 1;
  let newPartnerSeq = 1;

  const getOrCreateStudioRef = (studioName, city, rowNumber) => {
    if (!studioName) {
      return { ref: null, action: 'none' };
    }

    const found = findStudioInIndexes(studioName, studioIndexes);
    if (found) {
      return {
        ref: found,
        action: found.source === 'existing' ? 'matched_existing' : 'matched_preview_created',
      };
    }

    const created = {
      id: `new-studio-${newStudioSeq++}`,
      name: studioName,
      city: city || null,
      source: 'new',
      createdRow: rowNumber,
    };

    studioIndexes.byId.set(created.id, created);

    const matchKey = buildStudioMatchKey(created.name);
    if (matchKey && !studioIndexes.byMatch.has(matchKey)) {
      studioIndexes.byMatch.set(matchKey, created);
    }
    const rawKey = normalizeStudioKeyRaw(created.name);
    if (rawKey && !studioIndexes.byRaw.has(rawKey)) {
      studioIndexes.byRaw.set(rawKey, created);
    }

    return { ref: created, action: 'will_create' };
  };

  return rows.map((row) => {
    const nextRow = { ...row };
    const data = row.data || {};

    if (!row.valid) {
      nextRow.preview = {
        studio: { action: 'invalid', id: null, name: data.studio_name || null, city: data.city || null, createdRow: null },
        designer: { action: 'invalid', id: null, name: data.designer_name || null, phone: data.phone || null, createdRow: null, willUpdateStudio: false },
        binding: { partnerId: null, partnerName: data.designer_name || null, studioId: null, studioName: data.studio_name || null },
        order: { action: 'invalid' },
        conflict: { required: false, type: null },
      };
      return nextRow;
    }

    const incomingStudio = getOrCreateStudioRef(data.studio_name, data.city, row.rowNumber);
    const incomingStudioRef = incomingStudio.ref;

    const partnerCandidate = findPartnerInListByPhoneAndName(
      virtualPartners,
      data.phone || data.phone_tail7,
      data.designer_name,
    );

    const currentStudioRef = partnerCandidate?.studio_id != null
      ? (studioIndexes.byId.get(partnerCandidate.studio_id)
        || studioIndexes.byId.get(String(partnerCandidate.studio_id))
        || studioIndexes.byId.get(Number(partnerCandidate.studio_id))
        || null)
      : null;

    const hasStudioConflict = Boolean(
      partnerCandidate
      && data.studio_name
      && currentStudioRef
      && incomingStudioRef
      && Number(currentStudioRef.id) !== Number(incomingStudioRef.id)
      && !sameStudioAlias(currentStudioRef.name, incomingStudioRef.name),
    );

    let partnerRef = partnerCandidate;
    let partnerAction = 'matched_existing';

    let effectiveStudioId = null;
    let effectiveStudioName = null;

    if (!partnerRef) {
      effectiveStudioId = incomingStudioRef ? incomingStudioRef.id : null;
      effectiveStudioName = incomingStudioRef ? incomingStudioRef.name : null;

      partnerRef = {
        id: `new-partner-${newPartnerSeq++}`,
        designer_name: data.designer_name,
        phone: data.phone || `+${data.phone_tail7}`,
        city: data.city,
        studio_id: effectiveStudioId,
        source: 'new',
        createdRow: row.rowNumber,
      };
      virtualPartners.push(partnerRef);
      partnerAction = 'will_create';
    } else {
      if (partnerRef.source === 'new') {
        partnerAction = 'matched_preview_created';
      }

      if (!incomingStudioRef) {
        effectiveStudioId = partnerRef.studio_id;
        effectiveStudioName = currentStudioRef ? currentStudioRef.name : null;
      } else if (hasStudioConflict) {
        effectiveStudioId = partnerRef.studio_id;
        effectiveStudioName = currentStudioRef ? currentStudioRef.name : null;
      } else {
        effectiveStudioId = incomingStudioRef.id;
        effectiveStudioName = incomingStudioRef.name;
      }

      partnerRef.studio_id = effectiveStudioId;

      if (data.city && data.city !== String(partnerRef.city || '')) {
        partnerRef.city = data.city;
      }
      if (data.phone && data.phone !== String(partnerRef.phone || '')) {
        partnerRef.phone = data.phone;
      }
    }

    const oldStudioId = partnerCandidate?.studio_id == null ? null : partnerCandidate.studio_id;
    const willUpdateStudio = !hasStudioConflict && oldStudioId !== effectiveStudioId;

    const importKey = makeOrderImportKey({
      partnerId: partnerRef.id,
      studioId: effectiveStudioId,
      date: data.date,
      amount: data.amount,
      city: data.city,
      comment: data.comment,
    });

    const isDuplicate = existingImportKeys.has(importKey) || previewCreatedOrderKeys.has(importKey);
    if (!isDuplicate) {
      previewCreatedOrderKeys.add(importKey);
    }

    nextRow.preview = {
      studio: {
        action: incomingStudio.action,
        id: incomingStudioRef ? incomingStudioRef.id : null,
        name: incomingStudioRef ? incomingStudioRef.name : null,
        city: incomingStudioRef ? (incomingStudioRef.city || data.city || null) : null,
        createdRow: incomingStudioRef ? incomingStudioRef.createdRow : null,
      },
      designer: {
        action: partnerAction,
        id: partnerRef.id,
        name: partnerRef.designer_name,
        phone: partnerRef.phone || null,
        createdRow: partnerRef.createdRow,
        willUpdateStudio,
      },
      binding: {
        partnerId: partnerRef.id,
        partnerName: partnerRef.designer_name,
        studioId: effectiveStudioId,
        studioName: effectiveStudioName,
      },
      order: {
        action: hasStudioConflict ? 'needs_confirmation' : (isDuplicate ? 'duplicate' : 'will_create'),
      },
      conflict: {
        required: hasStudioConflict,
        type: hasStudioConflict ? 'partner_studio_mismatch' : null,
        currentStudio: currentStudioRef
          ? { id: currentStudioRef.id, name: currentStudioRef.name }
          : null,
        incomingStudio: incomingStudioRef
          ? { id: incomingStudioRef.id, name: incomingStudioRef.name }
          : null,
      },
    };

    return nextRow;
  });
}

async function ensureStudioForImport(studioName, city, counters) {
  if (!studioName) return null;

  let studio = await findStudioByName(studioName);
  if (!studio) {
    const created = await insertRow('baza_studios', { name: studioName, city: city || null });
    counters.createdStudios += 1;
    return { id: created.id, name: created.name, city: created.city };
  }

  if (city && city !== String(studio.city || '')) {
    await updateRows('baza_studios', { city }, { id: studio.id });
    counters.updatedStudios += 1;
    studio = { ...studio, city };
  }

  return studio;
}

function validateImportRow(data) {
  const errors = [];
  if (!normalizeSpaces(data.designer_name)) errors.push(ERR_DESIGNER_REQUIRED);
  if (!getPhoneTail7(data.phone)) errors.push(ERR_PHONE_REQUIRED);
  if (!parseOrderDate(data.date)) errors.push(ERR_DATE_INVALID);

  const amount = parseAmount(data.amount);
  if (!Number.isFinite(amount) || amount <= 0) errors.push(ERR_AMOUNT_INVALID);

  if (!normalizeSpaces(data.city)) errors.push(ERR_CITY_REQUIRED);

  return errors;
}

router.get('/template', (req, res) => {
  try {
    const format = String(req.query.format || 'xlsx').toLowerCase();

    if (format === 'csv') {
      const csvLine = TEMPLATE_HEADERS.join(';');
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename="import-template.csv"');
      res.send(`\uFEFF${csvLine}\n`);
      return;
    }

    const ws = XLSX.utils.aoa_to_sheet([TEMPLATE_HEADERS]);
    ws['!cols'] = [
      { wch: 28 },
      { wch: 34 },
      { wch: 18 },
      { wch: 16 },
      { wch: 40 },
      { wch: 16 },
      { wch: 18 },
    ];

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, '\u0418\u043C\u043F\u043E\u0440\u0442');

    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="import-template.xlsx"');
    res.send(Buffer.from(buffer));
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.post('/', uploadFile.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return fail(res, 'VALIDATION_ERROR', 'File is not uploaded');
    }

    const parsedPayload = importPreviewPayloadSchema.safeParse({ file: req.file });
    if (!parsedPayload.success) {
      return fail(res, 'VALIDATION_ERROR', 'Invalid preview payload', parsedPayload.error.flatten());
    }

    const workbook = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true, raw: true });
    const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
    const matrix = XLSX.utils.sheet_to_json(firstSheet, { header: 1, defval: '' });

    if (!matrix.length) {
      return fail(res, 'VALIDATION_ERROR', 'File is empty');
    }

    const header = matrix[0].map((h) => normalizeSpaces(h));
    const expected = TEMPLATE_HEADERS.map((h) => normalizeSpaces(h));

    if (header.length < expected.length || expected.some((h, i) => header[i] !== h)) {
      return fail(
        res,
        'VALIDATION_ERROR',
        'Wrong header format. Use import template.',
        {
          expectedHeaders: TEMPLATE_HEADERS,
          actualHeaders: header,
        },
      );
    }

    const rawRows = mapRowsFromSheet(matrix);
    const rows = await createPreviewResolution(rawRows);

    return res.json({
      rows,
      totalRows: rows.length,
      totalValid: rows.filter((r) => r.valid).length,
      previewRows: rows.slice(0, 20),
      expectedHeaders: TEMPLATE_HEADERS,
    });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', `Preview parse error: ${err.message}`, undefined, 500);
  }
});

router.post('/confirm', async (req, res) => {
  try {
    const parsedPayload = importConfirmPayloadSchema.safeParse(req.body || {});
    if (!parsedPayload.success) {
      return fail(res, 'VALIDATION_ERROR', 'Invalid confirm payload', parsedPayload.error.flatten());
    }

    const payloadRows = Array.isArray(req.body.rows)
      ? req.body.rows
      : (Array.isArray(req.body.partners)
        ? req.body.partners.map((data, idx) => ({ rowNumber: idx + 2, valid: true, errors: [], data }))
        : []);

    if (!payloadRows.length) {
      return fail(res, 'VALIDATION_ERROR', 'No data for import');
    }

    const counters = {
      createdStudios: 0,
      updatedStudios: 0,
      createdDesigners: 0,
      updatedDesigners: 0,
      createdOrders: 0,
      skippedDuplicates: 0,
    };

    const errors = [];

    for (const row of payloadRows) {
      const rowNumber = row.rowNumber || null;
      const data = row.data || row;

      const studioName = normalizeSpaces(data.studio_name);
      const designerName = normalizeSpaces(data.designer_name);
      const phone = normalizePhoneForStorage(data.phone);
      const phoneTail7 = data.phone_tail7 || getPhoneTail7(data.phone);
      const date = parseOrderDate(data.date);
      const amount = parseAmount(data.amount);
      const city = normalizeSpaces(data.city);
      const comment = normalizeSpaces(data.comment);
      const conflictAction = String(row.conflictAction || '').trim();

      const rowErrors = validateImportRow({
        designer_name: designerName,
        phone: phone || phoneTail7,
        date,
        amount,
        city,
      });

      if (rowErrors.length) {
        errors.push({ rowNumber, reason: rowErrors.join('; ') });
        continue;
      }

      let partner = await findPartnerByPhoneAndName(phone || phoneTail7, designerName);
      const currentStudioId = partner?.studio_id == null ? null : Number(partner.studio_id);
      const currentStudio = currentStudioId
        ? await queryOne('baza_studios', { select: 'id, name, city', filters: { id: currentStudioId } })
        : null;

      const incomingStudioFound = studioName ? await findStudioByName(studioName) : null;

      const hasStudioConflict = Boolean(
        partner
        && studioName
        && currentStudio
        && (
          (!incomingStudioFound && !sameStudioAlias(studioName, currentStudio.name))
          || (incomingStudioFound && Number(incomingStudioFound.id) !== Number(currentStudio.id)
            && !sameStudioAlias(incomingStudioFound.name, currentStudio.name))
        ),
      );

      if (hasStudioConflict && !['keep_existing_studio', 'move_to_import_studio'].includes(conflictAction)) {
        errors.push({ rowNumber, reason: ERR_STUDIO_CONFLICT_CONFIRM });
        continue;
      }

      let effectiveStudioId = currentStudioId;
      let targetStudio = null;

      if (studioName) {
        const shouldMoveToIncoming = !hasStudioConflict || !partner || conflictAction === 'move_to_import_studio';

        if (shouldMoveToIncoming) {
          targetStudio = await ensureStudioForImport(studioName, city, counters);
          effectiveStudioId = targetStudio?.id || null;
        }
      } else if (!partner) {
        effectiveStudioId = null;
      }

      if (!partner) {
        const created = await insertRow('baza_partners', {
          designer_name: designerName,
          phone: phone || `+${phoneTail7}`,
          city,
          status: 'active',
          studio_id: effectiveStudioId,
        });
        partner = await findPartnerByPhoneAndName(phone || phoneTail7, designerName);
        if (!partner) {
          partner = { id: created.id, designer_name: designerName, phone: phone || `+${phoneTail7}`, city, studio_id: effectiveStudioId };
        }
        counters.createdDesigners += 1;
      } else {
        const needStudioUpdate = Number(partner.studio_id || 0) !== Number(effectiveStudioId || 0);
        const needCityUpdate = city && city !== String(partner.city || '');
        const needPhoneUpdate = phone && phone !== String(partner.phone || '');

        if (needStudioUpdate || needCityUpdate || needPhoneUpdate) {
          await updateRows(
            'baza_partners',
            {
              studio_id: effectiveStudioId,
              city: needCityUpdate ? city : partner.city,
              phone: needPhoneUpdate ? phone : partner.phone,
            },
            { id: partner.id },
          );
          counters.updatedDesigners += 1;
          partner = {
            ...partner,
            studio_id: effectiveStudioId,
            city: needCityUpdate ? city : partner.city,
            phone: needPhoneUpdate ? phone : partner.phone,
          };
        }
      }

      if (!partner?.id) {
        errors.push({ rowNumber, reason: 'ERR_PARTNER_NOT_RESOLVED' });
        continue;
      }

      const importKey = makeOrderImportKey({
        partnerId: partner.id,
        studioId: effectiveStudioId,
        date,
        amount,
        city,
        comment,
      });

      const exists = await queryOne('baza_sales', {
        select: 'id',
        filters: { import_key: importKey },
      });
      if (exists) {
        counters.skippedDuplicates += 1;
        continue;
      }

      await insertRow('baza_sales', {
        partner_id: partner.id,
        date,
        product: 'Order',
        amount: Number(amount),
        comment: comment || null,
        import_key: importKey,
      });
      counters.createdOrders += 1;
    }

    return res.json({
      studios: { created: counters.createdStudios, updated: counters.updatedStudios },
      designers: { created: counters.createdDesigners, updated: counters.updatedDesigners },
      orders: { created: counters.createdOrders, skippedDuplicates: counters.skippedDuplicates },
      errors,
    });
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

export default router;
