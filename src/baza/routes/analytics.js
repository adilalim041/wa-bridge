import { Router } from 'express';
import XLSX from 'xlsx';
import { getAnalyticsDashboard, getAnalyticsExportRows } from '../services/analyticsService.js';
import { okItem, fail } from '../utils/apiResponse.js';
import { analyticsQuerySchema } from '../validators/analytics.js';

const router = Router();

router.get('/', async (req, res) => {
  try {
    const parsed = analyticsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return fail(res, 'VALIDATION_ERROR', 'Invalid analytics query params', parsed.error.flatten());
    }

    const asOf = new Date();
    const item = await getAnalyticsDashboard(parsed.data, { asOf });
    return okItem(res, item);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

router.get('/export', async (req, res) => {
  try {
    const parsed = analyticsQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return fail(res, 'VALIDATION_ERROR', 'Invalid analytics query params', parsed.error.flatten());
    }

    const asOf = new Date();
    const rows = await getAnalyticsExportRows(parsed.data, { asOf });

    const data = rows.map((row) => ({
      '\u0421\u0442\u0443\u0434\u0438\u044f': row.studio,
      '\u0414\u0438\u0437\u0430\u0439\u043d\u0435\u0440': row.designer,
      '\u0413\u043e\u0440\u043e\u0434': row.city,
      '\u0421\u0442\u0430\u0442\u0443\u0441\u044b': row.statuses,
      '\u0414\u0430\u0442\u0430': row.date,
      '\u0422\u043e\u0432\u0430\u0440': row.product,
      '\u0421\u0443\u043c\u043c\u0430': row.amount,
      '\u041a\u043e\u043c\u043c\u0435\u043d\u0442\u0430\u0440\u0438\u0439': row.comment,
    }));

    const ws = XLSX.utils.json_to_sheet(data);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, '\u041f\u0440\u043e\u0434\u0430\u0436\u0438');

    ws['!cols'] = [
      { wch: 24 }, { wch: 28 }, { wch: 16 }, { wch: 20 },
      { wch: 12 }, { wch: 20 }, { wch: 14 }, { wch: 32 }
    ];

    const buffer = XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="analytics.xlsx"');
    res.send(Buffer.from(buffer));
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

export default router;
