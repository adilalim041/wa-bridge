import { Router } from 'express';
import { listSales } from '../services/salesService.js';
import { okList, fail } from '../utils/apiResponse.js';
import { salesQuerySchema } from '../validators/sales.js';

const router = Router();

router.get('/', async (req, res) => {
  try {
    const parsed = salesQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      return fail(res, 'VALIDATION_ERROR', 'Invalid sales query params', parsed.error.flatten());
    }

    const {
      page,
      limit,
      search,
      dateFrom,
      dateTo,
      from,
      to,
      minAmount,
      maxAmount,
      sortBy,
      sortOrder,
    } = parsed.data;

    const result = await listSales({
      page,
      limit,
      search,
      dateFrom: dateFrom || from,
      dateTo: dateTo || to,
      minAmount,
      maxAmount,
      sortBy,
      sortOrder,
    });

    return okList(res, result.items, result.meta, result.extra);
  } catch (err) {
    return fail(res, 'INTERNAL_ERROR', err.message, undefined, 500);
  }
});

export default router;
