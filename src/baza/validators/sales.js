import { z } from 'zod';

const optionalString = z.string().trim().optional();
const optionalIntFromQuery = (fallback, min, max) =>
  z
    .preprocess((v) => (v === undefined || v === '' ? fallback : Number(v)), z.number().int().min(min).max(max))
    .optional()
    .transform((v) => (v === undefined ? fallback : v));

export const salesQuerySchema = z.object({
  page: optionalIntFromQuery(1, 1, Number.MAX_SAFE_INTEGER),
  limit: optionalIntFromQuery(20, 1, 100),
  search: optionalString,
  dateFrom: optionalString,
  dateTo: optionalString,
  from: optionalString,
  to: optionalString,
  city: optionalString,
  minAmount: z.preprocess((v) => (v === undefined || v === '' ? undefined : Number(v)), z.number().finite().optional()),
  maxAmount: z.preprocess((v) => (v === undefined || v === '' ? undefined : Number(v)), z.number().finite().optional()),
  sortBy: optionalString,
  sortOrder: optionalString,
});
