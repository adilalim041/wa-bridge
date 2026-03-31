import { z } from 'zod';

const optionalString = z.string().trim().optional();

export const analyticsQuerySchema = z.object({
  dateFrom: optionalString,
  dateTo: optionalString,
  from: optionalString,
  to: optionalString,
  city: optionalString,
  status: z.union([z.string(), z.array(z.string())]).optional(),
  granularity: z.enum(['day', 'week', 'month']).optional(),
});
