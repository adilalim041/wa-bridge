import { z } from 'zod';

export const importPreviewPayloadSchema = z.object({
  file: z.object({
    originalname: z.string().min(1),
    size: z.number().int().positive(),
  }),
});

export const importConfirmPayloadSchema = z
  .object({
    rows: z.array(z.any()).optional(),
    partners: z.array(z.any()).optional(),
  })
  .refine((value) => {
    return (Array.isArray(value.rows) && value.rows.length > 0)
      || (Array.isArray(value.partners) && value.partners.length > 0);
  }, {
    message: 'rows or partners is required',
  });
