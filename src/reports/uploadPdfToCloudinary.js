/**
 * uploadPdfToCloudinary.js
 *
 * Uploads a base64-encoded PDF to Cloudinary under omoikiri_crm/reports/.
 * Uses resource_type: 'raw' — required for non-image/video binary files.
 *
 * Gotcha: Cloudinary ignores the file extension in public_id for raw uploads.
 * The secure_url will include the .pdf extension only if you keep it in public_id.
 * We strip it here and let Cloudinary's format param handle the extension.
 */

import { Readable } from 'stream';
import { v2 as cloudinary } from 'cloudinary';
import { config, logger } from '../config.js';

// Configure once at module load — safe because config is populated before imports.
cloudinary.config({
  cloud_name: config.cloudinaryCloudName,
  api_key: config.cloudinaryApiKey,
  api_secret: config.cloudinaryApiSecret,
});

const FOLDER = 'omoikiri_crm/reports';

/**
 * Upload a PDF to Cloudinary.
 *
 * @param {string} base64Pdf  Raw base64 string (no data-URL prefix).
 * @param {string} filename   Friendly filename, e.g. "report-2026-04-20.pdf".
 *                            Used as public_id (without .pdf extension).
 * @returns {Promise<{ url: string, publicId: string }>}
 * @throws {Error} on Cloudinary failure after retries.
 */
export async function uploadReportPdf(base64Pdf, filename) {
  if (!base64Pdf || typeof base64Pdf !== 'string') {
    throw new Error('uploadReportPdf: base64Pdf must be a non-empty string');
  }
  if (!filename || typeof filename !== 'string') {
    throw new Error('uploadReportPdf: filename must be a non-empty string');
  }

  // Strip .pdf extension for public_id — Cloudinary appends it via format
  const publicId = filename.replace(/\.pdf$/i, '');
  const buffer = Buffer.from(base64Pdf, 'base64');

  if (buffer.length === 0) {
    throw new Error('uploadReportPdf: decoded PDF buffer is empty — check base64 input');
  }

  let lastError;

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const result = await new Promise((resolve, reject) => {
        const uploadStream = cloudinary.uploader.upload_stream(
          {
            folder: FOLDER,
            public_id: publicId,
            resource_type: 'raw',
            format: 'pdf',
            timeout: 60_000,
          },
          (error, uploadResult) => {
            if (error) return reject(error);
            resolve(uploadResult);
          }
        );

        Readable.from(buffer).pipe(uploadStream);
      });

      logger.info(
        { publicId: result.public_id, bytes: buffer.length },
        'uploadReportPdf: upload successful'
      );

      return {
        url: result.secure_url,
        publicId: result.public_id,
      };
    } catch (err) {
      lastError = err;
      if (attempt < 2) {
        const delay = 2000 * (attempt + 1);
        logger.warn(
          { err, attempt, delay },
          'uploadReportPdf: Cloudinary upload failed — retrying'
        );
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }

  logger.error({ err: lastError, filename }, 'uploadReportPdf: all retries exhausted');
  throw lastError;
}
