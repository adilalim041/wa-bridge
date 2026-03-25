import { Readable } from 'stream';
import { downloadMediaMessage } from 'baileys';
import { v2 as cloudinary } from 'cloudinary';
import { config, logger } from '../config.js';

const cloudinaryConfigured = Boolean(
  config.cloudinaryCloudName && config.cloudinaryApiKey && config.cloudinaryApiSecret
);
let hasLoggedMissingCloudinaryConfig = false;

// Circuit breaker: stop trying Cloudinary if it fails repeatedly
const circuitBreaker = {
  failures: 0,
  lastFailure: 0,
  openUntil: 0, // timestamp when circuit closes again
  THRESHOLD: 3, // failures within window to trip
  WINDOW_MS: 60_000, // 1 minute window
  COOLDOWN_MS: 5 * 60_000, // 5 minutes cooldown
  isOpen() {
    if (Date.now() < this.openUntil) return true;
    // Reset if cooldown passed
    if (this.openUntil > 0 && Date.now() >= this.openUntil) {
      this.failures = 0;
      this.openUntil = 0;
    }
    return false;
  },
  recordFailure() {
    const now = Date.now();
    // Reset counter if last failure was outside window
    if (now - this.lastFailure > this.WINDOW_MS) this.failures = 0;
    this.failures++;
    this.lastFailure = now;
    if (this.failures >= this.THRESHOLD) {
      this.openUntil = now + this.COOLDOWN_MS;
      logger.warn(`Cloudinary circuit breaker OPEN — skipping uploads for 5 minutes (${this.failures} failures)`);
    }
  },
  recordSuccess() {
    this.failures = 0;
    this.openUntil = 0;
  },
};

cloudinary.config({
  cloud_name: config.cloudinaryCloudName,
  api_key: config.cloudinaryApiKey,
  api_secret: config.cloudinaryApiSecret,
});

function getMediaDescriptor(msg) {
  if (msg.imageMessage) {
    return {
      mediaType: 'image',
      mimeType: msg.imageMessage.mimetype || 'image/jpeg',
      fileName: null,
      resourceType: 'image',
    };
  }

  if (msg.videoMessage) {
    return {
      mediaType: 'video',
      mimeType: msg.videoMessage.mimetype || 'video/mp4',
      fileName: null,
      resourceType: 'video',
    };
  }

  if (msg.audioMessage || msg.pttMessage) {
    const audio = msg.audioMessage || msg.pttMessage;
    return {
      mediaType: 'audio',
      mimeType: audio.mimetype || 'audio/ogg; codecs=opus',
      fileName: null,
      resourceType: 'video',
    };
  }

  if (msg.documentMessage) {
    return {
      mediaType: 'document',
      mimeType: msg.documentMessage.mimetype || 'application/octet-stream',
      fileName: msg.documentMessage.fileName || null,
      resourceType: 'raw',
    };
  }

  if (msg.stickerMessage) {
    return {
      mediaType: 'sticker',
      mimeType: msg.stickerMessage.mimetype || 'image/webp',
      fileName: null,
      resourceType: 'image',
    };
  }

  return null;
}

export async function processMedia(message, sessionId) {
  try {
    if (!cloudinaryConfigured) {
      if (!hasLoggedMissingCloudinaryConfig) {
        logger.warn('Cloudinary is not configured. Media uploads are disabled.');
        hasLoggedMissingCloudinaryConfig = true;
      }
      return null;
    }

    if (circuitBreaker.isOpen()) {
      logger.debug({ sessionId }, 'Cloudinary circuit breaker open — skipping upload');
      return null;
    }

    const descriptor = getMediaDescriptor(message?.message);
    if (!descriptor) {
      return null;
    }

    const buffer = await downloadMediaMessage(message, 'buffer', {});
    if (!buffer || buffer.length === 0) {
      logger.warn(`[${sessionId}] Empty media buffer. Skipping upload.`);
      return null;
    }

    const folder = `wa-bridge/${sessionId}`;
    const publicId = `${message?.key?.id || 'media'}_${Date.now()}`;

    const result = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream(
        {
          folder,
          public_id: publicId,
          resource_type: descriptor.resourceType,
          timeout: 30000,
        },
        (error, uploadResult) => {
          if (error) {
            reject(error);
            return;
          }

          resolve(uploadResult);
        }
      );

      Readable.from(buffer).pipe(uploadStream);
    });

    circuitBreaker.recordSuccess();

    return {
      url: result.secure_url,
      publicId: result.public_id,
      mediaType: descriptor.mediaType,
      mimeType: descriptor.mimeType,
      fileSize: buffer.length,
      fileName: descriptor.fileName,
    };
  } catch (error) {
    circuitBreaker.recordFailure();
    logger.error({ err: error, sessionId, messageId: message?.key?.id }, 'Failed to process media');
    return null;
  }
}
