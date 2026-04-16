import crypto from 'crypto';
import { fail } from '../utils/apiResponse.js';

/**
 * Authentication middleware for BAZA routes.
 * Accepts API key via X-Api-Key header or apiKey query param.
 *
 * HTTP Basic Auth was removed — the standalone BAZA frontend never shipped
 * and stored passwords in plain text. Future BAZA auth will go through
 * Supabase Auth when BAZA is merged into Omoikiri.
 */
export default function bazaAuth(req, res, next) {
  const apiKey = req.headers['x-api-key'] || req.query.apiKey;
  const API_KEY = process.env.API_KEY;

  if (apiKey && API_KEY && apiKey.length === API_KEY.length) {
    try {
      if (crypto.timingSafeEqual(Buffer.from(apiKey), Buffer.from(API_KEY))) {
        req.user = { username: 'api', role: 'admin' };
        return next();
      }
    } catch {
      // length mismatch guard triggered — fall through to 401
    }
  }

  return fail(res, 'UNAUTHORIZED', 'Auth required', null, 401);
}
