import crypto from 'crypto';
import { queryOne } from '../database.js';
import { fail } from '../utils/apiResponse.js';

function unauthorized(res) {
  res.setHeader('WWW-Authenticate', 'Basic realm="baza"');
  return fail(res, 'UNAUTHORIZED', 'Auth required', null, 401);
}

/**
 * Dual authentication middleware for BAZA routes.
 * Accepts either:
 *   1. API key via X-Api-Key header or apiKey query param (wa-dashboard / Omoikiri)
 *   2. HTTP Basic Auth with username:password (standalone BAZA frontend)
 */
export default async function bazaAuth(req, res, next) {
  // Method 1: API key (from wa-dashboard / Omoikiri)
  const apiKey = req.headers['x-api-key'] || req.query.apiKey;
  const API_KEY = process.env.API_KEY;

  if (apiKey && API_KEY && apiKey.length === API_KEY.length) {
    try {
      if (crypto.timingSafeEqual(Buffer.from(apiKey), Buffer.from(API_KEY))) {
        req.user = { username: 'api', role: 'admin' };
        return next();
      }
    } catch {
      // Fall through to Basic Auth
    }
  }

  // Method 2: HTTP Basic Auth (from standalone BAZA frontend)
  const header = req.headers.authorization || '';
  if (!header.startsWith('Basic ')) {
    return unauthorized(res);
  }

  const base64 = header.slice(6).trim();
  let decoded = '';
  try {
    decoded = Buffer.from(base64, 'base64').toString('utf8');
  } catch {
    return unauthorized(res);
  }

  const separatorIndex = decoded.indexOf(':');
  if (separatorIndex < 0) {
    return unauthorized(res);
  }

  const username = decoded.slice(0, separatorIndex);
  const password = decoded.slice(separatorIndex + 1);

  try {
    const user = await queryOne('baza_users', {
      select: 'username, role',
      filters: { username, password, active: true },
    });

    if (!user) {
      return unauthorized(res);
    }

    req.user = { username: user.username, role: user.role };
    return next();
  } catch {
    return unauthorized(res);
  }
}
