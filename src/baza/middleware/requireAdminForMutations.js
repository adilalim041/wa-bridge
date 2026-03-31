import { fail } from '../utils/apiResponse.js';

function canManagerMutate(req) {
  if (req.user?.role !== 'manager') return false;
  if (req.method !== 'POST') return false;
  return /^\/tasks\/\d+\/(complete|proof)$/.test(req.path || '');
}

export default function requireAdminForMutations(req, res, next) {
  if (req.method === 'GET') {
    return next();
  }

  if (req.user?.role === 'admin') {
    return next();
  }

  if (canManagerMutate(req)) {
    return next();
  }

  return fail(res, 'FORBIDDEN', 'Read-only access', null, 403);
}
