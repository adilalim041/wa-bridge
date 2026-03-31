import { Router } from 'express';
import { okItem } from '../utils/apiResponse.js';

const router = Router();

router.get('/', (req, res) => {
  return okItem(res, {
    username: req.user.username,
    role: req.user.role,
  });
});

export default router;
