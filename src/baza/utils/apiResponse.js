export function okItem(res, item) {
  return res.json({ item });
}

export function okList(res, items, meta = {}, extra) {
  const payload = { items, meta };
  if (extra !== undefined) {
    payload.extra = extra;
  }
  return res.json(payload);
}

export function ok(res, payload) {
  return res.json(payload);
}

export function fail(res, code, message, details, status = 400) {
  const error = { code, message };
  if (details !== undefined) {
    error.details = details;
  }
  return res.status(status).json({ error });
}
