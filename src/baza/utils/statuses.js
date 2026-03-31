function toDate(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  return Number.isNaN(d.getTime()) ? null : d;
}

const MS_PER_DAY = 86_400_000;

export function computeFlags({ lastOrderDate, ordersCount2y, sum2y }, { asOf = new Date() } = {}) {
  const vip = Number(ordersCount2y || 0) > 4 || Number(sum2y || 0) > 2000000;
  const last = toDate(lastOrderDate);
  const recencyDays = last ? Math.floor((asOf - last) / MS_PER_DAY) : Infinity;
  const sleeping = recencyDays >= 120;
  const active = !sleeping;

  return {
    isVip: vip,
    isActive: active,
    isSleeping: sleeping,
  };
}

export function getDesignerStatusInfo({ lastOrderDate, orders2y, revenue2y, now = new Date() }) {
  const statuses = [];
  const flags = computeFlags(
    { lastOrderDate, ordersCount2y: orders2y, sum2y: revenue2y },
    { asOf: now }
  );
  if (flags.isVip) statuses.push('vip');

  if (flags.isSleeping) {
    statuses.push('sleeping');
  } else {
    statuses.push('active');
  }

  let primaryStatus = 'active';
  if (flags.isSleeping) primaryStatus = 'sleeping';
  else if (flags.isVip) primaryStatus = 'vip';

  return {
    statuses,
    primaryStatus,
    vip: flags.isVip,
    sleeping: flags.isSleeping,
  };
}
