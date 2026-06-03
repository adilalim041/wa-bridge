import 'dotenv/config';
import { fileURLToPath } from 'url';
import { supabase as defaultDb } from '../storage/supabase.js';
import { businessMinutesBetween } from '../ai/dialogSessions.js';

const DEFAULT_SESSION_IDS = [
  'astana-renat-rabochiy-reklama',
  'almaty-rabochiy-reklama',
];

const NON_BUSINESS_TAGS = new Set(['сотрудник', 'спам', 'личное', 'коллега']);
const BASE_GAP_MS = 4 * 60 * 60 * 1000;
const MAX_EXTENDED_GAP_MS = 72 * 60 * 60 * 1000;
const MAX_BUSINESS_GAP_MINUTES = 8 * 60;

const AD_TRIGGER_PATTERNS = [
  /меня интересует\s+измельчитель\s+от бренда\s+omoikiri/i,
  /хочу сантехнику\s+от\s+омойкири/i,
  /омойкири\s+по скидке/i,
  /omoikiri\s+по скидке/i,
  /впервые вижу.*люксовая сантехника/i,
  /ссылка:\s*привет!\s*можно узнать/i,
];

const PRODUCT_OR_PRICE_PATTERNS = [
  /измельчител/i,
  /сантехник/i,
  /мойк/i,
  /смесител/i,
  /фильтр/i,
  /дозатор/i,
  /цена|стоим|сколько|прайс|каталог|скидк/i,
];

const SERVICE_OR_BACKOFFICE_PATTERNS = [
  /гарант|сервис|рекламац|дефект|треснул|картридж|кранбукс|протека/i,
  /доставк|накладн|чек|фискальн|касс|оплат|банк|1с/i,
  /образцы мебели|остатки|долги|реализац/i,
];

const MANAGER_SALES_REPLY_PATTERNS = [
  /от\s+\d[\d\s]*(?:тг|₸)?/i,
  /цена|стоим|со скидк|каталог|прайс|\[image\]|\[document/i,
  /мы представляем японский бренд|omoikiri/i,
];

function isPersonalJid(jid) {
  const raw = String(jid || '');
  if (raw.includes('@g.us') || raw.includes('120363') || raw.includes('@lid')) return false;
  const digits = raw.split('@')[0].replace(/\D/g, '');
  return digits.length >= 10 && digits.length <= 12;
}

function textOf(messages, fromMe = null) {
  return (messages || [])
    .filter((m) => fromMe === null || Boolean(m.from_me) === fromMe)
    .map((m) => String(m.body || '').trim())
    .filter(Boolean)
    .join('\n');
}

function hasAny(patterns, text) {
  return patterns.some((pattern) => pattern.test(text));
}

function firstAt(messages) {
  return messages[0]?.timestamp || null;
}

function lastAt(messages) {
  return messages[messages.length - 1]?.timestamp || null;
}

function snippet(messages) {
  return (messages || [])
    .slice(0, 4)
    .map((m) => `${m.from_me ? 'M' : 'C'} ${String(m.body || `[${m.message_type || 'message'}]`).replace(/\s+/g, ' ').slice(0, 90)}`)
    .join(' | ');
}

function shouldStitchPair(prev, next, prevMessages, nextMessages) {
  if (!prevMessages.length || !nextMessages.length) return false;
  if ((prev.message_count || prevMessages.length || 0) > 5) return false;

  const prevLast = lastAt(prevMessages) || prev.last_message_at;
  const nextFirst = firstAt(nextMessages) || next.started_at;
  const rawGap = new Date(nextFirst).getTime() - new Date(prevLast).getTime();
  if (rawGap < BASE_GAP_MS || rawGap > MAX_EXTENDED_GAP_MS) return false;

  const businessGap = businessMinutesBetween(prevLast, nextFirst);
  if (businessGap > MAX_BUSINESS_GAP_MINUTES) return false;

  const allText = `${textOf(prevMessages)}\n${textOf(nextMessages)}`;
  const prevIncoming = textOf(prevMessages, false);
  const nextManager = textOf(nextMessages, true);
  const hasIncomingStart = prevMessages.some((m) => !m.from_me);
  const nextHasManagerReply = nextMessages.some((m) => m.from_me);

  if (!hasIncomingStart || !nextHasManagerReply) return false;
  if (hasAny(SERVICE_OR_BACKOFFICE_PATTERNS, allText) && !hasAny(AD_TRIGGER_PATTERNS, allText)) {
    return false;
  }

  const explicitAdTrigger = hasAny(AD_TRIGGER_PATTERNS, allText);
  const leadStart = hasAny(PRODUCT_OR_PRICE_PATTERNS, prevIncoming)
    && hasAny(MANAGER_SALES_REPLY_PATTERNS, nextManager);

  return explicitAdTrigger || leadStart;
}

async function fetchAllDialogs(db, sessionIds, sinceIso) {
  const out = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await db
      .from('dialog_sessions')
      .select('id, session_id, remote_jid, started_at, last_message_at, message_count, status')
      .in('session_id', sessionIds)
      .gte('last_message_at', sinceIso)
      .order('session_id')
      .order('remote_jid')
      .order('last_message_at')
      .range(from, from + pageSize - 1);

    if (error) throw new Error(`dialog_sessions fetch failed: ${error.message}`);
    out.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return out.filter((d) => isPersonalJid(d.remote_jid));
}

async function fetchMessagesByDialog(db, dialogIds) {
  const out = new Map();
  for (let i = 0; i < dialogIds.length; i += 100) {
    const part = dialogIds.slice(i, i + 100);
    const { data, error } = await db
      .from('messages')
      .select('id, dialog_session_id, session_id, remote_jid, from_me, timestamp, body, message_type')
      .in('dialog_session_id', part)
      .order('timestamp', { ascending: true });
    if (error) throw new Error(`messages fetch failed: ${error.message}`);
    for (const row of data || []) {
      if (!out.has(row.dialog_session_id)) out.set(row.dialog_session_id, []);
      out.get(row.dialog_session_id).push(row);
    }
  }
  return out;
}

async function fetchExcludedJids(db, jids) {
  const excluded = new Set();
  if (!jids.length) return excluded;

  for (let i = 0; i < jids.length; i += 500) {
    const { data, error } = await db
      .from('chat_tags')
      .select('remote_jid, tags')
      .in('remote_jid', jids.slice(i, i + 500));
    if (error) throw new Error(`chat_tags fetch failed: ${error.message}`);
    for (const row of data || []) {
      const tags = (row.tags || []).map((tag) => String(tag).toLowerCase());
      if (tags.some((tag) => NON_BUSINESS_TAGS.has(tag))) excluded.add(row.remote_jid);
    }
  }

  return excluded;
}

function buildMergePlan(dialogs, messagesByDialog, excludedJids) {
  const byKey = new Map();
  for (const d of dialogs) {
    if (excludedJids.has(d.remote_jid)) continue;
    const key = `${d.session_id}:::${d.remote_jid}`;
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(d);
  }

  const pairs = [];
  for (const arr of byKey.values()) {
    arr.sort((a, b) => {
      const aFirst = firstAt(messagesByDialog.get(a.id) || []) || a.started_at || a.last_message_at;
      const bFirst = firstAt(messagesByDialog.get(b.id) || []) || b.started_at || b.last_message_at;
      return new Date(aFirst).getTime() - new Date(bFirst).getTime();
    });

    let canonical = null;
    let canonicalMessages = null;
    for (const current of arr) {
      const currentMessages = messagesByDialog.get(current.id) || [];
      if (!canonical) {
        canonical = current;
        canonicalMessages = currentMessages;
        continue;
      }

      if (shouldStitchPair(canonical, current, canonicalMessages || [], currentMessages)) {
        pairs.push({
          from_dialog_session_id: current.id,
          to_dialog_session_id: canonical.id,
          session_id: current.session_id,
          remote_jid: current.remote_jid,
          moved_messages: currentMessages.length,
          from_snippet: snippet(currentMessages),
          to_snippet: snippet(canonicalMessages),
        });
        canonicalMessages = [...(canonicalMessages || []), ...currentMessages].sort(
          (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
        );
        canonical.message_count = canonicalMessages.length;
        canonical.last_message_at = lastAt(canonicalMessages) || canonical.last_message_at;
      } else {
        canonical = current;
        canonicalMessages = currentMessages;
      }
    }
  }

  return pairs;
}

async function applyMergePlan(db, pairs) {
  let movedMessages = 0;
  let clearedAnalyses = 0;

  const byTarget = new Map();
  for (const pair of pairs) {
    if (!byTarget.has(pair.to_dialog_session_id)) byTarget.set(pair.to_dialog_session_id, []);
    byTarget.get(pair.to_dialog_session_id).push(pair.from_dialog_session_id);
  }

  for (const pair of pairs) {
    const { count: beforeCount, error: countError } = await db
      .from('messages')
      .select('id', { count: 'exact', head: true })
      .eq('dialog_session_id', pair.from_dialog_session_id);
    if (countError) throw new Error(`message count failed ${pair.from_dialog_session_id}: ${countError.message}`);

    const { error } = await db
      .from('messages')
      .update({ dialog_session_id: pair.to_dialog_session_id })
      .eq('dialog_session_id', pair.from_dialog_session_id);
    if (error) throw new Error(`message stitch failed ${pair.from_dialog_session_id}: ${error.message}`);
    movedMessages += beforeCount || 0;

    const { count: aiBeforeCount, error: aiCountError } = await db
      .from('chat_ai')
      .select('id', { count: 'exact', head: true })
      .in('dialog_session_id', [pair.from_dialog_session_id, pair.to_dialog_session_id]);
    if (aiCountError) throw new Error(`chat_ai count failed ${pair.from_dialog_session_id}: ${aiCountError.message}`);

    const { error: aiError } = await db
      .from('chat_ai')
      .delete()
      .in('dialog_session_id', [pair.from_dialog_session_id, pair.to_dialog_session_id]);
    if (aiError) throw new Error(`chat_ai cleanup failed ${pair.from_dialog_session_id}: ${aiError.message}`);
    clearedAnalyses += aiBeforeCount || 0;
  }

  for (const [targetId, mergedIds] of byTarget) {
    const allIds = [targetId, ...mergedIds];
    const { data: msgRows, error: msgError } = await db
      .from('messages')
      .select('dialog_session_id, timestamp')
      .eq('dialog_session_id', targetId)
      .order('timestamp', { ascending: true });
    if (msgError) throw new Error(`target recount failed ${targetId}: ${msgError.message}`);

    const count = msgRows?.length || 0;
    const first = msgRows?.[0]?.timestamp || null;
    const last = msgRows?.[count - 1]?.timestamp || null;

    if (count > 0) {
      const { error: targetError } = await db
        .from('dialog_sessions')
        .update({
          started_at: first,
          last_message_at: last,
          message_count: count,
          status: 'open',
        })
        .eq('id', targetId);
      if (targetError) throw new Error(`target update failed ${targetId}: ${targetError.message}`);
    }

    const { error: mergedError } = await db
      .from('dialog_sessions')
      .update({ message_count: 0, status: 'closed' })
      .in('id', mergedIds);
    if (mergedError) throw new Error(`merged session close failed ${targetId}: ${mergedError.message}`);
  }

  return { moved_messages: movedMessages, cleared_analyses: clearedAnalyses };
}

export async function recountDialogSessions({
  db = defaultDb,
  since = '2026-05-01T00:00:00.000Z',
  sessionIds = DEFAULT_SESSION_IDS,
} = {}) {
  const dialogs = await fetchAllDialogs(db, sessionIds, since);
  const messagesByDialog = await fetchMessagesByDialog(db, dialogs.map((d) => d.id));
  let updated = 0;
  let emptied = 0;

  for (const d of dialogs) {
    const messages = messagesByDialog.get(d.id) || [];
    if (messages.length === 0) {
      if ((d.message_count || 0) !== 0 || d.status !== 'closed') {
        const { error } = await db
          .from('dialog_sessions')
          .update({ message_count: 0, status: 'closed' })
          .eq('id', d.id);
        if (error) throw new Error(`empty recount failed ${d.id}: ${error.message}`);
        emptied++;
      }
      continue;
    }

    const first = firstAt(messages);
    const last = lastAt(messages);
    const patch = {
      started_at: first,
      last_message_at: last,
      message_count: messages.length,
    };

    if (
      d.started_at !== first ||
      d.last_message_at !== last ||
      (d.message_count || 0) !== messages.length
    ) {
      const { error } = await db
        .from('dialog_sessions')
        .update(patch)
        .eq('id', d.id);
      if (error) throw new Error(`recount failed ${d.id}: ${error.message}`);
      updated++;
    }
  }

  return { scanned_dialogs: dialogs.length, updated, emptied };
}

export async function repairDialogSessionStitching({
  db = defaultDb,
  dryRun = true,
  since = '2026-05-01T00:00:00.000Z',
  sessionIds = DEFAULT_SESSION_IDS,
} = {}) {
  const dialogs = await fetchAllDialogs(db, sessionIds, since);
  const dialogIds = dialogs.map((d) => d.id);
  const messagesByDialog = await fetchMessagesByDialog(db, dialogIds);
  const jids = [...new Set(dialogs.map((d) => d.remote_jid).filter(Boolean))];
  const excludedJids = await fetchExcludedJids(db, jids);
  const pairs = buildMergePlan(dialogs, messagesByDialog, excludedJids);

  if (dryRun) {
    return {
      dry_run: true,
      scanned_dialogs: dialogs.length,
      excluded_jids: excludedJids.size,
      merge_pairs: pairs.length,
      sample: pairs.slice(0, 20),
    };
  }

  const applied = await applyMergePlan(db, pairs);
  return {
    dry_run: false,
    scanned_dialogs: dialogs.length,
    excluded_jids: excludedJids.size,
    merge_pairs: pairs.length,
    ...applied,
  };
}

function argValue(name, fallback = null) {
  const prefix = `--${name}=`;
  const found = process.argv.find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const commonArgs = {
    since: argValue('since', '2026-05-01T00:00:00.000Z'),
    sessionIds: argValue('sessions')
      ? argValue('sessions').split(',').map((x) => x.trim()).filter(Boolean)
      : DEFAULT_SESSION_IDS,
  };
  const result = process.argv.includes('--recount')
    ? await recountDialogSessions(commonArgs)
    : await repairDialogSessionStitching({
        ...commonArgs,
        dryRun: !process.argv.includes('--apply'),
      });
  console.log(JSON.stringify(result, null, 2));
}
