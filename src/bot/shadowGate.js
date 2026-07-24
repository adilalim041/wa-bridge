import { logger } from '../config.js';
import { serviceClient } from '../storage/supabase.js';
import { evaluateBotContactPolicy } from './contactPolicy.js';

const BOT_ENGINE_MODE = String(process.env.BOT_ENGINE_MODE || 'off').trim().toLowerCase();
const MAX_MESSAGE_AGE_MS = 15 * 60 * 1000;
const MAX_QUEUE_SIZE = 500;
const MAX_CONCURRENCY = 2;

const queue = [];
let activeWorkers = 0;

async function loadContactTags(remoteJid) {
  const { data, error } = await serviceClient
    .from('chat_tags')
    .select('tags')
    .eq('remote_jid', remoteJid)
    .maybeSingle();

  if (error) {
    return { tags: [], ok: false, error };
  }

  return {
    tags: Array.isArray(data?.tags) ? data.tags : [],
    ok: true,
    error: null,
  };
}

async function recordDecision(message, policy, tags) {
  const { error } = await serviceClient
    .from('bot_message_decisions')
    .upsert(
      {
        session_id: message.sessionId,
        message_id: message.messageId,
        remote_jid: message.remoteJid,
        mode: 'shadow',
        action: policy.eligible ? 'eligible' : 'skip',
        reason: policy.reason,
        contact_tags: tags,
        evaluated_at: new Date().toISOString(),
      },
      { onConflict: 'session_id,message_id', ignoreDuplicates: true }
    );

  if (error) {
    logger.warn(
      { err: error, sessionId: message.sessionId, messageId: message.messageId },
      'Bot shadow decision was not persisted'
    );
  }
}

async function evaluateMessage(message) {
  const timestampMs = new Date(message.timestamp).getTime();
  const messageAgeMs = Date.now() - timestampMs;

  if (
    message.fromMe ||
    message.chatType !== 'personal' ||
    !Number.isFinite(timestampMs) ||
    messageAgeMs < 0 ||
    messageAgeMs > MAX_MESSAGE_AGE_MS
  ) {
    return;
  }

  const tagResult = await loadContactTags(message.remoteJid);
  const policy = evaluateBotContactPolicy({
    fromMe: message.fromMe,
    chatType: message.chatType,
    tags: tagResult.tags,
    tagLookupOk: tagResult.ok,
    messageAgeMs,
    maxMessageAgeMs: MAX_MESSAGE_AGE_MS,
  });

  if (!tagResult.ok) {
    logger.warn(
      {
        err: tagResult.error,
        sessionId: message.sessionId,
        messageId: message.messageId,
      },
      'Bot shadow gate failed closed because contact tags were unavailable'
    );
  }

  await recordDecision(message, policy, tagResult.tags);
}

function drainQueue() {
  while (activeWorkers < MAX_CONCURRENCY && queue.length > 0) {
    const message = queue.shift();
    activeWorkers += 1;

    evaluateMessage(message)
      .catch((error) => {
        logger.error(
          { err: error, sessionId: message.sessionId, messageId: message.messageId },
          'Unexpected bot shadow gate failure'
        );
      })
      .finally(() => {
        activeWorkers -= 1;
        drainQueue();
      });
  }
}

export function enqueueBotShadowEvaluation(message) {
  if (BOT_ENGINE_MODE !== 'shadow') return false;

  if (queue.length >= MAX_QUEUE_SIZE) {
    logger.warn(
      { queueSize: queue.length, sessionId: message?.sessionId },
      'Bot shadow queue is full; evaluation skipped'
    );
    return false;
  }

  queue.push(message);
  drainQueue();
  return true;
}

export function getBotShadowQueueStats() {
  return {
    mode: BOT_ENGINE_MODE,
    queued: queue.length,
    activeWorkers,
    maxQueueSize: MAX_QUEUE_SIZE,
    maxConcurrency: MAX_CONCURRENCY,
  };
}
