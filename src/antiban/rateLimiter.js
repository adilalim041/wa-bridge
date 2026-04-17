import { logger } from '../config.js';

// ── Per-session diversity via FNV-1a hash ─────────────────────────────────────
//
// Background (W2 anti-ban #3, 2026-04-17): before this change every session
// had IDENTICAL rate limits (8/JID/min, 15 global/min) and IDENTICAL typing
// delay (2-4s flat). For 6 sessions on the same Railway IP that looks like a
// bot cluster — the Meta detector sees "6 accounts with the same throughput
// ceiling hitting the same API patterns". Real humans don't do that; one
// manager types fast, another slowly, one handles 50 contacts/day, another 5.
//
// The fix: derive a stable 0.7..1.3 multiplier from the sessionId (same
// FNV-1a hash used for browser fingerprint selection — consistency matters
// so the multiplier survives reconnects and Railway restarts).
// ─────────────────────────────────────────────────────────────────────────────

function fnvHash(str) {
  let hash = 2166136261;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = (hash * 16777619) >>> 0;
  }
  return hash;
}

function sessionMultiplier(sessionId) {
  // Normalize hash into [0.7, 1.3] — 60% total spread. Floor of 0.7 guarantees
  // even the "slowest" session keeps at least 5-6 msgs/min per contact, which
  // is plenty for human-paced support.
  const h = fnvHash(sessionId);
  const normalized = (h % 1000) / 1000; // 0.0..0.999
  return 0.7 + normalized * 0.6;
}

// ── Typing delay scaled by message length ─────────────────────────────────────
//
// W2 anti-ban #4, 2026-04-17: old code used randomDelay(2000, 4000) — a flat
// 2-4s regardless of whether the manager sent "Да" or a 5-paragraph pricing
// breakdown. Humans spend longer typing longer messages; flat delay is a
// detectable signal.
//
// Model: 1500ms base + 40ms per char (cap effective length at 150 to prevent
// extreme delays on long copy-paste content), then ±20% jitter. Real typing
// speed varies — fast ~60 WPM (~200ms/char), average phone ~30 WPM (~400ms).
// Our model sits between for short messages and caps at ~9s for long ones —
// beyond that the manager's UX suffers more than anti-ban improves.
// ─────────────────────────────────────────────────────────────────────────────

function messageTextLength(content) {
  if (!content || typeof content !== 'object') return 0;
  if (typeof content.text === 'string') return content.text.length;
  if (typeof content.caption === 'string') return content.caption.length;
  return 0;
}

function typingDelayMs(textLength) {
  const effectiveLen = Math.min(textLength, 150);
  const base = 1500 + effectiveLen * 40; // 1500ms (media/empty) → 7500ms (150+ chars)
  const jitter = 0.8 + Math.random() * 0.4; // 80-120%
  return Math.round(base * jitter);
}

// Exported because some callers (e.g. manual drafts) want a generic delay
// without the composing presence dance.
export function randomDelay(min = 1000, max = 3000) {
  const delay = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise((resolve) => setTimeout(resolve, delay));
}

export async function sendWithDelay(sock, jid, content) {
  const textLen = messageTextLength(content);
  const delayMs = typingDelayMs(textLen);

  try {
    await sock.presenceSubscribe(jid);
    await sock.sendPresenceUpdate('composing', jid);
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    const result = await sock.sendMessage(jid, content);
    await sock.sendPresenceUpdate('paused', jid);
    return result;
  } catch (error) {
    try {
      await sock.sendPresenceUpdate('paused', jid);
    } catch (presenceError) {
      logger.warn({ err: presenceError, jid }, 'Failed to reset presence state');
    }

    throw error;
  }
}

export class RateLimiter {
  constructor(multiplier = 1.0) {
    this.perJid = new Map();
    this.global = [];
    this.windowMs = 60_000;
    // Base limits are conservative for a shared-IP deployment of 6+ sessions.
    // `multiplier` adds per-session diversity (see sessionMultiplier above).
    // Math.max(1, ...) floor ensures no session ends up completely silenced
    // at the extreme low end of the multiplier range.
    this.multiplier = multiplier;
    this.maxPerJid = Math.max(1, Math.round(8 * multiplier));
    this.maxGlobal = Math.max(1, Math.round(15 * multiplier));
  }

  prune(now = Date.now()) {
    const threshold = now - this.windowMs;

    for (const [jid, timestamps] of this.perJid.entries()) {
      const filtered = timestamps.filter((timestamp) => timestamp >= threshold);
      if (filtered.length > 0) {
        this.perJid.set(jid, filtered);
      } else {
        this.perJid.delete(jid);
      }
    }

    this.global = this.global.filter((timestamp) => timestamp >= threshold);
  }

  canSend(jid) {
    this.prune();
    return (this.perJid.get(jid) ?? []).length < this.maxPerJid;
  }

  canSendGlobal() {
    this.prune();
    return this.global.length < this.maxGlobal;
  }

  recordSend(jid) {
    const now = Date.now();
    this.prune(now);
    const jidTimestamps = this.perJid.get(jid) ?? [];
    jidTimestamps.push(now);
    this.perJid.set(jid, jidTimestamps);
    this.global.push(now);
  }
}

const limiters = new Map();

export function getRateLimiter(sessionId) {
  if (!limiters.has(sessionId)) {
    const multiplier = sessionMultiplier(sessionId);
    const limiter = new RateLimiter(multiplier);
    logger.info(
      {
        sessionId,
        multiplier: Number(multiplier.toFixed(2)),
        maxPerJid: limiter.maxPerJid,
        maxGlobal: limiter.maxGlobal,
      },
      'Rate limiter initialized (per-session multiplier applied)'
    );
    limiters.set(sessionId, limiter);
  }

  return limiters.get(sessionId);
}

export function removeRateLimiter(sessionId) {
  limiters.delete(sessionId);
}
