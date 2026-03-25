import { logger } from '../config.js';

export function randomDelay(min = 1000, max = 3000) {
  const delay = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise((resolve) => setTimeout(resolve, delay));
}

export async function sendWithDelay(sock, jid, content) {
  try {
    await sock.presenceSubscribe(jid);
    await sock.sendPresenceUpdate('composing', jid);
    await randomDelay(2000, 4000);
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
  constructor() {
    this.perJid = new Map();
    this.global = [];
    this.windowMs = 60_000;
    this.maxPerJid = 10;
    this.maxGlobal = 30;
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
    limiters.set(sessionId, new RateLimiter());
  }

  return limiters.get(sessionId);
}

export function removeRateLimiter(sessionId) {
  limiters.delete(sessionId);
}
