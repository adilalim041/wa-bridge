import crypto from 'crypto';
import { supabase } from './supabase.js';

const INSTANCE_ID = crypto.randomUUID();
const HEARTBEAT_INTERVAL = 60 * 1000;
const LOCK_TIMEOUT = 60 * 1000;

const heartbeatTimers = new Map();

export async function acquireLock(sessionId) {
  try {
    const { error: upsertError } = await supabase
      .from('session_lock')
      .upsert(
        {
          session_id: sessionId,
          instance_id: INSTANCE_ID,
          locked_at: new Date().toISOString(),
          heartbeat_at: new Date().toISOString(),
        },
        { onConflict: 'session_id' }
      );
    if (upsertError) {
      console.error('Failed to acquire lock:', upsertError.message);
      return false;
    }
    console.log(`Session lock acquired (instance: ${INSTANCE_ID.slice(0, 8)})`);
    return true;
  } catch (error) {
    console.error('Failed to acquire lock:', error.message);
    return false;
  }
}

export function startHeartbeat(sessionId) {
  if (heartbeatTimers.has(sessionId)) {
    return;
  }

  const timer = setInterval(async () => {
    try {
      const { error } = await supabase
        .from('session_lock')
        .update({ heartbeat_at: new Date().toISOString() })
        .eq('session_id', sessionId)
        .eq('instance_id', INSTANCE_ID);

      if (error) {
        console.error('Heartbeat update failed:', error.message);
      }
    } catch (error) {
      console.error('Heartbeat update failed:', error.message);
    }
  }, HEARTBEAT_INTERVAL);

  heartbeatTimers.set(sessionId, timer);
}

export function stopHeartbeat(sessionId) {
  const timer = heartbeatTimers.get(sessionId);
  if (timer) {
    clearInterval(timer);
    heartbeatTimers.delete(sessionId);
  }
}

export async function releaseLock(sessionId) {
  stopHeartbeat(sessionId);

  try {
    const { error } = await supabase
      .from('session_lock')
      .delete()
      .eq('session_id', sessionId)
      .eq('instance_id', INSTANCE_ID);

    if (error) {
      console.error('Failed to release lock:', error.message);
      return;
    }

    console.log('Session lock released');
  } catch (error) {
    console.error('Failed to release lock:', error.message);
  }
}

export async function releaseAllLocks() {
  for (const sessionId of Array.from(heartbeatTimers.keys())) {
    await releaseLock(sessionId);
  }
}

export { INSTANCE_ID };
