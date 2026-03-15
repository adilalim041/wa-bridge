import { supabase } from '../storage/supabase.js';
import { stopHealthMonitor } from '../monitor.js';
import { acquireLock, releaseLock, startHeartbeat, stopHeartbeat } from '../storage/sessionLock.js';
import { stopVersionChecker } from '../versionChecker.js';
import {
  activateSession,
  clearConnectionState,
  deactivateSession,
  getConnectionState,
  startConnection,
} from './connection.js';

async function updateSessionPhone(sessionId, sock) {
  const phone = sock?.user?.id?.split(':')[0] ?? null;
  if (!phone) {
    return;
  }

  await supabase
    .from('session_config')
    .update({
      phone_number: phone,
      updated_at: new Date().toISOString(),
    })
    .eq('session_id', sessionId);
}

function bindSocketMetadataUpdates(sessionId, sock) {
  sock.ev.on('connection.update', async ({ connection }) => {
    if (connection === 'open') {
      await updateSessionPhone(sessionId, sock);
    }
  });
}

class SessionManager {
  constructor() {
    this.sessions = new Map();
  }

  async startAll() {
    const { data: configs, error } = await supabase
      .from('session_config')
      .select('*')
      .eq('is_active', true)
      .eq('auto_start', true)
      .order('created_at', { ascending: true });

    if (error) {
      console.error('Failed to load session configs:', error.message);
      return;
    }

    if (!configs || configs.length === 0) {
      console.log('No active sessions configured. Add sessions via API.');
      return;
    }

    console.log(`Starting ${configs.length} session(s)...`);

    for (const session of configs) {
      try {
        await this.startSession(session.session_id);
      } catch (error) {
        console.error(`Failed to start session ${session.session_id}:`, error.message);
      }
    }
  }

  async startSession(sessionId) {
    if (this.sessions.has(sessionId)) {
      console.log(`Session ${sessionId} is already running`);
      return this.sessions.get(sessionId);
    }

    console.log(`Starting session: ${sessionId}`);

    const lockAcquired = await acquireLock(sessionId);
    if (!lockAcquired) {
      console.error(`Cannot start ${sessionId} - locked by another instance`);
      return null;
    }

    startHeartbeat(sessionId);
    activateSession(sessionId);

    const entry = { sock: null };
    this.sessions.set(sessionId, entry);

    try {
      const { sock } = await startConnection({
        sessionId,
        onSocket: (newSock) => {
          const currentEntry = this.sessions.get(sessionId);
          if (currentEntry) {
            currentEntry.sock = newSock;
            bindSocketMetadataUpdates(sessionId, newSock);
          }
        },
      });

      entry.sock = sock;
      await updateSessionPhone(sessionId, sock);
      return entry;
    } catch (error) {
      this.sessions.delete(sessionId);
      deactivateSession(sessionId);
      stopHeartbeat(sessionId);
      await releaseLock(sessionId);
      throw error;
    }
  }

  async stopSession(sessionId) {
    const entry = this.sessions.get(sessionId);
    deactivateSession(sessionId);

    if (entry?.sock) {
      console.log(`Stopping session: ${sessionId}`);
      try {
        entry.sock.end?.(undefined);
      } catch {
        // ignore socket shutdown errors
      }
    }

    stopHealthMonitor(sessionId);
    stopVersionChecker(sessionId);
    stopHeartbeat(sessionId);
    await releaseLock(sessionId);
    clearConnectionState(sessionId);
    this.sessions.delete(sessionId);
  }

  async stopAll() {
    const sessionIds = Array.from(this.sessions.keys());
    for (const sessionId of sessionIds) {
      await this.stopSession(sessionId);
    }
  }

  getSession(sessionId) {
    return this.sessions.get(sessionId);
  }

  getSessions() {
    return this.sessions;
  }

  getSessionState(sessionId) {
    return getConnectionState(sessionId);
  }

  getAllStates() {
    const states = {};
    for (const [sessionId] of this.sessions) {
      states[sessionId] = getConnectionState(sessionId);
    }
    return states;
  }
}

export const sessionManager = new SessionManager();
