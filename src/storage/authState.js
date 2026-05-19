import { BufferJSON, initAuthCreds, makeCacheableSignalKeyStore, proto } from 'baileys';
import { logger } from '../config.js';
import { supabase } from './supabase.js';

const TABLE = 'auth_state';
const KEY_READ_BATCH_SIZE = 100;
const CREDS_SAVE_DEBOUNCE_MS = 2_000;

function serialize(value) {
  return JSON.stringify(value, BufferJSON.replacer);
}

function deserialize(value) {
  return JSON.parse(value, BufferJSON.reviver);
}

async function loadCreds(sessionId) {
  try {
    const key = `${sessionId}:creds`;
    const { data, error } = await supabase
      .from(TABLE)
      .select('value')
      .eq('key', key)
      .maybeSingle();

    if (error) {
      logger.error({ err: error, sessionId }, 'Failed to load auth credentials');
      return null;
    }

    return data?.value ? deserialize(data.value) : null;
  } catch (error) {
    logger.error({ err: error, sessionId }, 'Unexpected error while loading auth credentials');
    return null;
  }
}

async function saveCreds(sessionId, creds) {
  try {
    const key = `${sessionId}:creds`;
    const { error } = await supabase.from(TABLE).upsert(
      {
        key,
        session_id: sessionId,
        type: 'creds',
        value: serialize(creds),
        updated_at: new Date().toISOString(),
      },
      { onConflict: 'key' }
    );

    if (error) {
      logger.error({ err: error, sessionId }, 'Failed to save auth credentials');
    }
  } catch (error) {
    logger.error({ err: error, sessionId }, 'Unexpected error while saving auth credentials');
  }
}

export async function useSupabaseAuthState(sessionId) {
  const creds = (await loadCreds(sessionId)) || initAuthCreds();
  let credsSaveTimer = null;
  let credsSavePromise = null;
  let credsSaveResolve = null;
  let credsSaveReject = null;

  function scheduleCredsSave() {
    if (!credsSavePromise) {
      credsSavePromise = new Promise((resolve, reject) => {
        credsSaveResolve = resolve;
        credsSaveReject = reject;
      });
    }

    if (credsSaveTimer) {
      clearTimeout(credsSaveTimer);
    }

    credsSaveTimer = setTimeout(async () => {
      credsSaveTimer = null;
      const resolve = credsSaveResolve;
      const reject = credsSaveReject;
      credsSavePromise = null;
      credsSaveResolve = null;
      credsSaveReject = null;

      try {
        await saveCreds(sessionId, creds);
        resolve?.();
      } catch (error) {
        reject?.(error);
      }
    }, CREDS_SAVE_DEBOUNCE_MS);

    return credsSavePromise;
  }

  const keyStore = {
    get: async (type, ids) => {
      const result = {};
      const uniqueIds = [...new Set(ids || [])];

      for (const id of uniqueIds) {
        result[id] = null;
      }

      if (uniqueIds.length === 0) {
        return result;
      }

      const keyPairs = uniqueIds.map((id) => ({
        id,
        key: `${sessionId}:${type}:${id}`,
      }));

      for (let i = 0; i < keyPairs.length; i += KEY_READ_BATCH_SIZE) {
        const batch = keyPairs.slice(i, i + KEY_READ_BATCH_SIZE);

        try {
          const { data, error } = await supabase
            .from(TABLE)
            .select('key, value')
            .in('key', batch.map((entry) => entry.key));

          if (error) {
            logger.error({ err: error, type, count: batch.length }, 'Failed to batch-load auth keys');
            continue;
          }

          const rowsByKey = new Map((data || []).map((row) => [row.key, row.value]));

          for (const { id, key } of batch) {
            const rawValue = rowsByKey.get(key);
            if (!rawValue) {
              result[id] = null;
              continue;
            }

            try {
              let value = deserialize(rawValue);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              result[id] = value;
            } catch (error) {
              logger.error({ err: error, type, id }, 'Failed to deserialize auth key');
              result[id] = null;
            }
          }
        } catch (error) {
          logger.error({ err: error, type, count: batch.length }, 'Unexpected error while batch-loading auth keys');
        }
      }

      return result;
    },
    set: async (data) => {
      try {
        const upserts = [];
        const deletions = [];

        for (const [type, entries] of Object.entries(data)) {
          for (const [id, value] of Object.entries(entries)) {
            const key = `${sessionId}:${type}:${id}`;
            if (value) {
              upserts.push({
                key,
                session_id: sessionId,
                type,
                value: serialize(value),
                updated_at: new Date().toISOString(),
              });
            } else {
              deletions.push(
                supabase.from(TABLE).delete().eq('key', key)
              );
            }
          }
        }

        if (deletions.length > 0) {
          await Promise.all(deletions);
        }

        if (upserts.length > 0) {
          const { error } = await supabase.from(TABLE).upsert(upserts, {
            onConflict: 'key',
          });

          if (error) {
            logger.error({ err: error, sessionId }, 'Failed to persist auth keys');
          }
        }
      } catch (error) {
        logger.error({ err: error, sessionId }, 'Unexpected error while persisting auth keys');
      }
    },
  };

  const cachedKeyStore = makeCacheableSignalKeyStore(
    keyStore,
    logger.child?.({ module: 'baileys-auth-cache', sessionId }) ?? logger
  );

  return {
    state: {
      creds,
      keys: cachedKeyStore,
    },
    saveCreds: async () => {
      await scheduleCredsSave();
    },
  };
}
