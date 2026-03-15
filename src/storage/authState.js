import { BufferJSON, initAuthCreds, proto } from 'baileys';
import { logger } from '../config.js';
import { supabase } from './supabase.js';

const TABLE = 'auth_state';

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

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const result = {};

          await Promise.all(
            ids.map(async (id) => {
              try {
                const key = `${sessionId}:${type}:${id}`;
                const { data, error } = await supabase
                  .from(TABLE)
                  .select('value')
                  .eq('key', key)
                  .maybeSingle();

                if (error) {
                  logger.error({ err: error, key }, 'Failed to load auth key');
                  return;
                }

                if (!data?.value) {
                  result[id] = null;
                  return;
                }

                let value = deserialize(data.value);
                if (type === 'app-state-sync-key' && value) {
                  value = proto.Message.AppStateSyncKeyData.fromObject(value);
                }

                result[id] = value;
              } catch (error) {
                logger.error({ err: error, type, id }, 'Unexpected error while loading auth key');
                result[id] = null;
              }
            })
          );

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
      },
    },
    saveCreds: async () => {
      await saveCreds(sessionId, creds);
    },
  };
}
