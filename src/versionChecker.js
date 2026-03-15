import { fetchLatestBaileysVersion } from 'baileys';

let currentVersion = null;
let checkInterval = null;
const listeners = new Map();

export function setCurrentVersion(version) {
  currentVersion = version;
}

export function startVersionChecker(sessionId, onVersionChanged) {
  const CHECK_INTERVAL = 24 * 60 * 60 * 1000;

  if (sessionId && typeof onVersionChanged === 'function') {
    listeners.set(sessionId, onVersionChanged);
  }

  if (!checkInterval) {
    checkInterval = setInterval(async () => {
      try {
        const { version } = await fetchLatestBaileysVersion();
        const newVersionStr = version.join('.');
        const currentVersionStr = currentVersion ? currentVersion.join('.') : 'unknown';

        if (currentVersionStr !== newVersionStr) {
          console.log(`WA version changed: ${currentVersionStr} -> ${newVersionStr}. Reconnecting...`);
          currentVersion = version;
          for (const listener of listeners.values()) {
            try {
              listener(version);
            } catch {
              // ignore listener failures
            }
          }
        }
      } catch {
        console.log('Version check failed, will retry in 24h');
      }
    }, CHECK_INTERVAL);
  }
}

export function stopVersionChecker(sessionId) {
  if (sessionId) {
    listeners.delete(sessionId);
  } else {
    listeners.clear();
  }

  if (listeners.size > 0) {
    return;
  }

  if (checkInterval) {
    clearInterval(checkInterval);
    checkInterval = null;
  }
}
