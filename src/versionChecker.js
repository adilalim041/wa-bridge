import { fetchLatestBaileysVersion } from 'baileys';

let currentVersion = null;
let checkInterval = null;

export function setCurrentVersion(version) {
  currentVersion = version;
}

export function startVersionChecker(onVersionChanged) {
  const CHECK_INTERVAL = 24 * 60 * 60 * 1000;

  if (checkInterval) {
    return;
  }

  checkInterval = setInterval(async () => {
    try {
      const { version } = await fetchLatestBaileysVersion();
      const newVersionStr = version.join('.');
      const currentVersionStr = currentVersion ? currentVersion.join('.') : 'unknown';

      if (currentVersionStr !== newVersionStr) {
        console.log(`WA version changed: ${currentVersionStr} -> ${newVersionStr}. Reconnecting...`);
        currentVersion = version;
        if (typeof onVersionChanged === 'function') {
          onVersionChanged(version);
        }
      }
    } catch {
      console.log('Version check failed, will retry in 24h');
    }
  }, CHECK_INTERVAL);
}

export function stopVersionChecker() {
  if (checkInterval) {
    clearInterval(checkInterval);
    checkInterval = null;
  }
}
