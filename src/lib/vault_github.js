/**
 * vault_github.js — helper for writing to the ObsidianVault via GitHub Contents API.
 *
 * Copied from ObsidianVault/system/lib/vault_github.js (Node 18+ global fetch required).
 *
 * Usage:
 *   import { writeVault, readVault, listVault, writeHeartbeat } from './lib/vault_github.js';
 *   await writeHeartbeat('omoikiri', 'healthy', details);
 *
 * Env:
 *   VAULT_WRITE_TOKEN — fine-grained PAT, scope ObsidianVault/Contents=R/W
 *   VAULT_REPO        — defaults to "adilalim041/ObsidianVault"
 *   VAULT_BRANCH      — defaults to "main"
 *
 * Security: NEVER log the Authorization header or VAULT_WRITE_TOKEN value.
 */

const REPO = process.env.VAULT_REPO || 'adilalim041/ObsidianVault';
const BRANCH = process.env.VAULT_BRANCH || 'main';
const API = 'https://api.github.com';

function assertToken() {
  if (!process.env.VAULT_WRITE_TOKEN) {
    throw new Error('VAULT_WRITE_TOKEN env missing');
  }
}

function headers() {
  assertToken();
  return {
    Authorization: `Bearer ${process.env.VAULT_WRITE_TOKEN}`,
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
  };
}

/**
 * Read file from vault. Returns string or null if 404.
 */
export async function readVault(path) {
  const url = `${API}/repos/${REPO}/contents/${encodeURIComponent(path).replace(/%2F/g, '/')}?ref=${BRANCH}`;
  const res = await fetch(url, { headers: headers() });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`readVault ${path}: ${res.status}`);
  const json = await res.json();
  return Buffer.from(json.content, 'base64').toString('utf-8');
}

/**
 * Raw read (no auth, public repo). Preferred for read-only/high-frequency polling.
 */
export async function readVaultRaw(path) {
  const url = `https://raw.githubusercontent.com/${REPO}/${BRANCH}/${path}`;
  const res = await fetch(url);
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`readVaultRaw ${path}: ${res.status}`);
  return await res.text();
}

/**
 * Write (create or update) file. Handles sha lookup for updates automatically.
 */
export async function writeVault(path, content, commitMessage) {
  const url = `${API}/repos/${REPO}/contents/${encodeURIComponent(path).replace(/%2F/g, '/')}`;

  // Fetch current sha if the file already exists (required for updates)
  let sha = null;
  const existing = await fetch(`${url}?ref=${BRANCH}`, { headers: headers() });
  if (existing.ok) {
    sha = (await existing.json()).sha;
  }

  const body = {
    message: commitMessage || `chore(${path}): update`,
    content: Buffer.from(content, 'utf-8').toString('base64'),
    branch: BRANCH,
  };
  if (sha) body.sha = sha;

  const res = await fetch(url, {
    method: 'PUT',
    headers: { ...headers(), 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`writeVault ${path}: ${res.status} ${text}`);
  }
  return await res.json();
}

/**
 * List files in a directory (non-recursive).
 * Returns array of { name, path, sha, type }.
 */
export async function listVault(dirPath) {
  const url = `${API}/repos/${REPO}/contents/${encodeURIComponent(dirPath).replace(/%2F/g, '/')}?ref=${BRANCH}`;
  const res = await fetch(url, { headers: headers() });
  if (res.status === 404) return [];
  if (!res.ok) throw new Error(`listVault ${dirPath}: ${res.status}`);
  return await res.json();
}

/**
 * Write the current service heartbeat to system/status/{service}.json.
 * Always bumps updated_at, even if details are unchanged.
 *
 * @param {string} service  - service identifier, e.g. "omoikiri"
 * @param {string} status   - "healthy" | "degraded" | "down" | "unknown"
 * @param {object} details  - service-specific fields (arbitrary JSON)
 */
export async function writeHeartbeat(service, status, details = {}) {
  const payload = {
    service,
    updated_at: new Date().toISOString(),
    status,
    details,
    version: '1.0',
  };
  return writeVault(
    `system/status/${service}.json`,
    JSON.stringify(payload, null, 2) + '\n',
    `status(${service}): heartbeat`
  );
}
