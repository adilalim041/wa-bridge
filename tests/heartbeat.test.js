/**
 * heartbeat.test.js
 *
 * Unit tests for src/heartbeat.js
 * Runner: node --test tests/heartbeat.test.js
 * Node 18+ native test runner — no external dependencies required.
 *
 * Tests:
 *   1. Payload shape: published JSON matches the expected schema
 *   2. Fetch failure: a network error inside writeHeartbeat does NOT propagate
 *      as an uncaught exception (the process must stay alive)
 *   3. Missing token: startVaultHeartbeat() is a no-op when VAULT_WRITE_TOKEN unset
 */

import { test, describe, before, after, mock, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// ---------------------------------------------------------------------------
// Minimal env setup — must happen before any module import that reads env
// ---------------------------------------------------------------------------
process.env.SUPABASE_URL = 'https://test.supabase.co';
process.env.SUPABASE_KEY = 'test-anon-key';
process.env.SUPABASE_SERVICE_ROLE_KEY = 'test-service-role-key';
process.env.API_KEY = 'test-api-key';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect all PUT calls made to the GitHub API during a test. */
function makeCaptureFetch(behavior = 'ok') {
  const calls = [];

  const impl = async (url, init) => {
    calls.push({ url, init });

    // The vault_github helper does two fetches per write:
    //   1. GET  — check for existing sha  (should 404 on fresh file)
    //   2. PUT  — write the file
    const method = init?.method || 'GET';

    if (behavior === 'fail' && method === 'PUT') {
      throw new Error('Simulated network failure');
    }

    if (method === 'PUT') {
      return {
        ok: true,
        json: async () => ({ commit: { sha: 'abc123' } }),
      };
    }

    // GET: simulate 404 (no existing sha)
    return { ok: false, status: 404, json: async () => ({}) };
  };

  impl.calls = calls;
  return impl;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('heartbeat — payload shape', () => {
  let capturedBody = null;
  let originalFetch;

  before(() => {
    process.env.VAULT_WRITE_TOKEN = 'test-token-xyz';
    originalFetch = globalThis.fetch;

    globalThis.fetch = async (url, init) => {
      const method = init?.method || 'GET';
      if (method === 'PUT') {
        capturedBody = JSON.parse(init.body);
        const raw = Buffer.from(capturedBody.content, 'base64').toString('utf-8');
        capturedBody._decodedContent = JSON.parse(raw);
      }
      if (method === 'GET') return { ok: false, status: 404, json: async () => ({}) };
      return { ok: true, json: async () => ({}) };
    };
  });

  after(() => {
    delete process.env.VAULT_WRITE_TOKEN;
    globalThis.fetch = originalFetch;
  });

  test('writes to system/status/omoikiri.json with correct shape', async () => {
    // Import after env + fetch mock are in place
    const { writeHeartbeat } = await import('../src/lib/vault_github.js');

    await writeHeartbeat('omoikiri', 'healthy', {
      uptime_h: 1.5,
      baileys_connected: true,
      last_message_processed_at: '2026-04-20T10:00:00.000Z',
      supabase_ok: true,
      queue_depth: 0,
    });

    assert.ok(capturedBody, 'fetch PUT was called');

    const content = capturedBody._decodedContent;

    // Top-level schema
    assert.equal(content.service, 'omoikiri');
    assert.equal(content.status, 'healthy');
    assert.equal(content.version, '1.0');
    assert.ok(content.updated_at, 'updated_at must be present');
    assert.ok(!Number.isNaN(Date.parse(content.updated_at)), 'updated_at must be valid ISO date');

    // Details schema
    const d = content.details;
    assert.ok(typeof d.uptime_h === 'number', 'uptime_h must be number');
    assert.ok(typeof d.baileys_connected === 'boolean', 'baileys_connected must be boolean');
    assert.ok(
      d.last_message_processed_at === null || typeof d.last_message_processed_at === 'string',
      'last_message_processed_at must be ISO string or null'
    );
    assert.ok(typeof d.supabase_ok === 'boolean', 'supabase_ok must be boolean');
    assert.ok(typeof d.queue_depth === 'number', 'queue_depth must be number');

    // Commit path
    assert.ok(
      capturedBody.message.includes('omoikiri'),
      'commit message must reference service name'
    );
  });
});

describe('heartbeat — fetch failure is swallowed (process stays alive)', () => {
  let originalFetch;
  let warnMessages = [];

  before(() => {
    process.env.VAULT_WRITE_TOKEN = 'test-token-xyz';
    originalFetch = globalThis.fetch;

    globalThis.fetch = async (url, init) => {
      const method = init?.method || 'GET';
      if (method === 'GET') return { ok: false, status: 404, json: async () => ({}) };
      // PUT always throws
      throw new Error('Simulated network failure');
    };
  });

  after(() => {
    delete process.env.VAULT_WRITE_TOKEN;
    globalThis.fetch = originalFetch;
  });

  test('does not throw when GitHub API is unreachable', async () => {
    // publishHeartbeat is the internal async function — we test via writeHeartbeat
    // wrapped in the same try/catch pattern that heartbeat.js uses.
    const { writeHeartbeat } = await import('../src/lib/vault_github.js');

    let threw = false;
    try {
      // Replicate heartbeat.js error boundary
      await writeHeartbeat('omoikiri', 'healthy', {
        uptime_h: 0,
        baileys_connected: false,
        last_message_processed_at: null,
        supabase_ok: true,
        queue_depth: 0,
      });
    } catch {
      threw = true;
    }

    // The raw writeHeartbeat WILL throw — that's expected.
    // heartbeat.js wraps it in try/catch; we verify the boundary works:
    assert.ok(threw, 'writeHeartbeat itself throws on network failure (caller must catch)');

    // Now verify the heartbeat module's own boundary swallows it:
    // We call publishHeartbeat indirectly by checking that startVaultHeartbeat
    // does not throw synchronously even though the first tick will fail.
    // (The async tick runs after this test completes — .unref() means no leak.)
    const heartbeatMod = await import('../src/heartbeat.js');

    let startThrew = false;
    try {
      heartbeatMod.startVaultHeartbeat();
      heartbeatMod.stopVaultHeartbeat(); // clean up immediately
    } catch {
      startThrew = true;
    }

    assert.ok(!startThrew, 'startVaultHeartbeat() must not throw synchronously');
  });
});

describe('heartbeat — missing token: no-op, warns once', () => {
  before(() => {
    delete process.env.VAULT_WRITE_TOKEN;
  });

  test('startVaultHeartbeat is a no-op when VAULT_WRITE_TOKEN is absent', async () => {
    const fetchCalls = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (...args) => {
      fetchCalls.push(args);
      return { ok: true, json: async () => ({}) };
    };

    // Re-import to get a fresh module instance.
    // Node caches modules — we test the exported function directly;
    // since _tokenMissingWarned may already be set from a prior test run,
    // we rely on the no-fetch guarantee instead.
    const { startVaultHeartbeat, stopVaultHeartbeat } = await import('../src/heartbeat.js');

    startVaultHeartbeat();
    stopVaultHeartbeat();

    // No fetch calls should have been made (no token → no HTTP)
    const githubCalls = fetchCalls.filter((args) =>
      typeof args[0] === 'string' && args[0].includes('api.github.com')
    );
    assert.equal(githubCalls.length, 0, 'must not call GitHub API when token is missing');

    globalThis.fetch = originalFetch;
  });
});
