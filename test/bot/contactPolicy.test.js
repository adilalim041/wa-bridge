import assert from 'node:assert/strict';
import test from 'node:test';
import { evaluateBotContactPolicy } from '../../src/bot/contactPolicy.js';

const baseInput = {
  fromMe: false,
  chatType: 'personal',
  tagLookupOk: true,
  messageAgeMs: 1_000,
};

test('allows confirmed business contacts', () => {
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: ['клиент'] }),
    { eligible: true, reason: 'business_contact' }
  );
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: ['партнёр'] }),
    { eligible: true, reason: 'business_contact' }
  );
});

test('denied tags override a business tag', () => {
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: ['клиент', 'сотрудник'] }),
    { eligible: false, reason: 'denied_contact_tag' }
  );
});

test('denies employee, spam, personal and unknown contacts', () => {
  for (const tag of ['сотрудник', 'спам', 'личное', 'неизвестно']) {
    assert.deepEqual(
      evaluateBotContactPolicy({ ...baseInput, tags: [tag] }),
      { eligible: false, reason: 'denied_contact_tag' }
    );
  }
});

test('fails closed when tags are missing or unavailable', () => {
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: [] }),
    { eligible: false, reason: 'unclassified_contact' }
  );
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: ['клиент'], tagLookupOk: false }),
    { eligible: false, reason: 'tag_lookup_failed' }
  );
});

test('rejects outgoing, group and stale messages', () => {
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: ['клиент'], fromMe: true }),
    { eligible: false, reason: 'outgoing_message' }
  );
  assert.deepEqual(
    evaluateBotContactPolicy({ ...baseInput, tags: ['клиент'], chatType: 'group' }),
    { eligible: false, reason: 'non_personal_chat' }
  );
  assert.deepEqual(
    evaluateBotContactPolicy({
      ...baseInput,
      tags: ['клиент'],
      messageAgeMs: 20 * 60 * 1000,
    }),
    { eligible: false, reason: 'stale_message' }
  );
});
