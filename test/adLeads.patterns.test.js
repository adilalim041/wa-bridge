import test from 'node:test';
import assert from 'node:assert/strict';

import { isAdLeadSession, matchAdLeadPattern } from '../src/lib/adLeads.js';

test('matches a known advertising template', () => {
  const match = matchAdLeadPattern('Хочу сантехнику от ОМОЙКИРИ по скидке!');
  assert.equal(match?.key, 'discount_sanitary');
});

test('does not treat a generic product question as an advertising template', () => {
  assert.equal(matchAdLeadPattern('Здравствуйте, сколько стоит мойка?'), null);
});

test('only configured WhatsApp accounts are advertising sources', () => {
  assert.equal(isAdLeadSession('astana-renat-rabochiy-reklama'), true);
  assert.equal(isAdLeadSession('almaty-rabochiy-reklama'), true);
  assert.equal(isAdLeadSession('astana-nursultan'), false);
});
