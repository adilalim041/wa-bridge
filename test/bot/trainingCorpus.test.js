import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildConversationTurns,
  buildTrainingExample,
  isEligibleAnalysis,
  isEligibleBusinessContact,
  redactTrainingText,
} from '../../src/bot/trainingCorpus.js';

test('only confirmed business contact tags are eligible', () => {
  assert.equal(isEligibleBusinessContact(['клиент']), true);
  assert.equal(isEligibleBusinessContact(['партнёр']), true);
  assert.equal(isEligibleBusinessContact(['клиент', 'сотрудник']), false);
  assert.equal(isEligibleBusinessContact(['неизвестно']), false);
  assert.equal(isEligibleBusinessContact([]), false);
});

test('redacts common identifiers from message text', () => {
  const result = redactTrainingText(
    'Пишите +7 777 123 45 67 или test@example.com, сайт https://example.com/a',
  );
  assert.equal(result, 'Пишите [phone] или [email], сайт [url]');
});

test('groups consecutive messages and keeps only complete user-assistant pairs', () => {
  const turns = buildConversationTurns([
    { from_me: true, timestamp: '2026-01-01T10:00:00Z', body: 'Старое приветствие' },
    { from_me: false, timestamp: '2026-01-01T10:01:00Z', body: 'Здравствуйте' },
    { from_me: false, timestamp: '2026-01-01T10:02:00Z', body: 'Нужна мойка' },
    { from_me: true, timestamp: '2026-01-01T10:03:00Z', body: 'Добрый день. Какой размер?' },
    { from_me: false, timestamp: '2026-01-01T10:04:00Z', body: 'Пока уточню' },
  ]);

  assert.deepEqual(turns, [
    { role: 'user', content: 'Здравствуйте Нужна мойка' },
    { role: 'assistant', content: 'Добрый день. Какой размер?' },
  ]);
});

test('requires a strong analysis without manager issues or risks', () => {
  const base = {
    customer_type: 'end_client',
    consultation_score: 8,
    manager_issues: [],
    risk_flags: [],
  };

  assert.equal(isEligibleAnalysis(base), true);
  assert.equal(isEligibleAnalysis({ ...base, consultation_score: 6 }), false);
  assert.equal(isEligibleAnalysis({ ...base, manager_issues: ['no_followup'] }), false);
  assert.equal(isEligibleAnalysis({ ...base, customer_type: 'employee' }), false);
});

test('builds a sanitized training example and excludes non-business contacts', () => {
  const analysis = {
    customer_type: 'partner',
    consultation_score: 9,
    manager_issues: [],
    risk_flags: [],
    intent: 'product_consultation',
    lead_source: 'organic',
    deal_stage: 'consultation',
  };
  const messages = [
    { from_me: false, timestamp: '2026-01-01T10:00:00Z', body: 'Каталог на test@example.com' },
    { from_me: true, timestamp: '2026-01-01T10:01:00Z', body: 'Отправлю на test@example.com' },
  ];

  const example = buildTrainingExample({ analysis, tags: ['партнёр'], messages });
  assert.equal(example.schema, 'omoikiri-wa-training/v1');
  assert.equal(example.turns[0].content, 'Каталог на [email]');
  assert.equal(buildTrainingExample({ analysis, tags: ['сотрудник'], messages }), null);
});
