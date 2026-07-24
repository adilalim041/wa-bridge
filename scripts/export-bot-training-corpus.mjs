import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

import { createClient } from '@supabase/supabase-js';

import {
  buildTrainingExample,
  isEligibleAnalysis,
  isEligibleBusinessContact,
} from '../src/bot/trainingCorpus.js';

const args = process.argv.slice(2);
const readArg = (name, fallback) => {
  const index = args.indexOf(name);
  return index >= 0 ? args[index + 1] : fallback;
};

const limit = Math.max(1, Math.min(Number(readArg('--limit', 1000)) || 1000, 5000));
const outputDir = path.resolve(readArg('--out-dir', 'output/bot-corpus'));
const supabaseUrl = process.env.SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !serviceRoleKey) {
  throw new Error('SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
}

const supabase = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false, autoRefreshToken: false },
});

function chunks(values, size) {
  const result = [];
  for (let index = 0; index < values.length; index += size) {
    result.push(values.slice(index, index + size));
  }
  return result;
}

async function fetchAnalyses() {
  const pageSize = 1000;
  const rows = [];

  for (let from = 0; from < 20000; from += pageSize) {
    const { data, error } = await supabase
      .from('chat_ai')
      .select(
        'id,dialog_session_id,session_id,remote_jid,analysis_date,analyzed_at,customer_type,intent,lead_source,deal_stage,consultation_score,manager_issues,risk_flags',
      )
      .not('dialog_session_id', 'is', null)
      .not('remote_jid', 'is', null)
      .order('analyzed_at', { ascending: false })
      .range(from, from + pageSize - 1);

    if (error) throw new Error(`chat_ai read failed: ${error.message}`);
    rows.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }

  const latestByDialog = new Map();
  for (const row of rows) {
    const key = String(row.dialog_session_id);
    if (!latestByDialog.has(key)) latestByDialog.set(key, row);
  }

  return [...latestByDialog.values()].filter(isEligibleAnalysis);
}

async function fetchTags(remoteJids) {
  const tagsByJid = new Map();
  for (const part of chunks([...new Set(remoteJids)], 200)) {
    const { data, error } = await supabase
      .from('chat_tags')
      .select('remote_jid,tags')
      .in('remote_jid', part);
    if (error) throw new Error(`chat_tags read failed: ${error.message}`);
    for (const row of data || []) tagsByJid.set(row.remote_jid, row.tags || []);
  }
  return tagsByJid;
}

function selectDiverseAnalyses(analyses, tagsByJid) {
  const eligible = analyses.filter((row) => isEligibleBusinessContact(tagsByJid.get(row.remote_jid)));
  const buckets = new Map();

  for (const row of eligible) {
    const key = [
      row.session_id || 'unknown',
      row.customer_type || 'unknown',
      row.intent || 'unknown',
      row.deal_stage || 'unknown',
    ].join('|');
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key).push(row);
  }

  const selected = [];
  while (selected.length < limit) {
    let added = false;
    for (const bucket of buckets.values()) {
      const row = bucket.shift();
      if (!row) continue;
      selected.push(row);
      added = true;
      if (selected.length >= limit) break;
    }
    if (!added) break;
  }
  return selected;
}

async function fetchMessages(dialogIds) {
  const messagesByDialog = new Map();
  for (const part of chunks(dialogIds, 80)) {
    const { data, error } = await supabase
      .from('messages')
      .select('dialog_session_id,from_me,timestamp,body,message_type')
      .in('dialog_session_id', part)
      .order('timestamp', { ascending: true })
      .limit(10000);
    if (error) throw new Error(`messages read failed: ${error.message}`);

    for (const row of data || []) {
      const key = String(row.dialog_session_id);
      if (!messagesByDialog.has(key)) messagesByDialog.set(key, []);
      messagesByDialog.get(key).push(row);
    }
  }
  return messagesByDialog;
}

async function atomicWrite(filePath, content) {
  const tempPath = `${filePath}.tmp`;
  await fs.writeFile(tempPath, content, 'utf8');
  await fs.rename(tempPath, filePath);
}

const analyses = await fetchAnalyses();
const tagsByJid = await fetchTags(analyses.map((row) => row.remote_jid));
const selected = selectDiverseAnalyses(analyses, tagsByJid);
const messagesByDialog = await fetchMessages(selected.map((row) => row.dialog_session_id));

const examples = selected
  .map((analysis) =>
    buildTrainingExample({
      analysis,
      tags: tagsByJid.get(analysis.remote_jid),
      messages: messagesByDialog.get(String(analysis.dialog_session_id)) || [],
    }),
  )
  .filter(Boolean);

const categoryCounts = {};
for (const example of examples) {
  const key = `${example.metadata.contact_type}:${example.metadata.intent}`;
  categoryCounts[key] = (categoryCounts[key] || 0) + 1;
}

const manifest = {
  schema: 'omoikiri-wa-training-manifest/v1',
  generated_at: new Date().toISOString(),
  requested_limit: limit,
  eligible_analyses: analyses.length,
  selected_dialogs: selected.length,
  exported_examples: examples.length,
  category_counts: categoryCounts,
  privacy: {
    includes_contact_identity_fields: false,
    includes_phone_number_fields: false,
    includes_remote_jids: false,
    includes_media_urls: false,
    redacts_emails_urls_and_phones: true,
    free_text_may_contain_customer_provided_names_or_addresses: true,
  },
  runtime: {
    database_writes: false,
    whatsapp_sends: false,
    bot_mode_changed: false,
  },
};

await fs.mkdir(outputDir, { recursive: true });
await atomicWrite(
  path.join(outputDir, 'omoikiri-wa-training-v1.jsonl'),
  `${examples.map((example) => JSON.stringify(example)).join('\n')}\n`,
);
await atomicWrite(path.join(outputDir, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`);

console.log(JSON.stringify({ outputDir, ...manifest }, null, 2));
