import 'dotenv/config';
import { fileURLToPath } from 'url';
import { supabase as defaultDb } from '../storage/supabase.js';
import {
  CUSTOMER_TYPES,
  LEGACY_CUSTOMER_TYPES,
} from '../ai/tagConstants.js';
import {
  LEAD_SOURCE_CANONICAL,
  canonicalLeadSource,
} from '../ai/leadSourceConstants.js';
import { normalizeManagerIssues } from '../ai/managerIssueConstants.js';

const CUSTOMER_TYPE_SET = new Set(CUSTOMER_TYPES);
const LEAD_SOURCE_SET = new Set(LEAD_SOURCE_CANONICAL);

function canonicalCustomerType(rawValue) {
  if (!rawValue) return 'unknown';
  const value = String(rawValue).trim();
  if (CUSTOMER_TYPE_SET.has(value)) return value;
  return LEGACY_CUSTOMER_TYPES[value] || 'unknown';
}

function canonicalSource(rawValue) {
  const value = canonicalLeadSource(rawValue);
  return LEAD_SOURCE_SET.has(value) ? value : 'unknown';
}

async function fetchChatAiRows(db) {
  const out = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    const { data, error } = await db
      .from('chat_ai')
      .select('id, customer_type, lead_source, manager_issues')
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`chat_ai fetch failed: ${error.message}`);
    out.push(...(data || []));
    if (!data || data.length < pageSize) break;
  }
  return out;
}

function buildPatch(row) {
  const nextCustomerType = canonicalCustomerType(row.customer_type);
  const nextLeadSource = canonicalSource(row.lead_source);
  const nextIssues = normalizeManagerIssues(row.manager_issues || []);

  const patch = {};
  if ((row.customer_type || null) !== nextCustomerType) patch.customer_type = nextCustomerType;
  if ((row.lead_source || null) !== nextLeadSource) patch.lead_source = nextLeadSource;
  if (JSON.stringify(row.manager_issues || []) !== JSON.stringify(nextIssues)) {
    patch.manager_issues = nextIssues;
  }

  return patch;
}

export async function normalizeChatAiHistory({ db = defaultDb, dryRun = true } = {}) {
  const rows = await fetchChatAiRows(db);
  const changes = [];

  for (const row of rows) {
    const patch = buildPatch(row);
    if (Object.keys(patch).length === 0) continue;
    changes.push({
      id: row.id,
      before: {
        customer_type: row.customer_type,
        lead_source: row.lead_source,
        manager_issues: row.manager_issues || [],
      },
      patch,
    });
  }

  if (dryRun) {
    return {
      dry_run: true,
      scanned_rows: rows.length,
      changed_rows: changes.length,
      customer_type_changes: changes.filter((c) => 'customer_type' in c.patch).length,
      lead_source_changes: changes.filter((c) => 'lead_source' in c.patch).length,
      manager_issue_changes: changes.filter((c) => 'manager_issues' in c.patch).length,
      sample: changes.slice(0, 20),
    };
  }

  let updated = 0;
  for (const change of changes) {
    const { error } = await db
      .from('chat_ai')
      .update(change.patch)
      .eq('id', change.id);
    if (error) throw new Error(`chat_ai normalize failed ${change.id}: ${error.message}`);
    updated++;
  }

  return {
    dry_run: false,
    scanned_rows: rows.length,
    changed_rows: changes.length,
    updated,
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const result = await normalizeChatAiHistory({
    dryRun: !process.argv.includes('--apply'),
  });
  console.log(JSON.stringify(result, null, 2));
}
