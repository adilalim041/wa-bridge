// ---------------------------------------------------------------------------
// database.js — Re-exports the shared Supabase client + query helpers
// Adapted from bazav2/server/database.js to use the wa-bridge shared client.
// ---------------------------------------------------------------------------

import { supabase } from '../storage/supabase.js';

// Re-export the shared client (service-role, bypasses RLS)
export { supabase };

// --- Helper: turn a Supabase response into data or throw --------------------

function unwrap({ data, error }) {
  if (error) throw error;
  return data;
}

// --- Query helpers ----------------------------------------------------------

/**
 * Fetch rows from a table with optional filtering, ordering, limit & range.
 *
 * @param {string} table
 * @param {object}  options
 * @param {string}  options.select   - column expression (default '*')
 * @param {object}  options.filters  - key/value pairs applied as .eq()
 * @param {{ column: string, ascending?: boolean }} options.order
 * @param {number}  options.limit
 * @param {[number, number]} options.range - inclusive [from, to]
 * @returns {Promise<object[]>}
 */
export async function queryAll(
  table,
  { select = '*', filters = {}, order, limit, range } = {},
) {
  let query = supabase.from(table).select(select);

  for (const [col, val] of Object.entries(filters)) {
    query = query.eq(col, val);
  }

  if (order) {
    query = query.order(order.column, { ascending: order.ascending ?? true });
  }

  if (range) {
    query = query.range(range[0], range[1]);
  } else if (limit !== undefined) {
    query = query.limit(limit);
  }

  return unwrap(await query);
}

/**
 * Fetch a single row (or null) from a table.
 *
 * @param {string} table
 * @param {object}  options        - same as queryAll (select, filters)
 * @returns {Promise<object|null>}
 */
export async function queryOne(table, { select = '*', filters = {} } = {}) {
  let query = supabase.from(table).select(select);

  for (const [col, val] of Object.entries(filters)) {
    query = query.eq(col, val);
  }

  return unwrap(await query.limit(1).maybeSingle());
}

/**
 * Insert a single row and return it (with generated id).
 */
export async function insertRow(table, row) {
  return unwrap(await supabase.from(table).insert(row).select().single());
}

/**
 * Insert multiple rows and return them.
 */
export async function insertRows(table, rows) {
  return unwrap(await supabase.from(table).insert(rows).select());
}

/**
 * Update rows matching filters and return the updated rows.
 */
export async function updateRows(table, updates, filters = {}) {
  let query = supabase.from(table).update(updates);

  for (const [col, val] of Object.entries(filters)) {
    query = query.eq(col, val);
  }

  return unwrap(await query.select());
}

/**
 * Delete rows matching filters.
 */
export async function deleteRows(table, filters = {}) {
  let query = supabase.from(table).delete();

  for (const [col, val] of Object.entries(filters)) {
    query = query.eq(col, val);
  }

  return unwrap(await query);
}

/**
 * Call a Supabase/Postgres RPC function.
 */
export async function rpc(functionName, params = {}) {
  return unwrap(await supabase.rpc(functionName, params));
}

// --- Pure utility -----------------------------------------------------------

/**
 * Normalise a string value for case-insensitive comparisons.
 */
export function normalizeName(value) {
  return String(value || '').trim().toLowerCase();
}
