/**
 * Single source of truth for manager_issues values.
 *
 * These values are stored in chat_ai.manager_issues, rendered in the dashboard,
 * used by analytics filters, and produced by daily analysis.
 */

export const MANAGER_ISSUES = [
  'slow_response',
  'no_response',
  'no_followup',
  'short_template_only',
  'poor_consultation',
  'no_photos',
  'no_showroom_invite',
  'no_upsell',
  'rude_tone',
  'formal_tone',
  'no_alternative',
];

export const MANAGER_ISSUE_SET = new Set(MANAGER_ISSUES);

export const LEGACY_MANAGER_ISSUES = {
  slow_first_response: 'slow_response',
};

export function resolveManagerIssue(issue) {
  if (!issue) return undefined;
  const value = String(issue).trim();
  if (MANAGER_ISSUE_SET.has(value)) return value;
  return LEGACY_MANAGER_ISSUES[value];
}

export function normalizeManagerIssues(issues, { preserveUnknown = false } = {}) {
  if (!Array.isArray(issues)) return [];
  const out = [];
  const seen = new Set();
  for (const issue of issues) {
    const raw = issue == null ? '' : String(issue).trim();
    const normalized = resolveManagerIssue(issue);
    const value = normalized || (preserveUnknown ? raw : undefined);
    if (!value || seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

export const MANAGER_ISSUE_PROMPT_LIST = MANAGER_ISSUES.join(', ');
