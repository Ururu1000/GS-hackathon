import { SEVERITIES, STATUSES, type AlertStatus, type FeedAlert, type Severity } from '../api/types'

export interface Filters {
  status: ReadonlySet<AlertStatus>
  severity: ReadonlySet<Severity>
  /** Rule id, or '' for all rules. */
  rule: string
  query: string
}

export const defaultFilters: Filters = {
  status: new Set<AlertStatus>(['open', 'acked']),
  severity: new Set<Severity>(SEVERITIES),
  rule: '',
  query: '',
}

export function matches(a: FeedAlert, f: Filters, skip?: 'status' | 'severity' | 'rule'): boolean {
  if (skip !== 'status' && !f.status.has(a.status)) return false
  if (skip !== 'severity' && !f.severity.has(a.severity)) return false
  if (skip !== 'rule' && f.rule && a.rule.id !== f.rule) return false
  if (f.query) {
    const q = f.query.toLowerCase()
    const hay = `${a.title}\n${a.body}\n${a.rule.name}\n${a.team}\n${a.fields.map((x) => x.value).join('\n')}`.toLowerCase()
    if (!hay.includes(q)) return false
  }
  return true
}

/** Counts per facet value, each computed with the other filters applied. */
export function facetCounts(alerts: Iterable<FeedAlert>, f: Filters) {
  const status = Object.fromEntries(STATUSES.map((s) => [s, 0])) as Record<AlertStatus, number>
  const severity = Object.fromEntries(SEVERITIES.map((s) => [s, 0])) as Record<Severity, number>
  const rules = new Map<string, { name: string; count: number }>()
  for (const a of alerts) {
    if (matches(a, f, 'status')) status[a.status]++
    if (matches(a, f, 'severity')) severity[a.severity]++
    const r = rules.get(a.rule.id) ?? { name: a.rule.name, count: 0 }
    if (matches(a, f, 'rule')) r.count++
    rules.set(a.rule.id, r)
  }
  return { status, severity, rules }
}

// Persist filters per browser (a convenience, never required).
const KEY = 'beacon.filters.v1'

export function loadFilters(): Filters {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return defaultFilters
    const j = JSON.parse(raw) as { status?: string[]; severity?: string[]; rule?: string }
    return {
      status: new Set((j.status ?? []).filter((s): s is AlertStatus => (STATUSES as readonly string[]).includes(s))),
      severity: new Set((j.severity ?? []).filter((s): s is Severity => (SEVERITIES as readonly string[]).includes(s))),
      rule: typeof j.rule === 'string' ? j.rule : '',
      query: '',
    }
  } catch {
    return defaultFilters
  }
}

export function saveFilters(f: Filters) {
  try {
    localStorage.setItem(KEY, JSON.stringify({ status: [...f.status], severity: [...f.severity], rule: f.rule }))
  } catch {
    /* storage unavailable */
  }
}
