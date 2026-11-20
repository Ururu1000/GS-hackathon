// Pure helpers for the rule editor: condition-tree edits by node address, YAML
// round-tripping, path suggestions, starting templates and sample events.

import { Document, isScalar, parseDocument, visit, YAMLMap, YAMLSeq } from 'yaml'
import type { Condition, CondOp, Leaf, RuleSpec, Scalar } from '../api/types'

// ---- condition tree ----------------------------------------------------------

export const isLeaf = (c: Condition): c is Leaf => typeof (c as Leaf).op === 'string'
export const isGroup = (c: Condition): c is { all: Condition[] } | { any: Condition[] } => 'all' in c || 'any' in c

export function kids(c: Condition): Condition[] {
  if ('all' in c) return c.all
  if ('any' in c) return c.any
  if ('not' in c) return [c.not]
  return []
}

function withKids(c: Condition, next: Condition[]): Condition {
  if ('all' in c) return { all: next }
  if ('any' in c) return { any: next }
  if ('not' in c) return { not: next[0]! }
  return c
}

const addr = (node: string) => (node === '' ? [] : node.split('.').map(Number))

export function getNode(root: Condition, node: string): Condition | undefined {
  let c: Condition | undefined = root
  for (const i of addr(node)) c = c ? kids(c)[i] : undefined
  return c
}

/** Replace the node at `node` (`""` = root); `null` removes it from its group. */
export function setNode(root: Condition, node: string, next: Condition | null): Condition {
  const path = addr(node)
  if (path.length === 0) return next ?? { all: [] }
  const rec = (c: Condition, depth: number): Condition => {
    const i = path[depth]!
    const list = kids(c)
    if (depth === path.length - 1) {
      if (next === null) {
        // Removing the only child of a `not` removes the `not` too: handled by the caller.
        return withKids(c, list.filter((_, j) => j !== i))
      }
      return withKids(c, list.map((x, j) => (j === i ? next : x)))
    }
    return withKids(c, list.map((x, j) => (j === i ? rec(x, depth + 1) : x)))
  }
  return rec(root, 0)
}

export const parentOf = (node: string) => (node.includes('.') ? node.slice(0, node.lastIndexOf('.')) : '')

export function addChild(root: Condition, group: string, child: Condition): Condition {
  const g = getNode(root, group)
  if (!g || !isGroup(g)) return root
  return setNode(root, group, withKids(g, [...kids(g), child]))
}

export function moveChild(root: Condition, node: string, delta: -1 | 1): Condition {
  const parent = parentOf(node)
  const g = getNode(root, parent)
  if (!g || !isGroup(g)) return root
  const list = [...kids(g)]
  const i = Number(node.split('.').pop())
  const j = i + delta
  if (j < 0 || j >= list.length) return root
  ;[list[i], list[j]] = [list[j]!, list[i]!]
  return setNode(root, parent, withKids(g, list))
}

/** Remove a node; a `not` left empty goes with it. */
export function removeNode(root: Condition, node: string): Condition {
  let target = node
  while (target !== '' && 'not' in (getNode(root, parentOf(target)) ?? {})) target = parentOf(target)
  if (target === '') return { all: [] }
  return setNode(root, target, null)
}

export function toggleNot(root: Condition, node: string): Condition {
  const c = getNode(root, node)
  if (!c) return root
  return setNode(root, node, 'not' in c ? c.not : { not: c })
}

export const newLeaf = (path = 'raw.', op: CondOp = 'eq', value: Scalar | Scalar[] = ''): Leaf => ({ path, op, value })

export const OP_LABEL: Record<CondOp, string> = {
  eq: 'is',
  neq: 'is not',
  gt: 'is more than',
  gte: 'is at least',
  lt: 'is less than',
  lte: 'is at most',
  in: 'is one of',
  not_in: 'is none of',
  contains: 'contains',
  starts_with: 'starts with',
  regex: 'matches regex',
  exists: 'is present',
  not_exists: 'is missing',
}

export const NO_VALUE: readonly CondOp[] = ['exists', 'not_exists']
export const LIST_OPS: readonly CondOp[] = ['in', 'not_in']

/** Fix the value's shape after an operator change. */
export function retargetOp(leaf: Leaf, op: CondOp): Leaf {
  const { value, ...rest } = leaf
  if (NO_VALUE.includes(op)) return { ...rest, op }
  const list = Array.isArray(value) ? value : value === undefined || value === '' ? [] : [value]
  if (LIST_OPS.includes(op)) return { ...rest, op, value: list }
  return { ...rest, op, value: Array.isArray(value) ? (value[0] ?? '') : (value ?? '') }
}

// ---- typed values ---------------------------------------------------------------

export type ValueKind = 'text' | 'number' | 'bool'

export const kindOf = (v: unknown): ValueKind => (typeof v === 'number' ? 'number' : typeof v === 'boolean' ? 'bool' : 'text')

export function parseScalar(text: string, kind: ValueKind): Scalar {
  if (kind === 'number') {
    const n = Number(text.replace(/[_,\s]/g, ''))
    return text.trim() !== '' && Number.isFinite(n) ? n : text
  }
  if (kind === 'bool') return text.trim().toLowerCase() === 'true'
  return text
}

export const showScalar = (v: unknown) => (v === undefined || v === null ? '' : String(v))

// ---- paths from an event, for suggestions ------------------------------------------

export function flattenPaths(prefix: string, v: unknown, out: { path: string; value: unknown }[] = [], depth = 0): { path: string; value: unknown }[] {
  if (depth > 8 || out.length > 400) return out
  if (v !== null && typeof v === 'object') {
    const entries = Array.isArray(v) ? v.slice(0, 3).map((x, i) => [String(i), x] as const) : Object.entries(v)
    for (const [k, x] of entries) flattenPaths(prefix ? `${prefix}.${k}` : k, x, out, depth + 1)
  } else if (prefix) out.push({ path: prefix, value: v })
  return out
}

// ---- YAML ---------------------------------------------------------------------------

/** Config-file style: leaves, field entries and short lists on one line. */
export function toYaml(rule: RuleSpec): string {
  const doc = new Document(rule)
  visit(doc, {
    Map(_, node) {
      if (node instanceof YAMLMap && (node.has('op') || (node.has('path') && node.has('label') && node.items.length === 2))) node.flow = true
    },
    Seq(_, node) {
      if (node instanceof YAMLSeq && node.items.every((i) => isScalar(i))) node.flow = true
    },
  })
  return doc.toString({ lineWidth: 0 })
}

export type YamlResult = { ok: true; rule: RuleSpec } | { ok: false; message: string; line: number | null; col: number | null }

export function fromYaml(text: string): YamlResult {
  const doc = parseDocument(text, { prettyErrors: true, uniqueKeys: true })
  const e = doc.errors[0]
  if (e) {
    const pos = e.linePos?.[0]
    return { ok: false, message: e.message.split('\n')[0]!.replace(/ at line \d+, column \d+:?$/, ''), line: pos?.line ?? null, col: pos?.col ?? null }
  }
  const v = doc.toJS() as unknown
  if (v === null || typeof v !== 'object' || Array.isArray(v))
    return { ok: false, message: 'A rule is a mapping (id:, team:, when:, …)', line: 1, col: 1 }
  const r = v as Partial<RuleSpec>
  for (const k of ['id', 'team', 'when', 'route', 'present'] as const)
    if (r[k] === undefined) return { ok: false, message: `Missing \`${k}:\``, line: null, col: null }
  return { ok: true, rule: r as RuleSpec }
}

// ---- misc ------------------------------------------------------------------------------

export const slugify = (s: string) =>
  s
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 63)

export const ruleEquals = (a: RuleSpec, b: RuleSpec) => JSON.stringify(a) === JSON.stringify(b)

// ---- samples & starting points (Appendix A) ------------------------------------------------

export interface SampleEvent {
  id: string
  label: string
  source: string
  event: unknown
}

export const SAMPLES: SampleEvent[] = [
  {
    id: 'workflow-failed',
    label: 'Workflow failure (Kafka)',
    source: 'kafka:workflows',
    event: {
      type: 'WORKFLOW_FAILED',
      workflowId: 'WF-20251023-0042',
      workflowName: 'order-settlement',
      step: 'validate-counterparty',
      severity: 'ERROR',
      environment: 'prod',
      timestamp: '2026-10-23T09:14:02.331Z',
      error: { code: 'TIMEOUT', message: 'Counterparty service did not respond in 5000 ms' },
    },
  },
  {
    id: 'prometheus',
    label: 'Metric threshold breach (webhook)',
    source: 'http:alertmanager',
    event: {
      alertname: 'HighErrorRate',
      labels: { service: 'payments-api', region: 'eu-west', severity: 'critical' },
      annotations: { summary: 'Error rate 12% over the last 5 minutes' },
      startsAt: '2026-10-23T09:13:00Z',
      status: 'firing',
    },
  },
  {
    id: 'trade-rejected',
    label: 'Business event (Kafka, envelope)',
    source: 'kafka:trades',
    event: {
      meta: { schema: 'trade.rejected.v1', producer: 'trade-gateway', ts: 1761210842000 },
      payload: { tradeId: 'T-99877', desk: 'rates', notional: 25000000, currency: 'EUR', reason: 'LIMIT_BREACH' },
    },
  },
  {
    id: 'pricing-log',
    label: 'Log line (pricing-engine)',
    source: 'file:pricing-engine',
    event: {
      ts: '2026-10-23T09:15:40.112Z',
      level: 'WARN',
      service: 'pricing-engine',
      instance: 'pricing-engine-2',
      instrument: 'EURUSD 5Y',
      latency_ms: 1840,
      msg: 'quote latency above threshold',
    },
  },
]

export interface RuleTemplate {
  id: string
  label: string
  description: string
  sample: string
  make(team: string): RuleSpec
}

export const TEMPLATES: RuleTemplate[] = [
  {
    id: 'workflow',
    label: 'Workflow failures',
    description: 'A workflow or job failed in production.',
    sample: 'workflow-failed',
    make: (team) => ({
      id: `${team}-workflow-failed`,
      name: 'Workflow failed (prod)',
      team,
      enabled: true,
      when: {
        all: [
          { path: 'fields.type', op: 'eq', value: 'WORKFLOW_FAILED' },
          { path: 'fields.environment', op: 'eq', value: 'prod' },
        ],
      },
      route: [{ to: `team:${team}` }],
      present: {
        title: 'Workflow {{ raw.workflowName }} failed at {{ raw.step }}',
        body: '{{ raw.error.code }}: {{ raw.error.message }}',
        fields: [
          { path: 'raw.workflowId', label: 'Workflow ID' },
          { path: 'raw.step', label: 'Step' },
        ],
        color: 'red',
      },
      dedup: { key: ['raw.workflowId', 'raw.step'], window: '10m' },
    }),
  },
  {
    id: 'prometheus',
    label: 'Prometheus alerts',
    description: 'Alertmanager webhooks for one service.',
    sample: 'prometheus',
    make: (team) => ({
      id: `${team}-prometheus`,
      name: 'Service alerts',
      team,
      enabled: true,
      when: {
        all: [
          { path: 'schema', op: 'eq', value: 'prometheus.alert' },
          { path: 'fields.service', op: 'eq', value: 'payments-api' },
          { not: { path: 'fields.resolved', op: 'eq', value: true } },
        ],
      },
      route: [{ to: `team:${team}` }],
      present: {
        title: '{{ fields.type }} on {{ fields.service }} ({{ fields.region }})',
        body: '{{ raw.annotations.summary }}',
        fields: [{ path: 'fields.region', label: 'Region' }],
        color: 'orange',
      },
      dedup: { key: ['fields.type', 'fields.service', 'fields.region'], window: '30m' },
      auto_resolve: { recover_when: { path: 'fields.resolved', op: 'eq', value: true } },
    }),
  },
  {
    id: 'business',
    label: 'Business events',
    description: 'A domain event over a threshold, e.g. large rejected trades.',
    sample: 'trade-rejected',
    make: (team) => ({
      id: `${team}-business-event`,
      name: 'Large rejected trades',
      team,
      enabled: true,
      when: {
        all: [
          { path: 'raw.meta.schema', op: 'eq', value: 'trade.rejected.v1' },
          { path: 'raw.payload.notional', op: 'gt', value: 10000000 },
        ],
      },
      severity: 'warning',
      route: [{ to: `team:${team}`, channels: ['email', 'ui'] }],
      present: {
        title: 'Rejected {{ raw.payload.currency }} {{ raw.payload.notional | money }} trade {{ raw.payload.tradeId }}',
        body: 'Reason: {{ raw.payload.reason }}',
        fields: [{ path: 'raw.payload.tradeId', label: 'Trade ID' }],
        color: 'amber',
      },
      dedup: { key: ['raw.payload.tradeId'], window: '10m' },
    }),
  },
  {
    id: 'blank',
    label: 'Start from scratch',
    description: 'An empty rule. Paste an event to see its fields.',
    sample: 'workflow-failed',
    make: (team) => ({
      id: `${team}-new-rule`,
      name: '',
      team,
      enabled: true,
      when: { all: [newLeaf()] },
      route: [{ to: `team:${team}` }],
      present: { title: '', fields: [], color: 'grey' },
    }),
  },
]
