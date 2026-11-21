// Seed data for the dev mock: the Appendix A sample events (randomized), the
// seed rules as real rule specs (spec §4.3, matching config/rules), people and
// teams (config/people, config/teams) and the Appendix B runbooks. The mock's
// toy engine (engine.ts) evaluates the rules, so rules edited in the UI change
// what the feed does.

import type { ImportedStep } from '../src/api/screens.ts'
import type { DeliveryStatus, FeedAlert, PersonView, RuleSpec, TeamView } from '../src/api/types.ts'
import { maskAddress } from '../src/lib/redact.ts'

const pick = <T>(xs: readonly T[]): T => xs[Math.floor(Math.random() * xs.length)]!
const int = (lo: number, hi: number) => lo + Math.floor(Math.random() * (hi - lo + 1))

export const RULES: RuleSpec[] = [
  {
    id: 'settlements-workflow-failed',
    name: 'Settlement workflow failed (prod)',
    team: 'settlements',
    enabled: true,
    when: {
      all: [
        { path: 'fields.type', op: 'eq', value: 'WORKFLOW_FAILED' },
        { path: 'raw.workflowName', op: 'eq', value: 'order-settlement' },
        { path: 'fields.environment', op: 'eq', value: 'prod' },
      ],
    },
    route: [{ to: 'oncall:settlements', channels: ['phone', 'sms'] }],
    present: {
      title: 'Settlement workflow {{ raw.workflowId }} failed at {{ raw.step }}',
      body: '{{ raw.error.code }}: {{ raw.error.message }}',
      fields: [
        { path: 'raw.workflowId', label: 'Workflow ID' },
        { path: 'raw.step', label: 'Step' },
        { path: 'raw.error.message', label: 'Error' },
        { path: 'raw.error.code', label: 'Code' },
      ],
      color: 'red',
    },
    dedup: { key: ['raw.workflowId', 'raw.step'], window: '10m' },
    runbook: 'settlement-timeout',
    escalate_after: '10m',
  },
  {
    id: 'anna-payments-critical',
    name: 'Critical on payments-api (Anna)',
    team: 'payments',
    enabled: true,
    when: {
      all: [
        { path: 'fields.service', op: 'eq', value: 'payments-api' },
        { path: 'fields.severity', op: 'eq', value: 'critical' },
        { not: { path: 'fields.resolved', op: 'eq', value: true } },
      ],
    },
    severity: 'critical',
    route: [{ to: 'user:anna' }],
    present: {
      title: '{{ fields.type }} on {{ fields.service }} ({{ fields.region }})',
      body: '{{ raw.annotations.summary }}',
      fields: [
        { path: 'fields.region', label: 'Region' },
        { path: 'raw.annotations.summary', label: 'Summary' },
      ],
      color: 'red',
    },
    dedup: { key: ['fields.type', 'fields.service', 'fields.region'], window: '30m' },
    auto_resolve: { recover_when: { path: 'fields.resolved', op: 'eq', value: true } },
    runbook: 'payments-high-error-rate',
  },
  {
    id: 'rates-rejected-trades',
    name: 'Large rejected rates trades',
    team: 'rates',
    enabled: true,
    when: {
      all: [
        { path: 'raw.meta.schema', op: 'eq', value: 'trade.rejected.v1' },
        { path: 'raw.payload.desk', op: 'eq', value: 'rates' },
        { path: 'raw.payload.notional', op: 'gt', value: 10_000_000 },
      ],
    },
    severity: 'warning',
    route: [{ to: 'team:rates', channels: ['email', 'ui'] }],
    present: {
      title: 'Rejected {{ raw.payload.currency }} {{ raw.payload.notional | money }} trade {{ raw.payload.tradeId }}',
      body: 'Reason: {{ raw.payload.reason }}',
      fields: [
        { path: 'raw.payload.tradeId', label: 'Trade ID' },
        { path: 'raw.payload.desk', label: 'Desk' },
        { path: 'raw.payload.reason', label: 'Reason' },
        { path: 'raw.meta.producer', label: 'Producer' },
      ],
      color: 'amber',
    },
    dedup: { key: ['raw.payload.tradeId'], window: '10m' },
    runbook: 'trade-rejected',
  },
  {
    id: 'pricing-engine-warnings',
    name: 'Pricing engine warnings',
    team: 'pricing',
    enabled: true,
    when: {
      all: [
        { path: 'fields.service', op: 'eq', value: 'pricing-engine' },
        { path: 'fields.severity_rank', op: 'gte', value: 2 },
      ],
    },
    route: [{ to: 'team:pricing', channels: ['ui'] }],
    present: {
      title: 'pricing-engine {{ fields.severity }} on {{ fields.instrument }}',
      body: '{{ raw.msg }} ({{ raw.latency_ms }} ms on {{ raw.instance }})',
      fields: [
        { path: 'fields.instrument', label: 'Instrument' },
        { path: 'raw.instance', label: 'Instance' },
        { path: 'raw.latency_ms', label: 'Latency (ms)' },
      ],
      color: 'yellow',
    },
    dedup: { key: ['fields.instrument'], window: '5m' },
    runbook: 'pricing-latency',
  },
  {
    id: 'payments-deployments',
    name: 'Payments deployments',
    team: 'payments',
    enabled: true,
    when: {
      all: [
        { path: 'schema', op: 'eq', value: 'ci.deployment' },
        { path: 'fields.service', op: 'eq', value: 'payments-api' },
      ],
    },
    severity: 'info',
    route: [{ to: 'team:payments', channels: ['ui'] }],
    present: {
      title: 'Deployed {{ raw.service }} {{ raw.version }} to {{ raw.region }}',
      body: 'By {{ raw.actor }}, pipeline #{{ raw.pipeline }}',
      fields: [
        { path: 'raw.version', label: 'Version' },
        { path: 'raw.region', label: 'Region' },
        { path: 'raw.actor', label: 'Deployed by' },
      ],
      color: 'blue',
    },
    dedup: { key: ['raw.service', 'raw.version', 'raw.region'], window: '1h' },
  },
]

export const TEAMS: TeamView[] = [
  { id: 'settlements', name: 'Settlements', members: ['bob', 'carol', 'erop'], lead: 'carol', oncall: { settlements: ['bob', 'carol'] }, default_channels: [], revision: 1 },
  { id: 'payments', name: 'Payments', members: ['anna', 'frank'], lead: 'frank', oncall: { payments: ['frank', 'anna'] }, default_channels: ['email'], revision: 1 },
  { id: 'rates', name: 'Rates', members: ['bob', 'erin'], lead: 'bob', oncall: {}, default_channels: ['email'], revision: 1 },
  { id: 'pricing', name: 'Pricing', members: ['dave', 'erin'], lead: 'dave', oncall: { pricing: ['dave', 'erin'] }, default_channels: [], revision: 1 },
]

const person = (id: string, name: string, phone: string, preferences: PersonView['preferences'] = []): PersonView => ({
  id,
  name,
  contact: { email: `${id}@example.test`, phone },
  teams: [],
  preferences,
  revision: 1,
})

export const PEOPLE: PersonView[] = [
  person('anna', 'Anna Example', '+44 7700 900123', [
    { match: { severity: 'critical', service: 'payments-api' }, channels: ['sms'], when: { between: '08:00-20:00', tz: 'Europe/London' }, otherwise: ['email'] },
    { match: { severity: 'warning' }, channels: ['email'] },
    { default: ['ui'] },
  ]),
  person('bob', 'Bob Example', '+44 7700 900124'),
  person('carol', 'Carol Example', '+44 7700 900125', [
    { match: { severity: 'critical' }, channels: ['sms'] },
    { default: ['email'] },
  ]),
  person('dave', 'Dave Example', '+44 7700 900126'),
  person('erin', 'Erin Example', '+44 7700 900127', [{ match: { severity: ['critical', 'error'] }, channels: ['email'] }]),
  person('frank', 'Frank Example', '+44 7700 900128'),
  person('erop', 'erop (demo)', '+44 7700 900100', [{ default: ['ui'] }]),
]

// ---- Runbooks (Appendix B), in the internal model (spec §6.1) -------------

export type RunbookStepDef = ImportedStep

export interface RunbookDef {
  id: string
  title: string
  format: 'yaml' | 'markdown' | 'link' | 'text'
  team: string | null
  url?: string
  text?: string
  steps: RunbookStepDef[]
}

export const RUNBOOKS: RunbookDef[] = [
  {
    id: 'settlement-timeout',
    title: 'Settlement timeout',
    format: 'yaml',
    team: 'settlements',
    steps: [
      {
        id: 'check-counterparty',
        kind: 'action',
        text: 'Is counterparty service healthy?',
        action: { name: 'http_get', label: 'Check health', params: { url: 'http://counterparty-service/health' }, risk: 'low' },
        approval: 'none',
      },
      {
        id: 'retry',
        kind: 'action',
        text: 'Retry workflow {{workflowId}}',
        action: { name: 'retry_workflow', label: 'Retry workflow', params: { workflowId: '{{workflowId}}' }, risk: 'low' },
        approval: 'none',
        resolves: true,
      },
      {
        id: 'escalate',
        kind: 'action',
        text: 'If the retry fails twice: escalate to counterparty-support',
        action: { name: 'escalate', label: 'Escalate', params: { team: 'counterparty-support' }, risk: 'high' },
        approval: 'required',
        after_failures: { step: 'retry', count: 2 },
      },
    ],
  },
  {
    id: 'payments-high-error-rate',
    title: 'HighErrorRate on payments-api',
    format: 'markdown',
    team: 'payments',
    text: `# HighErrorRate on payments-api
1. Open the payments dashboard and check if errors are in one region only.
2. If one region: fail traffic over to the other region (needs approval from the team lead).
3. If all regions: check the last deployment and roll it back.
4. Post an update in the #payments-incidents channel every 15 minutes.`,
    steps: [
      { id: 'md-1', kind: 'manual', text: 'Open the payments dashboard and check if errors are in one region only ({{ fields.region }} is affected).' },
      { id: 'md-2', kind: 'manual', text: 'If one region: fail traffic over to the other region.', hint: 'The runbook says this needs approval from the team lead.' },
      { id: 'md-3', kind: 'manual', text: 'If all regions: check the last deployment and roll it back.' },
      { id: 'md-4', kind: 'manual', text: 'Post an update in the #payments-incidents channel every 15 minutes.' },
    ],
  },
  {
    id: 'pricing-latency',
    title: 'Pricing latency',
    format: 'text',
    team: 'pricing',
    text: "When pricing latency goes over a second, it's usually the cache. Restart the pricing engine instance named in the alert, wait 2 minutes, and if latency is still high, call the pricing on-call person.",
    steps: [
      {
        id: 'restart',
        kind: 'action',
        text: 'Restart {{ raw.instance }}',
        action: { name: 'restart_service', label: 'Restart instance', params: { service: 'pricing-engine', instance: '{{ raw.instance }}' }, risk: 'high' },
        approval: 'required',
        resolves: true,
      },
    ],
  },
  {
    id: 'trade-rejected',
    title: 'Rates desk: rejected trade playbook',
    format: 'link',
    team: 'rates',
    url: 'https://wiki.example.test/rates/rejected-trades',
    steps: [],
  },
]

// ---- Event generators (Appendix A shapes) --------------------------------

const workflowPool = Array.from({ length: 10 }, (_, i) => `WF-20261023-${String(40 + i).padStart(4, '0')}`)
const steps = ['validate-counterparty', 'validate-counterparty', 'reserve-funds', 'book-settlement']
const errors = [
  { code: 'TIMEOUT', message: 'Counterparty service did not respond in 5000 ms' },
  { code: 'REJECTED', message: 'Counterparty rejected the instruction: SSI mismatch' },
  { code: 'TIMEOUT', message: 'Funds service did not respond in 3000 ms' },
]
const instruments = ['EURUSD 5Y', 'GBPUSD 2Y', 'USD SOFR 10Y', 'EUR ESTR 3M', 'JPY TONA 7Y', 'CHF SARON 1Y']

export function workflowFailed(at: Date) {
  const err = pick(errors)
  return {
    type: 'WORKFLOW_FAILED',
    workflowId: pick(workflowPool),
    workflowName: 'order-settlement',
    step: pick(steps),
    severity: Math.random() < 0.15 ? 'CRITICAL' : 'ERROR',
    environment: 'prod',
    timestamp: at.toISOString(),
    error: { ...err },
  }
}

export function highErrorRate(at: Date) {
  const pct = int(6, 19)
  return {
    alertname: pick(['HighErrorRate', 'HighErrorRate', 'HighLatency']),
    labels: { service: 'payments-api', region: pick(['eu-west', 'us-east']), severity: 'critical' },
    annotations: { summary: `Error rate ${pct}% over the last 5 minutes` },
    startsAt: new Date(at.getTime() - 60_000).toISOString().replace(/\.\d{3}Z$/, 'Z'),
    // Now and then the monitoring system reports recovery (auto_resolve.recover_when).
    status: Math.random() < 0.06 ? 'resolved' : 'firing',
  }
}

const tradePool = Array.from({ length: 30 }, () => `T-${int(90000, 99999)}`)

export function tradeRejected(at: Date) {
  return {
    meta: { schema: 'trade.rejected.v1', producer: 'trade-gateway', ts: at.getTime() },
    payload: {
      tradeId: pick(tradePool),
      desk: 'rates',
      notional: int(11, 80) * 1_000_000 + pick([0, 0, 500_000]),
      currency: pick(['EUR', 'EUR', 'USD', 'GBP']),
      reason: pick(['LIMIT_BREACH', 'LIMIT_BREACH', 'CREDIT_CHECK_FAILED', 'STALE_PRICE']),
    },
  }
}

export function pricingWarn(at: Date) {
  return {
    ts: at.toISOString(),
    level: Math.random() < 0.1 ? 'ERROR' : 'WARN',
    service: 'pricing-engine',
    instance: `pricing-engine-${int(1, 4)}`,
    instrument: pick(instruments),
    latency_ms: int(1010, 2400),
    msg: 'quote latency above threshold',
  }
}

export function deployment(at: Date) {
  return {
    kind: 'deployment',
    service: 'payments-api',
    version: `v2.14.${int(0, 40)}`,
    region: pick(['eu-west', 'us-east']),
    actor: pick(['ci-bot', 'anna', 'alexey']),
    pipeline: int(4000, 4999),
    at: at.toISOString(),
  }
}

/** A random event, weighted so the feed has hot dedup keys and fresh alerts. */
export function randomEvent(at: Date): { source: string; raw: unknown } {
  const r = Math.random()
  if (r < 0.32) return { source: 'http:alertmanager', raw: highErrorRate(at) }
  if (r < 0.58) return { source: 'file:pricing-engine', raw: pricingWarn(at) }
  if (r < 0.8) return { source: 'kafka:workflows', raw: workflowFailed(at) }
  if (r < 0.96) return { source: 'kafka:trades', raw: tradeRejected(at) }
  return { source: 'http:ci', raw: deployment(at) }
}

// ---- Deliveries, planned once when an alert opens (spec §4.4) -------------

/** [recipient, channel] pairs, `ui` dropped (the feed is the UI channel). */
export type DeliveryPlan = [string, string][]

const hash = (s: string) => [...s].reduce((h, c) => (h * 31 + c.charCodeAt(0)) >>> 0, 7)

/** Deterministic fake delivery_jobs rows for a planned alert. */
export function deliveriesFor(a: FeedAlert, plan: DeliveryPlan, people: Map<string, PersonView>, now = Date.now()): DeliveryStatus[] {
  const opened = Date.parse(a.first_seen)
  const stopped = a.acked_at ?? a.resolved_at
  return plan.filter(([, channel]) => channel !== 'ui').map(([recipient, channel], i) => {
    const h = hash(a.id + i)
    const p = people.get(recipient.replace(/^user:/, ''))
    const rawAddress = (channel === 'email' ? p?.contact.email : p?.contact.phone) ?? '—'
    const address = maskAddress(channel, rawAddress)
    const sendAt = opened + 1000 + (h % 2500)
    const base = { id: `dj_${a.id.slice(4)}_${i}`, recipient, channel, address }
    if (stopped && Date.parse(stopped) < sendAt)
      return { ...base, state: 'cancelled', attempts: 0, last_error: null, sent_at: null, updated_at: stopped }
    if (channel === 'sms' && h % 6 === 0) {
      // Fake SMS gateway outage: retried, then dead-lettered.
      const at = new Date(Math.min(now, sendAt + 90_000)).toISOString()
      return { ...base, state: now < sendAt + 90_000 ? 'in_flight' : 'dead', attempts: now < sendAt + 90_000 ? 3 : 5, last_error: 'fake-sms-gateway: 503 Service Unavailable', sent_at: null, updated_at: at }
    }
    if (now < sendAt) return { ...base, state: 'pending', attempts: 0, last_error: null, sent_at: null, updated_at: a.first_seen }
    const sent = new Date(sendAt).toISOString()
    return { ...base, state: 'sent', attempts: 1 + (h % 4 === 0 ? 1 : 0), last_error: null, sent_at: sent, updated_at: sent }
  })
}
