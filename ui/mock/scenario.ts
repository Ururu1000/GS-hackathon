// Seed data for the dev mock: the Appendix A sample events, randomized, and
// hand-written stand-ins for the seed rules (spec §4.3). The real backend does
// matching and rendering; this only needs to look plausible in the feed.

import type { AlertField, ColorToken, Severity } from '../src/api/types.ts'

export interface MockRule {
  id: string
  name: string
  team: string
  color: ColorToken
  source: string
  schema: string
  /** Returns null if the event does not match. */
  match(raw: any): null | {
    key: string
    severity: Severity
    title: string
    body: string
    fields: AlertField[]
  }
}

const money = (n: number) => n.toLocaleString('en-US')
const pick = <T>(xs: readonly T[]): T => xs[Math.floor(Math.random() * xs.length)]!
const int = (lo: number, hi: number) => lo + Math.floor(Math.random() * (hi - lo + 1))
const f = (path: string, label: string, value: unknown): AlertField => ({ path, label, value: String(value ?? '—') })

export const RULES: MockRule[] = [
  {
    id: 'settlements-workflow-failed',
    name: 'Settlement workflow failed (prod)',
    team: 'settlements',
    color: 'red',
    source: 'kafka:workflows',
    schema: 'workflow.failed',
    match(raw) {
      if (raw?.type !== 'WORKFLOW_FAILED' || raw.workflowName !== 'order-settlement' || raw.environment !== 'prod') return null
      return {
        key: `${raw.workflowId}|${raw.step}`,
        severity: raw.severity === 'CRITICAL' ? 'critical' : 'error',
        title: `Workflow ${raw.workflowName} failed at ${raw.step}`,
        body: `${raw.error?.code}: ${raw.error?.message}`,
        fields: [
          f('raw.workflowId', 'Workflow ID', raw.workflowId),
          f('raw.step', 'Step', raw.step),
          f('raw.error.code', 'Error code', raw.error?.code),
          f('raw.environment', 'Environment', raw.environment),
        ],
      }
    },
  },
  {
    id: 'payments-high-error-rate',
    name: 'Payments API alerts (critical)',
    team: 'payments',
    color: 'red',
    source: 'http:alertmanager',
    schema: 'prometheus.alert',
    match(raw) {
      if (!raw?.alertname || raw.labels?.service !== 'payments-api' || raw.status !== 'firing') return null
      return {
        key: `${raw.alertname}|${raw.labels.region}`,
        severity: raw.labels.severity === 'critical' ? 'critical' : 'warning',
        title: `${raw.alertname} on ${raw.labels.service} (${raw.labels.region})`,
        body: raw.annotations?.summary ?? '',
        fields: [
          f('fields.service', 'Service', raw.labels.service),
          f('raw.labels.region', 'Region', raw.labels.region),
          f('raw.labels.severity', 'Upstream severity', raw.labels.severity),
        ],
      }
    },
  },
  {
    id: 'rates-rejected-trades',
    name: 'Rejected rates trades > 10M',
    team: 'rates',
    color: 'amber',
    source: 'kafka:trades',
    schema: 'trade.rejected.v1',
    match(raw) {
      const p = raw?.payload
      if (raw?.meta?.schema !== 'trade.rejected.v1' || p?.desk !== 'rates' || !(p.notional > 10_000_000)) return null
      return {
        key: String(p.tradeId),
        severity: 'warning',
        title: `Rejected ${p.currency} ${money(p.notional)} trade ${p.tradeId}`,
        body: `Reason: ${p.reason}`,
        fields: [
          f('raw.payload.desk', 'Desk', p.desk),
          f('raw.payload.reason', 'Reason', p.reason),
          f('raw.meta.producer', 'Producer', raw.meta.producer),
        ],
      }
    },
  },
  {
    id: 'pricing-engine-warnings',
    name: 'Pricing engine warnings, per instrument',
    team: 'pricing',
    color: 'yellow',
    source: 'file:pricing-engine',
    schema: 'log.line',
    match(raw) {
      if (raw?.service !== 'pricing-engine' || !['WARN', 'ERROR'].includes(raw.level)) return null
      return {
        key: String(raw.instrument),
        severity: raw.level === 'ERROR' ? 'error' : 'warning',
        title: `pricing-engine: ${raw.msg} on ${raw.instrument}`,
        body: `Instance ${raw.instance}, latency ${raw.latency_ms} ms (threshold 1000 ms)`,
        fields: [
          f('raw.instrument', 'Instrument', raw.instrument),
          f('raw.instance', 'Instance', raw.instance),
          f('raw.latency_ms', 'Latency (ms)', raw.latency_ms),
        ],
      }
    },
  },
  {
    id: 'payments-deployments',
    name: 'Payments deployments',
    team: 'payments',
    color: 'blue',
    source: 'http:ci',
    schema: 'ci.deployment',
    match(raw) {
      if (raw?.kind !== 'deployment' || raw.service !== 'payments-api') return null
      return {
        key: `${raw.service}|${raw.version}|${raw.region}`,
        severity: 'info',
        title: `Deployed ${raw.service} ${raw.version} to ${raw.region}`,
        body: `By ${raw.actor}, pipeline #${raw.pipeline}`,
        fields: [
          f('raw.version', 'Version', raw.version),
          f('raw.region', 'Region', raw.region),
          f('raw.actor', 'Deployed by', raw.actor),
        ],
      }
    },
  },
]

export const TEAMS = [
  { id: 'settlements', name: 'Settlements' },
  { id: 'payments', name: 'Payments' },
  { id: 'rates', name: 'Rates' },
  { id: 'pricing', name: 'Pricing' },
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
    status: 'firing',
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
