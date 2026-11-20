import { once } from 'node:events'
import { createServer, type Server } from 'node:http'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import type { RuleSpec, RunbookStep } from '../src/api/types'
import { createMock } from './mock'
import { RULES, workflowFailed } from './scenario'

let mock: ReturnType<typeof createMock>
let server: Server
let base: string
let csrf = ''

async function request(path: string, status = 200, method = 'GET', body?: unknown) {
  const response = await fetch(base + path, {
    method,
    headers: { 'content-type': 'application/json', 'x-csrf-token': csrf },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const json = await response.json()
  expect(response.status, JSON.stringify(json)).toBe(status)
  return json
}

beforeEach(async () => {
  vi.stubEnv('MOCK_RATE', '0')
  vi.stubEnv('MOCK_SEED', '0')
  mock = createMock()
  server = createServer((req, res) => {
    if (!mock.handle(req, res)) res.writeHead(404).end()
  }).listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (!address || typeof address === 'string') throw new Error('Expected an HTTP port')
  base = `http://127.0.0.1:${address.port}`
  csrf = (await request('/api/session')).csrf_token
})

afterEach(async () => {
  vi.useRealTimers()
  vi.unstubAllEnvs()
  mock.close()
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()))
})

const failedWorkflow = () => ({ ...workflowFailed(new Date()), environment: 'prod' })

it('reports invalid draft field shapes and uses the supplied source for matching', async () => {
  const rule = { ...RULES[0]!, when: { path: 'source', op: 'eq', value: 'kafka:workflows' } }
  const invalid = { ...rule, present: { ...rule.present, fields: 'raw.step' } }
  const event = failedWorkflow()
  const diagnosed = await request('/api/rules/test', 200, 'POST', { rule: invalid, event })
  expect(diagnosed).toMatchObject({ card: null, errors: [{ path: 'present.fields' }] })
  const tested = await request('/api/rules/test', 200, 'POST', { rule, event, source: 'kafka:workflows' })
  expect(tested).toMatchObject({ matched: true })
})

it('merges events with the same dedup key and separates events with missing keys', async () => {
  const rule: RuleSpec = {
    ...RULES[0]!,
    id: 'test-dedup',
    when: { path: 'raw.test', op: 'eq', value: true },
    dedup: { key: ['raw.id'], window: '10m' },
  }
  await request('/api/rules', 201, 'POST', { rule })
  const ingest = async (raw: object) => {
    const { matched_alerts } = await request('/ingest/http:test', 202, 'POST', { test: true, ...raw })
    expect(matched_alerts).toHaveLength(1)
    return String(matched_alerts[0])
  }
  const unkeyed = await ingest({})
  expect(await ingest({})).not.toBe(unkeyed)
  const keyed = await ingest({ id: 'same' })
  expect(await ingest({ id: 'same' })).toBe(keyed)
  expect(await request(`/api/alerts/${unkeyed}`)).toMatchObject({ count: 1 })
  expect(await request(`/api/alerts/${keyed}`)).toMatchObject({ count: 2 })
})

it('accepts rules without optional dedup and digest keys', async () => {
  const rule: RuleSpec = { ...RULES[0]!, id: 'optional-keys', dedup: { window: '10m' }, group: { window: '5m' } }
  const created = await request('/api/rules', 201, 'POST', { rule })
  expect(created.rule).toMatchObject({ dedup: { window: '10m' }, group: { window: '5m' } })
  const tested = await request('/api/rules/test', 200, 'POST', { rule, event: failedWorkflow() })
  expect(tested).toMatchObject({ errors: [], card: { dedup_key: [] } })
})

it('rejects an old proposal after the rule is deleted and recreated', async () => {
  const rule = { ...RULES[0]!, id: 'recreated-rule' }
  const original = await request('/api/rules', 201, 'POST', { rule })
  const proposal = await request('/api/proposals', 201, 'POST', {
    rule_id: rule.id, base_revision: original.revision, kind: 'other', summary: 'Change title',
    proposed_rule: { ...rule, name: 'Proposed' },
  })
  const path = `/api/rules/${rule.id}`
  await request(`${path}?expected_revision=${original.revision}`, 200, 'DELETE')
  const recreated = await request('/api/rules', 201, 'POST', { rule: { ...rule, name: 'Recreated' } })
  expect(recreated.revision).toBeGreaterThan(original.revision)
  expect(await request(`/api/proposals/${proposal.id}/accept`, 409, 'POST')).toMatchObject({ error: { code: 'revision_conflict' } })
  expect(await request(path)).toMatchObject({ rule: { name: 'Recreated' }, revision: recreated.revision })
})

it('cancels a queued action when its alert resolves before execution', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout'] })
  const { matched_alerts: [id] } = await request('/ingest/kafka:workflows', 202, 'POST', failedWorkflow())
  const path = `/api/alerts/${id}`
  const alert = await request(path)
  await request(`${path}/steps/retry/run`, 202, 'POST')
  await request(`${path}/resolve`, 200, 'POST', { expected_version: alert.version })
  await vi.runAllTimersAsync()
  const resolved = await request(path)
  const retry = resolved.runbook.steps.find((step: RunbookStep) => step.id === 'retry')
  expect(retry).toMatchObject({ state: 'cancelled', runs: [{ started_at: null, state: 'cancelled' }] })
  expect(resolved.timeline).toContainEqual(expect.objectContaining({ message: expect.stringContaining('Cancelled') }))
})

it.each([{}, { instance: null }, { instance: ' ' }, { instance: { id: 'node-1' } }])(
  'disables runbook actions with incomplete or invalid event parameters %j',
  async (params) => {
    const rule = { ...RULES[0]!, id: 'missing-instance', runbook: 'pricing-latency' }
    await request('/api/rules', 201, 'POST', { rule })
    await request('/ingest/kafka:workflows', 202, 'POST', { ...failedWorkflow(), ...params })
    const { items } = await request('/api/alerts?rule=missing-instance')
    const path = `/api/alerts/${items[0].id}`
    const detail = await request(path)
    const restart = detail.runbook.steps.find((step: RunbookStep) => step.action?.name === 'restart_service')
    expect(restart.disabled_reason).toContain('instance')
    expect(await request(`${path}/steps/${restart.id}/run`, 409, 'POST')).toMatchObject({ error: { code: 'invalid_transition' } })
  },
)

it('rejects a rotation name already reserved by another team', async () => {
  mock.close()
  mock = createMock({ demoUserId: 'dave' })
  csrf = (await request('/api/session')).csrf_token
  const team = await request('/api/teams/pricing')
  expect(await request('/api/teams/pricing', 422, 'PUT', {
    ...team, expected_revision: team.revision, oncall: { settlements: ['dave'] },
  })).toMatchObject({ error: { details: [{ path: 'oncall.settlements' }] } })
  expect(await request('/api/teams/pricing')).toMatchObject({ oncall: team.oncall, revision: team.revision })
})

it('discovers unknown events, previews a mapping without saving, and applies the published revision', async () => {
  const source = 'http:playground'
  const event = { trade_limit: { desk: 'EXAMPLE', amount: 42 }, token: 'demo-only' }
  await request('/ingest/playground', 202, 'POST', event)
  await request('/ingest/playground', 202, 'POST', event)
  const discovered = await request('/api/schemas/discovered')
  expect(discovered).toContainEqual(expect.objectContaining({
    source, schema: expect.stringMatching(/^unknown:/), count: 2,
    sample: { ...event, token: '[REDACTED]' },
  }))
  expect(await request('/api/sources')).toContainEqual(expect.objectContaining({ id: source, events_24h: 2 }))
  const spec = {
    schema: 'custom.trade-limit', priority: 50, detect: { source, path: 'trade_limit.amount', op: 'exists' },
    fields: { service: { path: 'trade_limit.desk', transform: 'lower' } },
  }
  expect(await request('/api/schemas/preview', 200, 'POST', { spec, source, event }))
    .toMatchObject({ matched: true, fields: { service: 'example' } })
  expect(await request('/api/schemas')).toEqual([])
  expect(await request('/api/schemas', 201, 'POST', { spec })).toMatchObject({ revision: 1 })
  expect(await request('/api/schemas', 409, 'POST', { spec })).toMatchObject({ error: { code: 'revision_conflict' } })
  const rule = { ...RULES[0]!, when: { path: 'fields.service', op: 'eq', value: 'example' } }
  expect(await request('/api/rules/test', 200, 'POST', { rule, source, event }))
    .toMatchObject({ matched: true, schema: spec.schema })
})

it('previews and edits a runbook import before attaching the saved checklist to an alert', async () => {
  const draft = { format: 'markdown', title: 'Imported checklist', team: 'settlements',
    content: '# Checklist\n1. Find {{raw.workflowId}}\n2. Read the audit history' }
  const preview = await request('/api/runbooks/import', 200, 'POST', draft)
  expect(preview.steps).toHaveLength(2)
  const { items: before } = await request('/api/runbooks')
  expect(before.some((book: { id: string }) => book.id === preview.id)).toBe(false)
  const saved = await request('/api/runbooks', 201, 'POST', {
    ...draft, ...preview, steps: [{ ...preview.steps[0], text: 'Inspect {{raw.workflowId}}' }],
  })
  const rule = { ...RULES[0]!, id: 'with-imported-book', runbook: saved.id }
  await request('/api/rules', 201, 'POST', { rule })
  const workflow = failedWorkflow()
  await request('/ingest/kafka:workflows', 202, 'POST', workflow)
  const { items: [alert] } = await request('/api/alerts?rule=with-imported-book')
  expect(await request(`/api/alerts/${alert.id}`)).toMatchObject({
    runbook: { id: saved.id, steps: [{ kind: 'manual', text: `Inspect ${workflow.workflowId}` }] },
  })
})

it('runs an escalation through the eligible simulated phone and consumes the SMS reply once', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout'] })
  const { matched_alerts: [id] } = await request('/ingest/kafka:workflows', 202, 'POST', failedWorkflow())
  const path = `/api/alerts/${id}`
  for (let i = 0; i < 2; i++) {
    await request(`${path}/steps/retry/run`, 202, 'POST')
    await vi.runAllTimersAsync()
  }
  const gated = await request(`${path}/steps/escalate/run`, 409, 'POST')
  expect(gated.error.code).toBe('approval_required')
  await request(`/api/approvals/${gated.error.approval_id}/approve`, 403, 'POST')
  const { items } = await request('/api/simulator/messages')
  const sms = items.find((item: { alert_id: string; approval_challenge: string | null }) => item.alert_id === id && item.approval_challenge)
  expect(sms.recipient).toBe('user:carol')
  const reply = { receipt_id: sms.receipt_id, text: `Y ${sms.approval_challenge}` }
  const result = await request('/api/simulator/reply', 200, 'POST', reply)
  expect(result).toMatchObject({ state: 'approved' })
  await request('/api/simulator/reply', 409, 'POST', reply)
  await vi.runAllTimersAsync()
  const detail = await request(path)
  const step = detail.runbook.steps.find((entry: RunbookStep) => entry.id === 'escalate')
  expect(step).toMatchObject({ state: 'ok', approval_request: { decided_by: 'user:carol', state: 'approved' } })
  expect(step.approval_request).not.toHaveProperty('approval_challenge')
  expect(step.approval_request).not.toHaveProperty('sms_code_hash')
})

it('keeps the same recipient scope when loading historical pages and counts occurrences', async () => {
  const base = { ...RULES[0]!, when: { path: 'raw.history', op: 'exists' }, dedup: { key: ['raw.history'], window: '10m' } }
  await request('/api/rules', 201, 'POST', { rule: { ...base, id: 'history-bob', route: [{ to: 'user:bob', channels: ['ui'] }] } })
  await request('/api/rules', 201, 'POST', { rule: { ...base, id: 'history-carol', route: [{ to: 'user:carol', channels: ['ui'] }] } })
  await request('/ingest/playground', 202, 'POST', { history: 1 })
  await request('/ingest/playground', 202, 'POST', { history: 1 })
  await request('/ingest/playground', 202, 'POST', { history: 2 })
  const scope = '/api/alerts?team=settlements&recipient=bob&limit=1'
  const first = await request(scope)
  expect(first.items).toHaveLength(1)
  expect(first.items[0].rule.id).toBe('history-bob')
  expect(first.next_cursor).toBeTruthy()
  const second = await request(scope + '&cursor=' + encodeURIComponent(first.next_cursor))
  expect(second.items[0].rule.id).toBe('history-bob')
  expect(second.next_cursor).toBeNull()
  expect(second.items[0].id).not.toBe(first.items[0].id)
  const detail = await request(`/api/alerts/${second.items[0].id}`)
  expect(detail.count).toBe(2)
  expect(detail.occurrences.counts).toEqual([2])
})
