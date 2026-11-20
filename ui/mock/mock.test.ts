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
