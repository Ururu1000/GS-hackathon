import type { IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'
import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { afterEach, describe, expect, it } from 'vitest'
import { Runbook } from '../src/alert/Runbook'
import { errorText, HttpError } from '../src/api/client'
import type { FeedAlert, RunbookView } from '../src/api/types'
import { diffLines } from '../src/lib/diff'
import { maskAddress, redactText, redactValue } from '../src/lib/redact'
import { parseHash } from '../src/lib/router'
import { fromYaml } from '../src/rules/draft'
import { compileSafeRegex, testRule, validateRule } from './engine'
import { createMock } from './mock'
import { RULES } from './scenario'

interface MockResponse {
  status: number
  headers: Record<string, string>
  body: string
  json: () => any
}

function callMock(
  mock: ReturnType<typeof createMock>,
  method: string,
  url: string,
  opts: { headers?: Record<string, string>; body?: string | object } = {},
): Promise<MockResponse> {
  return new Promise((resolve, reject) => {
    const payload = opts.body === undefined ? '' : typeof opts.body === 'string' ? opts.body : JSON.stringify(opts.body)
    const req = Readable.from(payload ? [Buffer.from(payload)] : []) as unknown as IncomingMessage
    req.method = method
    req.url = url
    req.headers = Object.fromEntries(Object.entries(opts.headers ?? {}).map(([k, v]) => [k.toLowerCase(), v]))

    let status = 200
    let headers: Record<string, string> = {}
    const chunks: string[] = []
    const res = {
      writeHead(s: number, h?: Record<string, string>) {
        status = s
        headers = { ...headers, ...(h ?? {}) }
        return res
      },
      write(chunk: string | Buffer) {
        chunks.push(String(chunk))
        return true
      },
      end(chunk?: string | Buffer) {
        if (chunk) chunks.push(String(chunk))
        const body = chunks.join('')
        resolve({ status, headers, body, json: () => JSON.parse(body) })
        return res
      },
    } as unknown as ServerResponse

    try {
      const handled = mock.handle(req, res)
      if (!handled) reject(new Error(`Unhandled route: ${method} ${url}`))
    } catch (e) {
      reject(e)
    }
  })
}

let activeMock: ReturnType<typeof createMock> | null = null
function bootMock() {
  process.env.MOCK_SEED = '10'
  process.env.MOCK_RATE = '0'
  delete process.env.MOCK_CSRF_TOKEN
  activeMock = createMock()
  return activeMock
}

afterEach(() => {
  activeMock?.close()
  activeMock = null
})

describe('mock API security & privacy controls', () => {
  it('issues a non-static CSRF token and requires CSRF on ingest & mock controls', async () => {
    const mock = bootMock()
    const s = await callMock(mock, 'GET', '/api/session')
    expect(s.status).toBe(200)
    const csrf = s.json().csrf_token as string
    expect(csrf).not.toBe('mock-csrf-token')
    expect(csrf.length).toBeGreaterThanOrEqual(24)

    expect((await callMock(mock, 'POST', '/ingest/http:playground', { body: { x: 1 } })).status).toBe(403)
    expect((await callMock(mock, 'POST', '/api/__mock/burst?n=1')).status).toBe(403)
    expect((await callMock(mock, 'POST', '/api/__mock/workflow-engine?ok=1')).status).toBe(403)

    const okIngest = await callMock(mock, 'POST', '/ingest/http:playground', {
      headers: { 'x-csrf-token': csrf },
      body: { x: 1 },
    })
    expect(okIngest.status).toBe(202)
  })

  it('enforces authorization on preferences, teams (no lead self-promotion), rules, and proposals', async () => {
    const mock = bootMock()
    const csrf = (await callMock(mock, 'GET', '/api/session')).json().csrf_token as string
    const h = { 'x-csrf-token': csrf }

    // Cannot edit another user's preferences
    const badPref = await callMock(mock, 'PUT', '/api/people/anna/preferences', {
      headers: h,
      body: { preferences: [{ default: ['ui'] }], expected_revision: 1 },
    })
    expect(badPref.status).toBe(403)

    // Can edit own preferences
    const okPref = await callMock(mock, 'PUT', '/api/people/erop/preferences', {
      headers: h,
      body: { preferences: [{ default: ['email'] }], expected_revision: 1 },
    })
    expect(okPref.status).toBe(200)

    // Cannot promote self to team lead on settlements (carol is lead)
    const badTeam = await callMock(mock, 'PUT', '/api/teams/settlements', {
      headers: h,
      body: { members: ['bob', 'carol', 'erop'], lead: 'erop', expected_revision: 1 },
    })
    expect(badTeam.status).toBe(403)

    // Cannot create or edit rules for teams user doesn't belong to
    const otherRule = { ...RULES[0]!, id: 'unauth-payments-rule', team: 'payments' }
    expect((await callMock(mock, 'POST', '/api/rules', { headers: h, body: { rule: otherRule } })).status).toBe(403)
    expect(
      (await callMock(mock, 'DELETE', '/api/rules/anna-payments-critical?expected_revision=1', { headers: h })).status,
    ).toBe(403)

    // Can create rule for own team (settlements)
    const ownRule = { ...RULES[0]!, id: 'settlements-custom-rule', team: 'settlements' }
    expect((await callMock(mock, 'POST', '/api/rules', { headers: h, body: { rule: ownRule } })).status).toBe(201)

    // Cannot accept/reject proposal for pricing team
    expect((await callMock(mock, 'POST', '/api/proposals/prp_auto_pricing/accept', { headers: h })).status).toBe(403)
  })

  it('prevents approval self-approval and omits plaintext sms_code from alert details', async () => {
    const mock = bootMock()
    const csrf = (await callMock(mock, 'GET', '/api/session')).json().csrf_token as string
    const h = { 'x-csrf-token': csrf }

    // Create a settlements workflow alert and trigger 2 failed retries to unlock escalate
    const ing = await callMock(mock, 'POST', '/ingest/kafka:workflows', {
      headers: h,
      body: {
        type: 'WORKFLOW_FAILED',
        workflowId: 'WF-SEC-001',
        workflowName: 'order-settlement',
        step: 'validate-counterparty',
        severity: 'ERROR',
        environment: 'prod',
        timestamp: new Date().toISOString(),
        error: { code: 'TIMEOUT', message: 'Bearer supersecrettoken12345 from alice@corp.test' },
        api_key: 'sk-live-secret-9999',
      },
    })
    expect(ing.status).toBe(202)
    const alertId = ing.json().matched_alerts[0] as string

    // Check raw sample & deliveries redaction on alert detail
    const d1 = (await callMock(mock, 'GET', `/api/alerts/${alertId}`)).json()
    expect(d1.samples[0].raw.api_key).toBe('[REDACTED]')
    expect(d1.samples[0].raw.error.message).not.toContain('supersecrettoken12345')
    expect(d1.samples[0].raw.error.message).not.toContain('alice@corp.test')
    for (const del of d1.deliveries) {
      expect(del.address).not.toContain('7700')
      expect(del.address).not.toContain('bob@example.test')
      expect(del.address).toMatch(/•••|\*\*\*|—/)
    }

    // Trigger retry step; on pricing alert (non-member), runStep must return 403
    const alerts = (await callMock(mock, 'GET', '/api/alerts?team=pricing')).json().items as FeedAlert[]
    if (alerts.length > 0) {
      const forbiddenRun = await callMock(mock, 'POST', `/api/alerts/${alerts[0]!.id}/steps/restart/run`, {
        headers: h,
        body: { expected_version: alerts[0]!.version },
      })
      expect(forbiddenRun.status).toBe(403)
    }

    // Also verify Runbook component never renders sms_code and enforces maxLength
    const rb: RunbookView = {
      id: 'rb',
      revision: 1,
      title: 'Test',
      format: 'yaml',
      url: null,
      text: null,
      steps: [
        {
          id: 'esc',
          kind: 'action',
          text: 'Escalate',
          hint: null,
          action: { name: 'escalate', label: 'Escalate', params: { team: 'ops' }, risk: 'high' },
          approval: 'required',
          state: 'awaiting_approval',
          disabled_reason: null,
          done_by: null,
          done_at: null,
          runs: [],
          approval_request: {
            id: 'apr_1',
            state: 'pending',
            requested_by: 'user:bob',
            requested_at: new Date().toISOString(),
            expires_at: new Date(Date.now() + 600_000).toISOString(),
            eligible: ['user:carol'],
            can_decide: true,
            sms_sent: true,
            decided_by: null,
            decided_at: null,
            note: null,
          },
        },
      ],
    }
    const html = renderToStaticMarkup(
      createElement(Runbook, {
        runbook: rb,
        alert: d1,
        now: Date.now(),
        onChanged: () => {},
        onToast: () => {},
      }),
    )
    expect(html).not.toContain('reply <code>Y')
    expect(html).toContain('maxLength="2000"')
    expect(html).toContain('reply to the SMS sent to your phone')
  })

  it('minimizes PII on /api/people/directory and /api/people and scopes /api/events/recent & /api/proposals', async () => {
    const mock = bootMock()

    const dir = (await callMock(mock, 'GET', '/api/people/directory')).json().items
    expect(dir.length).toBeGreaterThan(0)
    for (const entry of dir) {
      expect(entry.contact).toBeUndefined()
      expect(entry.preferences).toBeUndefined()
    }

    const ppl = (await callMock(mock, 'GET', '/api/people')).json().items
    const self = ppl.find((p: any) => p.id === 'erop')
    const anna = ppl.find((p: any) => p.id === 'anna')
    expect(self.can_edit).toBe(true)
    expect(self.contact.email).toBe('erop@example.test')
    expect(anna.can_edit).toBe(false)
    expect(anna.contact).toEqual({})
    expect(anna.preferences).toEqual([])

    const count = (await callMock(mock, 'GET', '/api/proposals/count?team=pricing')).json()
    expect(count.pending).toBe(1)
    const settlementsProps = (await callMock(mock, 'GET', '/api/proposals?team=settlements')).json().items
    expect(settlementsProps).toEqual([])
    expect((await callMock(mock, 'GET', '/api/proposals?team=secret-team')).status).toBe(403)
    expect((await callMock(mock, 'GET', '/api/events/recent?team=secret-team')).status).toBe(403)
  })

  it('rejects malformed URI encodings, invalid sources, oversized bodies, and over-limit notes', async () => {
    const mock = bootMock()
    const csrf = (await callMock(mock, 'GET', '/api/session')).json().csrf_token as string
    const h = { 'x-csrf-token': csrf }

    expect((await callMock(mock, 'GET', '/api/alerts/%E0%A4%A')).status).toBe(400)
    expect((await callMock(mock, 'POST', '/ingest/bad%20source!', { headers: h, body: {} })).status).toBe(400)
    expect((await callMock(mock, 'POST', '/ingest/http:playground', { headers: h, body: 'x'.repeat(300_000) })).status).toBe(413)

    const longSummary = await callMock(mock, 'POST', '/api/proposals', {
      headers: h,
      body: {
        rule_id: 'settlements-workflow-failed',
        base_revision: 1,
        kind: 'other',
        summary: 'a'.repeat(201),
        note: '',
        proposed_rule: RULES[0],
      },
    })
    expect(longSummary.status).toBe(422)
  })
})

describe('hardening helpers: regex, YAML, router, diff, redaction, errorText', () => {
  it('blocks ReDoS nested quantifiers, backreferences, and oversized patterns', () => {
    expect(compileSafeRegex('(a+)+$').ok).toBe(false)
    expect(compileSafeRegex('(a)\\1').ok).toBe(false)
    expect(compileSafeRegex('a'.repeat(300)).ok).toBe(false)
    expect(compileSafeRegex('^prod-[a-z0-9]+$').ok).toBe(true)

    const errs = validateRule({ ...RULES[0]!, when: { path: 'raw.s', op: 'regex', value: '(a+)+$' } })
    expect(errs).toContainEqual(expect.objectContaining({ path: 'when.value' }))
  })

  it('handles deeply nested events in testRule without stack overflow', () => {
    let deep: Record<string, unknown> = { leaf: 1 }
    for (let i = 0; i < 60; i++) deep = { nested: deep }
    const res = testRule(RULES[0]!, deep)
    expect(res.schema).toMatch(/^unknown:/)
  })

  it('hardens fromYaml against forbidden keys and malformed required field types', () => {
    expect(fromYaml('id: x\nteam: y\nwhen: not-an-obj\nroute: []\npresent: { title: hi }\n').ok).toBe(false)
    expect(fromYaml('id: x\nteam: y\nwhen: { path: raw.a, op: eq, value: 1 }\nroute: bad\npresent: { title: hi }\n').ok).toBe(false)
    expect(fromYaml('id: x\nteam: y\nconstructor: evil\nwhen: { path: raw.a, op: eq, value: 1 }\nroute: [{ to: team:y }]\npresent: { title: hi }\n').ok).toBe(false)
  })

  it('safely handles malformed URL hash percent-encoding without throwing', () => {
    expect(parseHash('#/rules/%E0%A4%A')).toEqual({ view: 'rules', id: null, from: null })
  })

  it('bounds diffLines on large inputs without allocating unbounded LCS tables', () => {
    const a = Array.from({ length: 600 }, (_, i) => `line-${i}`).join('\n')
    const b = Array.from({ length: 600 }, (_, i) => (i === 300 ? 'changed' : `line-${i}`)).join('\n')
    const d = diffLines(a, b)
    expect(d.filter((l) => l.op !== ' ')).toEqual([
      { op: '-', text: 'line-300' },
      { op: '+', text: 'changed' },
    ])
  })

  it('masks addresses, redacts secrets/PII, and hides 5xx internal error messages', () => {
    expect(maskAddress('email', 'anna@example.test')).toBe('a***@example.test')
    expect(maskAddress('sms', '+44 7700 900123')).toBe('+44 •••• ••0123')
    expect(redactText('Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abcdefghij.klmnopqrst')).not.toContain('eyJhbGciOiJIUzI1NiJ9')
    expect(redactValue({ token: 'abc', nested: { phone: '+44 7700 900123', ok: 'fine' } })).toEqual({
      token: '[REDACTED]',
      nested: { phone: '[REDACTED]', ok: 'fine' },
    })
    expect(errorText(new HttpError(500, { error: { code: 'internal', message: 'pg error at /srv/db.rs:42' } }))).toBe(
      'Something went wrong on the server. Please try again.',
    )
  })
})
