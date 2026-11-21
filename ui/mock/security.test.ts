import type { IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'
import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { afterEach, describe, expect, it } from 'vitest'
import { Runbook, safeUrl } from '../src/alert/Runbook'
import { errorText, HttpError } from '../src/api/client'
import type { FeedAlert, RunbookView } from '../src/api/types'
import { diffLines } from '../src/lib/diff'
import { maskAddress, redactText, redactValue } from '../src/lib/redact'
import { parseHash } from '../src/lib/router'
import { fromYaml } from '../src/rules/draft'
import { compileSafeRegex, testRule, validateRule } from './engine'
import { prepareRunbook, validateActionUrl } from './import-runbook'
import { createMock } from './mock'
import { RULES } from './scenario'
import { parseMapping } from './schemas'
import { createSimulator } from './simulator'

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

    // Check raw sample, alert card presentation & deliveries redaction on alert detail
    const d1 = (await callMock(mock, 'GET', `/api/alerts/${alertId}`)).json()
    expect(d1.samples[0].raw.api_key).toBe('[REDACTED]')
    expect(d1.samples[0].raw.error.message).not.toContain('supersecrettoken12345')
    expect(d1.samples[0].raw.error.message).not.toContain('alice@corp.test')
    expect(d1.body).not.toContain('supersecrettoken12345')
    expect(d1.body).not.toContain('alice@corp.test')
    for (const f of d1.fields) {
      expect(f.value).not.toContain('supersecrettoken12345')
      expect(f.value).not.toContain('alice@corp.test')
    }
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
  it('blocks ReDoS nested quantifiers, quantified alternation, backreferences, and oversized patterns', () => {
    expect(compileSafeRegex('(a+)+$').ok).toBe(false)
    expect(compileSafeRegex('(a|aa)+$').ok).toBe(false)
    expect(compileSafeRegex('(a)\\1').ok).toBe(false)
    expect(compileSafeRegex('a'.repeat(300)).ok).toBe(false)
    expect(compileSafeRegex('^prod-[a-z0-9]+$').ok).toBe(true)
    expect(compileSafeRegex('^(prod|staging)$').ok).toBe(true)

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

describe('PR #14 security & privacy controls: runbook import, schemas, simulator, feed params', () => {
  it('enforces team authz before parsing, blocks SSRF & prototype keys, and redacts imported runbooks', async () => {
    const mock = bootMock()
    const csrf = (await callMock(mock, 'GET', '/api/session')).json().csrf_token as string
    const h = { 'x-csrf-token': csrf }

    // Non-member team gets 403 immediately even with malformed YAML
    const unauthImport = await callMock(mock, 'POST', '/api/runbooks/import', {
      headers: h,
      body: { format: 'yaml', team: 'pricing', title: 'Evil', content: ':::not-yaml' },
    })
    expect(unauthImport.status).toBe(403)

    // SSRF targets and credentialed/templated URLs are rejected for http_get
    for (const badUrl of [
      'http://169.254.169.254/latest/meta-data/',
      'http://127.0.0.1:8080/admin',
      'http://localhost/health',
      'http://[::1]/health',
      'http://[::ffff:127.0.0.1]/health',
      'http://2130706433/health',
      'http://0.0.0.0/health',
      'http://metadata.google.internal/computeMetadata/v1/',
      'http://{{raw.host}}/health',
      'http://user:pass@counterparty-service/health',
      'javascript:alert(1)',
    ]) {
      expect(validateActionUrl(badUrl), badUrl).toBe(false)
      const res = await callMock(mock, 'POST', '/api/runbooks/import', {
        headers: h,
        body: {
          format: 'yaml',
          team: 'settlements',
          title: 'SSRF check',
          content: `steps:\n  - action: http_get\n    url: "${badUrl}"\n`,
        },
      })
      expect(res.status, badUrl).toBe(422)
    }

    // Valid service URL and safeUrl credential check
    expect(validateActionUrl('http://counterparty-service/health')).toBe(true)
    expect(safeUrl('https://user:pass@wiki.example/runbook')).toBeNull()
    expect(safeUrl('javascript:alert(1)')).toBeNull()
    expect(safeUrl('https://wiki.example/runbook')).toBe('https://wiki.example/runbook')

    // Prototype keys in YAML runbook are rejected
    expect(() =>
      prepareRunbook({
        format: 'yaml',
        team: 'settlements',
        title: 'Proto check',
        content: 'constructor: evil\nsteps:\n  - check: ok\n',
      }),
    ).toThrow(/Forbidden key/i)

    // Imported runbook redacts deleted steps and renders raw secrets safely on alerts
    const draft = {
      format: 'markdown',
      team: 'settlements',
      title: 'Redaction check',
      content: '1. Inspect {{ raw.api_key }} and {{ raw.error.message }}\n2. SECRET_DELETED_LINE password=supersecret',
    }
    const prev = (await callMock(mock, 'POST', '/api/runbooks/import', { headers: h, body: draft })).json()
    expect(prev.steps).toHaveLength(2)
    // User removes step 2 before saving
    const saved = await callMock(mock, 'POST', '/api/runbooks', {
      headers: h,
      body: { ...draft, ...prev, steps: [prev.steps[0]] },
    })
    expect(saved.status).toBe(201)
    const bookId = saved.json().id as string

    await callMock(mock, 'POST', '/api/rules', {
      headers: h,
      body: { rule: { ...RULES[0]!, id: 'settlements-redacted-rb', runbook: bookId } },
    })
    const ing = await callMock(mock, 'POST', '/ingest/kafka:workflows', {
      headers: h,
      body: {
        type: 'WORKFLOW_FAILED',
        workflowId: 'WF-RB-SEC',
        workflowName: 'order-settlement',
        step: 'validate-counterparty',
        severity: 'ERROR',
        environment: 'prod',
        timestamp: new Date().toISOString(),
        error: { code: 'TIMEOUT', message: 'Bearer topsecret99999 from bob@corp.test' },
        api_key: 'sk-live-do-not-leak',
      },
    })
    const alertId = ing.json().matched_alerts.find((id: string) => id)
    const list = (await callMock(mock, 'GET', '/api/alerts?rule=settlements-redacted-rb')).json().items
    const detail = (await callMock(mock, 'GET', `/api/alerts/${list[0]?.id ?? alertId}`)).json()
    expect(detail.runbook.text).not.toContain('SECRET_DELETED_LINE')
    expect(detail.runbook.text).not.toContain('supersecret')
    expect(detail.runbook.steps[0].text).not.toContain('sk-live-do-not-leak')
    expect(detail.runbook.steps[0].text).not.toContain('topsecret99999')
    expect(detail.runbook.steps[0].text).not.toContain('bob@corp.test')
    expect(detail.runbook.steps[0].text).toContain('[REDACTED]')
  })

  it('prevents schema mapping secret-key aliasing, prototype keys, and invalid preview sources', async () => {
    const mock = bootMock()
    const csrf = (await callMock(mock, 'GET', '/api/session')).json().csrf_token as string
    const h = { 'x-csrf-token': csrf }

    // Forbidden keys & excessive path depth rejected
    expect('error' in parseMapping({
      schema: 'custom.proto',
      detect: { path: 'a', op: 'exists' },
      fields: { ok: { value: { constructor: 'bad' } } },
    })).toBe(true)
    expect('error' in parseMapping({
      schema: 'custom.deep',
      detect: { path: Array(20).fill('a').join('.'), op: 'exists' },
      fields: {},
    })).toBe(true)

    // Invalid source rejected on preview
    const badSource = await callMock(mock, 'POST', '/api/schemas/preview', {
      headers: h,
      body: {
        spec: { schema: 'custom.ev', detect: { path: 'x', op: 'exists' }, fields: {} },
        event: { x: 1 },
        source: 'bad source!',
      },
    })
    expect(badSource.status).toBe(422)

    // Aliasing sensitive source path (api_key -> label) must stay redacted
    const preview = await callMock(mock, 'POST', '/api/schemas/preview', {
      headers: h,
      body: {
        spec: {
          schema: 'custom.leak',
          detect: { path: 'kind', op: 'eq', value: 'x' },
          fields: {
            label: { path: 'api_key' },
            contact_tag: { path: '/nested/email' },
            safe: { path: 'kind' },
          },
        },
        event: { kind: 'x', api_key: 'sk-live-secret-alias', nested: { email: 'alice@corp.test' } },
        source: 'http:playground',
      },
    })
    expect(preview.status).toBe(200)
    expect(preview.json().fields).toEqual({
      label: '[REDACTED]',
      contact_tag: '[REDACTED]',
      safe: 'x',
    })
  })

  it('blocks SMS approval brute-force bypass, enforces rate limits, and validates feed query params', async () => {
    const mock = bootMock()
    const csrf = (await callMock(mock, 'GET', '/api/session')).json().csrf_token as string
    const h = { 'x-csrf-token': csrf }

    // Invalid recipient / cursor rejected on /api/alerts & /api/stream
    expect((await callMock(mock, 'GET', '/api/alerts?recipient=../admin')).status).toBe(400)
    expect((await callMock(mock, 'GET', '/api/alerts?cursor=not-a-valid-cursor')).status).toBe(400)
    expect((await callMock(mock, 'GET', '/api/stream?recipient=bad%20id')).status).toBe(400)

    // Security headers present on JSON responses
    const s = await callMock(mock, 'GET', '/api/session')
    expect(s.headers['X-Content-Type-Options']).toBe('nosniff')
    expect(s.headers['Cache-Control']).toBe('no-store')

    // Create a settlements runbook with an immediate approval-required high-risk step
    const rbDraft = {
      format: 'yaml',
      team: 'settlements',
      title: 'Immediate escalate',
      content: 'steps:\n  - action: escalate\n    team: ops\n',
    }
    const rbPrev = (await callMock(mock, 'POST', '/api/runbooks/import', { headers: h, body: rbDraft })).json()
    const rbSaved = (await callMock(mock, 'POST', '/api/runbooks', { headers: h, body: { ...rbDraft, ...rbPrev } })).json()
    await callMock(mock, 'POST', '/api/rules', {
      headers: h,
      body: {
        rule: {
          ...RULES[0]!,
          id: 'settlements-fast-approval',
          when: { path: 'raw.fast_approval', op: 'exists' },
          dedup: { key: ['raw.fast_approval'], window: '10m' },
          runbook: rbSaved.id,
        },
      },
    })
    const ingB = await callMock(mock, 'POST', '/ingest/http:playground', { headers: h, body: { fast_approval: 'B' } })
    const idB = ingB.json().matched_alerts[0] as string
    const reqB = await callMock(mock, 'POST', `/api/alerts/${idB}/steps/step-1/run`, { headers: h, body: {} })
    expect(reqB.status).toBe(409)

    const ingA = await callMock(mock, 'POST', '/ingest/http:playground', { headers: h, body: { fast_approval: 'A' } })
    const idA = ingA.json().matched_alerts[0] as string
    const reqA = await callMock(mock, 'POST', `/api/alerts/${idA}/steps/step-1/run`, { headers: h, body: {} })
    expect(reqA.status).toBe(409)

    const msgs = (await callMock(mock, 'GET', '/api/simulator/messages')).json().items as any[]
    const smsA = msgs.find((m) => m.alert_id === idA && m.approval_challenge)
    const smsB = msgs.find((m) => m.alert_id === idB && m.approval_challenge)
    expect(smsA).toBeTruthy()
    expect(smsB).toBeTruthy()

    // Decide the newer approval (A) first via its bound receipt
    const decA = await callMock(mock, 'POST', '/api/simulator/reply', {
      headers: h,
      body: { from: smsA.to, text: smsA.approval_challenge.replace(/^Y/, 'N'), receipt_id: smsA.receipt_id },
    })
    expect(decA.json().outcome).toBe('rejected')

    // Now try 5 wrong guesses WITHOUT receipt_id: MUST count against pending B (not return already_decided from A!)
    const wrongCode = smsB.approval_challenge === 'Y 9999' ? 'Y 9998' : 'Y 9999'
    for (let i = 0; i < 5; i++) {
      const bad = await callMock(mock, 'POST', '/api/simulator/reply', {
        headers: h,
        body: { from: smsB.to, text: wrongCode },
      })
      expect(bad.json().outcome).toBe('wrong_code')
    }
    // 6th attempt even with the REAL code for B MUST be locked out
    const locked = await callMock(mock, 'POST', '/api/simulator/reply', {
      headers: h,
      body: { from: smsB.to, text: smsB.approval_challenge },
    })
    expect(locked.json().outcome).toBe('too_many_attempts')

    // Verify sms_attempts is never leaked on alert detail
    const detailB = (await callMock(mock, 'GET', `/api/alerts/${idB}`)).json()
    expect(detailB.runbook.steps[0].approval_request).not.toHaveProperty('sms_attempts')
    expect(detailB.runbook.steps[0].approval_request).not.toHaveProperty('sms_code_hash')

    // Rate limiting kicks in after 30 replies
    let hit429 = false
    for (let i = 0; i < 35; i++) {
      const r = await callMock(mock, 'POST', '/api/simulator/reply', {
        headers: h,
        body: { from: smsB.to, text: wrongCode },
      })
      if (r.status === 429) {
        hit429 = true
        break
      }
    }
    expect(hit429).toBe(true)
  })

  it('scopes simulator approval challenges to team members and preserves workflow IDs during phone redaction', () => {
    const sim = createSimulator(
      (team) => team === 'settlements' || team === 'pricing',
      (team) => team === 'settlements',
    )
    sim.send(
      'pricing',
      {
        receipt_id: 'sms_pricing_1',
        job_id: 'apr_pricing_1',
        alert_id: 'alt_pricing_1',
        channel: 'sms',
        recipient: 'user:dave',
        to: '+447700900126',
        text: 'Approve Restart? Reply Y 4321 or N 4321.',
        approval_challenge: 'Y 4321',
        duplicates: 0,
        received_at: new Date().toISOString(),
      },
      { id: 'apr_pricing_1', step: 'restart' },
      '+447700900126',
    )
    const listed = sim.list().items
    expect(listed).toHaveLength(1)
    expect(listed[0]!.approval_challenge).toBeNull()
    expect(listed[0]!.to).toMatch(/^\+\d{1,3} •••• ••0126$/)
    expect(listed[0]!.to).not.toContain('7700')
    expect(listed[0]!.text).not.toContain('4321')
    expect(sim.approvalSms('+447700900126', () => true, () => true)).toEqual({ kind: 'none' })
    sim.close()

    // Workflow IDs with embedded dates must not be mangled by phone redaction, while real phones are masked
    expect(redactText('Inspect WF-20261023-0040')).toBe('Inspect WF-20261023-0040')
    expect(redactText('Call +44 7700 900125 about WF-20261023-0040')).toBe('Call +44 •••• ••0125 about WF-20261023-0040')
  })
})
