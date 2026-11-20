// Dev mock of the Beacon API (ui/contract/openapi.yaml). In-memory, no deps.
// Used two ways:
// - inside `vite` dev as middleware (default when BEACON_API_URL is unset)
// - standalone via `npm run mock` (mock/server.ts), which also serves dist/
//
// It runs a toy engine (engine.ts): generated Appendix A events → schema
// detection → the rules in RULES (editable through /api/rules) → dedup by key
// → alert upserts streamed over SSE with the snapshot protocol. Runbooks,
// approvals, preferences and proposals are kept in memory too. Knobs (env):
//   MOCK_SEED=60           alerts to seed at startup (try 5000)
//   MOCK_RATE=2            events per second (try 200)
//   MOCK_STALE=0.03        chance to re-send an old version after an upsert
//                          (exercises the client's version check)
//   MOCK_RESNAPSHOT_S=0    close every stream every N s, forcing reconnect +
//                          fresh snapshot (simulates a rebalance)
//   MOCK_RETRY_OK=0        1 = the mock workflow engine accepts retries
//                          (default: retries fail, so "retry fails twice →
//                          escalate" can be shown); also POST /api/__mock/workflow-engine?ok=1

import type { IncomingMessage, ServerResponse } from 'node:http'
import type {
  AlertDetail,
  AlertStatus,
  ApiError,
  ApprovalRequest,
  EventSample,
  Execution,
  FeedAlert,
  PersonView,
  Preference,
  Proposal,
  ProposalCreate,
  RecentEvent,
  RemoveEvent,
  RuleItem,
  RuleSpec,
  RuleTestRequest,
  RuleWrite,
  RunbookSummary,
  Session,
  Severity,
  SnapshotEvent,
  TeamView,
  TimelineEntry,
  TransitionRequest,
} from '../src/api/types.ts'
import { inWindow, parseWindow, resolveChannels } from '../src/prefs/resolve.ts'
import { applyRule, context, dedupKey, parseDuration, recovers, testRule, validateRule, type Ctx } from './engine.ts'
import { failedRuns, runtimeOf, viewRunbook, type RunbookRuntime } from './runbooks.ts'
import { deliveriesFor, PEOPLE, randomEvent, RULES, RUNBOOKS, TEAMS, type DeliveryPlan } from './scenario.ts'

interface MockAlert {
  feed: FeedAlert
  key: string
  /** Pinned event context of the opening event (runbook templates use it). */
  ctx: Ctx
  plan: DeliveryPlan
  runbookId: string | null
  runbook: RunbookRuntime
  /** Version at the last status change; 409s compare against this. */
  statusVersion: number
  samples: EventSample[]
  timeline: TimelineEntry[]
  lastDedupNote: number
}

interface RuleRec {
  rule: RuleSpec
  revision: number
  updated_at: string
  updated_by: string
  /** hour epoch → matching events */
  fires: Map<number, number>
}

interface Client {
  res: ServerResponse
  team: string | null
}

const env = (k: string, d: number) => {
  const v = Number(process.env[k])
  return Number.isFinite(v) && process.env[k] !== '' && process.env[k] !== undefined ? v : d
}

const CSRF = 'mock-csrf-token'
const ME = 'user:erop'
const SNAPSHOT_LIMIT = 5000
const MAX_SAMPLES_HEAD = 3
const RECENT_MAX = 50
const HOUR = 3_600_000
const clone = <T>(v: T): T => structuredClone(v)

export function createMock() {
  const seed = env('MOCK_SEED', 60)
  const rate = env('MOCK_RATE', 2)
  const staleChance = env('MOCK_STALE', 0.03)
  const resnapshotS = env('MOCK_RESNAPSHOT_S', 0)
  let retryOk = env('MOCK_RETRY_OK', 0) === 1

  const alerts = new Map<string, MockAlert>()
  /** open|acked, dedup-active alert id by `${rule}|${key}` */
  const active = new Map<string, string>()
  const clients = new Set<Client>()
  const rules = new Map<string, RuleRec>()
  const people = new Map<string, PersonView>(PEOPLE.map((p) => [p.id, clone(p)]))
  const teams = new Map<string, TeamView>(TEAMS.map((t) => [t.id, clone(t)]))
  const proposals = new Map<string, Proposal>()
  const recent: RecentEvent[] = []
  let idSeq = 0
  let evSeq = 0
  let seq = 0
  const startedAt = new Date(Date.now() - 7 * 86_400_000).toISOString()
  for (const r of RULES) rules.set(r.id, { rule: clone(r), revision: 1, updated_at: startedAt, updated_by: 'config', fires: new Map() })

  const newId = (at: number) =>
    `alt_${at.toString(36).padStart(9, '0')}${(idSeq++).toString(36).padStart(4, '0')}`
  const uid = (p: string) => `${p}_${Date.now().toString(36)}${(seq++).toString(36)}`

  // ---- delivery planning: once, when an alert opens (spec §4.4) ------------

  function expand(to: string): string[] {
    const [kind, id] = to.split(':') as [string, string]
    if (kind === 'user') return [id]
    if (kind === 'team') return teams.get(id)?.members ?? []
    if (kind === 'oncall') for (const t of teams.values()) if (t.oncall[id]?.length) return [t.oncall[id]![0]!]
    return []
  }

  function plan(rule: RuleSpec, ctx: Ctx, severity: Severity, at: Date): DeliveryPlan {
    const byUser = new Map<string, Set<string>>()
    const facts = { severity, service: ctx.fields.service as string | undefined, team: rule.team }
    const teamDefault = teams.get(rule.team)?.default_channels ?? []
    for (const r of rule.route) {
      for (const u of expand(r.to)) {
        const chans = r.channels?.length ? r.channels : resolveChannels(people.get(u)?.preferences ?? [], facts, at, teamDefault).channels
        const set = byUser.get(u) ?? new Set()
        for (const c of chans) set.add(c)
        byUser.set(u, set)
      }
    }
    return [...byUser].flatMap(([u, set]) => [...set].filter((c) => c !== 'ui').map((c): [string, string] => [`user:${u}`, c]))
  }

  // ---- engine ------------------------------------------------------------

  function note(a: MockAlert, kind: string, actor: string, message: string, at = new Date()) {
    a.timeline.push({ at: at.toISOString(), actor, kind, message })
  }

  /** Bump the version so open detail panes refetch; publish over SSE. */
  function touch(a: MockAlert) {
    const prev = a.feed
    a.feed = { ...prev, version: prev.version + 1 }
    publish(a, prev)
  }

  function ingest(source: string, raw: unknown, at = new Date(), broadcast = true): string[] {
    const touched: string[] = []
    const eventId = `evt_${at.getTime().toString(36)}${(evSeq++).toString(36)}`
    const sample: EventSample = { event_id: eventId, received_at: at.toISOString(), raw }
    const ctx = context(raw, source)
    recent.unshift({ ...sample, source, schema: ctx.schema })
    if (recent.length > RECENT_MAX) recent.length = RECENT_MAX
    for (const rec of rules.values()) {
      const rule = rec.rule
      if (rule.enabled === false) continue
      if (recovers(rule, ctx)) {
        const k = `${rule.id}|${dedupKey(rule, ctx)}`
        const a = alerts.get(active.get(k) ?? '')
        if (a && a.feed.status !== 'resolved') {
          const prev = transition(a, 'resolved', 'system', 'recovered', at)
          touched.push(a.feed.id)
          if (broadcast) publish(a, prev)
        }
        continue
      }
      const m = applyRule(rule, ctx)
      if (!m) continue
      const hour = Math.floor(at.getTime() / HOUR)
      rec.fires.set(hour, (rec.fires.get(hour) ?? 0) + 1)
      const k = `${rule.id}|${m.key}`
      const existing = alerts.get(active.get(k) ?? '')
      const window = parseDuration(rule.dedup?.window) ?? 10 * 60_000
      if (existing && at.getTime() - Date.parse(existing.feed.last_seen) <= window) {
        const prev = existing.feed
        existing.feed = { ...prev, count: prev.count + 1, last_seen: at.toISOString(), version: prev.version + 1 }
        if (existing.samples.length > MAX_SAMPLES_HEAD) existing.samples[MAX_SAMPLES_HEAD] = sample
        else existing.samples.push(sample)
        if (at.getTime() - existing.lastDedupNote > 60_000) {
          existing.lastDedupNote = at.getTime()
          note(existing, 'deduplicated', 'system', `Happened again (${existing.feed.count} times so far)`, at)
        }
        touched.push(existing.feed.id)
        if (broadcast) publish(existing, prev)
        continue
      }
      // Outside the window the old alert retires from dedup but stays open.
      if (existing) active.delete(k)
      const id = newId(at.getTime())
      const feed: FeedAlert = {
        id,
        team: rule.team,
        version: 1,
        rule: { id: rule.id, revision: rec.revision, name: rule.name ?? rule.id },
        status: 'open',
        severity: m.severity,
        color: m.color,
        title: m.title,
        body: m.body,
        fields: m.fields,
        count: 1,
        first_seen: at.toISOString(),
        last_seen: at.toISOString(),
        source,
        schema: ctx.schema,
        acked_by: null,
        acked_at: null,
        resolved_by: null,
        resolved_at: null,
        resolved_reason: null,
      }
      const a: MockAlert = {
        feed,
        key: k,
        ctx,
        plan: plan(rule, ctx, m.severity, at),
        runbookId: rule.runbook ?? null,
        runbook: new Map(),
        statusVersion: 1,
        samples: [sample],
        timeline: [{ at: at.toISOString(), actor: `rule:${rule.id}`, kind: 'opened', message: `Alert rule “${rule.name ?? rule.id}” raised this alert` }],
        lastDedupNote: at.getTime(),
      }
      alerts.set(id, a)
      active.set(k, id)
      touched.push(id)
      if (broadcast) publish(a, null)
    }
    return touched
  }

  function transition(a: MockAlert, to: AlertStatus, actor: string, reason: string | null, at = new Date()) {
    const prev = a.feed
    const iso = at.toISOString()
    const next: FeedAlert = { ...prev, status: to, version: prev.version + 1 }
    if (to === 'acked') Object.assign(next, { acked_by: actor, acked_at: iso })
    if (to === 'resolved') {
      Object.assign(next, { resolved_by: actor, resolved_at: iso, resolved_reason: reason })
      if (active.get(a.key) === prev.id) active.delete(a.key)
    }
    a.feed = next
    a.statusVersion = next.version
    note(a, to === 'acked' ? 'acked' : 'resolved', actor, to === 'acked' ? 'Acknowledged' : `Resolved (${reason ?? 'user'})`, at)
    return prev
  }

  function remove(a: MockAlert, reason: RemoveEvent['reason']) {
    alerts.delete(a.feed.id)
    if (active.get(a.key) === a.feed.id) active.delete(a.key)
    const rm: RemoveEvent = { id: a.feed.id, version: a.feed.version + 1, reason }
    for (const c of clients) if (visible(c.team, a.feed)) send(c.res, 'alert.remove', rm)
  }

  // ---- runbook actions: off the alert pipeline, async (spec §6.2) ---------

  const def = (a: MockAlert) => RUNBOOKS.find((r) => r.id === a.runbookId) ?? null
  const stepDef = (a: MockAlert, step: string) => def(a)?.steps.find((s) => s.id === step) ?? null

  function actionOutcome(a: MockAlert, name: string, params: Record<string, string>): { ok: boolean; output: string } {
    switch (name) {
      case 'http_get':
        return { ok: true, output: `HTTP 200 from ${params.url} · {"status":"degraded","p99_ms":5120}` }
      case 'retry_workflow':
        return retryOk
          ? { ok: true, output: `Workflow ${params.workflowId} completed on retry` }
          : { ok: false, output: `Workflow ${params.workflowId} failed again at ${String((a.ctx.raw as any)?.step ?? 'the same step')}: TIMEOUT` }
      case 'escalate':
        return { ok: true, output: `Escalated to ${params.team}; their on-call person was paged` }
      case 'restart_service':
        return { ok: true, output: `${params.instance} restarted; quote latency back to 180 ms` }
      default:
        return { ok: false, output: `No such action "${name}"` }
    }
  }

  function execute(a: MockAlert, step: string, actor: string): Execution {
    const d = stepDef(a, step)!
    const rt = runtimeOf(a.runbook, step)
    const view = viewRunbook(def(a)!, a.ctx, a.runbook, ME).steps.find((s) => s.id === step)!
    const ex: Execution = { id: uid('exe'), state: 'queued', requested_by: actor, started_at: null, finished_at: null, output: null }
    rt.runs.push(ex)
    const label = d.action?.label ?? step
    touch(a)
    setTimeout(() => {
      ex.state = 'running'
      ex.started_at = new Date().toISOString()
      touch(a)
      setTimeout(() => {
        const out = actionOutcome(a, d.action!.name, view.action!.params)
        ex.state = out.ok ? 'ok' : 'failed'
        ex.output = out.output
        ex.finished_at = new Date().toISOString()
        note(a, 'step_run', actor, `${out.ok ? 'Ran' : 'Failed'} “${label}”: ${out.output}`)
        if (out.ok && d.resolves && a.feed.status !== 'resolved') {
          const prev = transition(a, 'resolved', 'system', 'runbook')
          publish(a, prev)
        } else touch(a)
      }, 1200)
    }, 400)
    return ex
  }

  // ---- SSE -----------------------------------------------------------------

  const visible = (team: string | null, a: FeedAlert) => team === null || a.team === team

  function send(res: ServerResponse, event: string, data: unknown) {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
  }

  function publish(a: MockAlert, prev: FeedAlert | null) {
    for (const c of clients) {
      if (!visible(c.team, a.feed)) continue
      send(c.res, 'alert.upsert', a.feed)
      // Out-of-order redelivery, as a replica or the outbox might produce.
      if (prev && Math.random() < staleChance) send(c.res, 'alert.upsert', prev)
    }
  }

  function page(team: string | null, q?: URLSearchParams) {
    const statuses = q?.getAll('status') ?? []
    const sevs = q?.getAll('severity') ?? []
    const ruleIds = q?.getAll('rule') ?? []
    let items = [...alerts.values()]
      .map((a) => a.feed)
      .filter((a) => visible(team, a))
      .filter((a) => !statuses.length || statuses.includes(a.status))
      .filter((a) => !sevs.length || sevs.includes(a.severity))
      .filter((a) => !ruleIds.length || ruleIds.includes(a.rule.id))
      .sort((x, y) => (x.first_seen < y.first_seen ? 1 : x.first_seen > y.first_seen ? -1 : x.id < y.id ? 1 : -1))
    const cursor = q?.get('cursor')
    if (cursor) {
      const [fs, id] = Buffer.from(cursor, 'base64url').toString().split('|')
      items = items.filter((a) => a.first_seen < fs! || (a.first_seen === fs && a.id < id!))
    }
    const limit = Math.min(Number(q?.get('limit') ?? SNAPSHOT_LIMIT) || SNAPSHOT_LIMIT, SNAPSHOT_LIMIT)
    const slice = items.slice(0, limit)
    const last = slice[slice.length - 1]
    const next_cursor = items.length > limit && last ? Buffer.from(`${last.first_seen}|${last.id}`).toString('base64url') : null
    return { items: slice, next_cursor }
  }

  function openStream(req: IncomingMessage, res: ServerResponse, url: URL) {
    const team = url.searchParams.get('team') || null
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    })
    res.write('retry: 1500\n\n')
    const client: Client = { res, team }
    // Subscribe first, then snapshot (trivially consistent here: one thread).
    clients.add(client)
    const p = page(team)
    const snap: SnapshotEvent = { team, items: p.items, next_cursor: p.next_cursor, server_time: new Date().toISOString() }
    send(res, 'snapshot', snap)
    req.on('close', () => clients.delete(client))
  }

  // ---- projections -----------------------------------------------------------

  function ruleItem(rec: RuleRec, now = Date.now()): RuleItem {
    const h = Math.floor(now / HOUR)
    const hourly = Array.from({ length: 24 }, (_, i) => rec.fires.get(h - 23 + i) ?? 0)
    let last: number | null = null
    for (const a of alerts.values()) if (a.feed.rule.id === rec.rule.id) last = Math.max(last ?? 0, Date.parse(a.feed.last_seen))
    return {
      rule: clone(rec.rule),
      revision: rec.revision,
      updated_at: rec.updated_at,
      updated_by: rec.updated_by,
      stats: {
        fires_24h: hourly.reduce((s, n) => s + n, 0),
        open_alerts: [...alerts.values()].filter((a) => a.feed.rule.id === rec.rule.id && a.feed.status !== 'resolved').length,
        last_fired_at: last === null ? null : new Date(last).toISOString(),
        hourly,
      },
    }
  }

  function personView(p: PersonView): PersonView {
    return { ...clone(p), teams: [...teams.values()].filter((t) => t.members.includes(p.id)).map((t) => t.id) }
  }

  function validatePrefs(prefs: unknown): { path: string; message: string }[] {
    const out: { path: string; message: string }[] = []
    if (!Array.isArray(prefs)) return [{ path: 'preferences', message: 'must be a list' }]
    const known = ['phone', 'sms', 'email', 'ui']
    prefs.forEach((p: any, i) => {
      const at = `preferences.${i}`
      const chans = (k: string, v: unknown) => {
        if (!Array.isArray(v)) return out.push({ path: `${at}.${k}`, message: 'must be a list of channels' })
        for (const c of v) if (!known.includes(c)) out.push({ path: `${at}.${k}`, message: `unknown channel "${c}"` })
      }
      if (p && 'default' in p) {
        chans('default', p.default)
        if (i !== prefs.length - 1) out.push({ path: at, message: 'the default must be the last line' })
        return
      }
      if (!p || typeof p.match !== 'object') return out.push({ path: at, message: 'needs a match' })
      chans('channels', p.channels)
      if (p.otherwise !== undefined) chans('otherwise', p.otherwise)
      if (p.when) {
        if (!parseWindow(String(p.when.between))) out.push({ path: `${at}.when.between`, message: 'use HH:MM-HH:MM' })
        else if (inWindow(p.when, new Date()) === null) out.push({ path: `${at}.when.tz`, message: `unknown time zone "${p.when.tz}"` })
      }
    })
    return out
  }

  // ---- HTTP ----------------------------------------------------------------

  function json(res: ServerResponse, status: number, body: unknown) {
    res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
    res.end(JSON.stringify(body))
  }
  const err = (res: ServerResponse, status: number, code: string, message: string, extra: Partial<ApiError['error']> = {}) =>
    json(res, status, { error: { code, message, ...extra } } satisfies ApiError)

  async function body(req: IncomingMessage): Promise<any> {
    const chunks: Buffer[] = []
    for await (const c of req) chunks.push(c as Buffer)
    const s = Buffer.concat(chunks).toString()
    return s ? JSON.parse(s) : {}
  }

  /** Async write handler: CSRF check + JSON body, errors mapped to ApiError. */
  function write(req: IncomingMessage, res: ServerResponse, fn: (b: any) => void) {
    void (async () => {
      if (req.headers['x-csrf-token'] !== CSRF) return err(res, 403, 'csrf', 'Missing or wrong X-CSRF-Token')
      let b: any
      try {
        b = await body(req)
      } catch {
        return err(res, 400, 'bad_request', 'Body is not JSON')
      }
      fn(b ?? {})
    })()
    return true
  }

  function detail(a: MockAlert): AlertDetail {
    const rb = def(a)
    return {
      ...a.feed,
      samples: a.samples,
      runbook: rb ? viewRunbook(rb, a.ctx, a.runbook, ME) : null,
      deliveries: deliveriesFor(a.feed, a.plan, people),
      timeline: a.timeline,
    }
  }

  /** Returns true if the request was handled. */
  function handle(req: IncomingMessage, res: ServerResponse): boolean {
    const url = new URL(req.url ?? '/', 'http://mock')
    const path = url.pathname
    const method = req.method ?? 'GET'
    let m: RegExpMatchArray | null

    if (path === '/api/session' && method === 'GET') {
      const session: Session = { user: { id: 'erop', name: 'erop (demo)' }, teams: [...teams.values()].map((t) => ({ id: t.id, name: t.name })), csrf_token: CSRF }
      return json(res, 200, session), true
    }
    if (path === '/api/stream' && method === 'GET') return openStream(req, res, url), true
    if (path === '/api/alerts' && method === 'GET') return json(res, 200, page(url.searchParams.get('team') || null, url.searchParams)), true

    if ((m = path.match(/^\/api\/alerts\/([^/]+)$/)) && method === 'GET') {
      const a = alerts.get(decodeURIComponent(m[1]!))
      if (!a) return err(res, 404, 'not_found', 'No such alert'), true
      return json(res, 200, detail(a)), true
    }

    if ((m = path.match(/^\/api\/alerts\/([^/]+)\/(ack|resolve)$/)) && method === 'POST') {
      const id = decodeURIComponent(m[1]!)
      const op = m[2] as 'ack' | 'resolve'
      return write(req, res, (reqBody: TransitionRequest) => {
        if (typeof reqBody.expected_version !== 'number') return err(res, 400, 'bad_request', 'expected_version is required')
        const a = alerts.get(id)
        if (!a) return err(res, 404, 'not_found', 'No such alert')
        const target: AlertStatus = op === 'ack' ? 'acked' : 'resolved'
        if (a.feed.status === target) return json(res, 200, a.feed)
        if (op === 'ack' && a.feed.status === 'resolved')
          return err(res, 409, 'invalid_transition', 'This alert is already resolved', { current: a.feed })
        if (a.statusVersion > reqBody.expected_version) {
          const by = a.feed.status === 'acked' ? a.feed.acked_by : a.feed.resolved_by
          return err(res, 409, 'version_conflict', `Alert was ${a.feed.status} by ${by ?? 'someone'} meanwhile`, { current: a.feed })
        }
        const prev = transition(a, target, ME, op === 'resolve' ? 'user' : null)
        publish(a, prev)
        json(res, 200, a.feed)
      })
    }

    // ---- runbook steps & approvals
    if ((m = path.match(/^\/api\/alerts\/([^/]+)\/steps\/([^/]+)\/(run|check)$/)) && method === 'POST') {
      const [id, step, op] = [decodeURIComponent(m[1]!), decodeURIComponent(m[2]!), m[3]!]
      return write(req, res, (b: { done?: boolean }) => {
        const a = alerts.get(id)
        if (!a) return err(res, 404, 'not_found', 'No such alert')
        const d = stepDef(a, step)
        if (!d) return err(res, 404, 'not_found', 'No such runbook step')
        const rt = runtimeOf(a.runbook, step)
        if (op === 'check') {
          if (d.kind !== 'manual') return err(res, 400, 'bad_request', 'Only manual steps are ticked off')
          const done = b.done !== false
          rt.done_by = done ? ME : null
          rt.done_at = done ? new Date().toISOString() : null
          note(a, 'step_run', ME, `${done ? 'Done' : 'Unticked'}: ${viewRunbook(def(a)!, a.ctx, a.runbook, ME).steps.find((s) => s.id === step)!.text}`)
          touch(a)
          return json(res, 200, { ok: true })
        }
        if (d.kind !== 'action' || !d.action) return err(res, 400, 'bad_request', 'This step has no action to run')
        if (a.feed.status === 'resolved') return err(res, 409, 'invalid_transition', 'This alert is already resolved')
        const view = viewRunbook(def(a)!, a.ctx, a.runbook, ME).steps.find((s) => s.id === step)!
        if (view.disabled_reason) return err(res, 409, 'invalid_transition', view.disabled_reason)
        if (['queued', 'running'].includes(view.state)) return err(res, 409, 'invalid_transition', 'This step is already running')
        if (d.approval === 'required') {
          const pending = view.approval_request?.state === 'pending' ? view.approval_request : null
          if (pending) return err(res, 409, 'approval_required', 'Waiting for approval', { approval_id: pending.id })
          const lead = teams.get(a.feed.team)?.lead
          const now = Date.now()
          const request: ApprovalRequest = {
            id: uid('apr'),
            state: 'pending',
            requested_by: ME,
            requested_at: new Date(now).toISOString(),
            expires_at: new Date(now + 15 * 60_000).toISOString(),
            eligible: [...new Set([...(lead ? [`user:${lead}`] : []), ME])],
            can_decide: false,
            sms_code: String(1000 + Math.floor(Math.random() * 9000)),
            decided_by: null,
            decided_at: null,
            note: null,
          }
          rt.approvals.push(request)
          note(a, 'approval', ME, `Asked for approval to run “${d.action.label}” (${view.text})`)
          touch(a)
          return err(res, 409, 'approval_required', 'This step needs approval first', { approval_id: request.id })
        }
        const ex = execute(a, step, ME)
        json(res, 202, { execution_id: ex.id })
      })
    }

    if ((m = path.match(/^\/api\/approvals\/([^/]+)\/(approve|reject)$/)) && method === 'POST') {
      const [id, op] = [decodeURIComponent(m[1]!), m[2]!]
      return write(req, res, (b: { note?: string }) => {
        for (const a of alerts.values())
          for (const [step, rt] of a.runbook) {
            const r = rt.approvals.find((x) => x.id === id)
            if (!r) continue
            if (r.state !== 'pending' || Date.parse(r.expires_at) <= Date.now())
              return err(res, 409, 'invalid_transition', `This request is already ${r.state === 'pending' ? 'expired' : r.state}`)
            if (!r.eligible.includes(ME)) return err(res, 403, 'forbidden', 'You are not an approver for this step')
            if (a.feed.status === 'resolved') return err(res, 409, 'invalid_transition', 'The alert was resolved meanwhile')
            r.state = op === 'approve' ? 'approved' : 'rejected'
            r.decided_by = ME
            r.decided_at = new Date().toISOString()
            r.note = b.note?.trim() || null
            const label = stepDef(a, step)?.action?.label ?? step
            note(a, 'approval', ME, `${op === 'approve' ? 'Approved' : 'Rejected'} “${label}”${r.note ? `: ${r.note}` : ''}`)
            // Approve = consume + audit + execution intent, together.
            if (op === 'approve') return json(res, 200, { execution_id: execute(a, step, r.requested_by).id })
            touch(a)
            return json(res, 200, { ok: true })
          }
        err(res, 404, 'not_found', 'No such approval request')
      })
    }

    // ---- rules
    if (path === '/api/rules' && method === 'GET') {
      const team = url.searchParams.get('team')
      const items = [...rules.values()].filter((r) => !team || r.rule.team === team).map((r) => ruleItem(r))
      items.sort((x, y) => x.rule.team.localeCompare(y.rule.team) || (x.rule.name ?? x.rule.id).localeCompare(y.rule.name ?? y.rule.id))
      return json(res, 200, { items }), true
    }
    if (path === '/api/rules/test' && method === 'POST') {
      return write(req, res, (b: RuleTestRequest) => {
        if (!b.rule || typeof b.rule !== 'object') return err(res, 400, 'bad_request', 'rule is required')
        json(res, 200, testRule(b.rule, b.event, b.source))
      })
    }
    if (path === '/api/rules' && method === 'POST') {
      return write(req, res, (b: RuleWrite) => {
        const problems = validateRule(b.rule)
        if (problems.length) return err(res, 422, 'invalid', 'The rule has problems', { details: problems })
        if (rules.has(b.rule.id)) return err(res, 409, 'revision_conflict', `A rule with id "${b.rule.id}" already exists`, { current: ruleItem(rules.get(b.rule.id)!) })
        const rec: RuleRec = { rule: clone(b.rule), revision: 1, updated_at: new Date().toISOString(), updated_by: ME, fires: new Map() }
        rules.set(b.rule.id, rec)
        json(res, 201, ruleItem(rec))
      })
    }
    if ((m = path.match(/^\/api\/rules\/([^/]+)$/))) {
      const id = decodeURIComponent(m[1]!)
      const rec = rules.get(id)
      if (method === 'GET') return rec ? json(res, 200, ruleItem(rec)) : err(res, 404, 'not_found', 'No such rule'), true
      if (method === 'PUT')
        return write(req, res, (b: RuleWrite) => {
          if (!rec) return err(res, 404, 'not_found', 'No such rule')
          if (b.rule?.id !== id) return err(res, 400, 'bad_request', 'A rule’s id can’t be changed')
          if (b.expected_revision !== rec.revision)
            return err(res, 409, 'revision_conflict', `${rec.updated_by.replace(/^user:/, '')} changed this rule meanwhile (revision ${rec.revision})`, { current: ruleItem(rec) })
          const problems = validateRule(b.rule)
          if (problems.length) return err(res, 422, 'invalid', 'The rule has problems', { details: problems })
          Object.assign(rec, { rule: clone(b.rule), revision: rec.revision + 1, updated_at: new Date().toISOString(), updated_by: ME })
          json(res, 200, ruleItem(rec))
        })
      if (method === 'DELETE')
        return write(req, res, () => {
          if (!rec) return err(res, 404, 'not_found', 'No such rule')
          const exp = Number(url.searchParams.get('expected_revision'))
          if (exp !== rec.revision) return err(res, 409, 'revision_conflict', 'This rule changed meanwhile', { current: ruleItem(rec) })
          rules.delete(id)
          json(res, 200, { deleted: id })
        })
    }
    if (path === '/api/runbooks' && method === 'GET') {
      const items: RunbookSummary[] = RUNBOOKS.map((r) => ({ id: r.id, title: r.title, format: r.format, team: r.team, steps: r.steps.length }))
      return json(res, 200, { items }), true
    }
    if (path === '/api/events/recent' && method === 'GET') {
      const limit = Math.min(Number(url.searchParams.get('limit') ?? 20) || 20, RECENT_MAX)
      return json(res, 200, { items: recent.slice(0, limit) }), true
    }

    // ---- people & teams
    if (path === '/api/people' && method === 'GET') return json(res, 200, { items: [...people.values()].map(personView) }), true
    if ((m = path.match(/^\/api\/people\/([^/]+)(\/preferences)?$/))) {
      const p = people.get(decodeURIComponent(m[1]!))
      if (method === 'GET') return p ? json(res, 200, personView(p)) : err(res, 404, 'not_found', 'No such person'), true
      if (method === 'PUT' && m[2])
        return write(req, res, (b: { preferences: Preference[]; expected_revision: number }) => {
          if (!p) return err(res, 404, 'not_found', 'No such person')
          if (b.expected_revision !== p.revision) return err(res, 409, 'revision_conflict', 'These preferences changed meanwhile', { current: personView(p) })
          const problems = validatePrefs(b.preferences)
          if (problems.length) return err(res, 422, 'invalid', 'The preferences have problems', { details: problems })
          p.preferences = clone(b.preferences)
          p.revision++
          json(res, 200, personView(p))
        })
    }
    if (path === '/api/teams' && method === 'GET') return json(res, 200, { items: [...teams.values()].map(clone) }), true
    if ((m = path.match(/^\/api\/teams\/([^/]+)$/))) {
      const t = teams.get(decodeURIComponent(m[1]!))
      if (method === 'GET') return t ? json(res, 200, t) : err(res, 404, 'not_found', 'No such team'), true
      if (method === 'PUT')
        return write(req, res, (b: Partial<TeamView> & { expected_revision: number }) => {
          if (!t) return err(res, 404, 'not_found', 'No such team')
          if (b.expected_revision !== t.revision) return err(res, 409, 'revision_conflict', 'This team changed meanwhile', { current: t })
          const members = b.members ?? t.members
          const unknown = members.filter((u) => !people.has(u))
          if (unknown.length) return err(res, 422, 'invalid', `Unknown people: ${unknown.join(', ')}`, { details: [{ path: 'members', message: 'unknown person' }] })
          if (b.lead && !members.includes(b.lead)) return err(res, 422, 'invalid', 'The lead must be a member', { details: [{ path: 'lead', message: 'not a member' }] })
          Object.assign(t, {
            members,
            lead: b.lead ?? null,
            oncall: b.oncall ?? t.oncall,
            default_channels: b.default_channels ?? t.default_channels,
            revision: t.revision + 1,
          })
          json(res, 200, t)
        })
    }

    // ---- proposals
    if (path === '/api/proposals' && method === 'GET') {
      const items = [...proposals.values()].sort((x, y) => (x.created_at < y.created_at ? 1 : -1))
      return json(res, 200, { items }), true
    }
    if (path === '/api/proposals' && method === 'POST') {
      return write(req, res, (b: ProposalCreate) => {
        const rec = rules.get(b.rule_id)
        if (!rec) return err(res, 404, 'not_found', 'No such rule')
        const problems = validateRule(b.proposed_rule)
        if (problems.length) return err(res, 422, 'invalid', 'The proposed rule has problems', { details: problems })
        if (!b.summary?.trim()) return err(res, 422, 'invalid', 'Say in a line what should change', { details: [{ path: 'summary', message: 'required' }] })
        const base = b.base_revision === rec.revision ? rec.rule : null
        if (!base) return err(res, 409, 'revision_conflict', 'The rule changed while you were writing this', { current: ruleItem(rec) })
        const p: Proposal = {
          id: uid('prp'),
          rule_id: rec.rule.id,
          rule_name: rec.rule.name ?? rec.rule.id,
          kind: b.kind,
          summary: b.summary.trim(),
          note: b.note?.trim() ?? '',
          base_revision: rec.revision,
          base_rule: clone(base),
          proposed_rule: { ...clone(b.proposed_rule), id: rec.rule.id },
          author: ME,
          auto: false,
          alert_id: b.alert_id ?? null,
          created_at: new Date().toISOString(),
          state: 'pending',
          decided_by: null,
          decided_at: null,
        }
        proposals.set(p.id, p)
        const a = b.alert_id ? alerts.get(b.alert_id) : undefined
        if (a) {
          note(a, 'comment', ME, `Suggested a change to the rule: ${p.summary}`)
          touch(a)
        }
        json(res, 201, p)
      })
    }
    if ((m = path.match(/^\/api\/proposals\/([^/]+)\/(accept|reject)$/)) && method === 'POST') {
      const [id, op] = [decodeURIComponent(m[1]!), m[2]!]
      return write(req, res, () => {
        const p = proposals.get(id)
        if (!p) return err(res, 404, 'not_found', 'No such proposal')
        if (p.state !== 'pending') return err(res, 409, 'invalid_transition', `Already ${p.state}`)
        const rec = rules.get(p.rule_id)
        const decide = (state: Proposal['state']) => Object.assign(p, { state, decided_by: ME, decided_at: new Date().toISOString() })
        if (op === 'reject') return decide('rejected'), json(res, 200, p)
        if (!rec) return decide('stale'), err(res, 409, 'revision_conflict', 'The rule was deleted')
        // Accepting is an ordinary publish against the proposal's base revision.
        if (rec.revision !== p.base_revision) {
          decide('stale')
          return err(res, 409, 'revision_conflict', `The rule changed since this was proposed (now revision ${rec.revision}). Review it again.`, { current: ruleItem(rec) })
        }
        Object.assign(rec, { rule: clone(p.proposed_rule), revision: rec.revision + 1, updated_at: new Date().toISOString(), updated_by: ME })
        decide('accepted')
        json(res, 200, p)
      })
    }

    // ---- ingest & mock controls
    if ((m = path.match(/^\/ingest\/([^/]+)$/)) && method === 'POST') {
      const source = decodeURIComponent(m[1]!)
      void (async () => {
        try {
          const raw = await body(req)
          const touched = ingest(source, raw)
          json(res, 202, { id: `evt_${Date.now().toString(36)}`, matched_alerts: touched })
        } catch {
          err(res, 400, 'bad_request', 'Body is not JSON')
        }
      })()
      return true
    }
    if (path === '/api/__mock/burst' && method === 'POST') {
      const n = Math.min(Number(url.searchParams.get('n') ?? 200) || 200, 20_000)
      for (let i = 0; i < n; i++) {
        const e = randomEvent(new Date())
        ingest(e.source, e.raw)
      }
      return json(res, 200, { ingested: n, alerts: alerts.size }), true
    }
    if (path === '/api/__mock/workflow-engine' && method === 'POST') {
      retryOk = url.searchParams.get('ok') === '1'
      return json(res, 200, { retry_ok: retryOk }), true
    }
    if (path.startsWith('/api/') || path.startsWith('/ingest/')) {
      err(res, 404, 'not_found', `No mock route for ${method} ${path}`)
      return true
    }
    return false
  }

  // ---- seed + live tick -------------------------------------------------

  const now = Date.now()
  // Replay history over a span that grows with the seed size. Alerts are
  // resolved along the way so their dedup keys can open new alerts; without
  // that the small key pools would cap the feed at ~150 rows.
  const spanMs = Math.max(3, seed / 150) * HOUR
  const seedEvents = Array.from({ length: seed * 12 }, () => now - Math.random() * spanMs).sort((a, b) => a - b)
  for (const t of seedEvents) {
    if (alerts.size >= seed) break
    const e = randomEvent(new Date(t))
    ingest(e.source, e.raw, new Date(t), false)
    if (now - t > 1800_000 && Math.random() < 0.35) {
      const ids = [...active.values()]
      const a = alerts.get(ids[Math.floor(Math.random() * ids.length)]!)
      if (a && t - Date.parse(a.feed.last_seen) > 300_000) transition(a, 'resolved', pickActor(), 'user', new Date(t))
    }
  }
  // Age some of the history: older alerts are mostly acked or resolved.
  for (const a of alerts.values()) {
    if (a.feed.status === 'resolved') continue
    const age = now - Date.parse(a.feed.first_seen)
    const r = Math.random()
    const at = new Date(Date.parse(a.feed.last_seen) + 60_000 + Math.random() * 600_000)
    if (at.getTime() > now) continue
    if (age > 3600_000 && r < 0.6) transition(a, 'resolved', pickActor(), 'user', at)
    else if (age > 1200_000 && r < 0.4) transition(a, 'acked', pickActor(), null, at)
  }
  // Older history for the fire counts and the auto-suggested proposal.
  const pricing = rules.get('pricing-engine-warnings')
  if (pricing) {
    for (let h = 1; h < 24; h++) {
      const hour = Math.floor(now / HOUR) - h
      pricing.fires.set(hour, (pricing.fires.get(hour) ?? 0) + 8 + Math.floor(Math.random() * 14))
    }
    const proposed = clone(pricing.rule)
    proposed.group = { by: ['fields.instrument'], window: '5m' }
    proposed.dedup = { key: ['fields.instrument'], window: '15m' }
    proposals.set('prp_auto_pricing', {
      id: 'prp_auto_pricing',
      rule_id: pricing.rule.id,
      rule_name: pricing.rule.name ?? pricing.rule.id,
      kind: 'noisy',
      summary: 'Group into one digest per instrument every 5 minutes',
      note: 'This rule fired 412 times in the last 7 days. 96% of its alerts were acknowledged with no runbook step run, which usually means people skim them. Grouping per instrument keeps the signal and cuts the alerts to about one per instrument per 5 minutes.',
      base_revision: pricing.revision,
      base_rule: clone(pricing.rule),
      proposed_rule: proposed,
      author: 'system',
      auto: true,
      alert_id: null,
      created_at: new Date(now - 2 * HOUR).toISOString(),
      state: 'pending',
      decided_by: null,
      decided_at: null,
    })
  }

  const timers: NodeJS.Timeout[] = []
  if (rate > 0) {
    const perTick = Math.max(1, Math.round(rate / 10))
    const interval = Math.max(10, Math.round((1000 * perTick) / rate))
    timers.push(
      setInterval(() => {
        for (let i = 0; i < perTick; i++) {
          const e = randomEvent(new Date())
          ingest(e.source, e.raw)
        }
        // Quiet alerts auto-resolve (spec: auto_resolve.quiet_for), so the
        // open set stays around MOCK_SEED instead of growing forever.
        const open = [...alerts.values()].filter((a) => a.feed.status !== 'resolved')
        if (open.length > seed * 0.6 && Math.random() < 0.3 * perTick) {
          const quietest = open.sort((x, y) => (x.feed.last_seen < y.feed.last_seen ? -1 : 1))[0]
          if (quietest) publish(quietest, transition(quietest, 'resolved', 'system', 'quiet_for'))
        }
        // And old resolved alerts expire out of the feed.
        if (alerts.size > seed * 1.5 && Math.random() < 0.05) {
          const oldest = [...alerts.values()].filter((a) => a.feed.status === 'resolved').sort((x, y) => (x.feed.first_seen < y.feed.first_seen ? -1 : 1))[0]
          if (oldest) remove(oldest, 'expired')
        }
      }, interval),
    )
  }
  timers.push(setInterval(() => { for (const c of clients) c.res.write(': ping\n\n') }, 15_000))
  if (resnapshotS > 0) {
    timers.push(setInterval(() => { for (const c of clients) c.res.end() ; clients.clear() }, resnapshotS * 1000))
  }
  for (const t of timers) t.unref?.()

  return {
    handle,
    close() {
      for (const t of timers) clearInterval(t)
      for (const c of clients) c.res.end()
    },
    stats: () => ({ alerts: alerts.size, clients: clients.size, rules: rules.size }),
    /** Test hook: failed runs of a step on an alert. */
    failedRuns: (alertId: string, step: string) => failedRuns(alerts.get(alertId)?.runbook ?? new Map(), step),
  }
}

function pickActor() {
  return ['user:anna', 'user:bob', 'user:carol'][Math.floor(Math.random() * 3)]!
}
