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

import { createHash, randomBytes, timingSafeEqual } from 'node:crypto'
import type { IncomingMessage, ServerResponse } from 'node:http'
import type {
  AlertDetail,
  AlertStatus,
  ApiError,
  EventSample,
  Execution,
  FeedAlert,
  PersonDirectoryEntry,
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
import type { DiscoveredSchema, SchemaMapping, SourceView } from '../src/api/screens.ts'
import { createSimulator, type DeviceRecord } from './simulator.ts'
import { importRunbook, prepareRunbook } from './import-runbook.ts'
import { mappedContext, parseMapping, previewMapping } from './schemas.ts'
import { internationalPhone } from '../src/lib/phone.ts'
import { redactText, redactValue } from '../src/lib/redact.ts'
import { inWindow, parseWindow, resolveChannels } from '../src/prefs/resolve.ts'
import { applyRule, dedupKey, parseDuration, recovers, testRule, validateRule, type Ctx } from './engine.ts'
import { failedRuns, renderActionParams, runtimeOf, viewRunbook, type RunbookRuntime, type StoredApproval } from './runbooks.ts'
import { deliveriesFor, PEOPLE, randomEvent, RULES, RUNBOOKS, TEAMS, type DeliveryPlan, type RunbookDef } from './scenario.ts'

interface MockAlert {
  feed: FeedAlert
  key: string | null
  /** Pinned event context of the opening event (runbook templates use it). */
  ctx: Ctx
  plan: DeliveryPlan
  occurrences: Map<number, number>
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
  recipient: string | null
}

const env = (k: string, d: number) => {
  const v = Number(process.env[k])
  return Number.isFinite(v) && process.env[k] !== '' && process.env[k] !== undefined ? v : d
}

const SNAPSHOT_LIMIT = 5000
const MAX_SAMPLES_HEAD = 3
const RECENT_MAX = 50
const MAX_BODY_BYTES = 256 * 1024
const MAX_BURST = 5_000
const INGEST_WINDOW_MS = 60_000
const INGEST_MAX_PER_WINDOW = 120
const REPLY_WINDOW_MS = 60_000
const REPLY_MAX_PER_WINDOW = 30
const MAX_SSE_CLIENTS = 100
const SOURCE_RE = /^[a-z0-9_-]+:[a-z0-9._-]+$/i
const RECIPIENT_RE = /^[a-z0-9_-]{1,64}$/i
const HOUR = 3_600_000
const clone = <T>(v: T): T => structuredClone(v)

function verifySmsCode(code: string | undefined, expectedHash: string | null | undefined): boolean {
  if (!code || !expectedHash) return false
  const actual = createHash('sha256').update(code).digest()
  const expected = Buffer.from(expectedHash, 'hex')
  return actual.length === expected.length && timingSafeEqual(actual, expected)
}

function decodeCursor(cursor: string): [string, string] | null {
  if (cursor.length > 256 || !/^[A-Za-z0-9_-]+$/.test(cursor)) return null
  const decoded = Buffer.from(cursor, 'base64url').toString()
  const parts = decoded.split('|')
  if (parts.length !== 2 || !parts[0] || !parts[1]) return null
  return [parts[0], parts[1]]
}

function safeDecode(s: string): string | null {
  try {
    return decodeURIComponent(s)
  } catch {
    return null
  }
}

class BodyTooLargeError extends Error {}

interface StoredRecent {
  event: RecentEvent
  teams: Set<string>
}

export function createMock({ demoUserId = 'erop' }: { demoUserId?: string } = {}) {
  const demoUser = PEOPLE.find((person) => person.id === demoUserId)
  if (!demoUser) throw new Error(`Unknown mock user "${demoUserId}"`)
  const { id: ME_ID, name: demoUserName } = demoUser
  const ME = `user:${ME_ID}`
  const csrf = process.env.MOCK_CSRF_TOKEN || randomBytes(24).toString('base64url')
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
  const deletedRuleRevisions = new Map<string, number>()
  const people = new Map<string, PersonView>(PEOPLE.map((p) => [p.id, clone(p)]))
  const teams = new Map<string, TeamView>(TEAMS.map((t) => [t.id, clone(t)]))
  const proposals = new Map<string, Proposal>()
  const recent: StoredRecent[] = []
  const discovered = new Map<string, { entry: DiscoveredSchema; recent: StoredRecent }>()
  const schemas = new Map<string, SchemaMapping>()
  const runbooks = new Map(RUNBOOKS.map((book) => [book.id, book]))
  const sources = new Map<string, { last: string | null; hours: Map<number, number> }>(
    ['http:playground', 'http:alertmanager', 'http:ci', 'kafka:workflows', 'kafka:trades', 'file:pricing-engine']
      .map((id) => [id, { last: null, hours: new Map() }]),
  )
  const eventContext = (event: unknown, source: string) =>
    mappedContext(event, source, Array.from(schemas.values(), (item) => item.spec))
  const ingestHits: number[] = []
  const replyHits: number[] = []

  const allowedVisibleTeams = (): Set<string> => {
    const raw = process.env.MOCK_TEAMS?.trim()
    if (raw) return new Set(raw.split(',').map((s) => s.trim()).filter((id) => teams.has(id)))
    return new Set(teams.keys())
  }
  const canSeeTeam = (teamId: string | null) => teamId === null || allowedVisibleTeams().has(teamId)
  const isTeamMember = (teamId: string) => teams.get(teamId)?.members.includes(ME_ID) ?? false
  const isTeamLead = (teamId: string) => teams.get(teamId)?.lead === ME_ID
  const simulator = createSimulator(canSeeTeam, isTeamMember)
  const pendingDeviceAlerts = new Set<string>()
  const canEditPerson = (personId: string) => personId === ME_ID
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
    return [...byUser].flatMap(([u, set]) => [...set].map((c): [string, string] => [`user:${u}`, c]))
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
    const matchedTeams = new Set<string>()
    const eventId = `evt_${at.getTime().toString(36)}${(evSeq++).toString(36)}`
    const sample: EventSample = { event_id: eventId, received_at: at.toISOString(), raw }
    const ctx = eventContext(raw, source)
    const storedRecent: StoredRecent = { event: { ...sample, source, schema: ctx.schema }, teams: matchedTeams }
    recent.unshift(storedRecent)
    if (recent.length > RECENT_MAX) recent.length = RECENT_MAX
    const sourceStats = sources.get(source) ?? { last: null, hours: new Map<number, number>() }
    const hour = Math.floor(at.getTime() / HOUR)
    sourceStats.last = sample.received_at
    sourceStats.hours.set(hour, (sourceStats.hours.get(hour) ?? 0) + 1)
    for (const bucket of sourceStats.hours.keys()) if (bucket < hour - 23) sourceStats.hours.delete(bucket)
    if (sources.has(source) || sources.size < 100) sources.set(source, sourceStats)
    if (ctx.schema.startsWith('unknown:')) {
      const key = JSON.stringify([ctx.schema, source])
      const prior = discovered.get(key)?.entry
      discovered.delete(key)
      discovered.set(key, { entry: {
        schema: ctx.schema, source, count: (prior?.count ?? 0) + 1,
        first_seen: prior?.first_seen ?? sample.received_at, last_seen: sample.received_at, sample: raw,
      }, recent: storedRecent })
      if (discovered.size > RECENT_MAX) {
        const oldest = discovered.keys().next()
        if (!oldest.done) discovered.delete(oldest.value)
      }
    }
    for (const rec of rules.values()) {
      const rule = rec.rule
      if (rule.enabled === false) continue
      const key = dedupKey(rule, ctx)
      const k = key === null ? null : `${rule.id}|${key}`
      if (recovers(rule, ctx)) {
        matchedTeams.add(rule.team)
        const a = k === null ? undefined : alerts.get(active.get(k) ?? '')
        if (a && a.feed.status !== 'resolved') {
          const prev = transition(a, 'resolved', 'system', 'recovered', at)
          touched.push(a.feed.id)
          if (broadcast) publish(a, prev)
        }
        continue
      }
      const m = applyRule(rule, ctx)
      if (!m) continue
      matchedTeams.add(rule.team)
      rec.fires.set(hour, (rec.fires.get(hour) ?? 0) + 1)
      const existing = k === null ? undefined : alerts.get(active.get(k) ?? '')
      const window = parseDuration(rule.dedup?.window) ?? 10 * 60_000
      if (existing && at.getTime() - Date.parse(existing.feed.last_seen) <= window) {
        const prev = existing.feed
        existing.occurrences.set(hour, (existing.occurrences.get(hour) ?? 0) + 1)
        for (const bucket of existing.occurrences.keys()) if (bucket < hour - 23) existing.occurrences.delete(bucket)
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
      if (k !== null && existing) active.delete(k)
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
        occurrences: new Map([[hour, 1]]),
        runbookId: rule.runbook ?? null,
        runbook: new Map(),
        statusVersion: 1,
        samples: [sample],
        timeline: [{ at: at.toISOString(), actor: `rule:${rule.id}`, kind: 'opened', message: `Alert rule “${rule.name ?? rule.id}” raised this alert` }],
        lastDedupNote: at.getTime(),
      }
      alerts.set(id, a)
      if (a.plan.some(([, channel]) => channel === 'sms' || channel === 'phone')) pendingDeviceAlerts.add(id)
      if (k !== null) active.set(k, id)
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
      if (a.key !== null && active.get(a.key) === prev.id) active.delete(a.key)
    }
    a.feed = next
    a.statusVersion = next.version
    note(a, to === 'acked' ? 'acked' : 'resolved', actor, to === 'acked' ? 'Acknowledged' : `Resolved (${reason ?? 'user'})`, at)
    return prev
  }

  function remove(a: MockAlert, reason: RemoveEvent['reason']) {
    alerts.delete(a.feed.id)
    if (a.key !== null && active.get(a.key) === a.feed.id) active.delete(a.key)
    const rm: RemoveEvent = { id: a.feed.id, version: a.feed.version + 1, reason }
    for (const c of clients) if (visible(c.team, a, c.recipient)) send(c.res, 'alert.remove', rm)
  }

  // ---- runbook actions: off the alert pipeline, async (spec §6.2) ---------

  const def = (a: MockAlert) => a.runbookId ? runbooks.get(a.runbookId) ?? null : null
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
    const { params } = renderActionParams(d, a.ctx)
    const ex: Execution = { id: uid('exe'), state: 'queued', requested_by: actor, started_at: null, finished_at: null, output: null }
    rt.runs.push(ex)
    const label = d.action?.label ?? step
    touch(a)
    setTimeout(() => {
      if (a.feed.status === 'resolved') {
        ex.state = 'cancelled'
        ex.finished_at = new Date().toISOString()
        ex.output = 'Alert resolved before the action started'
        note(a, 'step_run', 'system', `Cancelled “${label}”: ${ex.output}`)
        touch(a)
        return
      }
      ex.state = 'running'
      ex.started_at = new Date().toISOString()
      touch(a)
      setTimeout(() => {
        const out = actionOutcome(a, d.action!.name, params)
        const safeOut = redactText(out.output, 300)
        ex.state = out.ok ? 'ok' : 'failed'
        ex.output = safeOut
        ex.finished_at = new Date().toISOString()
        note(a, 'step_run', actor, `${out.ok ? 'Ran' : 'Failed'} “${label}”: ${safeOut}`)
        if (out.ok && d.resolves && a.feed.status !== 'resolved') {
          const prev = transition(a, 'resolved', 'system', 'runbook')
          publish(a, prev)
        } else touch(a)
      }, 1200)
    }, 400)
    return ex
  }

  function syncDeviceReceipts() {
    for (const id of pendingDeviceAlerts) {
      const a = alerts.get(id)
      if (!a) {
        pendingDeviceAlerts.delete(id)
        continue
      }
      const jobs = deliveriesFor(a.feed, a.plan, people).filter((job) => job.channel === 'sms' || job.channel === 'phone')
      for (const job of jobs) {
        if (!job.sent_at || (job.channel !== 'sms' && job.channel !== 'phone')) continue
        simulator.send(a.feed.team, {
          receipt_id: job.id, job_id: job.id, alert_id: id, channel: job.channel, recipient: job.recipient,
          to: job.address, text: redactText(a.feed.title, 240), approval_challenge: null,
          duplicates: Math.max(0, job.attempts - 1), received_at: job.sent_at,
        })
      }
      if (!jobs.some((job) => job.state === 'pending')) pendingDeviceAlerts.delete(id)
    }
  }

  function completeApproval(
    a: MockAlert, step: string, approval: StoredApproval, op: 'approve' | 'reject', actor: string, noteText?: string,
  ): { state: 'approved' | 'rejected'; execution_id?: string } {
    const state = op === 'approve' ? 'approved' : 'rejected'
    approval.state = state
    approval.decided_by = actor
    approval.decided_at = new Date().toISOString()
    const safeNote = noteText?.trim() ? redactText(noteText.trim(), 2000) : null
    approval.note = safeNote
    const label = stepDef(a, step)?.action?.label ?? step
    note(a, 'approval', actor, `${op === 'approve' ? 'Approved' : 'Rejected'} “${label}”${safeNote ? `: ${redactText(safeNote, 240)}` : ''}`)
    if (op === 'approve') return { state, execution_id: execute(a, step, approval.requested_by).id }
    touch(a)
    return { state }
  }

  // ---- SSE -----------------------------------------------------------------

  const visible = (team: string | null, a: MockAlert, recipient: string | null) =>
    canSeeTeam(a.feed.team) && (team === null || a.feed.team === team) &&
    (recipient === null || a.plan.some(([target]) => target === `user:${recipient}`))

  function send(res: ServerResponse, event: string, data: unknown) {
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
  }

  function publish(a: MockAlert, prev: FeedAlert | null) {
    for (const c of clients) {
      if (!visible(c.team, a, c.recipient)) continue
      send(c.res, 'alert.upsert', a.feed)
      // Out-of-order redelivery, as a replica or the outbox might produce.
      if (prev && Math.random() < staleChance) send(c.res, 'alert.upsert', prev)
    }
  }

  function page(team: string | null, q?: URLSearchParams) {
    const statuses = q?.getAll('status') ?? []
    const sevs = q?.getAll('severity') ?? []
    const ruleIds = q?.getAll('rule') ?? []
    const recipient = q?.get('recipient') || null
    let items = [...alerts.values()]
      .filter((a) => visible(team, a, recipient))
      .map((a) => a.feed)
      .filter((a) => !statuses.length || statuses.includes(a.status))
      .filter((a) => !sevs.length || sevs.includes(a.severity))
      .filter((a) => !ruleIds.length || ruleIds.includes(a.rule.id))
      .sort((x, y) => (x.first_seen < y.first_seen ? 1 : x.first_seen > y.first_seen ? -1 : x.id < y.id ? 1 : -1))
    const cursor = q?.get('cursor')
    if (cursor) {
      const pair = decodeCursor(cursor)
      if (pair) {
        const [fs, id] = pair
        items = items.filter((a) => a.first_seen < fs || (a.first_seen === fs && a.id < id))
      }
    }
    const limit = Math.min(Number(q?.get('limit') ?? SNAPSHOT_LIMIT) || SNAPSHOT_LIMIT, SNAPSHOT_LIMIT)
    const slice = items.slice(0, limit)
    const last = slice[slice.length - 1]
    const next_cursor = items.length > limit && last ? Buffer.from(`${last.first_seen}|${last.id}`).toString('base64url') : null
    return { items: slice, next_cursor }
  }

  function openStream(req: IncomingMessage, res: ServerResponse, url: URL) {
    const team = url.searchParams.get('team') || null
    if (!canSeeTeam(team)) return err(res, 403, 'forbidden', 'You do not have access to this team')
    const recipient = url.searchParams.get('recipient') || null
    if (recipient !== null && !RECIPIENT_RE.test(recipient)) return err(res, 400, 'bad_request', 'Invalid recipient filter')
    if (clients.size >= MAX_SSE_CLIENTS) return err(res, 503, 'service_unavailable', 'Too many open streams')
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-store',
      'X-Content-Type-Options': 'nosniff',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    })
    res.write('retry: 1500\n\n')
    const client: Client = { res, team, recipient }
    // Subscribe first, then snapshot (trivially consistent here: one thread).
    clients.add(client)
    const params = new URLSearchParams({ limit: '100' })
    if (recipient) params.set('recipient', recipient)
    const p = page(team, params)
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
      can_edit: isTeamMember(rec.rule.team),
    }
  }

  function personTeams(id: string): string[] {
    return [...teams.values()].filter((t) => t.members.includes(id)).map((t) => t.id)
  }

  function directoryEntry(p: PersonView): PersonDirectoryEntry {
    return { id: p.id, name: p.name, teams: personTeams(p.id) }
  }

  function personView(p: PersonView): PersonView {
    const editable = canEditPerson(p.id)
    return {
      id: p.id,
      name: p.name,
      contact: editable ? clone(p.contact) : {},
      teams: personTeams(p.id),
      preferences: editable ? clone(p.preferences) : [],
      revision: p.revision,
      can_edit: editable,
    }
  }

  function teamView(t: TeamView): TeamView {
    return { ...clone(t), can_edit: isTeamLead(t.id) }
  }

  function proposalView(p: Proposal): Proposal {
    return { ...clone(p), can_review: isTeamMember(p.base_rule.team) }
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
    res.writeHead(status, {
      'Content-Type': 'application/json',
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
    })
    res.end(JSON.stringify(body))
  }
  const err = (res: ServerResponse, status: number, code: string, message: string, extra: Partial<ApiError['error']> = {}) =>
    json(res, status, { error: { code, message, ...extra } } satisfies ApiError)

  async function body(req: IncomingMessage): Promise<any> {
    const chunks: Buffer[] = []
    let total = 0
    for await (const c of req) {
      const buf = Buffer.isBuffer(c) ? c : Buffer.from(c as string)
      total += buf.length
      if (total > MAX_BODY_BYTES) throw new BodyTooLargeError(`Body exceeds ${MAX_BODY_BYTES} bytes`)
      chunks.push(buf)
    }
    const s = Buffer.concat(chunks).toString()
    return s ? JSON.parse(s) : {}
  }

  /** Async write handler: CSRF check + bounded JSON body, errors mapped to ApiError. */
  function write(req: IncomingMessage, res: ServerResponse, fn: (b: any) => void) {
    void (async () => {
      if (req.headers['x-csrf-token'] !== csrf) return err(res, 403, 'csrf', 'Missing or wrong X-CSRF-Token')
      let b: any
      try {
        b = await body(req)
      } catch (e) {
        if (e instanceof BodyTooLargeError) return err(res, 413, 'bad_request', e.message)
        return err(res, 400, 'bad_request', 'Body is not JSON')
      }
      fn(b ?? {})
    })()
    return true
  }

  function runbookSummary(book: RunbookDef): RunbookSummary {
    return { id: book.id, title: book.title, format: book.format, team: book.team, steps: book.steps.length }
  }

  function detail(a: MockAlert): AlertDetail {
    const rb = def(a)
    const lastHour = Math.floor(Date.parse(a.feed.last_seen) / HOUR)
    const firstHour = Math.max(lastHour - 23, Math.floor(Date.parse(a.feed.first_seen) / HOUR))
    return {
      ...a.feed,
      occurrences: {
        from: new Date(firstHour * HOUR).toISOString(), bucket_seconds: HOUR / 1000,
        counts: Array.from({ length: lastHour - firstHour + 1 }, (_, i) => a.occurrences.get(firstHour + i) ?? 0),
      },
      samples: a.samples.map((s) => ({ ...s, raw: redactValue(s.raw) })),
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
      const allowed = allowedVisibleTeams()
      const session: Session = {
        user: { id: ME_ID, name: demoUserName },
        teams: [...teams.values()].filter((t) => allowed.has(t.id)).map((t) => ({ id: t.id, name: t.name })),
        csrf_token: csrf,
        capabilities: { recipient_feed: true, mock_transports: true, runbook_import: true },
      }
      return json(res, 200, session), true
    }
    if (path === '/api/simulator/messages' && method === 'GET') {
      syncDeviceReceipts()
      const cursor = url.searchParams.get('cursor')
      if (cursor !== null && !/^\d{1,15}$/.test(cursor)) return err(res, 400, 'bad_request', 'Invalid receipt cursor'), true
      return json(res, 200, simulator.list(cursor === null ? undefined : Number(cursor))), true
    }
    if (path === '/api/simulator/stream' && method === 'GET') {
      syncDeviceReceipts()
      return simulator.stream(req, res), true
    }
    if (path === '/api/simulator/reply' && method === 'POST') {
      return write(req, res, (b: { from?: string; text?: string; receipt_id?: string }) => {
        const now = Date.now()
        while (replyHits.length && now - replyHits[0]! > REPLY_WINDOW_MS) replyHits.shift()
        if (replyHits.length >= REPLY_MAX_PER_WINDOW)
          return err(res, 429, 'rate_limited', 'Too many SMS reply attempts; try again shortly')
        replyHits.push(now)
        const from = typeof b.from === 'string' && internationalPhone(b.from)
        if (!from || typeof b.text !== 'string' || !b.text.trim() || b.text.length > 160 ||
            (b.receipt_id !== undefined && (typeof b.receipt_id !== 'string' || b.receipt_id.length > 128)))
          return err(res, 400, 'bad_request', 'A phone number and an SMS reply are required')
        const answer = b.text.trim().match(/^([YN])\s+(\d{4})$/i)
        const resolveApproval = (entry: DeviceRecord) => {
          if (!entry.approval) return null
          const alert = alerts.get(entry.message.alert_id)
          const step = entry.approval.step
          const approval = alert?.runbook.get(step)?.approvals.find((request) => request.id === entry.approval?.id)
          if (!alert || !approval) return null
          return { alert, step, approval }
        }
        const isPending = (entry: DeviceRecord) => {
          const found = resolveApproval(entry)
          return Boolean(
            found &&
            found.alert.feed.status !== 'resolved' &&
            found.approval.state === 'pending' &&
            Date.parse(found.approval.expires_at) > now &&
            found.approval.eligible.includes(entry.message.recipient) &&
            found.approval.requested_by !== entry.message.recipient,
          )
        }
        const codeMatches = (entry: DeviceRecord) => {
          const found = resolveApproval(entry)
          return Boolean(found && verifySmsCode(answer?.[2], found.approval.sms_code_hash))
        }
        const lookup = simulator.approvalSms(from, isPending, codeMatches, b.receipt_id)
        if (lookup.kind === 'none' || lookup.kind === 'ambiguous') return json(res, 200, { outcome: 'unknown' })
        if (lookup.kind === 'mismatch') {
          let anyActive = false
          for (const candidate of lookup.pending) {
            const found = resolveApproval(candidate)
            if (!found) continue
            const attempts = found.approval.sms_attempts ?? 0
            if (attempts < 5) {
              found.approval.sms_attempts = attempts + 1
              anyActive = true
            }
          }
          return json(res, 200, { outcome: anyActive ? 'wrong_code' : 'too_many_attempts' })
        }
        const entry = lookup.record
        const found = resolveApproval(entry)
        if (!found || !found.approval.eligible.includes(entry.message.recipient) || found.approval.requested_by === entry.message.recipient)
          return json(res, 200, { outcome: 'unknown' })
        const { alert: a, step, approval } = found
        if (a.feed.status === 'resolved' || approval.state !== 'pending' || Date.parse(approval.expires_at) <= now)
          return json(res, 200, { outcome: 'already_decided' })
        const attempts = approval.sms_attempts ?? 0
        if (attempts >= 5) return json(res, 200, { outcome: 'too_many_attempts' })
        if (!verifySmsCode(answer?.[2], approval.sms_code_hash)) {
          approval.sms_attempts = attempts + 1
          return json(res, 200, { outcome: 'wrong_code' })
        }
        const decision = answer![1]!.toUpperCase() === 'Y' ? 'approve' : 'reject'
        const result = completeApproval(a, step, approval, decision, entry.message.recipient, 'SMS reply')
        json(res, 200, { outcome: result.state, approval_id: approval.id, execution_id: result.execution_id })
      })
    }
    if (path === '/api/stream' && method === 'GET') return openStream(req, res, url), true
    if (path === '/api/alerts' && method === 'GET') {
      const team = url.searchParams.get('team') || null
      if (!canSeeTeam(team)) return err(res, 403, 'forbidden', 'You do not have access to this team'), true
      const recipient = url.searchParams.get('recipient') || null
      if (recipient !== null && !RECIPIENT_RE.test(recipient))
        return err(res, 400, 'bad_request', 'Invalid recipient filter'), true
      const cursor = url.searchParams.get('cursor')
      if (cursor !== null && !decodeCursor(cursor))
        return err(res, 400, 'bad_request', 'Invalid cursor'), true
      return json(res, 200, page(team, url.searchParams)), true
    }

    if ((m = path.match(/^\/api\/alerts\/([^/]+)$/)) && method === 'GET') {
      const id = safeDecode(m[1]!)
      if (id === null) return err(res, 400, 'bad_request', 'Invalid alert id encoding'), true
      const a = alerts.get(id)
      if (!a) return err(res, 404, 'not_found', 'No such alert'), true
      if (!canSeeTeam(a.feed.team)) return err(res, 403, 'forbidden', 'You do not have access to this alert'), true
      return json(res, 200, detail(a)), true
    }

    if ((m = path.match(/^\/api\/alerts\/([^/]+)\/(ack|resolve)$/)) && method === 'POST') {
      const id = safeDecode(m[1]!)
      if (id === null) return err(res, 400, 'bad_request', 'Invalid alert id encoding'), true
      const op = m[2] as 'ack' | 'resolve'
      return write(req, res, (reqBody: TransitionRequest) => {
        if (typeof reqBody.expected_version !== 'number') return err(res, 400, 'bad_request', 'expected_version is required')
        const a = alerts.get(id)
        if (!a) return err(res, 404, 'not_found', 'No such alert')
        if (!canSeeTeam(a.feed.team)) return err(res, 403, 'forbidden', 'You do not have access to this alert')
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
      const id = safeDecode(m[1]!)
      const step = safeDecode(m[2]!)
      if (id === null || step === null) return err(res, 400, 'bad_request', 'Invalid path encoding'), true
      const op = m[3]!
      return write(req, res, (b: { done?: boolean }) => {
        const a = alerts.get(id)
        if (!a) return err(res, 404, 'not_found', 'No such alert')
        if (!isTeamMember(a.feed.team)) return err(res, 403, 'forbidden', 'Only members of the alert’s team can run or check runbook steps')
        const d = stepDef(a, step)
        if (!d) return err(res, 404, 'not_found', 'No such runbook step')
        const rt = runtimeOf(a.runbook, step)
        if (op === 'check') {
          if (d.kind !== 'manual') return err(res, 400, 'bad_request', 'Only manual steps are ticked off')
          const done = b.done !== false
          rt.done_by = done ? ME : null
          rt.done_at = done ? new Date().toISOString() : null
          const stepText = redactText(viewRunbook(def(a)!, a.ctx, a.runbook, ME).steps.find((s) => s.id === step)!.text, 200)
          note(a, 'step_run', ME, `${done ? 'Done' : 'Unticked'}: ${stepText}`)
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
          // Separation of duties: eligible approvers exclude the requester.
          const eligible = lead && lead !== ME_ID ? [`user:${lead}`] : []
          const rawCode = String(1000 + (randomBytes(2).readUInt16BE(0) % 9000))
          const request: StoredApproval = {
            id: uid('apr'),
            state: 'pending',
            requested_by: ME,
            requested_at: new Date(now).toISOString(),
            expires_at: new Date(now + 15 * 60_000).toISOString(),
            eligible,
            can_decide: false,
            sms_sent: eligible.length > 0,
            sms_code_hash: createHash('sha256').update(rawCode).digest('hex'),
            sms_attempts: 0,
            decided_by: null,
            decided_at: null,
            note: null,
          }
          rt.approvals.push(request)
          for (const recipient of eligible) {
            const phone = people.get(recipient.replace(/^user:/, ''))?.contact.phone
            const to = phone ? internationalPhone(phone) : null
            if (!to) continue
            simulator.send(a.feed.team, {
              receipt_id: uid('sms'), job_id: request.id, alert_id: a.feed.id, channel: 'sms',
              recipient, to, text: `Approve “${d.action.label}” on ${redactText(a.feed.title, 160)}? Reply Y ${rawCode} or N ${rawCode}.`,
              approval_challenge: `Y ${rawCode}`, duplicates: 0, received_at: request.requested_at,
            }, { id: request.id, step }, to)
          }
          note(a, 'approval', ME, `Asked for approval to run “${d.action.label}” (${redactText(view.text, 160)})`)
          touch(a)
          return err(res, 409, 'approval_required', 'This step needs approval first', { approval_id: request.id })
        }
        const ex = execute(a, step, ME)
        json(res, 202, { execution_id: ex.id })
      })
    }

    if ((m = path.match(/^\/api\/approvals\/([^/]+)\/(approve|reject)$/)) && method === 'POST') {
      const id = safeDecode(m[1]!)
      if (id === null) return err(res, 400, 'bad_request', 'Invalid approval id encoding'), true
      const op = m[2]!
      return write(req, res, (b: { note?: string }) => {
        if (b.note !== undefined && (typeof b.note !== 'string' || b.note.length > 2000))
          return err(res, 422, 'invalid', 'Approval note must be at most 2000 characters', { details: [{ path: 'note', message: 'max 2000 chars' }] })
        for (const a of alerts.values())
          for (const [step, rt] of a.runbook) {
            const r = rt.approvals.find((x) => x.id === id)
            if (!r) continue
            if (r.state !== 'pending' || Date.parse(r.expires_at) <= Date.now())
              return err(res, 409, 'invalid_transition', `This request is already ${r.state === 'pending' ? 'expired' : r.state}`)
            if (r.requested_by === ME) return err(res, 403, 'forbidden', 'Requesters cannot approve or reject their own approval request')
            if (!r.eligible.includes(ME)) return err(res, 403, 'forbidden', 'You are not an approver for this step')
            if (a.feed.status === 'resolved') return err(res, 409, 'invalid_transition', 'The alert was resolved meanwhile')
            const result = completeApproval(a, step, r, op === 'approve' ? 'approve' : 'reject', ME, b.note)
            return json(res, 200, result.execution_id ? { execution_id: result.execution_id } : { ok: true })
          }
        err(res, 404, 'not_found', 'No such approval request')
      })
    }

    // ---- source discovery and schema mappings
    if (path === '/api/sources' && method === 'GET') {
      const cutoff = Math.floor(Date.now() / HOUR) - 23
      const items: SourceView[] = Array.from(sources, ([id, info]) => {
        const prefix = id.split(':', 1)[0]
        const kind = prefix === 'kafka' || prefix === 'file' ? prefix : 'http'
        return { id, type: kind, kind, last_seen: info.last,
          events_24h: [...info.hours].reduce((sum, [hour, count]) => hour >= cutoff ? sum + count : sum, 0) }
      })
      return json(res, 200, items), true
    }
    if (path === '/api/schemas/discovered' && method === 'GET') {
      const allowed = allowedVisibleTeams()
      const items = [...discovered.values()].reverse()
        .filter(({ recent: stored }) => stored.teams.size === 0 || [...stored.teams].some((team) => allowed.has(team)))
        .map(({ entry }) => ({ ...entry, sample: redactValue(entry.sample) }))
      return json(res, 200, items), true
    }
    if (path === '/api/schemas' && method === 'GET') return json(res, 200, [...schemas.values()]), true
    if (path === '/api/schemas/preview' && method === 'POST') {
      return write(req, res, (b: { spec: unknown; event: unknown; source?: string }) => {
        if (b.source !== undefined && (typeof b.source !== 'string' || b.source.length > 64 || !SOURCE_RE.test(b.source)))
          return err(res, 422, 'invalid', 'Invalid source name')
        const parsed = parseMapping(b.spec)
        if ('error' in parsed) return err(res, 422, 'invalid', parsed.error)
        const source = b.source ?? 'http:playground'
        const result = previewMapping(parsed.spec, b.event, source)
        json(res, 200, { ...result, fields: redactValue(result.fields) })
      })
    }
    if (path === '/api/schemas' && method === 'POST') {
      return write(req, res, (b: { spec: unknown; expected_revision?: number; schema?: string }) => {
        if (![...allowedVisibleTeams()].some(isTeamMember)) return err(res, 403, 'forbidden', 'A team member can publish mappings')
        const parsed = parseMapping(b.spec)
        if ('error' in parsed) return err(res, 422, 'invalid', parsed.error)
        const id = parsed.spec.schema
        // Like the API: an edit names the mapping it changes, so renaming can't overwrite another one.
        if (b.expected_revision !== undefined) {
          if (typeof b.schema !== 'string') return err(res, 400, 'bad_request', 'schema (the mapping being edited) is required with expected_revision')
          if (b.schema !== id) return err(res, 422, 'invalid', `A mapping can't be renamed; publish ${id} as a new mapping`, { details: [{ path: 'schema', message: 'cannot be renamed' }] })
        }
        const prior = schemas.get(id)
        if (b.expected_revision !== prior?.revision)
          return err(res, 409, 'revision_conflict', 'This schema mapping changed. Reload it before saving.', { current: prior })
        if (!prior && schemas.size >= 100) return err(res, 422, 'invalid', 'The mock is limited to 100 schema mappings')
        const saved: SchemaMapping = { spec: parsed.spec, revision: (prior?.revision ?? 0) + 1 }
        schemas.set(id, saved)
        json(res, prior ? 200 : 201, saved)
      })
    }

    // ---- rules
    if (path === '/api/rules' && method === 'GET') {
      const team = url.searchParams.get('team') || null
      if (!canSeeTeam(team)) return err(res, 403, 'forbidden', 'You do not have access to this team'), true
      const allowed = allowedVisibleTeams()
      const items = [...rules.values()]
        .filter((r) => allowed.has(r.rule.team) && (!team || r.rule.team === team))
        .map((r) => ruleItem(r))
      items.sort((x, y) => x.rule.team.localeCompare(y.rule.team) || (x.rule.name ?? x.rule.id).localeCompare(y.rule.name ?? y.rule.id))
      return json(res, 200, { items }), true
    }
    if (path === '/api/rules/test' && method === 'POST') {
      return write(req, res, (b: RuleTestRequest) => {
        if (!b.rule || typeof b.rule !== 'object') return err(res, 400, 'bad_request', 'rule is required')
        const source = typeof b.source === 'string' ? b.source : 'http:test'
        json(res, 200, testRule(b.rule, b.event, source, eventContext(b.event, source)))
      })
    }
    if (path === '/api/rules' && method === 'POST') {
      return write(req, res, (b: RuleWrite) => {
        const problems = validateRule(b.rule)
        if (problems.length) return err(res, 422, 'invalid', 'The rule has problems', { details: problems })
        if (!isTeamMember(b.rule.team)) return err(res, 403, 'forbidden', `Only members of team "${b.rule.team}" can create rules for it`)
        if (rules.has(b.rule.id)) return err(res, 409, 'revision_conflict', `A rule with id "${b.rule.id}" already exists`, { current: ruleItem(rules.get(b.rule.id)!) })
        const revision = (deletedRuleRevisions.get(b.rule.id) ?? 0) + 1
        const rec: RuleRec = { rule: clone(b.rule), revision, updated_at: new Date().toISOString(), updated_by: ME, fires: new Map() }
        rules.set(b.rule.id, rec)
        deletedRuleRevisions.delete(b.rule.id)
        json(res, 201, ruleItem(rec))
      })
    }
    if ((m = path.match(/^\/api\/rules\/([^/]+)$/))) {
      const id = safeDecode(m[1]!)
      if (id === null) return err(res, 400, 'bad_request', 'Invalid rule id encoding'), true
      if (method === 'GET') {
        const rec = rules.get(id)
        if (!rec) return err(res, 404, 'not_found', 'No such rule'), true
        if (!canSeeTeam(rec.rule.team)) return err(res, 403, 'forbidden', 'You do not have access to this rule'), true
        return json(res, 200, ruleItem(rec)), true
      }
      if (method === 'PUT')
        return write(req, res, (b: RuleWrite) => {
          const rec = rules.get(id)
          if (!rec) return err(res, 404, 'not_found', 'No such rule')
          if (!isTeamMember(rec.rule.team)) return err(res, 403, 'forbidden', `Only members of team "${rec.rule.team}" can edit this rule`)
          if (b.rule?.id !== id) return err(res, 400, 'bad_request', 'A rule’s id can’t be changed')
          if (b.expected_revision !== rec.revision)
            return err(res, 409, 'revision_conflict', `${rec.updated_by.replace(/^user:/, '')} changed this rule meanwhile (revision ${rec.revision})`, { current: ruleItem(rec) })
          const problems = validateRule(b.rule)
          if (problems.length) return err(res, 422, 'invalid', 'The rule has problems', { details: problems })
          if (!isTeamMember(b.rule.team)) return err(res, 403, 'forbidden', `Only members of team "${b.rule.team}" can move a rule to that team`)
          Object.assign(rec, { rule: clone(b.rule), revision: rec.revision + 1, updated_at: new Date().toISOString(), updated_by: ME })
          json(res, 200, ruleItem(rec))
        })
      if (method === 'DELETE')
        return write(req, res, () => {
          const rec = rules.get(id)
          if (!rec) return err(res, 404, 'not_found', 'No such rule')
          if (!isTeamMember(rec.rule.team)) return err(res, 403, 'forbidden', `Only members of team "${rec.rule.team}" can delete this rule`)
          const exp = Number(url.searchParams.get('expected_revision'))
          if (exp !== rec.revision) return err(res, 409, 'revision_conflict', 'This rule changed meanwhile', { current: ruleItem(rec) })
          rules.delete(id)
          deletedRuleRevisions.set(id, rec.revision)
          json(res, 200, { deleted: id })
        })
    }
    if (path === '/api/runbooks' && method === 'GET') {
      const allowed = allowedVisibleTeams()
      const items = [...runbooks.values()].filter((r) => r.team === null || allowed.has(r.team)).map(runbookSummary)
      return json(res, 200, { items }), true
    }
    if (path === '/api/runbooks/import' && method === 'POST') {
      return write(req, res, (input: unknown) => {
        const rawTeam = input && typeof input === 'object' && !Array.isArray(input)
          ? (input as Record<string, unknown>).team
          : undefined
        const requestedTeam = typeof rawTeam === 'string' ? rawTeam.trim() : ''
        if (requestedTeam && (!canSeeTeam(requestedTeam) || !isTeamMember(requestedTeam)))
          return err(res, 403, 'forbidden', 'Only team members can import runbooks for that team')
        try {
          const { draft, preview } = prepareRunbook(input)
          if (!canSeeTeam(draft.team) || !isTeamMember(draft.team)) return err(res, 403, 'forbidden', 'Only team members can import runbooks for that team')
          json(res, 200, preview)
        } catch (e) {
          err(res, 422, 'invalid', e instanceof Error ? e.message : 'Invalid runbook')
        }
      })
    }
    if (path === '/api/runbooks' && method === 'POST') {
      return write(req, res, (input: unknown) => {
        const rawTeam = input && typeof input === 'object' && !Array.isArray(input)
          ? (input as Record<string, unknown>).team
          : undefined
        const requestedTeam = typeof rawTeam === 'string' ? rawTeam.trim() : ''
        if (requestedTeam && (!canSeeTeam(requestedTeam) || !isTeamMember(requestedTeam)))
          return err(res, 403, 'forbidden', 'Only team members can import runbooks for that team')
        try {
          const book = importRunbook(input)
          if (book.team === null || !canSeeTeam(book.team) || !isTeamMember(book.team))
            return err(res, 403, 'forbidden', 'Only team members can import runbooks for that team')
          if (runbooks.has(book.id)) return err(res, 409, 'revision_conflict', 'A runbook with this id already exists')
          if (runbooks.size >= 100) return err(res, 422, 'invalid', 'The mock is limited to 100 runbooks')
          runbooks.set(book.id, book)
          json(res, 201, runbookSummary(book))
        } catch (e) {
          err(res, 422, 'invalid', e instanceof Error ? e.message : 'Invalid runbook')
        }
      })
    }
    if (path === '/api/events/recent' && method === 'GET') {
      const team = url.searchParams.get('team') || null
      if (!canSeeTeam(team)) return err(res, 403, 'forbidden', 'You do not have access to this team'), true
      const allowed = allowedVisibleTeams()
      const limit = Math.min(Math.max(1, Number(url.searchParams.get('limit') ?? 20) || 20), RECENT_MAX)
      const items = recent
        .filter((r) => (team ? r.teams.has(team) : r.teams.size === 0 || [...r.teams].some((t) => allowed.has(t))))
        .slice(0, limit)
        .map((r) => ({ ...r.event, raw: redactValue(r.event.raw) }))
      return json(res, 200, { items }), true
    }

    // ---- people & teams
    if (path === '/api/people/directory' && method === 'GET') {
      return json(res, 200, { items: [...people.values()].map(directoryEntry) }), true
    }
    if (path === '/api/people' && method === 'GET') return json(res, 200, { items: [...people.values()].map(personView) }), true
    if ((m = path.match(/^\/api\/people\/([^/]+)(\/preferences)?$/))) {
      const personId = safeDecode(m[1]!)
      if (personId === null) return err(res, 400, 'bad_request', 'Invalid person id encoding'), true
      const p = people.get(personId)
      if (method === 'GET') return p ? json(res, 200, personView(p)) : err(res, 404, 'not_found', 'No such person'), true
      if (method === 'PUT' && m[2])
        return write(req, res, (b: { preferences: Preference[]; expected_revision: number }) => {
          if (!p) return err(res, 404, 'not_found', 'No such person')
          if (!canEditPerson(p.id)) return err(res, 403, 'forbidden', 'You can only edit your own preferences')
          if (b.expected_revision !== p.revision) return err(res, 409, 'revision_conflict', 'These preferences changed meanwhile', { current: personView(p) })
          const problems = validatePrefs(b.preferences)
          if (problems.length) return err(res, 422, 'invalid', 'The preferences have problems', { details: problems })
          p.preferences = clone(b.preferences)
          p.revision++
          json(res, 200, personView(p))
        })
    }
    if (path === '/api/teams' && method === 'GET') return json(res, 200, { items: [...teams.values()].map(teamView) }), true
    if ((m = path.match(/^\/api\/teams\/([^/]+)$/))) {
      const teamId = safeDecode(m[1]!)
      if (teamId === null) return err(res, 400, 'bad_request', 'Invalid team id encoding'), true
      const t = teams.get(teamId)
      if (method === 'GET') return t ? json(res, 200, teamView(t)) : err(res, 404, 'not_found', 'No such team'), true
      if (method === 'PUT')
        return write(req, res, (b: Partial<TeamView> & { expected_revision: number }) => {
          if (!t) return err(res, 404, 'not_found', 'No such team')
          if (!isTeamLead(t.id)) return err(res, 403, 'forbidden', 'Only the team lead or an admin can edit this team')
          if (b.expected_revision !== t.revision) return err(res, 409, 'revision_conflict', 'This team changed meanwhile', { current: teamView(t) })
          const members = b.members ?? t.members
          const unknown = members.filter((u) => !people.has(u))
          if (unknown.length) return err(res, 422, 'invalid', `Unknown people: ${unknown.join(', ')}`, { details: [{ path: 'members', message: 'unknown person' }] })
          if (b.lead && !members.includes(b.lead)) return err(res, 422, 'invalid', 'The lead must be a member', { details: [{ path: 'lead', message: 'not a member' }] })
          const oncall = b.oncall ?? t.oncall
          const duplicate = Object.keys(oncall).find((schedule) =>
            [...teams.values()].some((other) => other.id !== t.id && Object.hasOwn(other.oncall, schedule)))
          if (duplicate !== undefined)
            return err(res, 422, 'invalid', 'The rotation name is already in use', { details: [{ path: `oncall.${duplicate}`, message: 'belongs to another team' }] })
          Object.assign(t, {
            members,
            lead: b.lead ?? null,
            oncall,
            default_channels: b.default_channels ?? t.default_channels,
            revision: t.revision + 1,
          })
          json(res, 200, teamView(t))
        })
    }

    // ---- proposals
    if (path === '/api/proposals/count' && method === 'GET') {
      const team = url.searchParams.get('team') || null
      if (!canSeeTeam(team)) return err(res, 403, 'forbidden', 'You do not have access to this team'), true
      const allowed = allowedVisibleTeams()
      const pending = [...proposals.values()].filter(
        (p) => p.state === 'pending' && allowed.has(p.base_rule.team) && (!team || p.base_rule.team === team),
      ).length
      return json(res, 200, { pending }), true
    }
    if (path === '/api/proposals' && method === 'GET') {
      const team = url.searchParams.get('team') || null
      const state = url.searchParams.get('state') || null
      if (!canSeeTeam(team)) return err(res, 403, 'forbidden', 'You do not have access to this team'), true
      const allowed = allowedVisibleTeams()
      const items = [...proposals.values()]
        .filter((p) => allowed.has(p.base_rule.team) && (!team || p.base_rule.team === team) && (!state || p.state === state))
        .sort((x, y) => (x.created_at < y.created_at ? 1 : -1))
        .map(proposalView)
      return json(res, 200, { items }), true
    }
    if (path === '/api/proposals' && method === 'POST') {
      return write(req, res, (b: ProposalCreate) => {
        const rec = rules.get(b.rule_id)
        if (!rec) return err(res, 404, 'not_found', 'No such rule')
        if (!canSeeTeam(rec.rule.team)) return err(res, 403, 'forbidden', 'You do not have access to this rule')
        const problems = validateRule(b.proposed_rule)
        if (problems.length) return err(res, 422, 'invalid', 'The proposed rule has problems', { details: problems })
        if (!b.summary?.trim()) return err(res, 422, 'invalid', 'Say in a line what should change', { details: [{ path: 'summary', message: 'required' }] })
        if (b.summary.trim().length > 200)
          return err(res, 422, 'invalid', 'Summary must be at most 200 characters', { details: [{ path: 'summary', message: 'max 200 chars' }] })
        if (b.note !== undefined && (typeof b.note !== 'string' || b.note.length > 4000))
          return err(res, 422, 'invalid', 'Note must be at most 4000 characters', { details: [{ path: 'note', message: 'max 4000 chars' }] })
        const base = b.base_revision === rec.revision ? rec.rule : null
        if (!base) return err(res, 409, 'revision_conflict', 'The rule changed while you were writing this', { current: ruleItem(rec) })
        const p: Proposal = {
          id: uid('prp'),
          rule_id: rec.rule.id,
          rule_name: rec.rule.name ?? rec.rule.id,
          kind: b.kind,
          summary: redactText(b.summary.trim(), 200),
          note: redactText(b.note?.trim() ?? '', 4000),
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
        if (a && canSeeTeam(a.feed.team)) {
          note(a, 'comment', ME, `Suggested a change to the rule: ${redactText(p.summary, 160)}`)
          touch(a)
        }
        json(res, 201, proposalView(p))
      })
    }
    if ((m = path.match(/^\/api\/proposals\/([^/]+)\/(accept|reject)$/)) && method === 'POST') {
      const id = safeDecode(m[1]!)
      if (id === null) return err(res, 400, 'bad_request', 'Invalid proposal id encoding'), true
      const op = m[2]!
      return write(req, res, () => {
        const p = proposals.get(id)
        if (!p) return err(res, 404, 'not_found', 'No such proposal')
        if (!isTeamMember(p.base_rule.team)) return err(res, 403, 'forbidden', `Only members of team "${p.base_rule.team}" can review this proposal`)
        if (p.state !== 'pending') return err(res, 409, 'invalid_transition', `Already ${p.state}`)
        const rec = rules.get(p.rule_id)
        const decide = (state: Proposal['state']) => Object.assign(p, { state, decided_by: ME, decided_at: new Date().toISOString() })
        if (op === 'reject') return decide('rejected'), json(res, 200, proposalView(p))
        if (!rec) return decide('stale'), err(res, 409, 'revision_conflict', 'The rule was deleted')
        // Accepting is an ordinary publish against the proposal's base revision.
        if (rec.revision !== p.base_revision) {
          decide('stale')
          return err(res, 409, 'revision_conflict', `The rule changed since this was proposed (now revision ${rec.revision}). Review it again.`, { current: ruleItem(rec) })
        }
        Object.assign(rec, { rule: clone(p.proposed_rule), revision: rec.revision + 1, updated_at: new Date().toISOString(), updated_by: ME })
        decide('accepted')
        json(res, 200, proposalView(p))
      })
    }

    // ---- ingest & mock controls (CSRF-protected + bounded, dev mock only)
    if ((m = path.match(/^\/ingest\/([^/]+)$/)) && method === 'POST') {
      const decoded = safeDecode(m[1]!)
      const source = decoded && !decoded.includes(':') && /^[a-z0-9._-]+$/i.test(decoded) ? `http:${decoded}` : decoded
      if (source === null || source.length > 64 || !SOURCE_RE.test(source))
        return err(res, 400, 'bad_request', 'Invalid ingest source name'), true
      return write(req, res, (raw: unknown) => {
        const now = Date.now()
        while (ingestHits.length && now - ingestHits[0]! > INGEST_WINDOW_MS) ingestHits.shift()
        if (ingestHits.length >= INGEST_MAX_PER_WINDOW)
          return err(res, 429, 'rate_limited', 'Ingest rate limit exceeded; try again shortly')
        ingestHits.push(now)
        const touched = ingest(source, raw)
        json(res, 202, { id: `evt_${now.toString(36)}`, matched_alerts: touched })
      })
    }
    if (path === '/api/__mock/burst' && method === 'POST') {
      return write(req, res, () => {
        const n = Math.min(Math.max(1, Number(url.searchParams.get('n') ?? 200) || 200), MAX_BURST)
        for (let i = 0; i < n; i++) {
          const e = randomEvent(new Date())
          ingest(e.source, e.raw)
        }
        json(res, 200, { ingested: n, alerts: alerts.size })
      })
    }
    if (path === '/api/__mock/workflow-engine' && method === 'POST') {
      return write(req, res, () => {
        retryOk = url.searchParams.get('ok') === '1'
        json(res, 200, { retry_ok: retryOk })
      })
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
  timers.push(setInterval(syncDeviceReceipts, 1000))
  timers.push(setInterval(() => { simulator.ping(); for (const c of clients) c.res.write(': ping\n\n') }, 15_000))
  if (resnapshotS > 0) {
    timers.push(setInterval(() => { for (const c of clients) c.res.end() ; clients.clear() }, resnapshotS * 1000))
  }
  for (const t of timers) t.unref?.()

  return {
    handle,
    close() {
      simulator.close()
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
