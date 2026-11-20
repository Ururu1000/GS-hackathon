// Dev mock of the Beacon API (ui/contract/openapi.yaml). In-memory, no deps.
// Used two ways:
// - inside `vite` dev as middleware (default when BEACON_API_URL is unset)
// - standalone via `npm run mock` (mock/server.ts), which also serves dist/
//
// It runs a toy "engine": generated Appendix A events → match → dedup by key
// → alert upserts streamed over SSE with the snapshot protocol. Knobs (env):
//   MOCK_SEED=60           alerts to seed at startup (try 5000)
//   MOCK_RATE=3            events per second (try 200)
//   MOCK_STALE=0.03        chance to re-send an old version after an upsert
//                          (exercises the client's version check)
//   MOCK_RESNAPSHOT_S=0    close every stream every N s, forcing reconnect +
//                          fresh snapshot (simulates a rebalance)

import type { IncomingMessage, ServerResponse } from 'node:http'
import type {
  AlertDetail,
  AlertStatus,
  ApiError,
  EventSample,
  FeedAlert,
  RemoveEvent,
  Session,
  SnapshotEvent,
  TimelineEntry,
  TransitionRequest,
} from '../src/api/types.ts'
import { deliveriesFor, randomEvent, RULES, TEAMS } from './scenario.ts'

interface MockAlert {
  feed: FeedAlert
  key: string
  /** Version at the last status change; 409s compare against this. */
  statusVersion: number
  samples: EventSample[]
  timeline: TimelineEntry[]
  lastDedupNote: number
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
const SNAPSHOT_LIMIT = 5000
const MAX_SAMPLES_HEAD = 3

export function createMock() {
  const seed = env('MOCK_SEED', 60)
  const rate = env('MOCK_RATE', 2)
  const staleChance = env('MOCK_STALE', 0.03)
  const resnapshotS = env('MOCK_RESNAPSHOT_S', 0)

  const alerts = new Map<string, MockAlert>()
  /** open|acked alert id by `${rule}|${key}` */
  const active = new Map<string, string>()
  const clients = new Set<Client>()
  let idSeq = 0
  let evSeq = 0

  const newId = (at: number) =>
    `alt_${at.toString(36).padStart(9, '0')}${(idSeq++).toString(36).padStart(4, '0')}`

  // ---- engine ------------------------------------------------------------

  function ingest(source: string, raw: unknown, at = new Date(), broadcast = true): string[] {
    const touched: string[] = []
    const eventId = `evt_${at.getTime().toString(36)}${(evSeq++).toString(36)}`
    const sample: EventSample = { event_id: eventId, received_at: at.toISOString(), raw }
    for (const rule of RULES) {
      const m = rule.match(raw)
      if (!m) continue
      const k = `${rule.id}|${m.key}`
      const existingId = active.get(k)
      const existing = existingId ? alerts.get(existingId) : undefined
      if (existing) {
        const prev = existing.feed
        existing.feed = { ...prev, count: prev.count + 1, last_seen: at.toISOString(), version: prev.version + 1 }
        if (existing.samples.length > MAX_SAMPLES_HEAD) existing.samples[MAX_SAMPLES_HEAD] = sample
        else existing.samples.push(sample)
        if (at.getTime() - existing.lastDedupNote > 60_000) {
          existing.lastDedupNote = at.getTime()
          existing.timeline.push({ at: at.toISOString(), actor: 'system', kind: 'deduplicated', message: `Duplicate events folded in; count is now ${existing.feed.count}` })
        }
        touched.push(existing.feed.id)
        if (broadcast) publish(existing, prev)
      } else {
        const id = newId(at.getTime())
        const feed: FeedAlert = {
          id,
          team: rule.team,
          version: 1,
          rule: { id: rule.id, revision: 1, name: rule.name },
          status: 'open',
          severity: m.severity,
          color: rule.color,
          title: m.title,
          body: m.body,
          fields: m.fields,
          count: 1,
          first_seen: at.toISOString(),
          last_seen: at.toISOString(),
          source,
          schema: rule.schema,
          acked_by: null,
          acked_at: null,
          resolved_by: null,
          resolved_at: null,
          resolved_reason: null,
        }
        const a: MockAlert = {
          feed,
          key: k,
          statusVersion: 1,
          samples: [sample],
          timeline: [{ at: at.toISOString(), actor: `rule:${rule.id}`, kind: 'opened', message: `Opened by rule “${rule.name}” (rev 1) from ${source}` }],
          lastDedupNote: at.getTime(),
        }
        alerts.set(id, a)
        active.set(k, id)
        touched.push(id)
        if (broadcast) publish(a, null)
      }
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
    a.timeline.push({
      at: iso,
      actor,
      kind: to === 'acked' ? 'acked' : 'resolved',
      message: to === 'acked' ? 'Acknowledged' : `Resolved (${reason ?? 'user'})`,
    })
    return prev
  }

  function remove(a: MockAlert, reason: RemoveEvent['reason']) {
    alerts.delete(a.feed.id)
    if (active.get(a.key) === a.feed.id) active.delete(a.key)
    const rm: RemoveEvent = { id: a.feed.id, version: a.feed.version + 1, reason }
    for (const c of clients) if (visible(c.team, a.feed)) send(c.res, 'alert.remove', rm)
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
    const rules = q?.getAll('rule') ?? []
    let items = [...alerts.values()]
      .map((a) => a.feed)
      .filter((a) => visible(team, a))
      .filter((a) => !statuses.length || statuses.includes(a.status))
      .filter((a) => !sevs.length || sevs.includes(a.severity))
      .filter((a) => !rules.length || rules.includes(a.rule.id))
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

  // ---- HTTP ----------------------------------------------------------------

  function json(res: ServerResponse, status: number, body: unknown) {
    res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
    res.end(JSON.stringify(body))
  }
  const err = (res: ServerResponse, status: number, code: string, message: string, current?: FeedAlert) =>
    json(res, status, { error: { code, message, ...(current ? { current } : {}) } } satisfies ApiError)

  async function body(req: IncomingMessage): Promise<any> {
    const chunks: Buffer[] = []
    for await (const c of req) chunks.push(c as Buffer)
    const s = Buffer.concat(chunks).toString()
    return s ? JSON.parse(s) : {}
  }

  /** Returns true if the request was handled. */
  function handle(req: IncomingMessage, res: ServerResponse): boolean {
    const url = new URL(req.url ?? '/', 'http://mock')
    const path = url.pathname
    const method = req.method ?? 'GET'

    if (path === '/api/session' && method === 'GET') {
      json(res, 200, { user: { id: 'erop', name: 'erop (demo)' }, teams: TEAMS, csrf_token: CSRF } satisfies Session)
      return true
    }
    if (path === '/api/stream' && method === 'GET') {
      openStream(req, res, url)
      return true
    }
    if (path === '/api/alerts' && method === 'GET') {
      json(res, 200, page(url.searchParams.get('team') || null, url.searchParams))
      return true
    }
    let m = path.match(/^\/api\/alerts\/([^/]+)$/)
    if (m && method === 'GET') {
      const a = alerts.get(decodeURIComponent(m[1]!))
      if (!a) return err(res, 404, 'not_found', 'No such alert'), true
      const detail: AlertDetail = { ...a.feed, samples: a.samples, runbook: null, deliveries: deliveriesFor(a.feed), timeline: a.timeline }
      json(res, 200, detail)
      return true
    }
    m = path.match(/^\/api\/alerts\/([^/]+)\/(ack|resolve)$/)
    if (m && method === 'POST') {
      const id = decodeURIComponent(m[1]!)
      const op = m[2] as 'ack' | 'resolve'
      void (async () => {
        if (req.headers['x-csrf-token'] !== CSRF) return err(res, 403, 'csrf', 'Missing or wrong X-CSRF-Token')
        let reqBody: TransitionRequest
        try {
          reqBody = await body(req)
        } catch {
          return err(res, 400, 'bad_request', 'Body is not JSON')
        }
        if (typeof reqBody.expected_version !== 'number') return err(res, 400, 'bad_request', 'expected_version is required')
        const a = alerts.get(id)
        if (!a) return err(res, 404, 'not_found', 'No such alert')
        const target: AlertStatus = op === 'ack' ? 'acked' : 'resolved'
        if (a.feed.status === target) return json(res, 200, a.feed)
        if (op === 'ack' && a.feed.status === 'resolved')
          return err(res, 409, 'invalid_transition', 'This alert is already resolved', a.feed)
        if (a.statusVersion > reqBody.expected_version) {
          const by = a.feed.status === 'acked' ? a.feed.acked_by : a.feed.resolved_by
          return err(res, 409, 'version_conflict', `Alert was ${a.feed.status} by ${by ?? 'someone'} meanwhile`, a.feed)
        }
        const prev = transition(a, target, 'user:erop', op === 'resolve' ? 'user' : null)
        publish(a, prev)
        json(res, 200, a.feed)
      })()
      return true
    }
    m = path.match(/^\/ingest\/([^/]+)$/)
    if (m && method === 'POST') {
      void (async () => {
        try {
          const raw = await body(req)
          const touched = ingest(decodeURIComponent(m![1]!), raw)
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
      json(res, 200, { ingested: n, alerts: alerts.size })
      return true
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
  const spanMs = Math.max(3, seed / 150) * 3600_000
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

  const timers: NodeJS.Timeout[] = []
  if (rate > 0) {
    const perTick = Math.max(1, Math.round(rate / 10))
    const interval = Math.max(10, Math.round((1000 * perTick) / rate))
    timers.push(
      setInterval(() => {
        for (let i = 0; i < perTick; i++) {
          // Bursty: occasionally a quick run of duplicates on the hottest key.
          const e = randomEvent(new Date())
          ingest(e.source, e.raw)
        }
        // Occasionally the "monitoring system" recovers an alert on its own.
        if (Math.random() < 0.02 * perTick) {
          const open = [...alerts.values()].filter((a) => a.feed.status !== 'resolved' && a.feed.rule.id === 'payments-high-error-rate')
          const a = open[Math.floor(Math.random() * open.length)]
          if (a) publish(a, transition(a, 'resolved', 'system', 'recovered'))
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
    stats: () => ({ alerts: alerts.size, clients: clients.size }),
  }
}

function pickActor() {
  return ['user:anna', 'user:alexey', 'user:ilya'][Math.floor(Math.random() * 3)]!
}
