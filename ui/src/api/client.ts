import type {
  AlertDetail,
  ApiError,
  FeedAlert,
  PersonDirectoryEntry,
  PersonView,
  Preference,
  Proposal,
  ProposalCreate,
  ProposalState,
  RecentEvent,
  RuleItem,
  RuleSpec,
  RuleTestRequest,
  RuleTestResult,
  RunbookSummary,
  Session,
  TeamView,
  TransitionRequest,
} from './types'

// Same-origin by default; set VITE_API_BASE for a UI served from elsewhere.
export const API_BASE: string = import.meta.env.VITE_API_BASE ?? ''

let csrfToken: string | null = null
let sessionPromise: Promise<Session> | null = null

export function resetSessionCache() {
  csrfToken = null
  sessionPromise = null
}

export class HttpError extends Error {
  readonly status: number
  readonly body: ApiError | null
  constructor(status: number, body: ApiError | null) {
    super(body?.error.message ?? `HTTP ${status}`)
    this.status = status
    this.body = body
  }
}

async function ensureCsrf(): Promise<string | null> {
  if (csrfToken) return csrfToken
  try {
    const s = await getSession()
    return s.csrf_token
  } catch {
    return null
  }
}

export async function request<T>(path: string, init?: RequestInit, _retriedCsrf = false): Promise<T> {
  const method = (init?.method ?? 'GET').toUpperCase()
  const write = method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS'
  if (write && !csrfToken && path !== '/api/session') await ensureCsrf()
  const res = await fetch(API_BASE + path, {
    credentials: 'same-origin',
    ...init,
    headers: {
      Accept: 'application/json',
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...(write && csrfToken ? { 'X-CSRF-Token': csrfToken } : {}),
      ...init?.headers,
    },
  })
  if (!res.ok) {
    let body: ApiError | null = null
    try {
      body = (await res.json()) as ApiError
    } catch {
      /* not JSON */
    }
    if (write && !_retriedCsrf && res.status === 403 && body?.error.code === 'csrf') {
      resetSessionCache()
      await ensureCsrf()
      return request<T>(path, init, true)
    }
    throw new HttpError(res.status, body)
  }
  return (await res.json()) as T
}

export async function getSession(): Promise<Session> {
  if (!sessionPromise) {
    sessionPromise = request<Session>('/api/session')
      .then((s) => {
        csrfToken = s.csrf_token
        return s
      })
      .catch((e) => {
        sessionPromise = null
        throw e
      })
  }
  return sessionPromise
}

export const getAlert = (id: string, signal?: AbortSignal) =>
  request<AlertDetail>(`/api/alerts/${encodeURIComponent(id)}`, { signal })

export type TransitionOp = 'ack' | 'resolve'

export type TransitionResult =
  | { ok: true; alert: FeedAlert }
  | { ok: false; message: string; current?: FeedAlert }

/** Ack or resolve. A 409 is a normal outcome (someone else acted first), so it
 * comes back as a value with the alert's current state, not as an exception. */
export async function transition(id: string, op: TransitionOp, expectedVersion: number): Promise<TransitionResult> {
  const body: TransitionRequest = { expected_version: expectedVersion }
  try {
    const alert = await request<FeedAlert>(`/api/alerts/${encodeURIComponent(id)}/${op}`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
    return { ok: true, alert }
  } catch (e) {
    if (e instanceof HttpError) return { ok: false, message: e.message, current: e.body?.error.current as FeedAlert | undefined }
    return { ok: false, message: e instanceof Error ? e.message : String(e) }
  }
}

export function streamUrl(team: string | null): string {
  return `${API_BASE}/api/stream${team ? `?team=${encodeURIComponent(team)}` : ''}`
}

const enc = encodeURIComponent
const post = <T>(path: string, body: unknown = {}, signal?: AbortSignal) =>
  request<T>(path, { method: 'POST', body: JSON.stringify(body), signal })
const put = <T>(path: string, body: unknown) => request<T>(path, { method: 'PUT', body: JSON.stringify(body) })

/** Message for a failed call, sanitized so internal 5xx errors never leak server internals. */
export function errorText(e: unknown): string {
  if (e instanceof HttpError) {
    if (e.status >= 500 || e.body?.error.code === 'internal') {
      return 'Something went wrong on the server. Please try again.'
    }
    const d = e.body?.error.details
    if (d?.length) {
      const summary = d
        .slice(0, 5)
        .map((x) => `${(x.path || 'rule').slice(0, 80)} ${x.message.slice(0, 160)}`)
        .join('; ')
      return `${e.message.slice(0, 200)}: ${summary}`
    }
    return e.message.slice(0, 240)
  }
  return (e instanceof Error ? e.message : String(e)).slice(0, 240)
}

export const isConflict = (e: unknown): e is HttpError => e instanceof HttpError && e.status === 409

// ---- rules -------------------------------------------------------------------

export const listRules = (team?: string | null) =>
  request<{ items: RuleItem[] }>(`/api/rules${team ? `?team=${enc(team)}` : ''}`).then((r) => r.items)
export const getRule = (id: string) => request<RuleItem>(`/api/rules/${enc(id)}`)
export const createRule = (rule: RuleSpec) => post<RuleItem>('/api/rules', { rule })
export const updateRule = (rule: RuleSpec, expectedRevision: number) =>
  put<RuleItem>(`/api/rules/${enc(rule.id)}`, { rule, expected_revision: expectedRevision })
export const deleteRule = (id: string, expectedRevision: number) =>
  request<{ deleted: string }>(`/api/rules/${enc(id)}?expected_revision=${expectedRevision}`, { method: 'DELETE' })
export const testRule = (body: RuleTestRequest, signal?: AbortSignal) =>
  post<RuleTestResult>('/api/rules/test', body, signal)
export const listRunbooks = () => request<{ items: RunbookSummary[] }>('/api/runbooks').then((r) => r.items)
export const recentEvents = (limit = 20, team?: string | null) => {
  const qs = new URLSearchParams({ limit: String(limit) })
  if (team) qs.set('team', team)
  return request<{ items: RecentEvent[] }>(`/api/events/recent?${qs.toString()}`).then((r) => r.items)
}

// ---- people & teams ------------------------------------------------------------

/** Minimal directory (id, name, teams) for recipient pickers — no contact PII or schedules. */
export const listDirectory = () =>
  request<{ items: PersonDirectoryEntry[] }>('/api/people/directory').then((r) => r.items)
export const listPeople = () => request<{ items: PersonView[] }>('/api/people').then((r) => r.items)
export const savePreferences = (id: string, preferences: Preference[], expectedRevision: number) =>
  put<PersonView>(`/api/people/${enc(id)}/preferences`, { preferences, expected_revision: expectedRevision })
export const listTeams = () => request<{ items: TeamView[] }>('/api/teams').then((r) => r.items)
export const saveTeam = (t: TeamView) =>
  put<TeamView>(`/api/teams/${enc(t.id)}`, {
    members: t.members,
    lead: t.lead,
    oncall: t.oncall,
    default_channels: t.default_channels,
    expected_revision: t.revision,
  })

// ---- proposals ---------------------------------------------------------------------

export const listProposals = (team?: string | null, state?: ProposalState) => {
  const qs = new URLSearchParams()
  if (team) qs.set('team', team)
  if (state) qs.set('state', state)
  const suffix = qs.toString() ? `?${qs.toString()}` : ''
  return request<{ items: Proposal[] }>(`/api/proposals${suffix}`).then((r) => r.items)
}
export const countPendingProposals = (team?: string | null) =>
  request<{ pending: number }>(`/api/proposals/count${team ? `?team=${enc(team)}` : ''}`).then((r) => r.pending)
export const createProposal = (p: ProposalCreate) => post<Proposal>('/api/proposals', p)
export const decideProposal = (id: string, op: 'accept' | 'reject') => post<Proposal>(`/api/proposals/${enc(id)}/${op}`)

// ---- runbook steps & approvals ---------------------------------------------------------

export type RunResult = { kind: 'started'; execution_id: string } | { kind: 'approval'; approval_id: string }

/** Run an action step. `409 approval_required` is a normal outcome, not an error. */
export async function runStep(alertId: string, step: string, expectedVersion: number): Promise<RunResult> {
  try {
    const r = await post<{ execution_id: string }>(`/api/alerts/${enc(alertId)}/steps/${enc(step)}/run`, { expected_version: expectedVersion })
    return { kind: 'started', execution_id: r.execution_id }
  } catch (e) {
    if (e instanceof HttpError && e.body?.error.code === 'approval_required' && e.body.error.approval_id)
      return { kind: 'approval', approval_id: e.body.error.approval_id }
    throw e
  }
}
export const checkStep = (alertId: string, step: string, done: boolean) =>
  post<{ ok: true }>(`/api/alerts/${enc(alertId)}/steps/${enc(step)}/check`, { done })
export const decideApproval = (id: string, op: 'approve' | 'reject', note?: string) =>
  post<{ execution_id?: string }>(`/api/approvals/${enc(id)}/${op}`, note ? { note } : {})

/** Ingest a raw event on a source, as an external system would. */
export const ingest = (source: string, raw: unknown) =>
  post<{ id: string; matched_alerts: string[] }>(`/ingest/${enc(source)}`, raw)
