import type { AlertDetail, ApiError, FeedAlert, Session, TransitionRequest } from './types'

// Same-origin by default; set VITE_API_BASE for a UI served from elsewhere.
export const API_BASE: string = import.meta.env.VITE_API_BASE ?? ''

let csrfToken: string | null = null

export class HttpError extends Error {
  readonly status: number
  readonly body: ApiError | null
  constructor(status: number, body: ApiError | null) {
    super(body?.error.message ?? `HTTP ${status}`)
    this.status = status
    this.body = body
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const write = init?.method !== undefined && init.method !== 'GET'
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
    throw new HttpError(res.status, body)
  }
  return (await res.json()) as T
}

export async function getSession(): Promise<Session> {
  const s = await request<Session>('/api/session')
  csrfToken = s.csrf_token
  return s
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
    if (e instanceof HttpError) return { ok: false, message: e.message, current: e.body?.error.current }
    return { ok: false, message: e instanceof Error ? e.message : String(e) }
  }
}

export function streamUrl(team: string | null): string {
  return `${API_BASE}/api/stream${team ? `?team=${encodeURIComponent(team)}` : ''}`
}
