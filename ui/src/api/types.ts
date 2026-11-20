// API contract the UI depends on. Mirrors ui/contract/openapi.yaml (the source
// of truth for the backend); keep the two in sync by hand until we generate one
// from the other.
//
// Conventions
// - Timestamps: RFC 3339, UTC, millisecond precision, `Z` suffix.
// - JSON field names: snake_case.
// - `version`: per-alert, strictly increasing integer. Every visible change to
//   an alert (count bump, status change, re-render) bumps it.

export type Severity = 'critical' | 'error' | 'warning' | 'info'
export const SEVERITIES: readonly Severity[] = ['critical', 'error', 'warning', 'info']

export type AlertStatus = 'open' | 'acked' | 'resolved'
export const STATUSES: readonly AlertStatus[] = ['open', 'acked', 'resolved']

/** Semantic colour token set by the rule's `present.color`. The UI maps it to
 * a theme colour and always shows the severity label next to it. Unknown
 * tokens render as `grey`. */
export type ColorToken = 'red' | 'orange' | 'amber' | 'yellow' | 'green' | 'blue' | 'purple' | 'grey'
export const COLOR_TOKENS: readonly ColorToken[] = [
  'red', 'orange', 'amber', 'yellow', 'green', 'blue', 'purple', 'grey',
]

export interface RuleRef {
  id: string
  /** Rule revision the alert was opened (and rendered) under. */
  revision: number
  /** Human name of the rule, for filters and the card. */
  name: string
}

/** One labelled value from the rule's `present.fields`, already rendered to a
 * string by the backend, in the rule's order. */
export interface AlertField {
  path: string
  label: string
  value: string
}

/** The feed projection: what a row in the live feed needs. Sent by
 * `GET /api/alerts`, in the SSE `snapshot` and `alert.upsert` events, and as
 * the result of ack/resolve. */
export interface FeedAlert {
  id: string
  team: string
  version: number
  rule: RuleRef
  status: AlertStatus
  severity: Severity
  color: ColorToken
  title: string
  body: string
  fields: AlertField[]
  /** Dedup counter: matching events folded into this alert. */
  count: number
  first_seen: string
  last_seen: string
  /** Transport source instance of the first event, e.g. `kafka:workflows`. */
  source: string
  /** Detected schema, or `unknown:<signature>`. */
  schema: string
  acked_by: string | null
  acked_at: string | null
  resolved_by: string | null
  resolved_at: string | null
  resolved_reason: string | null
}

export interface FeedPage {
  items: FeedAlert[]
  /** Opaque keyset cursor for the next (older) page, or null at the end. */
  next_cursor: string | null
}

// ---- Alert detail -------------------------------------------------------

export interface EventSample {
  event_id: string
  received_at: string
  raw: unknown
}

export type TimelineKind =
  | 'opened'
  | 'deduplicated'
  | 'acked'
  | 'resolved'
  | 'reopened'
  | 'notified'
  | 'step_run'
  | 'approval'
  | 'comment'

export interface TimelineEntry {
  at: string
  /** `user:<id>`, `system`, `rule:<id>` … */
  actor: string
  kind: TimelineKind | (string & {})
  message: string
}

/** Placeholder until runbooks land (M5). */
export interface RunbookView {
  id: string
  revision: number
  title: string
  format: 'yaml' | 'markdown' | 'link' | 'text'
  steps: unknown[]
}

/** `cancelled`: the alert was acked/resolved before the send. Unknown states
 * render generically. */
export type DeliveryState = 'pending' | 'in_flight' | 'sent' | 'cancelled' | 'dead'

/** One row of `delivery_jobs` for this alert (proposed by delivery, board #63).
 * Channels are an open set (`email`, `sms`, `phone`, `ui`, …). */
export interface DeliveryStatus {
  /** Delivery job id. */
  id: string
  /** `user:<id>` after team/on-call expansion. */
  recipient: string
  channel: string
  /** Where it was sent: email address, phone number… */
  address: string
  state: DeliveryState | (string & {})
  attempts: number
  last_error: string | null
  sent_at: string | null
  updated_at: string
}

export interface AlertDetail extends FeedAlert {
  /** First N and latest matching events (raw, untouched payloads). */
  samples: EventSample[]
  /** Null until runbooks ship. */
  runbook: RunbookView | null
  /** Delivery jobs for this alert, any order; the UI sorts them. */
  deliveries: DeliveryStatus[]
  timeline: TimelineEntry[]
}

// ---- Mutations ----------------------------------------------------------

export interface TransitionRequest {
  /** The alert `version` the user acted on. The server answers 409
   * `version_conflict` only if the alert's *status* changed after that
   * version; count-only bumps never conflict. */
  expected_version: number
  note?: string
}

export type ErrorCode =
  | 'version_conflict'
  | 'invalid_transition'
  | 'not_found'
  | 'forbidden'
  | 'csrf'
  | 'bad_request'
  | 'internal'

export interface ApiError {
  error: {
    code: ErrorCode | (string & {})
    message: string
    /** Present on `version_conflict` and `invalid_transition`. */
    current?: FeedAlert
  }
}

// ---- Session -------------------------------------------------------------

export interface Session {
  user: { id: string; name: string }
  /** Teams the user can see; the feed scope picker lists these. */
  teams: { id: string; name: string }[]
  /** Echo in the `X-CSRF-Token` header on every write. */
  csrf_token: string
}

// ---- SSE: GET /api/stream?team= -------------------------------------------

export interface SnapshotEvent {
  /** `null` = every team the session can see. */
  team: string | null
  items: FeedAlert[]
  next_cursor: string | null
  server_time: string
}

export type UpsertEvent = FeedAlert

export interface RemoveEvent {
  id: string
  /** Version at removal. Later upserts with version <= this are stale. */
  version: number
  reason: 'deleted' | 'out_of_scope' | 'expired'
}

export type StreamEvent =
  | { type: 'snapshot'; data: SnapshotEvent }
  | { type: 'alert.upsert'; data: UpsertEvent }
  | { type: 'alert.remove'; data: RemoveEvent }

export const STREAM_EVENT_TYPES = ['snapshot', 'alert.upsert', 'alert.remove'] as const
