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

// ---- Runbooks (spec §6), as attached to one alert instance ---------------

export type RunbookFormat = 'yaml' | 'markdown' | 'link' | 'text'

/** Where a step is. `idle` = not started; `done` = manual step ticked;
 * `queued`/`running`/`ok`/`failed`/`uncertain`/`cancelled` follow the latest execution;
 * `awaiting_approval`/`rejected` follow the latest approval request. */
export type StepState =
  | 'idle'
  | 'done'
  | 'queued'
  | 'running'
  | 'ok'
  | 'failed'
  | 'uncertain'
  | 'cancelled'
  | 'awaiting_approval'
  | 'rejected'

export interface StepAction {
  /** Registered action name: `http_get`, `retry_workflow`, `escalate`, … */
  name: string
  /** Human label, e.g. "Retry workflow". */
  label: string
  /** Rendered from the pinned event context; secrets already redacted. */
  params: Record<string, string>
  /** Risk sets the minimum approval level. */
  risk: 'low' | 'high'
}

export interface Execution {
  id: string
  state: 'queued' | 'running' | 'ok' | 'failed' | 'uncertain' | 'cancelled'
  requested_by: string
  started_at: string | null
  finished_at: string | null
  /** Short, redacted outcome ("200 OK", "workflow engine: 503"). */
  output: string | null
}

export interface ApprovalRequest {
  id: string
  state: 'pending' | 'approved' | 'rejected' | 'expired'
  requested_by: string
  requested_at: string
  expires_at: string
  /** `user:<id>` who may decide. */
  eligible: string[]
  /** Whether the session user is eligible to decide (and not barred by self-approval). */
  can_decide: boolean
  /** Whether an out-of-band SMS challenge was dispatched to eligible approvers (plaintext code is never returned). */
  sms_sent?: boolean
  decided_by: string | null
  decided_at: string | null
  note: string | null
}

export interface RunbookStep {
  /** Stable across revisions. */
  id: string
  kind: 'manual' | 'condition' | 'action'
  /** Rendered text with event values filled in. */
  text: string
  /** Guidance that is shown but not enforced ("needs approval from the team lead"). */
  hint: string | null
  action: StepAction | null
  approval: 'none' | 'required'
  state: StepState | (string & {})
  /** Set when the step can't run right now (invalid action, condition not met). */
  disabled_reason: string | null
  done_by: string | null
  done_at: string | null
  /** All executions of this step on this alert, oldest first. */
  runs: Execution[]
  /** The latest approval request, if any. */
  approval_request: ApprovalRequest | null
}

export interface RunbookView {
  id: string
  revision: number
  title: string
  format: RunbookFormat | (string & {})
  /** `link` runbooks: the wiki/Confluence URL (http/https only). */
  url: string | null
  /** `text` and `markdown` runbooks: the original, verbatim. */
  text: string | null
  steps: RunbookStep[]
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
  /** Masked destination (e.g. `a***@example.test`, `+44 •••• ••0123`) to avoid broadcasting personal contact PII. */
  address: string
  state: DeliveryState | (string & {})
  attempts: number
  last_error: string | null
  sent_at: string | null
  updated_at: string
}

export interface Occurrences {
  from: string
  bucket_seconds: number
  counts: number[]
}

export interface AlertDetail extends FeedAlert {
  occurrences?: Occurrences
  /** First N and latest matching events (secrets and PII redacted server-side). */
  samples: EventSample[]
  /** The runbook selected for this alert, with per-step state, or null. */
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
  | 'revision_conflict'
  | 'approval_required'
  | 'invalid'

export interface ApiError {
  error: {
    code: ErrorCode | (string & {})
    message: string
    /** Present on `version_conflict` and `invalid_transition` (a FeedAlert)
     * and on `revision_conflict` (the current RuleItem / PersonView / TeamView). */
    current?: unknown
    /** `approval_required`: the approval request that was opened. */
    approval_id?: string
    /** `invalid`: per-field problems, e.g. `{path: "when.all.0.value", message}`. */
    details?: { path: string; message: string }[]
  }
}

// ---- Session -------------------------------------------------------------

export interface Session {
  user: { id: string; name: string }
  /** Teams the user can see; the feed scope picker lists these. */
  teams: { id: string; name: string }[]
  /** Echo in the `X-CSRF-Token` header on every write. */
  csrf_token: string
  capabilities?: { recipient_feed?: boolean; mock_transports?: boolean; runbook_import?: boolean; llm_suggest?: boolean }
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

// ---- Rules (spec §4.3): the JSON form of config/rules/*.yaml ---------------

export type CondOp =
  | 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte'
  | 'in' | 'not_in' | 'contains' | 'starts_with' | 'regex'
  | 'exists' | 'not_exists'
export const COND_OPS: readonly CondOp[] = [
  'eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'in', 'not_in', 'contains', 'starts_with', 'regex', 'exists', 'not_exists',
]

export type Scalar = string | number | boolean | null

export interface Leaf {
  /** `raw.<json path>`, `fields.<normalized field>`, `source` or `schema`. */
  path: string
  op: CondOp
  value?: Scalar | Scalar[]
}
export type Condition = { all: Condition[] } | { any: Condition[] } | { not: Condition } | Leaf

export interface RouteSpec {
  /** `team:<id>`, `user:<id>` or `oncall:<schedule>`. */
  to: string
  /** Explicit channels; omitted = the recipients' preferences decide (§4.4). */
  channels?: string[]
}

export interface FieldSpec {
  path: string
  label: string
}

export interface RuleSpec {
  id: string
  name?: string
  team: string
  enabled?: boolean
  when: Condition
  /** Omitted = the event's normalized `fields.severity`. */
  severity?: Severity
  route: RouteSpec[]
  present: {
    title: string
    body?: string
    fields?: (FieldSpec | string)[]
    color?: ColorToken
  }
  dedup?: { key?: string[]; window: string }
  group?: { by?: string[]; window?: string | number }
  throttle?: { max: number; per: string }
  runbook?: string
  auto_resolve?: { recover_when?: Condition; quiet_for?: string }
  escalate_after?: string
  /** Unknown keys are kept as-is. */
  [k: string]: unknown
}

export interface RuleStats {
  fires_24h: number
  open_alerts: number
  last_fired_at: string | null
  /** Matching events per hour for the last 24 h, oldest first. */
  hourly: number[]
}

/** `GET /api/rules` item and `GET/PUT /api/rules/{id}` result. */
export interface RuleItem {
  rule: RuleSpec
  /** Compare-and-set token: send it back as `expected_revision`. */
  revision: number
  updated_at: string
  updated_by: string
  stats: RuleStats
  /** Whether the session user may edit or delete this rule. */
  can_edit?: boolean
}

export interface RuleWrite {
  rule: RuleSpec
  /** PUT only; 409 `revision_conflict` if the rule moved on. */
  expected_revision?: number
}

export type NodeResult = 'match' | 'no_match' | 'skipped' | 'error'

export interface NodeDiagnostic {
  /** Child indexes from the root `when` node, dot-separated; `""` = root. */
  node: string
  result: NodeResult
  /** The value found at the leaf's path (JSON), or absent when missing. */
  actual?: unknown
  message?: string
}

export interface RuleTestRequest {
  rule: RuleSpec
  /** Any JSON: the raw event body as a source would send it. */
  event: unknown
  /** Source the event arrives on, e.g. `kafka:workflows` (optional). */
  source?: string
}

/** `POST /api/rules/test`. Zero writes, zero dispatches. */
export interface RuleTestResult {
  matched: boolean
  /** Detected schema and normalized fields, as the rule sees them. */
  schema: string
  fields: Record<string, unknown>
  nodes: NodeDiagnostic[]
  /** The alert card the rule would produce (null if the rule is invalid). */
  card: {
    title: string
    body: string
    severity: Severity
    color: ColorToken
    fields: AlertField[]
    dedup_key: string[]
  } | null
  /** Template and dedup warnings, e.g. "raw.payload.ccy is missing". */
  warnings: string[]
  /** Validation errors that would block publishing. */
  errors: { path: string; message: string }[]
}

/** `GET /api/events/recent`: recently ingested events, for the test panel. */
export interface RecentEvent extends EventSample {
  source: string
  schema: string
}

export interface RunbookSummary {
  id: string
  /** Current published revision (older backends omit it). */
  revision?: number
  title: string
  format: RunbookFormat | (string & {})
  team: string | null
  steps: number
}

/** One step of a runbook's current revision, as templates (not rendered for an alert). */
export interface RunbookDetailStep {
  id: string
  kind: RunbookStep['kind']
  text: string
  action: { name: string; label: string } | null
  approval: 'none' | 'required'
}

/** `GET /api/runbooks/{id}`: the current revision, for post-incident diffs. */
export interface RunbookDetail {
  id: string
  revision: number
  title: string
  format: RunbookFormat | (string & {})
  team: string | null
  steps: RunbookDetailStep[]
}

// ---- People, teams, preferences (spec §4.4) ------------------------------

export interface TimeWindow {
  /** Half-open local-time window "HH:MM-HH:MM"; may wrap midnight. */
  between: string
  /** IANA zone, e.g. `Europe/London`. */
  tz: string
}

export interface PreferenceMatch {
  severity?: Severity | Severity[]
  service?: string
  team?: string
}

/** One personal preference line; the first match wins. */
export type Preference =
  | { match: PreferenceMatch; channels: string[]; when?: TimeWindow; otherwise?: string[] }
  | { default: string[] }

export interface PersonDirectoryEntry {
  id: string
  name: string
  teams: string[]
}

export interface PersonView {
  id: string
  name: string
  /** Omitted or masked for users other than the session user unless authorized. */
  contact: { email?: string; phone?: string }
  teams: string[]
  /** Empty/redacted for users other than the session user unless authorized. */
  preferences: Preference[]
  revision: number
  /** Whether the session user may view/edit this person's contact details and preferences. */
  can_edit?: boolean
}

export interface TeamView {
  id: string
  name: string
  members: string[]
  lead: string | null
  /** Schedule name → rotation order; the first person is on call. */
  oncall: Record<string, string[]>
  /** Step 3 of routing precedence; omitted/empty = `[ui]`. */
  default_channels: string[]
  revision: number
  /** Whether the session user (team lead or admin) may modify this team. */
  can_edit?: boolean
}

export const CHANNELS = ['phone', 'sms', 'email', 'ui'] as const

// ---- Proposals (spec §6.5) ------------------------------------------------

export type ProposalKind = 'noisy' | 'reroute' | 'template' | 'other'
export type ProposalState = 'pending' | 'accepted' | 'rejected' | 'stale'

export interface Proposal {
  id: string
  rule_id: string
  rule_name: string
  kind: ProposalKind | (string & {})
  /** One line: "Group by instrument every 5 minutes". */
  summary: string
  note: string
  /** The rule revision the change was written against. */
  base_revision: number
  base_rule: RuleSpec
  proposed_rule: RuleSpec
  author: string
  /** True when the system suggested it (e.g. "fired 400× this week, 95% acked with no action"). */
  auto: boolean
  /** Alert the suggestion came from, if any. */
  alert_id: string | null
  created_at: string
  state: ProposalState
  decided_by: string | null
  decided_at: string | null
  /** Whether the session user (owning team member or admin) may accept or reject this proposal. */
  can_review?: boolean
}

export interface ProposalCreate {
  rule_id: string
  base_revision: number
  kind: ProposalKind
  summary: string
  note: string
  proposed_rule: RuleSpec
  alert_id?: string
}
