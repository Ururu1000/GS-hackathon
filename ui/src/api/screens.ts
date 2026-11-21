import { post, request } from './client'
import type { Condition, RunbookFormat, RunbookSummary, StepAction } from './types'

export interface SourceView {
  id: string
  type: string
  kind: 'http' | 'kafka' | 'file'
  topics?: string[]
  last_seen: string | null
  events_24h: number
}

export interface DiscoveredSchema {
  schema: string
  source: string
  count: number
  first_seen: string
  last_seen: string
  sample: unknown
}

export interface SchemaField {
  path?: string
  value?: unknown
  transform?: 'lower' | 'upper' | 'trim'
  enum?: 'severity'
  as?: 'string' | 'number' | 'bool' | 'rfc3339' | 'epoch_ms' | 'epoch_s'
  equals?: unknown
}

export interface SchemaSpec {
  schema: string
  priority: number
  detect: Condition & { source?: string }
  fields: Record<string, SchemaField>
}

export interface SchemaMapping {
  spec: SchemaSpec
  revision: number
}

export interface SchemaPreview {
  matched: boolean
  fields: Record<string, unknown>
  warnings: string[]
}

export interface RunbookImport {
  format: RunbookFormat
  team: string
  title: string
  content: string
}

export interface ImportedStep {
  id: string
  kind: 'manual' | 'condition' | 'action'
  text: string
  hint?: string
  action?: StepAction
  approval?: 'none' | 'required'
  after_failures?: { step: string; count: number }
  resolves?: boolean
}

export interface RunbookPreview {
  id: string
  title: string
  steps: ImportedStep[]
}

export interface SimulatorMessage {
  id: number
  receipt_id: string
  job_id: string
  alert_id: string
  channel: 'sms' | 'phone'
  recipient: string
  to: string
  text: string
  approval_challenge: string | null
  duplicates: number
  received_at: string
}

export interface SimulatorPage {
  items: SimulatorMessage[]
  next_cursor: string | null
}

export const listSources = (signal?: AbortSignal) => request<SourceView[]>('/api/sources', { signal })
export const listDiscoveredSchemas = (signal?: AbortSignal) =>
  request<DiscoveredSchema[]>('/api/schemas/discovered', { signal })
export const listSchemaMappings = (signal?: AbortSignal) =>
  request<SchemaMapping[]>('/api/schemas', { signal })
/** `POST /api/schemas/suggest`: a reviewed-before-publish mapping proposal (nothing is stored). */
export interface SchemaSuggestion {
  provider: 'mock' | 'claude' | (string & {})
  model: string | null
  spec: SchemaSpec
  preview: SchemaPreview
  notes: string
}

export const suggestSchemaMapping = (input: { schema: string } | { event: unknown; source?: string }) =>
  post<SchemaSuggestion>('/api/schemas/suggest', input)

export const previewSchemaMapping = (spec: unknown, event: unknown, source: string, signal?: AbortSignal) =>
  post<SchemaPreview>('/api/schemas/preview', { spec, event, source }, signal)
/** `editing` is the stored mapping being changed; omit it to create a new one. */
export const saveSchemaMapping = (spec: unknown, editing?: { schema: string; revision: number }) =>
  post<SchemaMapping>('/api/schemas', { spec, expected_revision: editing?.revision, schema: editing?.schema })

export const previewRunbook = (draft: RunbookImport, signal?: AbortSignal) =>
  post<RunbookPreview>('/api/runbooks/import', draft, signal)
export const publishRunbook = (draft: RunbookImport, preview: RunbookPreview) =>
  post<RunbookSummary>('/api/runbooks', { ...draft, ...preview })

export const listSimulatorMessages = (cursor?: string, signal?: AbortSignal) =>
  request<SimulatorPage>(`/api/simulator/messages${cursor ? `?cursor=${encodeURIComponent(cursor)}` : ''}`, { signal })
export type SmsReplyOutcome = 'approved' | 'rejected' | 'acked' | 'already_decided' | 'wrong_code' | 'too_many_attempts' | 'unknown'

export interface SmsReplyResult {
  outcome: SmsReplyOutcome
  approval_id?: string
}

export const replyToSimulatorMessage = (from: string, text: string, receiptId?: string) =>
  post<SmsReplyResult>('/api/simulator/reply', { from, text, receipt_id: receiptId })

export type OutageChannel = 'sms' | 'phone'

/** A simulated provider outage on the demo gateway (`until` is RFC 3339). */
export interface GatewayOutage {
  channel: OutageChannel
  mode: 'down' | 'lose_response'
  until: string
}

export interface OutageList {
  outages: GatewayOutage[]
}

export const listOutages = (signal?: AbortSignal) => request<OutageList>('/api/simulator/outage', { signal })
export const startOutage = (channel: OutageChannel, seconds: number) =>
  post<OutageList>('/api/simulator/outage', { channel, seconds })
export const endOutage = (channel: OutageChannel) =>
  request<OutageList>(`/api/simulator/outage?channel=${channel}`, { method: 'DELETE' })
