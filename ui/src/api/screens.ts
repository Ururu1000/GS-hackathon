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
export const previewSchemaMapping = (spec: unknown, event: unknown, source: string, signal?: AbortSignal) =>
  post<SchemaPreview>('/api/schemas/preview', { spec, event, source }, signal)
export const saveSchemaMapping = (spec: unknown, expectedRevision?: number) =>
  post<SchemaMapping>('/api/schemas', { spec, expected_revision: expectedRevision })

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

export const replyToSimulatorMessage = (from: string, text: string) =>
  post<SmsReplyResult>('/api/simulator/reply', { from, text })
