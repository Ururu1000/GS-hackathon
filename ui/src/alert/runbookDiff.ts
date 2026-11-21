// Post-incident runbook diff (spec §11 M7): the runbook an alert froze at its
// first notification against the runbook's current revision. The frozen steps
// are rendered for the alert ({{workflowId}} filled in), the current ones are
// templates, so text is compared by matching the template, not by equality.

import type { RunbookDetail, RunbookDetailStep, RunbookStep, RunbookView } from '../api/types'

export type StepField = 'text' | 'kind' | 'action' | 'approval'

export interface StepChange {
  id: string
  before: RunbookStep
  after: RunbookDetailStep
  fields: StepField[]
}

export interface RunbookDiff {
  from: number
  to: number
  added: RunbookDetailStep[]
  removed: RunbookStep[]
  changed: StepChange[]
  /** The steps both revisions share appear in a different order. */
  reordered: boolean
}

const PLACEHOLDER = /\{\{[^{}]*\}\}/g

/** Whether `rendered` is `template` with its `{{…}}` placeholders filled in. */
export function templateMatches(template: string, rendered: string): boolean {
  if (template === rendered) return true
  const parts = template.split(PLACEHOLDER)
  if (parts.length === 1) return false
  const escaped = parts.map((p) => p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
  return new RegExp(`^${escaped.join('[\\s\\S]*?')}$`).test(rendered)
}

function changedFields(before: RunbookStep, after: RunbookDetailStep): StepField[] {
  const fields: StepField[] = []
  if (!templateMatches(after.text, before.text)) fields.push('text')
  if (before.kind !== after.kind) fields.push('kind')
  if ((before.action?.name ?? null) !== (after.action?.name ?? null) || (before.action?.label ?? null) !== (after.action?.label ?? null))
    fields.push('action')
  if (before.approval !== after.approval) fields.push('approval')
  return fields
}

/** Null when the alert's runbook is still the current revision. Steps are matched by id. */
export function diffRunbook(frozen: RunbookView, current: RunbookDetail): RunbookDiff | null {
  if (frozen.id !== current.id || frozen.revision === current.revision) return null
  const now = new Map(current.steps.map((s) => [s.id, s]))
  const then = new Set(frozen.steps.map((s) => s.id))
  const changed: StepChange[] = []
  for (const before of frozen.steps) {
    const after = now.get(before.id)
    if (!after) continue
    const fields = changedFields(before, after)
    if (fields.length) changed.push({ id: before.id, before, after, fields })
  }
  const sharedThen = frozen.steps.filter((s) => now.has(s.id)).map((s) => s.id)
  const sharedNow = current.steps.filter((s) => then.has(s.id)).map((s) => s.id)
  return {
    from: frozen.revision,
    to: current.revision,
    added: current.steps.filter((s) => !then.has(s.id)),
    removed: frozen.steps.filter((s) => !now.has(s.id)),
    changed,
    reordered: sharedThen.some((id, i) => sharedNow[i] !== id),
  }
}

/** Steps someone completed during the incident: ticked manual steps and actions whose last run succeeded. */
export function completedSteps(frozen: RunbookView): RunbookStep[] {
  return frozen.steps.filter((s) => s.done_at !== null || s.state === 'done' || s.state === 'ok')
}
