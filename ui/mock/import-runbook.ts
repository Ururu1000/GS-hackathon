import { parseDocument } from 'yaml'
import type { ImportedStep, RunbookImport, RunbookPreview } from '../src/api/screens.ts'
import type { RunbookDef } from './scenario.ts'

const MAX_CONTENT = 64 * 1024
const MAX_STEPS = 50
const MAX_STEP_TEXT = 2_000

const actions = {
  http_get: { label: 'Check health', risk: 'low', keys: ['url'] },
  retry_workflow: { label: 'Retry workflow', risk: 'low', keys: ['workflowId'] },
  escalate: { label: 'Escalate', risk: 'high', keys: ['team'] },
  restart_service: { label: 'Restart instance', risk: 'high', keys: ['service', 'instance'] },
} as const

const record = (input: unknown): input is Record<string, unknown> =>
  input !== null && typeof input === 'object' && !Array.isArray(input)
const text = (value: unknown) => typeof value === 'string' ? value.trim() : ''
const idFor = (title: string) => title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64)
const manual = (value: string, index: number): ImportedStep => ({
  id: `step-${index + 1}`, kind: 'manual', text: value.slice(0, MAX_STEP_TEXT),
  ...(/needs approval/i.test(value) ? { hint: 'The runbook mentions approval.' } : {}),
})

function webLink(value: string) {
  const url = new URL(value)
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password)
    throw new Error('Enter an HTTP or HTTPS link without credentials')
  return url.toString()
}

function yamlSteps(content: string): ImportedStep[] {
  const doc = parseDocument(content, { uniqueKeys: true })
  if (doc.errors[0]) throw new Error(doc.errors[0].message)
  const parsed: unknown = doc.toJS({ maxAliasCount: 20 })
  if (!record(parsed) || !Array.isArray(parsed.steps) || !parsed.steps.length || parsed.steps.length > MAX_STEPS)
    throw new Error('The YAML must contain between 1 and 50 steps')
  const steps: ImportedStep[] = []
  for (const entry of parsed.steps) {
    const index = steps.length
    if (typeof entry === 'string' && entry.trim()) {
      steps.push(manual(entry.trim(), index))
      continue
    }
    if (!record(entry)) throw new Error(`Step ${index + 1} must be a mapping or text`)
    const name = text(entry.action)
    const check = text(entry.check) || text(entry.text)
    const condition = text(entry.if)
    if (!name) {
      if (!check) throw new Error(`Step ${index + 1} needs a check or an action`)
      steps.push(manual(check, index))
      continue
    }
    if (!Object.hasOwn(actions, name)) throw new Error(`Step ${index + 1} uses an unregistered action`)
    const action = actions[name as keyof typeof actions]
    const params = Object.fromEntries(action.keys.map((key) => {
      const value = text(entry[key])
      if (!value) throw new Error(`Step ${index + 1} requires ${key}`)
      return [key, value]
    }))
    if (name === 'http_get') webLink(params.url!)
    const retry = condition === 'retry fails twice' ? steps.findLast((step) => step.action?.name === 'retry_workflow') : null
    if (condition && !retry) {
      steps.push({ id: `step-${index + 1}`, kind: 'condition', text: check || action.label, hint: condition })
      continue
    }
    steps.push({
      id: `step-${index + 1}`, kind: 'action', text: check || action.label,
      action: { name, label: action.label, risk: action.risk, params },
      approval: action.risk === 'high' || entry.approval === 'required' ? 'required' : 'none',
      ...(retry ? { after_failures: { step: retry.id, count: 2 } } : {}),
      ...(entry.resolves === true ? { resolves: true } : {}),
    })
  }
  return steps
}

export function prepareRunbook(input: unknown): { draft: RunbookImport; preview: RunbookPreview; url?: string } {
  if (!record(input)) throw new Error('Enter a runbook')
  const { format } = input
  const title = text(input.title)
  const team = text(input.team)
  const content = text(input.content)
  const id = idFor(title)
  if (!title || !id || title.length > 120 || !team) throw new Error('Enter a title of up to 120 characters and select a team')
  if (!content || content.length > MAX_CONTENT) throw new Error('Enter runbook content of up to 64 KiB')
  if (format !== 'yaml' && format !== 'markdown' && format !== 'link' && format !== 'text')
    throw new Error('Select a runbook format')
  const draft: RunbookImport = { title, team, format, content: typeof input.content === 'string' ? input.content : content }
  if (format === 'link') return { draft, preview: { id, title, steps: [] }, url: webLink(content) }
  if (format === 'yaml') return { draft, preview: { id, title, steps: yamlSteps(content) } }

  const lines = content.split(/\r?\n/).map((line) => line.trim()).filter(Boolean)
  const checklist = format === 'markdown'
    ? lines.flatMap((line) => {
      const match = line.match(/^(?:[-+*]|\d+[.)])\s+(?:\[[ xX]\]\s+)?(.+)$/)
      return match?.[1] ? [match[1]] : []
    })
    : lines
  const steps = (checklist.length ? checklist : [content]).slice(0, MAX_STEPS).map(manual)
  return { draft, preview: { id, title, steps } }
}

export function importRunbook(input: unknown): RunbookDef {
  const { draft, preview, url } = prepareRunbook(input)
  const edited = record(input) ? input.steps : undefined
  if (!Array.isArray(edited) || edited.length > MAX_STEPS || (draft.format !== 'link' && !edited.length))
    throw new Error('Preview and keep between 1 and 50 steps')
  const ids = new Set<string>()
  const originals = new Map(preview.steps.map((step) => [step.id, step]))
  const steps: ImportedStep[] = edited.map((value) => {
    if (!record(value)) throw new Error('Every step needs an id and text')
    const id = text(value.id)
    const label = text(value.text)
    if (!/^[a-zA-Z0-9-]{1,80}$/.test(id) || !label || label.length > MAX_STEP_TEXT || ids.has(id))
      throw new Error('Every step needs unique id and text of up to 2,000 characters')
    ids.add(id)
    const original = originals.get(id)
    return original ? { ...original, text: label } : { id, kind: 'manual', text: label }
  })
  if (steps.some((step) => step.after_failures && !ids.has(step.after_failures.step)))
    throw new Error('Keep the retry step that the escalation depends on')
  return {
    id: preview.id, title: draft.title, format: draft.format, team: draft.team, steps,
    ...(url ? { url } : { text: draft.content }),
  }
}
