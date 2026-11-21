import { parseDocument } from 'yaml'
import type { ImportedStep, RunbookImport, RunbookPreview } from '../src/api/screens.ts'
import { hasForbiddenKey, redactText } from '../src/lib/redact.ts'
import type { RunbookDef } from './scenario.ts'

const MAX_CONTENT = 64 * 1024
const MAX_STEPS = 50
const MAX_STEP_TEXT = 2_000
const MAX_HINT_TEXT = 500
const MAX_PARAM_LEN = 512
const MAX_URL_LEN = 2_048
const CTRL_RE = /[\x00-\x1f\x7f]/

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
const manual = (value: string, index: number): ImportedStep => {
  const clean = redactText(value, MAX_STEP_TEXT)
  return {
    id: `step-${index + 1}`, kind: 'manual', text: clean,
    ...(/needs approval/i.test(clean) ? { hint: 'The runbook mentions approval.' } : {}),
  }
}

function isBlockedActionHost(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, '').replace(/\.$/, '')
  if (!host || /[{}%]/.test(host)) return true
  if (
    host === 'localhost' ||
    host.endsWith('.localhost') ||
    host.endsWith('.local') ||
    host === 'metadata.google.internal' ||
    host === 'metadata.goog' ||
    host === 'instance-data'
  ) return true
  if (/^\d+$/.test(host) || /^0x[0-9a-f]+$/i.test(host)) return true
  const ipv4 = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/)
  if (ipv4) {
    const [a, b] = [Number(ipv4[1]), Number(ipv4[2])]
    if (a === 0 || a === 127) return true
    if (a === 169 && b === 254) return true
    if (a === 100 && b >= 64 && b <= 127) return true
    return false
  }
  if (host.includes(':')) {
    if (host === '::' || host === '::1' || /^0(:0)*(:1)?$/.test(host)) return true
    if (/^fe[89ab][0-9a-f]:/i.test(host) || /^f[cd][0-9a-f]{2}:/i.test(host)) return true
    if (host.startsWith('::ffff:')) return true
  }
  return false
}

function webLink(value: string, { blockLocal = false } = {}) {
  if (!value || value.length > MAX_URL_LEN || CTRL_RE.test(value))
    throw new Error('Enter an HTTP or HTTPS link without credentials')
  if (blockLocal) {
    const authority = value.replace(/^https?:\/\//i, '').split(/[/?#]/, 1)[0] ?? ''
    if (/[{}]/.test(authority))
      throw new Error('Action URL host cannot contain template placeholders')
  }
  const url = new URL(value)
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password)
    throw new Error('Enter an HTTP or HTTPS link without credentials')
  if (blockLocal && isBlockedActionHost(url.hostname))
    throw new Error('Action URL cannot target loopback, link-local, or metadata hosts')
  return url.toString()
}

export function validateActionUrl(value: string): boolean {
  try {
    webLink(value, { blockLocal: true })
    return true
  } catch {
    return false
  }
}

function yamlSteps(content: string): ImportedStep[] {
  const doc = parseDocument(content, { uniqueKeys: true })
  if (doc.errors[0]) throw new Error(doc.errors[0].message)
  let parsed: unknown
  try {
    parsed = doc.toJS({ maxAliasCount: 20 })
    JSON.stringify(parsed)
  } catch {
    throw new Error('Could not read YAML runbook')
  }
  const forbidden = hasForbiddenKey(parsed)
  if (forbidden) throw new Error(`Forbidden key "${forbidden}" in runbook YAML`)
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
    const name = Object.hasOwn(entry, 'action') ? text(entry.action) : ''
    const rawCheck = (Object.hasOwn(entry, 'check') ? text(entry.check) : '') ||
      (Object.hasOwn(entry, 'text') ? text(entry.text) : '')
    const check = rawCheck ? redactText(rawCheck, MAX_STEP_TEXT) : ''
    const rawCondition = Object.hasOwn(entry, 'if') ? text(entry.if) : ''
    const condition = rawCondition ? redactText(rawCondition, MAX_HINT_TEXT) : ''
    if (!name) {
      if (!check) throw new Error(`Step ${index + 1} needs a check or an action`)
      steps.push(manual(check, index))
      continue
    }
    if (!Object.hasOwn(actions, name)) throw new Error(`Step ${index + 1} uses an unregistered action`)
    const action = actions[name as keyof typeof actions]
    const params = Object.fromEntries(action.keys.map((key) => {
      const value = Object.hasOwn(entry, key) ? text(entry[key]) : ''
      if (!value || value.length > MAX_PARAM_LEN)
        throw new Error(`Step ${index + 1} requires ${key} (up to ${MAX_PARAM_LEN} characters)`)
      return [key, key === 'url' ? webLink(value, { blockLocal: true }) : redactText(value, MAX_PARAM_LEN)]
    }))
    const retry = condition === 'retry fails twice' ? steps.findLast((step) => step.action?.name === 'retry_workflow') : null
    if (condition && !retry) {
      steps.push({ id: `step-${index + 1}`, kind: 'condition', text: check || action.label, hint: condition })
      continue
    }
    steps.push({
      id: `step-${index + 1}`, kind: 'action', text: check || action.label,
      action: { name, label: action.label, risk: action.risk, params },
      approval: action.risk === 'high' || (Object.hasOwn(entry, 'approval') && entry.approval === 'required') ? 'required' : 'none',
      ...(retry ? { after_failures: { step: retry.id, count: 2 } } : {}),
      ...(Object.hasOwn(entry, 'resolves') && entry.resolves === true ? { resolves: true } : {}),
    })
  }
  return steps
}

export function prepareRunbook(input: unknown): { draft: RunbookImport; preview: RunbookPreview; url?: string } {
  if (!record(input)) throw new Error('Enter a runbook')
  const { format } = input
  const title = redactText(text(input.title), 120)
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
  if (draft.format === 'link' && edited.length > 0)
    throw new Error('Link runbooks cannot include checklist steps')
  const ids = new Set<string>()
  const originals = new Map(preview.steps.map((step) => [step.id, step]))
  const steps: ImportedStep[] = edited.map((value) => {
    if (!record(value)) throw new Error('Every step needs an id and text')
    const id = text(value.id)
    const rawLabel = text(value.text)
    const label = redactText(rawLabel, MAX_STEP_TEXT)
    if (!/^[a-zA-Z0-9-]{1,80}$/.test(id) || !label || rawLabel.length > MAX_STEP_TEXT || ids.has(id))
      throw new Error('Every step needs unique id and text of up to 2,000 characters')
    ids.add(id)
    const original = originals.get(id)
    return original ? { ...original, text: label } : { id, kind: 'manual', text: label }
  })
  if (steps.some((step) => step.after_failures && !ids.has(step.after_failures.step)))
    throw new Error('Keep the retry step that the escalation depends on')
  const storedText =
    draft.format === 'markdown'
      ? steps.map((step, index) => `${index + 1}. ${step.text}`).join('\n')
      : draft.format === 'text'
        ? steps.map((step) => step.text).join('\n')
        : undefined
  return {
    id: preview.id, title: draft.title, format: draft.format, team: draft.team, steps,
    ...(url ? { url } : storedText ? { text: storedText } : {}),
  }
}
