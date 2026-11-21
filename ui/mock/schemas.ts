import { isDeepStrictEqual } from 'node:util'
import type { SchemaField, SchemaPreview, SchemaSpec } from '../src/api/screens.ts'
import { COND_OPS, type Condition, type Scalar } from '../src/api/types.ts'
import { FORBIDDEN_KEYS, hasForbiddenKey, isSensitivePath, redactValue } from '../src/lib/redact.ts'
import { builtInMappings, evaluate, MISSING, unknownSignature, validateRule, type Ctx } from './engine.ts'

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
const pathPattern = /^(?:\/[\S]*|[^.\s]+(?:\.[^.\s]+)*)$/
const MAX_PATH_LEN = 256
const MAX_PATH_SEGMENTS = 16
const MAX_COND_NODES = 100
const MAX_VALUE_DEPTH = 8
const MAX_VALUE_JSON = 4_096
const MAX_CONVERTED_STR = 2_048
const severityRank: Record<string, number> = { info: 1, warning: 2, error: 3, critical: 4 }
const isoTime = (value: number) => Number.isFinite(value) && Math.abs(value) <= 8.64e15 ? new Date(value).toISOString() : undefined

function pathParts(path: string): string[] {
  return path.startsWith('/')
    ? path.slice(1).split('/').map((part) => part.replaceAll('~1', '/').replaceAll('~0', '~'))
    : path.split('.')
}

function validPath(path: unknown): path is string {
  if (typeof path !== 'string' || path.length > MAX_PATH_LEN || !pathPattern.test(path)) return false
  const parts = pathParts(path)
  return parts.length <= MAX_PATH_SEGMENTS && !parts.some((part) => FORBIDDEN_KEYS.has(part))
}

function validBoundedValue(value: unknown, depth = 0): boolean {
  if (depth > MAX_VALUE_DEPTH) return false
  if (value === undefined || value === null || typeof value !== 'object') return true
  if (Array.isArray(value)) return value.length <= 50 && value.every((item) => validBoundedValue(item, depth + 1))
  const entries = Object.entries(value as Record<string, unknown>)
  return entries.length <= 50 && entries.every(([k, v]) => !FORBIDDEN_KEYS.has(k) && validBoundedValue(v, depth + 1))
}

function safeStaticValue(value: unknown): boolean {
  if (value === undefined) return true
  if (!validBoundedValue(value)) return false
  try {
    return (JSON.stringify(value)?.length ?? 0) <= MAX_VALUE_JSON
  } catch {
    return false
  }
}

function read(raw: unknown, path: string): unknown {
  const parts = pathParts(path)
  if (parts.length > MAX_PATH_SEGMENTS) return MISSING
  let value = raw
  for (const part of parts) {
    if (!value || typeof value !== 'object' || !Object.hasOwn(value, part)) return MISSING
    value = (value as Record<string, unknown>)[part]
  }
  return value
}

function checkedCondition(value: unknown, depth = 0, budget = { count: 0 }): Condition | null {
  if (depth > 12 || ++budget.count > MAX_COND_NODES || !isRecord(value)) return null
  const variants = ['all', 'any', 'not', 'path'].filter((key) => Object.hasOwn(value, key))
  if (variants.length !== 1) return null
  const kind = variants[0]
  if (kind === 'not') {
    const child = checkedCondition(value.not, depth + 1, budget)
    return child ? { not: child } : null
  }
  if (kind === 'all' || kind === 'any') {
    const items = value[kind]
    if (!Array.isArray(items) || !items.length || items.length > 50) return null
    const children: Condition[] = []
    for (const item of items) {
      const child = checkedCondition(item, depth + 1, budget)
      if (!child) return null
      children.push(child)
    }
    return kind === 'all' ? { all: children } : { any: children }
  }
  if (!validPath(value.path)) return null
  const op = COND_OPS.find((candidate) => candidate === value.op)
  if (!op) return null
  const scalar = (v: unknown): v is Scalar => v === null || ['string', 'boolean', 'number'].includes(typeof v)
  const argument = value.value
  const valid = op === 'exists' || op === 'not_exists' || scalar(argument) ||
    (Array.isArray(argument) && argument.length <= 100 && argument.every(scalar))
  if (!valid) return null
  const leaf = {
    path: value.path, op,
    ...(scalar(argument) || (Array.isArray(argument) && argument.every(scalar)) ? { value: argument } : {}),
  }
  const issues = validateRule({ id: 'schema-detector', team: 'schema', when: { ...leaf, path: 'raw.value' }, route: [{ to: 'team:schema' }], present: { title: 'schema' } })
  return issues.length ? null : leaf
}

export function parseMapping(input: unknown): { spec: SchemaSpec } | { error: string } {
  const forbidden = hasForbiddenKey(input)
  if (forbidden) return { error: `Forbidden key "${forbidden}" in schema mapping` }
  if (!isRecord(input) || typeof input.schema !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$/.test(input.schema))
    return { error: 'Enter a schema name using letters, numbers, dots, dashes, or underscores' }
  const priority = input.priority ?? 0
  if (typeof priority !== 'number' || !Number.isInteger(priority) || priority < -2147483648 || priority > 2147483647)
    return { error: 'Priority must be a 32-bit integer' }
  const detector = checkedCondition(input.detect)
  if (!detector || !isRecord(input.detect)) return { error: 'Add a valid detect condition with payload-relative paths' }
  const source = input.detect.source
  if (source !== undefined && (typeof source !== 'string' || source.length > 64 || !/^[a-z0-9_-]+:[a-z0-9._-]+$/i.test(source)))
    return { error: 'Detect source must identify a source, such as http:playground' }
  const fields = input.fields ?? {}
  if (!isRecord(fields) || Object.keys(fields).length > 50) return { error: 'Fields must contain up to 50 named mappings' }
  const mapped: [string, SchemaField][] = []
  for (const [name, value] of Object.entries(fields)) {
    if (FORBIDDEN_KEYS.has(name) || !/^[a-zA-Z_][\w-]{0,63}$/.test(name) || !isRecord(value))
      return { error: `Invalid field: ${name}` }
    const { path, transform, enum: kind, as, equals } = value
    if ((path === undefined && !Object.hasOwn(value, 'value')) ||
        (path !== undefined && !validPath(path)) ||
        !safeStaticValue(value.value) ||
        !safeStaticValue(equals) ||
        (transform !== undefined && transform !== 'lower' && transform !== 'upper' && transform !== 'trim') ||
        (kind !== undefined && kind !== 'severity') ||
        (as !== undefined && as !== 'string' && as !== 'number' && as !== 'bool' && as !== 'rfc3339' && as !== 'epoch_ms' && as !== 'epoch_s'))
      return { error: `Field "${name}" needs a valid path or value and a supported conversion` }
    mapped.push([name, { path, value: value.value, transform, enum: kind, as, equals }])
  }
  return { spec: { schema: input.schema, priority, detect: source ? { ...detector, source } : detector, fields: Object.fromEntries(mapped) } }
}

function matches(condition: Condition, raw: unknown, source: string): boolean {
  if ('all' in condition) return condition.all.every((child) => matches(child, raw, source))
  if ('any' in condition) return condition.any.some((child) => matches(child, raw, source))
  if ('not' in condition) return !matches(condition.not, raw, source)
  const actual = read(raw, condition.path)
  return evaluate({ ...condition, path: 'raw.value' },
    { raw: actual === MISSING ? {} : { value: actual }, source, fields: {}, schema: '' })
}

function detected(spec: SchemaSpec, event: unknown, source: string) {
  return (!spec.detect.source || spec.detect.source === source) && matches(spec.detect, event, source)
}

function convert(value: unknown, field: SchemaField): unknown {
  if (field.equals !== undefined) return isDeepStrictEqual(value, field.equals)
  if (field.transform) {
    if (typeof value !== 'string') return undefined
    value = field.transform === 'lower' ? value.toLowerCase() : field.transform === 'upper' ? value.toUpperCase() : value.trim()
  }
  if (field.enum === 'severity') {
    if (typeof value !== 'string') return undefined
    const lower = value.toLowerCase()
    const aliases: Record<string, string> = { warn: 'warning', fatal: 'critical', crit: 'critical', err: 'error', informational: 'info' }
    const severity = aliases[lower] ?? lower
    return Object.hasOwn(severityRank, severity) ? severity : undefined
  }
  switch (field.as) {
    case 'string': {
      const str = typeof value === 'string' ? value : value === undefined ? undefined : JSON.stringify(value)
      return str === undefined ? undefined : str.slice(0, MAX_CONVERTED_STR)
    }
    case 'number': {
      const numeric = typeof value === 'string' && value.trim() ? Number(value) : value
      return typeof numeric === 'number' && Number.isFinite(numeric) ? numeric : undefined
    }
    case 'bool': return value === true || value === 'true' ? true : value === false || value === 'false' ? false : undefined
    case 'rfc3339': return typeof value === 'string' ? isoTime(Date.parse(value)) : undefined
    case 'epoch_ms': return typeof value === 'number' ? isoTime(value) : undefined
    case 'epoch_s': return typeof value === 'number' ? isoTime(value * 1000) : undefined
    default: return value
  }
}

export function previewMapping(spec: SchemaSpec, event: unknown, source: string): SchemaPreview {
  const mapped: [string, unknown][] = []
  const warnings: string[] = []
  for (const [name, field] of Object.entries(spec.fields)) {
    const sensitiveSource = field.path !== undefined && isSensitivePath(field.path)
    const extracted = field.path === undefined ? MISSING : read(event, field.path)
    if (sensitiveSource && extracted !== MISSING) {
      mapped.push([name, '[REDACTED]'])
      warnings.push(`${name}: sensitive source path redacted`)
      continue
    }
    const raw = extracted === MISSING ? field.value : extracted
    const value = raw === undefined ? undefined : convert(raw, field)
    if (value === undefined) warnings.push(`${name}: missing value or failed conversion`)
    else mapped.push([name, redactValue(value)])
  }
  const fields = Object.fromEntries(mapped)
  if (typeof fields.severity === 'string' && Object.hasOwn(severityRank, fields.severity))
    fields.severity_rank = severityRank[fields.severity]
  return { matched: detected(spec, event, source), fields, warnings }
}

export function mappedContext(event: unknown, source: string, mappings: Iterable<SchemaSpec>): Ctx {
  const candidates = [...mappings].filter((spec) => detected(spec, event, source)).map((spec) => ({
    schema: spec.schema, scoped: Boolean(spec.detect.source), priority: spec.priority,
    fields: () => previewMapping(spec, event, source).fields,
  }))
  let fallback: { schema: string; fields: Record<string, unknown> } | undefined
  for (const builtIn of builtInMappings(event)) {
    if (builtIn.fallback) {
      fallback = builtIn
    } else {
      candidates.push({
        schema: builtIn.schema, scoped: builtIn.preferredSource === source, priority: builtIn.priority,
        fields: () => builtIn.fields,
      })
    }
  }
  candidates.sort((a, b) => Number(b.scoped) - Number(a.scoped) || b.priority - a.priority)
  const first = candidates[0]
  const second = candidates.find((candidate) => candidate.schema !== first?.schema)
  if (!first && fallback) return { raw: event, source, schema: fallback.schema, fields: fallback.fields }
  if (first && !(second && first.scoped === second.scoped && first.priority === second.priority))
    return { raw: event, source, schema: first.schema, fields: first.fields() }
  return { raw: event, source, schema: unknownSignature(event), fields: {} }
}

// ---- Suggested mappings (BEACON_LLM=mock equivalent) ----------------------

/** Canonical fields a suggestion may fill, with the leaf names a name-based heuristic accepts. */
const SUGGEST_NAMES: [field: string, names: string[], extra?: Partial<SchemaField>][] = [
  ['type', ['type', 'event_type', 'eventtype', 'kind', 'alertname', 'name', 'event']],
  ['severity', ['severity', 'level', 'priority', 'sev'], { enum: 'severity' }],
  ['service', ['service', 'app', 'application', 'component', 'job']],
  ['environment', ['environment', 'env', 'stage']],
  ['occurred_at', ['timestamp', 'time', 'occurred_at', 'ts', 'created_at', 'eventtime']],
  ['message', ['message', 'msg', 'summary', 'description', 'text']],
]

function leafPaths(value: unknown, prefix = '', depth = 0, out: [string, unknown][] = []): [string, unknown][] {
  if (!isRecord(value) || depth > 6 || out.length >= 200) return out
  for (const [key, child] of Object.entries(value)) {
    if (FORBIDDEN_KEYS.has(key) || !/^[A-Za-z_][\w-]*$/.test(key)) continue
    const path = prefix ? `${prefix}.${key}` : key
    if (isSensitivePath(path)) continue
    if (isRecord(child)) leafPaths(child, path, depth + 1, out)
    else out.push([path, child])
  }
  return out
}

/**
 * A labelled, name-based mapping suggestion: no network, no model. Like the
 * API's mock provider it only picks paths that exist in the sample.
 */
export function suggestMapping(event: unknown, source: string): { spec: SchemaSpec; notes: string } | null {
  const leaves = leafPaths(event)
  if (!leaves.length) return null
  const fields: Record<string, SchemaField> = {}
  const used: string[] = []
  for (const [field, names, extra] of SUGGEST_NAMES) {
    const hit = leaves.find(([path, value]) =>
      !used.includes(path) && names.includes(path.split('.').pop()!.toLowerCase()) && (typeof value === 'string' || typeof value === 'number'))
    if (!hit) continue
    used.push(hit[0])
    const [path, value] = hit
    fields[field] = { path, ...extra, ...(field === 'occurred_at' ? { as: typeof value === 'number' ? (value > 1e11 ? 'epoch_ms' : 'epoch_s') : 'rfc3339' } : {}) }
  }
  const detectPaths = (used.length ? used : [leaves[0]![0]]).slice(0, 2)
  const name = detectPaths[0]!.replace(/[^A-Za-z0-9]+/g, '-').replace(/^-|-$/g, '').toLowerCase() || 'event'
  if (!fields.type) fields.type = { value: 'Custom event' }
  return {
    spec: {
      schema: `suggested.${name}`.slice(0, 80),
      priority: 50,
      detect: { all: detectPaths.map((path) => ({ path, op: 'exists' as const })), source },
      fields,
    },
    notes: `Matched ${used.length} field${used.length === 1 ? '' : 's'} by name. Check each path before saving.`,
  }
}
