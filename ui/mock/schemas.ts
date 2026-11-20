import { isDeepStrictEqual } from 'node:util'
import type { SchemaField, SchemaPreview, SchemaSpec } from '../src/api/screens.ts'
import { COND_OPS, type Condition, type Scalar } from '../src/api/types.ts'
import { context, evaluate, MISSING, unknownSignature, validateRule, type Ctx } from './engine.ts'

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)
const pathPattern = /^(?:\/[\S]*|[^.\s]+(?:\.[^.\s]+)*)$/
const unsafeKeys = new Set(['__proto__', 'constructor', 'prototype'])
const severityRank: Record<string, number> = { info: 1, warning: 2, error: 3, critical: 4 }
const isoTime = (value: number) => Number.isFinite(value) && Math.abs(value) <= 8.64e15 ? new Date(value).toISOString() : undefined

function read(raw: unknown, path: string): unknown {
  const parts = path.startsWith('/')
    ? path.slice(1).split('/').map((part) => part.replaceAll('~1', '/').replaceAll('~0', '~'))
    : path.split('.')
  let value = raw
  for (const part of parts) {
    if (!value || typeof value !== 'object' || !Object.hasOwn(value, part)) return MISSING
    value = (value as Record<string, unknown>)[part]
  }
  return value
}

function checkedCondition(value: unknown, depth = 0): Condition | null {
  if (depth > 12 || !isRecord(value)) return null
  const variants = ['all', 'any', 'not', 'path'].filter((key) => Object.hasOwn(value, key))
  if (variants.length !== 1) return null
  const kind = variants[0]
  if (kind === 'not') {
    const child = checkedCondition(value.not, depth + 1)
    return child ? { not: child } : null
  }
  if (kind === 'all' || kind === 'any') {
    const items = value[kind]
    if (!Array.isArray(items) || !items.length || items.length > 50) return null
    const children: Condition[] = []
    for (const item of items) {
      const child = checkedCondition(item, depth + 1)
      if (!child) return null
      children.push(child)
    }
    return kind === 'all' ? { all: children } : { any: children }
  }
  if (typeof value.path !== 'string' || !pathPattern.test(value.path)) return null
  const op = COND_OPS.find((candidate) => candidate === value.op)
  if (!op) return null
  const scalar = (v: unknown): v is Scalar => v === null || ['string', 'boolean', 'number'].includes(typeof v)
  const argument = value.value
  const valid = op === 'exists' || op === 'not_exists' || scalar(argument) || (Array.isArray(argument) && argument.every(scalar))
  if (!valid) return null
  const leaf = {
    path: value.path, op,
    ...(scalar(argument) || (Array.isArray(argument) && argument.every(scalar)) ? { value: argument } : {}),
  }
  const issues = validateRule({ id: 'schema-detector', team: 'schema', when: { ...leaf, path: 'raw.value' }, route: [{ to: 'team:schema' }], present: { title: 'schema' } })
  return issues.length ? null : leaf
}

export function parseMapping(input: unknown): { spec: SchemaSpec } | { error: string } {
  if (!isRecord(input) || typeof input.schema !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$/.test(input.schema))
    return { error: 'Enter a schema name using letters, numbers, dots, dashes, or underscores' }
  const priority = input.priority ?? 0
  if (typeof priority !== 'number' || !Number.isInteger(priority) || priority < -2147483648 || priority > 2147483647)
    return { error: 'Priority must be a 32-bit integer' }
  const detector = checkedCondition(input.detect)
  if (!detector || !isRecord(input.detect)) return { error: 'Add a valid detect condition with payload-relative paths' }
  const source = input.detect.source
  if (source !== undefined && (typeof source !== 'string' || !/^[a-z0-9_-]+:[a-z0-9._-]+$/i.test(source)))
    return { error: 'Detect source must identify a source, such as http:playground' }
  const fields = input.fields ?? {}
  if (!isRecord(fields) || Object.keys(fields).length > 50) return { error: 'Fields must contain up to 50 named mappings' }
  const mapped: [string, SchemaField][] = []
  for (const [name, value] of Object.entries(fields)) {
    if (unsafeKeys.has(name) || !/^[a-zA-Z_][\w-]*$/.test(name) || !isRecord(value))
      return { error: `Invalid field: ${name}` }
    const { path, transform, enum: kind, as, equals } = value
    if ((path === undefined && !Object.hasOwn(value, 'value')) ||
        (path !== undefined && (typeof path !== 'string' || !pathPattern.test(path))) ||
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
    case 'string': return typeof value === 'string' ? value : value === undefined ? undefined : JSON.stringify(value)
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
    const extracted = field.path === undefined ? MISSING : read(event, field.path)
    const raw = extracted === MISSING ? field.value : extracted
    const value = raw === undefined ? undefined : convert(raw, field)
    if (value === undefined) warnings.push(`${name}: missing value or failed conversion`)
    else mapped.push([name, value])
  }
  const fields = Object.fromEntries(mapped)
  if (typeof fields.severity === 'string' && Object.hasOwn(severityRank, fields.severity))
    fields.severity_rank = severityRank[fields.severity]
  return { matched: detected(spec, event, source), fields, warnings }
}

const builtInPriority: Record<string, number> = {
  'workflow.failed': 50, 'prometheus.alert': 50, 'log.line': 30, 'ci.deployment': 30,
}

export function mappedContext(event: unknown, source: string, mappings: Iterable<SchemaSpec>): Ctx {
  const baseline = context(event, source)
  const candidates = [...mappings].filter((spec) => detected(spec, event, source)).map((spec) => ({
    schema: spec.schema, scoped: Boolean(spec.detect.source), priority: spec.priority,
    fields: () => previewMapping(spec, event, source).fields,
  }))
  if (!baseline.schema.startsWith('unknown:') && !candidates.some((item) => item.schema === baseline.schema)) {
    candidates.push({
      schema: baseline.schema,
      scoped: source === 'http:alertmanager' && baseline.schema === 'prometheus.alert',
      priority: builtInPriority[baseline.schema] ?? 40,
      fields: () => baseline.fields,
    })
  }
  candidates.sort((a, b) => Number(b.scoped) - Number(a.scoped) || b.priority - a.priority)
  const [first, second] = candidates
  if (!first) return baseline
  if (second && first.scoped === second.scoped && first.priority === second.priority)
    return { ...baseline, schema: unknownSignature(event), fields: {} }
  return { ...baseline, schema: first.schema, fields: first.fields() }
}
