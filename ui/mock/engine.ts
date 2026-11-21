// Toy stand-in for the Rust engine, used by the dev mock only: schema
// detection + normalized fields, the typed condition tree (spec §4.3 semantics),
// template rendering and rule validation. The real engine is authoritative;
// this exists so the rule editor and test panel behave plausibly offline.

import type {
  AlertField,
  ColorToken,
  Condition,
  Leaf,
  NodeDiagnostic,
  RuleSpec,
  RuleTestResult,
  Scalar,
  Severity,
} from '../src/api/types.ts'
import { redactText, redactValue } from '../src/lib/redact.ts'
import { parseRuleStructure } from '../src/rules/structure.ts'

const SEVERITIES: readonly Severity[] = ['critical', 'error', 'warning', 'info']
const COLORS: readonly ColorToken[] = ['red', 'orange', 'amber', 'yellow', 'green', 'blue', 'purple', 'grey']
const CHANNELS = ['phone', 'sms', 'email', 'ui']

export const MISSING: unique symbol = Symbol('missing')
type Value = unknown | typeof MISSING

export interface Ctx {
  raw: unknown
  fields: Record<string, unknown>
  source: string
  schema: string
}

// ---- schema detection (stands in for config/schemas/*.yaml) ----------------

const isObj = (v: unknown): v is Record<string, any> => typeof v === 'object' && v !== null && !Array.isArray(v)

function sevOf(v: unknown): Severity | undefined {
  const s = typeof v === 'string' ? v.toLowerCase() : ''
  if (['critical', 'crit', 'fatal', 'page'].includes(s)) return 'critical'
  if (['error', 'err'].includes(s)) return 'error'
  if (['warning', 'warn'].includes(s)) return 'warning'
  if (['info', 'notice', 'debug'].includes(s)) return 'info'
  return undefined
}

const LOG_RANK: Record<string, number> = { DEBUG: 0, INFO: 1, WARN: 2, WARNING: 2, ERROR: 3, FATAL: 4 }

function compact(o: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined))
}

const MAX_SIG_DEPTH = 16
const MAX_SIG_PATHS = 500

/** Structural signature for unknown shapes: sorted paths, values ignored. */
export function unknownSignature(raw: unknown): string {
  const paths: string[] = []
  const walk = (v: unknown, p: string, depth: number) => {
    if (depth > MAX_SIG_DEPTH || paths.length >= MAX_SIG_PATHS) return
    if (Array.isArray(v)) v.slice(0, 1).forEach((x) => walk(x, `${p}[]`, depth + 1))
    else if (isObj(v)) for (const k of Object.keys(v)) walk(v[k], p ? `${p}.${k}` : k, depth + 1)
    else paths.push(p)
  }
  walk(raw, '', 0)
  const h = [...paths.sort().join('|')].reduce((h, c) => (h * 33 + c.charCodeAt(0)) >>> 0, 5381)
  return `unknown:${h.toString(16).padStart(8, '0')}`
}

interface BuiltInMapping {
  schema: string
  fields: Record<string, unknown>
  priority: number
  preferredSource?: string
  fallback?: true
}

export function* builtInMappings(raw: unknown): Generator<BuiltInMapping, void> {
  if (!isObj(raw)) return
  const r = raw
  if (typeof r.type === 'string' && 'workflowId' in r) {
    yield {
      schema: 'workflow.failed', priority: 50,
      fields: compact({ type: r.type, severity: sevOf(r.severity), environment: r.environment, workflow: r.workflowName, occurred_at: r.timestamp }),
    }
  }
  if (typeof r.alertname === 'string' && ['firing', 'resolved'].includes(r.status)) {
    yield {
      schema: 'prometheus.alert', priority: 50, preferredSource: 'http:alertmanager',
      fields: compact({
        type: r.alertname,
        severity: sevOf(r.labels?.severity),
        service: r.labels?.service,
        region: r.labels?.region,
        resolved: r.status === 'resolved',
        occurred_at: r.startsAt,
      }),
    }
  }
  if (isObj(r.meta) && typeof r.meta.schema === 'string' && 'payload' in r) {
    yield { schema: r.meta.schema, priority: 0, fallback: true, fields: compact({ type: r.meta.schema, service: r.meta.producer }) }
  }
  if (typeof r.level === 'string' && typeof r.msg === 'string') {
    yield {
      schema: 'log.line', priority: 30,
      fields: compact({
        service: r.service,
        severity: sevOf(r.level),
        severity_rank: LOG_RANK[r.level.toUpperCase()],
        instrument: r.instrument,
        instance: r.instance,
      }),
    }
  }
  if (r.kind === 'deployment') {
    yield { schema: 'ci.deployment', priority: 30, fields: compact({ type: 'deployment', service: r.service, severity: 'info', region: r.region }) }
  }
}

export function context(raw: unknown, source: string): Ctx {
  const first = builtInMappings(raw).next().value
  return { raw, source, schema: first?.schema ?? unknownSignature(raw), fields: first?.fields ?? {} }
}

// ---- paths -------------------------------------------------------------------

export function getPath(ctx: Ctx, path: string): Value {
  const segs = path.split('.')
  const head = segs.shift()
  let v: unknown
  if (head === 'raw') v = ctx.raw
  else if (head === 'fields') v = ctx.fields
  else if (head === 'source' && segs.length === 0) return ctx.source
  else if (head === 'schema' && segs.length === 0) return ctx.schema
  else return MISSING
  for (const s of segs) {
    if (Array.isArray(v) && /^\d+$/.test(s)) v = v[Number(s)]
    else if (isObj(v) && Object.hasOwn(v, s)) v = v[s]
    else return MISSING
    if (v === undefined) return MISSING
  }
  return v
}

/** Bare names in runbook templates (`{{workflowId}}`) look in raw, then fields. */
function lookup(ctx: Ctx, path: string): Value {
  if (/^(raw|fields)(\.|$)|^(source|schema)$/.test(path)) return getPath(ctx, path)
  const r = getPath(ctx, `raw.${path}`)
  return r !== MISSING ? r : getPath(ctx, `fields.${path}`)
}

// ---- conditions ----------------------------------------------------------------

export const isLeaf = (c: Condition): c is Leaf => typeof (c as Leaf).op === 'string'

const MAX_REGEX_LEN = 256
const MAX_REGEX_INPUT = 4096
const NESTED_QUANTIFIER_RE = /\((?:[^()\\]|\\.)*[+*?](?:[^()\\]|\\.)*\)\s*(?:[+*?]|\{\d+,?\d*\})/
const QUANTIFIED_ALT_RE = /\((?:[^()\\]|\\.)*\|(?:[^()\\]|\\.)*\)\s*(?:[+*?]|\{\d+,?\d*\})/
const BACKREF_RE = /\\[1-9]/

/** Compile a regex with basic ReDoS guards (production engine uses linear-time RE2/Rust regex). */
export function compileSafeRegex(pattern: string): { ok: true; re: RegExp } | { ok: false; error: string } {
  if (pattern.length > MAX_REGEX_LEN) return { ok: false, error: `regex too long (max ${MAX_REGEX_LEN} chars)` }
  if (BACKREF_RE.test(pattern)) return { ok: false, error: 'backreferences are not allowed in regex' }
  if (NESTED_QUANTIFIER_RE.test(pattern)) return { ok: false, error: 'nested quantifiers are not allowed (ReDoS risk)' }
  if (QUANTIFIED_ALT_RE.test(pattern)) return { ok: false, error: 'quantified alternation is not allowed (ReDoS risk)' }
  try {
    return { ok: true, re: new RegExp(pattern) }
  } catch (e) {
    return { ok: false, error: (e as Error).message }
  }
}

function cmp(a: unknown, b: unknown): number | null {
  if (typeof a === 'number' && typeof b === 'number') return a - b
  if (typeof a === 'string' && typeof b === 'string') return a < b ? -1 : a > b ? 1 : 0
  return null
}

function evalLeaf(l: Leaf, actual: Value): { ok: boolean; error?: string } {
  if (l.op === 'exists') return { ok: actual !== MISSING }
  if (l.op === 'not_exists') return { ok: actual === MISSING }
  if (actual === MISSING) return { ok: false }
  const v = l.value
  switch (l.op) {
    case 'eq':
      return { ok: actual === v }
    case 'neq':
      return { ok: typeof actual === typeof v && actual !== v }
    case 'gt':
    case 'gte':
    case 'lt':
    case 'lte': {
      const c = cmp(actual, v)
      if (c === null) return { ok: false }
      return { ok: l.op === 'gt' ? c > 0 : l.op === 'gte' ? c >= 0 : l.op === 'lt' ? c < 0 : c <= 0 }
    }
    case 'in':
    case 'not_in': {
      const list = Array.isArray(v) ? v : [v]
      const isScalar = actual === null || ['string', 'number', 'boolean'].includes(typeof actual)
      if (!isScalar) return { ok: false }
      const hit = list.includes(actual as Scalar)
      return { ok: l.op === 'in' ? hit : !hit }
    }
    case 'contains':
      if (typeof actual === 'string' && typeof v === 'string') return { ok: actual.includes(v) }
      if (Array.isArray(actual)) return { ok: actual.includes(v) }
      return { ok: false }
    case 'starts_with':
      return { ok: typeof actual === 'string' && typeof v === 'string' && actual.startsWith(v) }
    case 'regex': {
      if (typeof actual !== 'string') return { ok: false }
      const compiled = compileSafeRegex(String(v))
      if (!compiled.ok) return { ok: false, error: `bad regex: ${compiled.error}` }
      return { ok: compiled.re.test(actual.slice(0, MAX_REGEX_INPUT)) }
    }
    default:
      return { ok: false, error: `unknown operator ${String(l.op)}` }
  }
}

function children(c: Condition): Condition[] {
  if ('all' in c) return c.all
  if ('any' in c) return c.any
  if ('not' in c) return [c.not]
  return []
}

const childNode = (node: string, i: number) => (node === '' ? String(i) : `${node}.${i}`)

function skip(c: Condition, node: string, out: NodeDiagnostic[]) {
  out.push({ node, result: 'skipped' })
  children(c).forEach((ch, i) => skip(ch, childNode(node, i), out))
}

export function evaluate(c: Condition, ctx: Ctx, node = '', out: NodeDiagnostic[] = []): boolean {
  if (isLeaf(c)) {
    const actual = getPath(ctx, c.path)
    const r = evalLeaf(c, actual)
    out.push({
      node,
      result: r.error ? 'error' : r.ok ? 'match' : 'no_match',
      ...(actual === MISSING ? {} : { actual }),
      ...(r.error ? { message: r.error } : actual === MISSING && !['exists', 'not_exists'].includes(c.op) ? { message: `${c.path} is missing` } : {}),
    })
    return r.ok
  }
  const self: NodeDiagnostic = { node, result: 'no_match' }
  out.push(self)
  let ok: boolean
  if ('not' in c) {
    ok = !evaluate(c.not, ctx, childNode(node, 0), out)
  } else {
    const all = 'all' in c
    const list = all ? c.all : (c as { any: Condition[] }).any
    ok = all
    let decided = false
    list.forEach((ch, i) => {
      if (decided) return skip(ch, childNode(node, i), out)
      const r = evaluate(ch, ctx, childNode(node, i), out)
      if (all && !r) (ok = false), (decided = true)
      if (!all && r) (ok = true), (decided = true)
    })
  }
  self.result = ok ? 'match' : 'no_match'
  return ok
}

// ---- templates --------------------------------------------------------------------

const TAG = /\{\{\s*([^{}|]+?)\s*((?:\|\s*[a-z_]+(?:\([^)]*\))?\s*)*)\}\}/g

function show(v: unknown): string {
  if (typeof v === 'string') return v
  if (v === null) return 'null'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

function applyFilter(v: unknown, f: string): unknown {
  const m = f.match(/^([a-z_]+)(?:\((.*)\))?$/)
  const [name, arg] = [m?.[1], m?.[2]]
  switch (name) {
    case 'money':
      return typeof v === 'number' ? v.toLocaleString('en-US') : v
    case 'upper':
      return v === MISSING ? MISSING : show(v).toUpperCase()
    case 'lower':
      return v === MISSING ? MISSING : show(v).toLowerCase()
    case 'round':
      return typeof v === 'number' ? Math.round(v) : v
    case 'default':
      return v === MISSING || v === '' ? (arg ?? '').replace(/^["']|["']$/g, '') : v
    default:
      throw new Error(`unknown filter "${name}"`)
  }
}

export function templateErrors(t: string): string[] {
  const stripped = t.replace(TAG, '')
  return stripped.includes('{{') || stripped.includes('}}') ? ['unbalanced or malformed {{ … }}'] : []
}

export function render(t: string, ctx: Ctx, warnings: string[] = [], { requireScalar = false } = {}): string {
  return t.replace(TAG, (_all, path: string, filters: string) => {
    const name = path.trim()
    let v: unknown = lookup(ctx, name)
    if (requireScalar && (v === null || typeof v === 'object')) {
      warnings.push(`${name} must be a non-null scalar`)
      return ''
    }
    const fs = filters
      .split('|')
      .map((s) => s.trim())
      .filter(Boolean)
    try {
      for (const f of fs) v = applyFilter(v, f)
    } catch (e) {
      warnings.push((e as Error).message)
    }
    if (v === MISSING) {
      warnings.push(`${name} is missing in this event`)
      return ''
    }
    return show(v)
  })
}

// ---- rules ----------------------------------------------------------------------------

const humanize = (path: string) => {
  const last = path.split('.').pop() ?? path
  return last.replace(/([a-z])([A-Z])/g, '$1 $2').replace(/[_-]/g, ' ').replace(/^./, (c) => c.toUpperCase())
}

export function fieldSpecs(rule: RuleSpec): { path: string; label: string }[] {
  return (rule.present?.fields ?? []).map((f) => (typeof f === 'string' ? { path: f, label: humanize(f) } : f))
}

const DURATION = /^\d+(ms|s|m|h|d)$/
const PATH = /^(raw(\.[^.\s]+)*|fields\.[^.\s]+(\.[^.\s]+)*|source|schema)$/

function validateCond(c: Condition, at: string, out: { path: string; message: string }[]) {
  if ('all' in c || 'any' in c) {
    const kind = 'all' in c ? 'all' : 'any'
    const children = 'all' in c ? c.all : c.any
    if (!children.length) out.push({ path: `${at}.${kind}`, message: 'needs at least one condition' })
    children.forEach((child, i) => validateCond(child, `${at}.${kind}.${i}`, out))
    return
  }
  if ('not' in c) return validateCond(c.not, `${at}.not`, out)
  if (!PATH.test(c.path))
    out.push({ path: `${at}.path`, message: 'path must start with raw., fields., or be source/schema' })
  if (c.op === 'exists' || c.op === 'not_exists') return
  if (c.value === undefined) return out.push({ path: `${at}.value`, message: 'value is required' })
  if (['in', 'not_in'].includes(c.op) && (!Array.isArray(c.value) || c.value.length === 0))
    out.push({ path: `${at}.value`, message: 'needs a non-empty list' })
  if (['gt', 'gte', 'lt', 'lte'].includes(c.op) && typeof c.value !== 'number' && typeof c.value !== 'string')
    out.push({ path: `${at}.value`, message: 'needs a number (or a string to compare as text)' })
  if (c.op === 'regex') {
    const compiled = compileSafeRegex(String(c.value))
    if (!compiled.ok) out.push({ path: `${at}.value`, message: `invalid regex: ${compiled.error}` })
  }
}

const FORBIDDEN_RULE_KEYS = new Set(['__proto__', 'constructor', 'prototype'])

function checkForbiddenKeys(v: unknown, at: string, out: { path: string; message: string }[], depth = 0) {
  if (depth > 16 || v === null || typeof v !== 'object') return
  if (Array.isArray(v)) {
    v.forEach((x, i) => checkForbiddenKeys(x, `${at}.${i}`, out, depth + 1))
    return
  }
  for (const k of Object.keys(v as Record<string, unknown>)) {
    const next = at ? `${at}.${k}` : k
    if (FORBIDDEN_RULE_KEYS.has(k)) out.push({ path: next, message: `forbidden key "${k}"` })
    else checkForbiddenKeys((v as Record<string, unknown>)[k], next, out, depth + 1)
  }
}

export function validateRule(rule: unknown): { path: string; message: string }[] {
  const parsed = parseRuleStructure(rule)
  if ('problem' in parsed) return [parsed.problem]
  return validateRuleSemantics(parsed.rule)
}

function validateRuleSemantics(r: RuleSpec): { path: string; message: string }[] {
  const out: { path: string; message: string }[] = []
  checkForbiddenKeys(r, '', out)
  if (!/^[a-z0-9][a-z0-9-]{1,62}$/.test(r.id))
    out.push({ path: 'id', message: 'lower-case letters, digits and dashes, 2–63 characters' })
  if (!r.team) out.push({ path: 'team', message: 'team is required' })
  validateCond(r.when, 'when', out)
  if (r.route.length === 0) out.push({ path: 'route', message: 'add at least one recipient' })
  else
    r.route.forEach((rt, i) => {
      if (!/^(team|user|oncall):[\w-]+$/.test(rt.to))
        out.push({ path: `route.${i}.to`, message: 'team:<id>, user:<id> or oncall:<schedule>' })
      for (const ch of rt.channels ?? []) if (!CHANNELS.includes(ch)) out.push({ path: `route.${i}.channels`, message: `unknown channel "${ch}"` })
    })
  if (!r.present.title.trim())
    out.push({ path: 'present.title', message: 'a title template is required' })
  else {
    for (const m of templateErrors(r.present.title)) out.push({ path: 'present.title', message: m })
    for (const m of templateErrors(r.present.body ?? '')) out.push({ path: 'present.body', message: m })
    ;(r.present.fields ?? []).forEach((f, i) => {
      const p = typeof f === 'string' ? f : f.path
      if (!PATH.test(p)) out.push({ path: `present.fields.${i}`, message: 'invalid path' })
    })
  }
  if (r.dedup) {
    if (!DURATION.test(String(r.dedup.window))) out.push({ path: 'dedup.window', message: 'a duration like 10m, 1h' })
    ;(r.dedup.key ?? []).forEach((p, i) => {
      if (!PATH.test(p)) out.push({ path: `dedup.key.${i}`, message: 'invalid path' })
    })
  }
  if (r.throttle && (!(r.throttle.max > 0) || !DURATION.test(String(r.throttle.per))))
    out.push({ path: 'throttle', message: 'max > 0 and per like 1m' })
  if (r.escalate_after !== undefined && !DURATION.test(String(r.escalate_after)))
    out.push({ path: 'escalate_after', message: 'a duration like 10m' })
  return out
}

export interface Applied {
  severity: Severity
  color: ColorToken
  title: string
  body: string
  fields: AlertField[]
  dedup_key: string[]
}

function present(rule: RuleSpec, ctx: Ctx, warnings: string[]): Applied {
  const safeCtx: Ctx = {
    ...ctx,
    raw: redactValue(ctx.raw),
    fields: redactValue(ctx.fields) as Record<string, unknown>,
  }
  const title = redactText(render(rule.present.title, safeCtx, warnings), 300)
  const body = redactText(render(rule.present.body ?? '', safeCtx, warnings), 2000)
  const fields = fieldSpecs(rule).map(({ path, label }) => {
    const v = getPath(safeCtx, path)
    if (v === MISSING) warnings.push(`field "${label}": ${path} is missing in this event`)
    return { path, label, value: v === MISSING ? '—' : redactText(show(v), 500) }
  })
  const dedup_key = (rule.dedup?.key ?? []).map((p) => {
    const v = getPath(safeCtx, p)
    if (v === MISSING) warnings.push(`dedup key ${p} is missing: such events would not merge into one alert`)
    return v === MISSING ? '∅' : redactText(show(v), 200)
  })
  const fsev = ctx.fields.severity as Severity
  const severity = rule.severity ?? (SEVERITIES.includes(fsev) ? fsev : 'warning')
  const color = rule.present.color && COLORS.includes(rule.present.color) ? rule.present.color : 'grey'
  return { severity, color, title, body, fields, dedup_key }
}

/** Dedup identity only (no rendering): used to route recovery signals. */
export function dedupKey(rule: RuleSpec, ctx: Ctx): string | null {
  const key = rule.dedup?.key ?? []
  if (!key.length) return null
  const vals = key.map((p) => getPath(ctx, p))
  return vals.includes(MISSING) ? null : vals.map(show).join('|')
}

export function parseDuration(d: string | number | undefined): number | null {
  if (typeof d === 'number') return d * 1000
  const m = String(d ?? '').match(/^(\d+)(ms|s|m|h|d)$/)
  if (!m) return null
  return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 }[m[2] as 'ms']!
}

/** Match + render for the mock's ingest path. */
export function applyRule(rule: RuleSpec, ctx: Ctx): Applied | null {
  if (rule.enabled === false) return null
  if (!evaluate(rule.when, ctx)) return null
  return present(rule, ctx, [])
}

export function recovers(rule: RuleSpec, ctx: Ctx): boolean {
  const c = rule.auto_resolve?.recover_when
  return !!c && evaluate(c, ctx)
}

/** `POST /api/rules/test`: zero writes, zero dispatches. */
export function testRule(rule: unknown, event: unknown, source = 'http:test', ctx = context(event, source)): RuleTestResult {
  const parsed = parseRuleStructure(rule)
  if ('problem' in parsed)
    return { matched: false, schema: ctx.schema, fields: ctx.fields, nodes: [], card: null, warnings: [], errors: [parsed.problem] }
  const draft = parsed.rule
  const errors = validateRuleSemantics(draft)
  const nodes: NodeDiagnostic[] = []
  const warnings: string[] = []
  let matched = false
  let card: RuleTestResult['card'] = null
  const condOk = !errors.some((e) => e.path === 'when' || e.path.startsWith('when.'))
  if (condOk) matched = evaluate(draft.when, ctx, '', nodes)
  if (!errors.some((e) => e.path.startsWith('present.'))) {
    const a = present(draft, ctx, warnings)
    card = { title: a.title, body: a.body, severity: a.severity, color: a.color, fields: a.fields, dedup_key: a.dedup_key }
  }
  return { matched, schema: ctx.schema, fields: ctx.fields, nodes, card, warnings: [...new Set(warnings)], errors }
}
