import { COLOR_TOKENS, COND_OPS, SEVERITIES, type RuleSpec } from '../api/types.ts'

export interface RuleProblem {
  path: string
  message: string
}

type Check = (value: unknown, path: string) => RuleProblem | null

const problem = (path: string, message: string): RuleProblem => ({ path, message })
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)
const string: Check = (value, path) => typeof value === 'string' ? null : problem(path, 'must be text')
const number: Check = (value, path) => typeof value === 'number' && Number.isFinite(value) ? null : problem(path, 'must be a number')
const boolean: Check = (value, path) => typeof value === 'boolean' ? null : problem(path, 'must be true or false')
const choice = (options: readonly string[]): Check => (value, path) =>
  typeof value === 'string' && options.includes(value) ? null : problem(path, `must be one of ${options.join(', ')}`)
const scalar = (value: unknown) => value === null || typeof value === 'string' || typeof value === 'boolean'
  || (typeof value === 'number' && Number.isFinite(value))

function list(item: Check): Check {
  return (value, path) => {
    if (!Array.isArray(value)) return problem(path, 'must be a list')
    for (const [i, entry] of value.entries()) {
      const issue = item(entry, `${path}.${i}`)
      if (issue) return issue
    }
    return null
  }
}

function object(required: Record<string, Check>, optional: Record<string, Check> = {}): Check {
  return (value, path) => {
    if (!record(value)) return problem(path, 'must be a mapping')
    for (const [key, check] of [...Object.entries(required), ...Object.entries(optional)]) {
      const field = path ? `${path}.${key}` : key
      if (!Object.hasOwn(value, key)) {
        if (Object.hasOwn(required, key)) return problem(field, 'is required')
        continue
      }
      const issue = check(value[key], field)
      if (issue) return issue
    }
    return null
  }
}

const comparison = object({ path: string, op: choice(COND_OPS) }, {
  value: (value, path) => scalar(value) || (Array.isArray(value) && value.every(scalar))
    ? null : problem(path, 'must be a scalar or a list of scalars'),
})

function condition(value: unknown, path: string, depth = 0): RuleProblem | null {
  if (depth > 12) return problem(path, 'nested too deeply')
  if (!record(value)) return problem(path, 'must be a condition object')
  const child: Check = (entry, field) => condition(entry, field, depth + 1)
  const variants = ['all', 'any', 'not', 'path'].filter((key) => Object.hasOwn(value, key))
  if (variants.length > 1) return problem(path, 'must contain one condition form')
  const kind = variants[0]
  if (kind === 'all' || kind === 'any') return list(child)(value[kind], `${path}.${kind}`)
  if (kind === 'not') return child(value.not, `${path}.not`)
  return comparison(value, path)
}

const field: Check = (value, path) => typeof value === 'string' ? null : object({ path: string, label: string })(value, path)
const structure = object({
  id: string,
  team: string,
  when: condition,
  route: list(object({ to: string }, { channels: list(string) })),
  present: object({ title: string }, { body: string, fields: list(field), color: choice(COLOR_TOKENS) }),
}, {
  name: string,
  enabled: boolean,
  severity: choice(SEVERITIES),
  dedup: object({ window: string }, { key: list(string) }),
  group: object({}, { by: list(string), window: (value, path) => typeof value === 'string' ? null : number(value, path) }),
  throttle: object({ max: number, per: string }),
  runbook: string,
  auto_resolve: object({}, { recover_when: condition, quiet_for: string }),
  escalate_after: string,
})

export function parseRuleStructure(value: unknown): { rule: RuleSpec } | { problem: RuleProblem } {
  const issue = structure(value, '')
  if (issue) return { problem: issue }
  return { rule: value as RuleSpec }
}
