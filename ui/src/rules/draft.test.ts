import { describe, expect, it } from 'vitest'
import type { Condition, RuleSpec } from '../api/types'
import { diffLines, withContext } from '../lib/diff'
import { addChild, fromYaml, getNode, moveChild, parseScalar, removeNode, retargetOp, setNode, TEMPLATES, toggleNot, toYaml } from './draft'

const tree: Condition = {
  all: [
    { path: 'raw.a', op: 'eq', value: 1 },
    { any: [{ path: 'raw.b', op: 'eq', value: 'x' }, { not: { path: 'raw.c', op: 'exists' } }] },
  ],
}

describe('condition tree edits', () => {
  it('addresses nodes by child index, through not', () => {
    expect(getNode(tree, '1.1.0')).toEqual({ path: 'raw.c', op: 'exists' })
    expect(getNode(tree, '')).toBe(tree)
  })

  it('replaces, adds and moves without touching the original', () => {
    const next = setNode(tree, '1.0', { path: 'raw.b', op: 'neq', value: 'y' })
    expect(getNode(next, '1.0')).toEqual({ path: 'raw.b', op: 'neq', value: 'y' })
    expect(getNode(tree, '1.0')).toEqual({ path: 'raw.b', op: 'eq', value: 'x' })
    const added = addChild(tree, '1', { path: 'raw.d', op: 'gt', value: 2 })
    expect((getNode(added, '1') as { any: Condition[] }).any).toHaveLength(3)
    const moved = moveChild(tree, '1', -1)
    expect(getNode(moved, '0')).toEqual(tree.all[1])
  })

  it('removing the only child of a not removes the not', () => {
    const next = removeNode(tree, '1.1.0')
    expect((getNode(next, '1') as { any: Condition[] }).any).toEqual([{ path: 'raw.b', op: 'eq', value: 'x' }])
  })

  it('toggles not on and off', () => {
    const on = toggleNot(tree, '0')
    expect(getNode(on, '0')).toEqual({ not: { path: 'raw.a', op: 'eq', value: 1 } })
    expect(toggleNot(on, '0')).toEqual(tree)
  })

  it('reshapes values when the operator changes', () => {
    expect(retargetOp({ path: 'p', op: 'eq', value: 'A' }, 'in')).toEqual({ path: 'p', op: 'in', value: ['A'] })
    expect(retargetOp({ path: 'p', op: 'in', value: ['A', 'B'] }, 'eq')).toEqual({ path: 'p', op: 'eq', value: 'A' })
    expect(retargetOp({ path: 'p', op: 'eq', value: 'A' }, 'exists')).toEqual({ path: 'p', op: 'exists' })
  })

  it('parses typed values', () => {
    expect(parseScalar('10,000,000', 'number')).toBe(10_000_000)
    expect(parseScalar('abc', 'number')).toBe('abc')
    expect(parseScalar('0042', 'text')).toBe('0042')
    expect(parseScalar('true', 'bool')).toBe(true)
  })
})

describe('YAML', () => {
  const rule = TEMPLATES[2]!.make('rates') as RuleSpec

  it('round-trips a rule in config-file style', () => {
    const text = toYaml(rule)
    expect(text).toContain('- { path: raw.payload.notional, op: gt, value: 10000000 }')
    expect(text).toContain('channels: [ email, ui ]')
    const back = fromYaml(text)
    expect(back).toEqual({ ok: true, rule })
  })

  it('reports syntax errors with a position', () => {
    const r = fromYaml('id: x\nteam: [unclosed\n')
    expect(r.ok).toBe(false)
    if (!r.ok) expect(r.line).not.toBeNull()
  })

  it('rejects documents that are not rules', () => {
    expect(fromYaml('- a\n- b').ok).toBe(false)
    const r = fromYaml('id: x\nteam: y\n')
    expect(r.ok === false && r.message).toContain('when')
  })
})

describe('diff', () => {
  it('marks added and removed lines and collapses context', () => {
    const a = ['a', 'b', 'c', 'd', 'e', 'f', 'g'].join('\n')
    const b = ['a', 'b', 'c', 'X', 'e', 'f', 'g'].join('\n')
    const d = diffLines(a, b)
    expect(d.filter((l) => l.op !== ' ')).toEqual([{ op: '-', text: 'd' }, { op: '+', text: 'X' }])
    const c = withContext(d, 1)
    expect(c[0]).toEqual({ op: '…', text: '2 unchanged lines' })
    expect(c.at(-1)).toEqual({ op: '…', text: '2 unchanged lines' })
  })
})
