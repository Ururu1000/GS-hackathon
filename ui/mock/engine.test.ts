import { describe, expect, it } from 'vitest'
import { SAMPLES } from '../src/rules/draft'
import { context, evaluate, testRule, validateRule } from './engine'
import { RULES } from './scenario'

const ctx = context({ a: 1, n: null, s: 'prod', list: ['x'] }, 'http:t')

describe('condition semantics (spec §4.3)', () => {
  it('missing values compare false, even for neq and not_in; not negates that', () => {
    expect(evaluate({ path: 'raw.missing', op: 'neq', value: 1 }, ctx)).toBe(false)
    expect(evaluate({ path: 'raw.missing', op: 'not_in', value: [1] }, ctx)).toBe(false)
    expect(evaluate({ not: { path: 'raw.missing', op: 'eq', value: 1 } }, ctx)).toBe(true)
  })

  it('exists is true for a present null; types are not coerced', () => {
    expect(evaluate({ path: 'raw.n', op: 'exists' }, ctx)).toBe(true)
    expect(evaluate({ path: 'raw.a', op: 'eq', value: '1' }, ctx)).toBe(false)
    expect(evaluate({ path: 'raw.a', op: 'gt', value: '0' }, ctx)).toBe(false)
    expect(evaluate({ path: 'raw.list', op: 'contains', value: 'x' }, ctx)).toBe(true)
  })

  it('rejects a malformed regex at publish', () => {
    expect(validateRule({ ...RULES[0]!, when: { path: 'raw.s', op: 'regex', value: '(' } })).toContainEqual(
      expect.objectContaining({ path: 'when.value' }),
    )
  })
})

describe('seed rules against the Appendix A samples', () => {
  const sample = (id: string) => SAMPLES.find((s) => s.id === id)!
  const rule = (id: string) => RULES.find((r) => r.id === id)!

  it('Settlements: matches the workflow failure with step and error in red', () => {
    const r = testRule(rule('settlements-workflow-failed'), sample('workflow-failed').event)
    expect(r.matched).toBe(true)
    expect(r.card?.color).toBe('red')
    expect(r.card?.body).toBe('TIMEOUT: Counterparty service did not respond in 5000 ms')
  })

  it('Rates: renders the title; equality at the threshold does not match', () => {
    const ev = sample('trade-rejected').event as { payload: Record<string, unknown> }
    const r = testRule(rule('rates-rejected-trades'), ev)
    expect(r.card?.title).toBe('Rejected EUR 25,000,000 trade T-99877')
    expect(testRule(rule('rates-rejected-trades'), { ...ev, payload: { ...ev.payload, notional: 10_000_000 } }).matched).toBe(false)
  })

  it('Anna: critical payments-api matches; a recovery does not open an alert', () => {
    const ev = sample('prometheus').event as Record<string, unknown>
    expect(testRule(rule('anna-payments-critical'), ev).matched).toBe(true)
    expect(testRule(rule('anna-payments-critical'), { ...ev, status: 'resolved' }).matched).toBe(false)
  })

  it('Pricing: WARN or above from pricing-engine', () => {
    const ev = sample('pricing-log').event as Record<string, unknown>
    expect(testRule(rule('pricing-engine-warnings'), ev).matched).toBe(true)
    expect(testRule(rule('pricing-engine-warnings'), { ...ev, level: 'INFO' }).matched).toBe(false)
  })

  it('an unknown shape is still matchable on raw.* with a structural schema id', () => {
    const r = testRule({ ...rule('rates-rejected-trades'), when: { path: 'raw.thing.kind', op: 'eq', value: 'odd' } }, { thing: { kind: 'odd' } })
    expect(r.matched).toBe(true)
    expect(r.schema).toMatch(/^unknown:/)
  })
})
