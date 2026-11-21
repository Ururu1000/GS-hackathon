import { describe, expect, it } from 'vitest'
import type { RunbookDetail, RunbookStep, RunbookView } from '../api/types'
import { completedSteps, diffRunbook, templateMatches } from './runbookDiff'

const step = (id: string, text: string, extra: Partial<RunbookStep> = {}): RunbookStep => ({
  id, kind: 'action', text, hint: null, action: { name: id, label: id, params: {}, risk: 'low' } as RunbookStep['action'],
  approval: 'none', state: 'idle', disabled_reason: null, done_by: null, done_at: null, runs: [], approval_request: null, ...extra,
})
const frozen = (revision: number, steps: RunbookStep[]): RunbookView =>
  ({ id: 'settlement-timeout', revision, title: 'Settlement timeout', format: 'yaml', url: null, text: null, steps })
const current = (revision: number, steps: RunbookDetail['steps']): RunbookDetail =>
  ({ id: 'settlement-timeout', revision, title: 'Settlement timeout', format: 'yaml', team: 'settlements', steps })
const cur = (id: string, text: string, extra: Partial<RunbookDetail['steps'][number]> = {}) =>
  ({ id, kind: 'action' as const, text, action: { name: id, label: id }, approval: 'none' as const, ...extra })

describe('templateMatches', () => {
  it('matches a rendered step against its template', () => {
    expect(templateMatches('Retry workflow {{workflowId}}', 'Retry workflow WF-123')).toBe(true)
    expect(templateMatches('Check {{ fields.region }} ({{x}})', 'Check eu-west (7)')).toBe(true)
    expect(templateMatches('Retry workflow {{workflowId}} now', 'Retry workflow WF-123')).toBe(false)
    expect(templateMatches('Escalate', 'Escalate')).toBe(true)
    expect(templateMatches('Escalate.', 'Escalate!')).toBe(false)
    expect(templateMatches('a.b {{x}}', 'aXb 1')).toBe(false)
  })
})

describe('diffRunbook', () => {
  it('is null when the alert still has the current revision', () => {
    expect(diffRunbook(frozen(2, [step('a', 'A')]), current(2, [cur('a', 'B')]))).toBeNull()
  })

  it('reports added, removed and changed steps by id, ignoring rendered placeholders', () => {
    const d = diffRunbook(
      frozen(1, [step('check', 'Is it healthy?'), step('retry', 'Retry workflow WF-9'), step('old', 'Old step'), step('esc', 'Escalate')]),
      current(2, [cur('check', 'Is it healthy? Check the status page'), cur('retry', 'Retry workflow {{workflowId}}'), cur('new', 'Tell the desk', { kind: 'manual', action: null }), cur('esc', 'Escalate', { approval: 'required' })]),
    )!
    expect(d).toMatchObject({ from: 1, to: 2, reordered: false })
    expect(d.added.map((s) => s.id)).toEqual(['new'])
    expect(d.removed.map((s) => s.id)).toEqual(['old'])
    expect(d.changed.map((c) => [c.id, c.fields])).toEqual([['check', ['text']], ['esc', ['approval']]])
  })

  it('flags reordering of shared steps', () => {
    const d = diffRunbook(frozen(1, [step('a', 'A'), step('b', 'B')]), current(2, [cur('b', 'B'), cur('a', 'A')]))!
    expect(d.reordered).toBe(true)
    expect(d.changed).toEqual([])
  })
})

it('lists ticked manual steps and successful actions as completed', () => {
  const rb = frozen(1, [
    step('m', 'Manual', { kind: 'manual', action: null, state: 'done', done_by: 'user:anna', done_at: '2026-11-21T07:00:00Z' }),
    step('ok', 'Ran', { state: 'ok' }),
    step('f', 'Failed', { state: 'failed' }),
    step('i', 'Idle'),
  ])
  expect(completedSteps(rb).map((s) => s.id)).toEqual(['m', 'ok'])
})
