import { describe, expect, it } from 'vitest'
import { actorName, resolvedReasonText, resolvedText } from './badges'
import { timelineText } from './AlertDetail'
import { defaultFilters, isDefault } from '../feed/filters'

describe('actorName', () => {
  it('maps ids to readable names', () => {
    expect(actorName(null)).toBe('someone')
    expect(actorName('system')).toBe('Beacon')
    expect(actorName('rule:r1')).toBe('an alert rule')
    expect(actorName('user:sam')).toBe('sam')
  })
})

describe('resolvedText', () => {
  it('describes manual and automatic resolves', () => {
    expect(resolvedText('user:sam', 'user')).toBe('Resolved by sam')
    expect(resolvedText('system', 'quiet_for')).toBe('Resolved automatically: no new events for a while')
    expect(resolvedText('system', null)).toBe('Resolved automatically')
    expect(resolvedText('user:sam', 'recovered')).toBe('Resolved by sam: the source reported it recovered')
  })
  it('humanises unknown reasons', () => {
    expect(resolvedReasonText('some_new_reason')).toBe('some new reason')
    expect(resolvedReasonText(null)).toBeNull()
  })
})

describe('timelineText', () => {
  const entry = (kind: string, actor: string, message = 'msg') => ({ at: '2026-01-01T00:00:00Z', actor, kind, message })
  const none = { resolved_at: null, resolved_reason: null }

  it('uses the structured reason only for the current resolution', () => {
    const a = { resolved_at: '2026-01-01T00:00:00Z', resolved_reason: 'quiet_for' }
    expect(timelineText(entry('resolved', 'system'), a)).toBe('Resolved automatically: no new events for a while')
    expect(timelineText({ ...entry('resolved', 'system'), at: '2025-12-31T00:00:00Z' }, a)).toBe('Resolved automatically')
  })
  it('does not parse the server message', () => {
    expect(timelineText(entry('resolved', 'user:sam', 'Gone (weird)'), none)).toBe('Resolved by sam')
  })
  it('phrases acks and reopens from the kind, and passes other messages through', () => {
    expect(timelineText(entry('acked', 'user:sam'), none)).toBe('Acknowledged by sam')
    expect(timelineText(entry('reopened', 'system'), none)).toBe('Reopened: the problem came back')
    expect(timelineText(entry('deduplicated', 'system', 'Happened again'), none)).toBe('Happened again')
    expect(timelineText(entry('note', 'user:sam', 'Looking'), none)).toBe('Looking · sam')
  })
})

describe('isDefault', () => {
  it('is true for the defaults and false once anything changes', () => {
    expect(isDefault(defaultFilters)).toBe(true)
    expect(isDefault({ ...defaultFilters, query: 'x' })).toBe(false)
    expect(isDefault({ ...defaultFilters, rule: 'r1' })).toBe(false)
    expect(isDefault({ ...defaultFilters, status: new Set(['open', 'acked', 'resolved']) })).toBe(false)
  })
})
