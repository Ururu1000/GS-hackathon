import { describe, expect, it } from 'vitest'
import type { Preference } from '../api/types'
import { digestExclusive, inWindow, resolveChannels } from './resolve'

// Anna from Appendix A / config/people/anna.yaml.
const anna: Preference[] = [
  { match: { severity: 'critical', service: 'payments-api' }, channels: ['sms'], when: { between: '08:00-20:00', tz: 'Europe/London' }, otherwise: ['email'] },
  { match: { severity: 'warning' }, channels: ['email'] },
  { default: ['ui'] },
]
const crit = { severity: 'critical' as const, service: 'payments-api' }

describe('resolveChannels', () => {
  it('uses SMS inside [08:00, 20:00) London time and email outside', () => {
    expect(resolveChannels(anna, crit, new Date('2026-07-01T07:00:00Z')).channels).toEqual(['sms']) // 08:00 BST
    expect(resolveChannels(anna, crit, new Date('2026-07-01T06:59:00Z')).channels).toEqual(['email']) // 07:59 BST
    expect(resolveChannels(anna, crit, new Date('2026-07-01T19:00:00Z')).channels).toEqual(['email']) // 20:00 BST, end excluded
    expect(resolveChannels(anna, crit, new Date('2026-07-01T18:59:00Z')).channels).toEqual(['sms'])
  })

  it('follows the clock change, not a fixed UTC offset', () => {
    // 07:30 UTC is 07:30 GMT in January (outside) but 08:30 BST in July (inside).
    expect(resolveChannels(anna, crit, new Date('2026-01-15T07:30:00Z')).channels).toEqual(['email'])
    expect(resolveChannels(anna, crit, new Date('2026-07-15T07:30:00Z')).channels).toEqual(['sms'])
  })

  it('holds non-critical alerts for the digest outside hours, but sends critical by email', () => {
    const prefs: Preference[] = [
      { match: {}, channels: ['sms'], when: { between: '08:00-20:00', tz: 'Europe/London' }, otherwise: ['digest'] },
    ]
    const night = new Date('2026-01-15T03:00:00Z')
    expect(resolveChannels(prefs, { severity: 'warning' }, night).channels).toEqual(['digest'])
    expect(resolveChannels(prefs, crit, night).channels).toEqual(['email'])
    const both: Preference[] = [{ ...prefs[0], otherwise: ['email', 'digest'] } as Preference]
    expect(resolveChannels(both, crit, night).channels).toEqual(['email'])
  })

  it('keeps the digest exclusive (only In-app beside it) in the picker', () => {
    expect(digestExclusive(['sms', 'ui'], ['sms', 'ui', 'digest'])).toEqual(['ui', 'digest'])
    expect(digestExclusive(['digest'], ['email', 'digest'])).toEqual(['email'])
    expect(digestExclusive(['digest'], ['ui', 'digest'])).toEqual(['ui', 'digest'])
  })

  it('falls through lines, then the default, then the team, then in-app', () => {
    expect(resolveChannels(anna, { severity: 'warning' }, new Date())).toMatchObject({ channels: ['email'], line: 1 })
    expect(resolveChannels(anna, { severity: 'info' }, new Date())).toMatchObject({ channels: ['ui'], via: 'default' })
    expect(resolveChannels([], { severity: 'info' }, new Date(), ['email'])).toMatchObject({ channels: ['email'], via: 'team' })
    expect(resolveChannels([], { severity: 'info' }, new Date())).toMatchObject({ channels: ['ui'], via: 'ui' })
  })

  it('skips a windowed line with no otherwise when outside its hours', () => {
    const prefs: Preference[] = [
      { match: { severity: 'critical' }, channels: ['phone'], when: { between: '09:00-17:00', tz: 'UTC' } },
      { match: {}, channels: ['email'] },
    ]
    expect(resolveChannels(prefs, crit, new Date('2026-03-02T20:00:00Z'))).toMatchObject({ channels: ['email'], line: 1 })
  })

  it('handles windows across midnight and unknown zones', () => {
    expect(inWindow({ between: '22:00-06:00', tz: 'UTC' }, new Date('2026-03-02T23:30:00Z'))).toBe(true)
    expect(inWindow({ between: '22:00-06:00', tz: 'UTC' }, new Date('2026-03-02T12:00:00Z'))).toBe(false)
    expect(inWindow({ between: '08:00-20:00', tz: 'Mars/Olympus' }, new Date())).toBeNull()
    const r = resolveChannels([{ match: {}, channels: ['sms'], when: { between: 'soon', tz: 'UTC' }, otherwise: ['email'] }], crit, new Date())
    expect(r.channels).toEqual(['email'])
    expect(r.problem).toBeTruthy()
  })
})
