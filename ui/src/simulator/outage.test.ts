import { expect, it } from 'vitest'
import { secondsLeft } from './OutageControl'

it('counts outage seconds left up to the next whole second and never below zero', () => {
  const now = Date.parse('2026-11-21T07:00:00.000Z')
  expect(secondsLeft({ channel: 'sms', mode: 'down', until: '2026-11-21T07:01:00.000Z' }, now)).toBe(60)
  expect(secondsLeft({ channel: 'sms', mode: 'down', until: '2026-11-21T07:00:00.400Z' }, now)).toBe(1)
  expect(secondsLeft({ channel: 'phone', mode: 'down', until: '2026-11-21T06:59:00.000Z' }, now)).toBe(0)
})
