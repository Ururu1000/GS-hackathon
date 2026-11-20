// Replays the contract's SSE fixtures through the client reducer, so the
// fixtures (which the backend can also replay) and the UI can't drift apart.
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { STREAM_EVENT_TYPES, type StreamEvent } from '../api/types'
import { applyEvents, emptyFeed } from './feedStore'

const dir = resolve(import.meta.dirname, '../../contract/fixtures')

/** Minimal text/event-stream parser: event name + single/multi-line data. */
function parseSse(text: string): { event: string; data: string }[] {
  const out: { event: string; data: string }[] = []
  for (const block of text.split(/\n\n/)) {
    let event = 'message'
    const data: string[] = []
    for (const line of block.split('\n')) {
      if (line.startsWith(':') || line === '') continue
      const i = line.indexOf(':')
      const field = i < 0 ? line : line.slice(0, i)
      const value = i < 0 ? '' : line.slice(i + 1).replace(/^ /, '')
      if (field === 'event') event = value
      else if (field === 'data') data.push(value)
    }
    if (data.length) out.push({ event, data: data.join('\n') })
  }
  return out
}

describe('contract fixtures', () => {
  it('stream-basic.sse ends in the expected client state', () => {
    const events = parseSse(readFileSync(`${dir}/stream-basic.sse`, 'utf8'))
      .filter((e) => (STREAM_EVENT_TYPES as readonly string[]).includes(e.event))
      .map((e) => ({ type: e.event, data: JSON.parse(e.data) }) as StreamEvent)
    const expected = JSON.parse(readFileSync(`${dir}/stream-basic.expected.json`, 'utf8'))

    const s = applyEvents(emptyFeed, events)
    expect(s.order).toEqual(expected.order)
    for (const [id, v] of Object.entries(expected.versions)) expect(s.byId.get(id)?.version).toBe(v)
    for (const [id, c] of Object.entries(expected.counts)) expect(s.byId.get(id)?.count).toBe(c)
    for (const [id, st] of Object.entries(expected.statuses)) expect(s.byId.get(id)?.status).toBe(st)
    expect(s.stats.stale).toBe(expected.stale_dropped)
  })
})
