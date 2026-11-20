import { describe, expect, it } from 'vitest'
import type { FeedAlert, StreamEvent } from '../api/types'
import { appendHistory, applyAlert, applyEvents, compareFeed, emptyFeed, MAX_ROWS } from './feedStore'

function alert(id: string, version: number, over: Partial<FeedAlert> = {}): FeedAlert {
  return {
    id,
    team: 'rates',
    version,
    rule: { id: 'r', revision: 1, name: 'R' },
    status: 'open',
    severity: 'warning',
    color: 'amber',
    title: `t ${id}`,
    body: '',
    fields: [],
    count: version,
    first_seen: '2026-10-23T09:00:00.000Z',
    last_seen: '2026-10-23T09:00:00.000Z',
    source: 'test',
    schema: 'test',
    acked_by: null,
    acked_at: null,
    resolved_by: null,
    resolved_at: null,
    resolved_reason: null,
    ...over,
  }
}

const snap = (items: FeedAlert[]): StreamEvent => ({
  type: 'snapshot',
  data: { team: null, items, next_cursor: null, server_time: '2026-10-23T09:00:00.000Z' },
})
const up = (a: FeedAlert): StreamEvent => ({ type: 'alert.upsert', data: a })
const rm = (id: string, version: number): StreamEvent => ({
  type: 'alert.remove',
  data: { id, version, reason: 'deleted' },
})

describe('feed snapshot protocol', () => {
  it('snapshot replaces the page', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 1), alert('b', 1)])])
    s = applyEvents(s, [snap([alert('c', 1)])])
    expect(s.order).toEqual(['c'])
    expect(s.stats.snapshots).toBe(2)
    expect(s.inserted).toEqual([])
  })

  it('applies newer upserts and drops older or equal versions', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 5)])])
    s = applyEvents(s, [up(alert('a', 5, { count: 99 })), up(alert('a', 4, { count: 98 }))])
    expect(s.byId.get('a')?.count).toBe(5)
    expect(s.stats.stale).toBe(2)
    s = applyEvents(s, [up(alert('a', 6, { count: 6 }))])
    expect(s.byId.get('a')?.count).toBe(6)
  })

  it('inserts new alerts in (first_seen, id) desc order and reports them', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 1, { first_seen: '2026-10-23T09:00:00.000Z' })])])
    s = applyEvents(s, [
      up(alert('b', 1, { first_seen: '2026-10-23T09:05:00.000Z' })),
      up(alert('c', 1, { first_seen: '2026-10-23T08:55:00.000Z' })),
      up(alert('d', 1, { first_seen: '2026-10-23T09:05:00.000Z' })),
    ])
    expect(s.order).toEqual(['d', 'b', 'a', 'c'])
    expect([...s.inserted].sort()).toEqual(['b', 'c', 'd'])
  })

  it('a removal blocks a delayed upsert with an older or equal version', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 3)])])
    s = applyEvents(s, [rm('a', 4), up(alert('a', 4)), up(alert('a', 2))])
    expect(s.byId.has('a')).toBe(false)
    expect(s.stats.stale).toBe(2)
    // A genuinely newer version may come back (e.g. moved back into scope).
    s = applyEvents(s, [up(alert('a', 5))])
    expect(s.byId.get('a')?.version).toBe(5)
  })

  it('a removal older than the held version is ignored', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 7)])])
    s = applyEvents(s, [rm('a', 6)])
    expect(s.byId.has('a')).toBe(true)
  })

  it('a snapshot clears tombstones for the rows it contains', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 3)]), rm('a', 4)])
    s = applyEvents(s, [snap([alert('a', 2)])])
    expect(s.byId.get('a')?.version).toBe(2)
    s = applyEvents(s, [up(alert('a', 3))])
    expect(s.byId.get('a')?.version).toBe(3)
  })

  it('events buffered before the snapshot in a batch are replaced by it', () => {
    const s = applyEvents(emptyFeed, [up(alert('x', 1)), snap([alert('a', 1)]), up(alert('a', 2))])
    expect(s.order).toEqual(['a'])
    expect(s.byId.get('a')?.version).toBe(2)
  })

  it('returns the same state when only stale events arrive with no new drops', () => {
    const s = applyEvents(emptyFeed, [snap([alert('a', 2)])])
    expect(applyEvents(s, [])).toBe(s)
  })

  it('caps the page at MAX_ROWS, dropping the oldest', () => {
    const items = Array.from({ length: MAX_ROWS }, (_, i) =>
      alert(`a${String(i).padStart(5, '0')}`, 1, { first_seen: new Date(Date.UTC(2026, 0, 1) + i * 1000).toISOString() }),
    )
    let s = applyEvents(emptyFeed, [snap(items)])
    s = applyEvents(s, [up(alert('new', 1, { first_seen: '2027-01-01T00:00:00.000Z' }))])
    expect(s.order.length).toBe(MAX_ROWS)
    expect(s.order[0]).toBe('new')
    expect(s.byId.has('a00000')).toBe(false)
  })

  it('incremental ordering matches a full sort over random batches', () => {
    let seed = 42
    const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff)
    const versions = new Map<string, number>()
    let s = applyEvents(emptyFeed, [snap([])])
    for (let batch = 0; batch < 200; batch++) {
      const evs: StreamEvent[] = []
      for (let i = 0; i < 5; i++) {
        const id = `a${Math.floor(rnd() * 60)}`
        const v = (versions.get(id) ?? 0) + 1 + Math.floor(rnd() * 2)
        versions.set(id, v)
        const fs = new Date(Date.UTC(2026, 9, 23, 9, Math.floor(rnd() * 5))).toISOString()
        evs.push(rnd() < 0.2 ? rm(id, v) : up(alert(id, v, { first_seen: fs })))
      }
      s = applyEvents(s, evs)
      const expected = [...s.byId.values()].sort(compareFeed).map((a) => a.id)
      expect(s.order).toEqual(expected)
    }
  })

  it('mutation results go through the same version rule', () => {
    let s = applyEvents(emptyFeed, [snap([alert('a', 3)])])
    s = applyAlert(s, alert('a', 2, { status: 'acked' }))
    expect(s.byId.get('a')?.status).toBe('open')
    s = applyAlert(s, alert('a', 4, { status: 'acked' }))
    expect(s.byId.get('a')?.status).toBe('acked')
  })
})

it('pages history separately and applies only newer stream updates and removals', () => {
  const open = alert('latest', 3)
  const old = alert('older', 2)
  let state = applyEvents(emptyFeed, [snap([open]), rm('removed', 4)])
  state = appendHistory(state, { items: [alert('latest', 1), old, alert('removed', 3)], next_cursor: 'older-cursor' })
  expect(state.order).toEqual(['latest'])
  expect(state.history.get('latest')).toBeUndefined()
  expect(state.history.get('removed')).toBeUndefined()
  expect(state.history.get('older')).toEqual(old)
  expect(state.nextCursor).toBe('older-cursor')
  state = applyEvents(state, [up(alert('older', 1)), up(alert('older', 3))])
  expect(state.history.get('older')?.version).toBe(3)
  expect(state.inserted).toEqual([])
  state = applyEvents(state, [rm('older', 4), up(alert('older', 3))])
  expect(state.history.has('older')).toBe(false)
  state = applyEvents(state, [snap([open])])
  expect(state.history.size).toBe(0)
  expect(state.nextCursor).toBeNull()
})
