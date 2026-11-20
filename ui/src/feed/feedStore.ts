// Client side of the SSE snapshot protocol (spec §8, ui/contract/openapi.yaml).
//
// - `snapshot` replaces the page.
// - `alert.upsert` carries the absolute alert; it is applied only if its
//   version is newer than what we hold and newer than any removal we saw.
// - `alert.remove` drops the row and remembers the removal version, so a
//   delayed upsert cannot resurrect it.
//
// Pure functions over an immutable state, so the stream hook can batch a
// frame's worth of events into one React update and tests can drive it.

import type { FeedAlert, FeedPage, RemoveEvent, SnapshotEvent, StreamEvent } from '../api/types'

export const MAX_ROWS = 5000
const MAX_HISTORY = 5000
export const MAX_TOMBSTONES = 10_000

export interface FeedStats {
  snapshots: number
  upserts: number
  removes: number
  /** Upserts/removes dropped because their version was not newer. */
  stale: number
}

export interface FeedState {
  byId: ReadonlyMap<string, FeedAlert>
  /** Ids in feed order: first_seen desc, then id desc. */
  order: readonly string[]
  /** id → version at removal (bounded, oldest evicted first). */
  tombstones: ReadonlyMap<string, number>
  snapshotAt: string | null
  history: ReadonlyMap<string, FeedAlert>
  nextCursor: string | null
  snapshotOldest: FeedAlert | null
  /** Ids that entered the page in the last applied batch (not via snapshot). */
  inserted: readonly string[]
  stats: FeedStats
}

export const emptyFeed: FeedState = {
  byId: new Map(),
  order: [],
  tombstones: new Map(),
  snapshotAt: null,
  history: new Map(),
  nextCursor: null,
  snapshotOldest: null,
  inserted: [],
  stats: { snapshots: 0, upserts: 0, removes: 0, stale: 0 },
}

const ts = (s: string): number => {
  const n = Date.parse(s)
  return Number.isNaN(n) ? 0 : n
}

/** Feed order: newest first_seen first; ties broken by id, descending. */
export function compareFeed(a: FeedAlert, b: FeedAlert): number {
  const d = ts(b.first_seen) - ts(a.first_seen)
  if (d !== 0) return d
  return a.id < b.id ? 1 : a.id > b.id ? -1 : 0
}

function sortIds(byId: Map<string, FeedAlert>): string[] {
  return [...byId.values()].sort(compareFeed).map((a) => a.id)
}

/** Apply a batch of stream events in order. Returns the same state object if
 * nothing visible changed. */
export function applyEvents(state: FeedState, events: readonly StreamEvent[]): FeedState {
  if (events.length === 0) return state

  let byId: Map<string, FeedAlert> | null = null
  let tombstones: Map<string, number> | null = null
  let snapshotAt = state.snapshotAt
  let nextCursor = state.nextCursor
  let snapshotOldest = state.snapshotOldest
  let history: Map<string, FeedAlert> | null = null
  const older = () => (history ??= new Map(state.history))
  let membershipChanged = false
  let replaced = false
  const inserted = new Set<string>()
  /** Ids removed from the previous order (only used when not replaced). */
  const removed = new Set<string>()
  /** Existing ids whose sort key changed (re-inserted, not reported). */
  const moved = new Set<string>()
  const stats = { ...state.stats }

  const rows = () => (byId ??= new Map(state.byId))
  const graves = () => (tombstones ??= new Map(state.tombstones))

  for (const ev of events) {
    switch (ev.type) {
      case 'snapshot': {
        applySnapshot(ev.data)
        break
      }
      case 'alert.upsert': {
        const next = ev.data
        const recent = (byId ?? state.byId).get(next.id)
        const historical = (history ?? state.history).get(next.id)
        const cur = recent ?? historical
        const removedAt = (tombstones ?? state.tombstones).get(next.id)
        if ((cur && next.version <= cur.version) || (removedAt !== undefined && next.version <= removedAt)) {
          stats.stale++
          break
        }
        stats.upserts++
        if (!recent && historical) {
          older().set(next.id, next)
          break
        }
        rows().set(next.id, next)
        if (!cur) {
          membershipChanged = true
          if (snapshotOldest && compareFeed(next, snapshotOldest) > 0) moved.add(next.id)
          else inserted.add(next.id)
        } else if (cur.first_seen !== next.first_seen && !replaced) {
          // first_seen is immutable by contract; re-position defensively.
          membershipChanged = true
          removed.add(next.id)
          moved.add(next.id)
        }
        break
      }
      case 'alert.remove': {
        applyRemove(ev.data)
        break
      }
      default:
        // Unknown event types are ignored (forward compatibility).
        break
    }
  }

  function applySnapshot(snap: SnapshotEvent) {
    stats.snapshots++
    const fresh = new Map<string, FeedAlert>()
    for (const a of snap.items) fresh.set(a.id, a)
    byId = fresh
    // The snapshot is authoritative for what it contains.
    if ((tombstones ?? state.tombstones).size > 0) {
      for (const id of fresh.keys()) graves().delete(id)
    }
    snapshotAt = snap.server_time
    nextCursor = snap.next_cursor
    snapshotOldest = null
    if (nextCursor) {
      for (const alert of fresh.values()) {
        if (!snapshotOldest || compareFeed(alert, snapshotOldest) > 0) snapshotOldest = alert
      }
    }
    history = new Map()
    membershipChanged = true
    replaced = true
    inserted.clear()
  }

  function applyRemove(rm: RemoveEvent) {
    const recent = (byId ?? state.byId).get(rm.id)
    const historical = (history ?? state.history).get(rm.id)
    const cur = recent ?? historical
    const removedAt = (tombstones ?? state.tombstones).get(rm.id)
    if ((cur && rm.version < cur.version) || (removedAt !== undefined && rm.version <= removedAt)) {
      stats.stale++
      return
    }
    stats.removes++
    const g = graves()
    g.delete(rm.id) // re-insert to refresh its position in eviction order
    g.set(rm.id, rm.version)
    if (g.size > MAX_TOMBSTONES) {
      const oldest = g.keys().next().value
      if (oldest !== undefined) g.delete(oldest)
    }
    if (historical) older().delete(rm.id)
    if (recent) {
      rows().delete(rm.id)
      inserted.delete(rm.id)
      removed.add(rm.id)
      membershipChanged = true
    }
  }

  if (!byId && !tombstones && !history) {
    // Only stale events: keep the state, but surface the counters.
    return stats.stale === state.stats.stale ? state : { ...state, stats, inserted: [] }
  }

  const finalRows: Map<string, FeedAlert> = byId ?? new Map(state.byId)
  let order: readonly string[]
  if (!membershipChanged) order = state.order
  else if (replaced) order = sortIds(finalRows)
  else {
    // Incremental: drop removed ids, binary-insert new ones. Avoids a full
    // sort of a 5k page on every animation frame during bursts.
    const next = removed.size ? state.order.filter((id) => !removed.has(id)) : [...state.order]
    for (const id of new Set([...inserted, ...moved])) {
      const a = finalRows.get(id)
      if (!a) continue
      let lo = 0
      let hi = next.length
      while (lo < hi) {
        const mid = (lo + hi) >>> 1
        if (compareFeed(finalRows.get(next[mid]!)!, a) <= 0) lo = mid + 1
        else hi = mid
      }
      next.splice(lo, 0, id)
    }
    order = next
  }
  if (order.length > MAX_ROWS) {
    for (const id of order.slice(MAX_ROWS)) {
      finalRows.delete(id)
      inserted.delete(id)
    }
    order = order.slice(0, MAX_ROWS)
  }

  return {
    byId: finalRows,
    order,
    tombstones: tombstones ?? state.tombstones,
    snapshotAt,
    history: history ?? state.history,
    nextCursor,
    snapshotOldest,
    inserted: replaced && inserted.size === 0 ? [] : [...inserted],
    stats,
  }
}

/** Apply an alert returned by a mutation (ack/resolve or a 409's `current`)
 * through the same version rule as the stream. */
export function applyAlert(state: FeedState, alert: FeedAlert): FeedState {
  return applyEvents(state, [{ type: 'alert.upsert', data: alert }])
}

/** Page older alerts independently of the live SSE window. */
export function appendHistory(state: FeedState, page: FeedPage): FeedState {
  const history = new Map(state.history)
  for (const alert of page.items) {
    if (state.byId.has(alert.id) || (state.tombstones.get(alert.id) ?? -1) >= alert.version) continue
    const existing = history.get(alert.id)
    if (existing && existing.version >= alert.version) continue
    if (!existing && history.size >= MAX_HISTORY) break
    history.set(alert.id, alert)
  }
  return { ...state, history, nextCursor: history.size === MAX_HISTORY ? null : page.next_cursor, inserted: [] }
}
