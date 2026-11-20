import { useVirtualizer } from '@tanstack/react-virtual'
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { FeedAlert } from '../api/types'
import { AlertRow } from './AlertRow'

export const ROW_H = 66
const AT_TOP = ROW_H / 2

interface Props {
  alerts: readonly FeedAlert[]
  /** Ids that entered the page in the latest stream batch. */
  inserted: readonly string[]
  selectedId: string | null
  now: number
  onSelect: (id: string) => void
  empty: React.ReactNode
}

/** Virtualized feed. Keeps the rows under the user's eyes still when new
 * alerts land above them, and counts those in a "new alerts" pill. */
export function FeedList({ alerts, inserted, selectedId, now, onSelect, empty }: Props) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const prevIds = useRef<string[]>([])
  const [unseen, setUnseen] = useState<ReadonlySet<string>>(new Set())
  const [fresh, setFresh] = useState<ReadonlySet<string>>(new Set())

  const virtualizer = useVirtualizer({
    count: alerts.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_H,
    overscan: 10,
    getItemKey: (i) => alerts[i]?.id ?? i,
  })

  // Scroll anchoring: if the user is scrolled down, keep the first visible
  // row in place when rows are inserted or removed above it.
  useLayoutEffect(() => {
    const el = scrollRef.current
    const prev = prevIds.current
    const ids = alerts.map((a) => a.id)
    prevIds.current = ids
    if (!el || el.scrollTop <= AT_TOP || prev.length === 0) return
    const oldIdx = Math.floor(el.scrollTop / ROW_H)
    const anchor = prev[oldIdx]
    if (anchor === undefined) return
    const newIdx = ids.indexOf(anchor)
    if (newIdx >= 0 && newIdx !== oldIdx) el.scrollTop += (newIdx - oldIdx) * ROW_H
  }, [alerts])

  // New alerts: highlight briefly; if they landed above the viewport, count
  // them in the pill until the user scrolls back to the top.
  useEffect(() => {
    if (inserted.length === 0) return
    const el = scrollRef.current
    const visible = new Set(alerts.map((a) => a.id))
    const ids = inserted.filter((id) => visible.has(id))
    if (ids.length === 0) return
    setFresh((f) => new Set([...f, ...ids]))
    const t = setTimeout(() => setFresh((f) => new Set([...f].filter((id) => !ids.includes(id)))), 3000)
    if (el && el.scrollTop > AT_TOP) {
      const firstVisible = Math.floor(el.scrollTop / ROW_H)
      const above = ids.filter((id) => alerts.findIndex((a) => a.id === id) < firstVisible)
      if (above.length) setUnseen((u) => new Set([...u, ...above]))
    }
    return () => clearTimeout(t)
  }, [inserted])

  // Drop pill entries that were filtered or removed away.
  const idSet = useMemo(() => new Set(alerts.map((a) => a.id)), [alerts])
  let unseenCount = 0
  for (const id of unseen) if (idSet.has(id)) unseenCount++

  const onScroll = useCallback(() => {
    const el = scrollRef.current
    if (el && el.scrollTop <= AT_TOP) setUnseen((u) => (u.size ? new Set() : u))
  }, [])

  const jumpToTop = () => {
    scrollRef.current?.scrollTo({ top: 0, behavior: 'smooth' })
    setUnseen(new Set())
  }

  // Keep the keyboard selection in view.
  useEffect(() => {
    if (!selectedId) return
    const idx = alerts.findIndex((a) => a.id === selectedId)
    if (idx >= 0) virtualizer.scrollToIndex(idx, { align: 'auto' })
  }, [selectedId])

  return (
    <div className="feed-list-wrap">
      {unseenCount > 0 && (
        <button className="new-pill" onClick={jumpToTop}>
          ↑ {unseenCount} new alert{unseenCount === 1 ? '' : 's'}
        </button>
      )}
      <div
        ref={scrollRef}
        className="feed-list"
        onScroll={onScroll}
        role="listbox"
        aria-label="Alerts"
        aria-activedescendant={selectedId ? `row-${selectedId}` : undefined}
        tabIndex={0}
      >
        {alerts.length === 0 ? (
          empty
        ) : (
          <div style={{ height: virtualizer.getTotalSize(), position: 'relative' }}>
            {virtualizer.getVirtualItems().map((vi) => {
              const a = alerts[vi.index]
              if (!a) return null
              return (
                <div key={vi.key} className="vrow" style={{ height: ROW_H, transform: `translateY(${vi.start}px)` }}>
                  <AlertRow alert={a} selected={a.id === selectedId} fresh={fresh.has(a.id)} now={now} onSelect={onSelect} />
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
