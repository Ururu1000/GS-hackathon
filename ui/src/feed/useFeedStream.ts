import { useCallback, useEffect, useRef, useState } from 'react'
import { streamUrl } from '../api/client'
import { STREAM_EVENT_TYPES, type FeedAlert, type StreamEvent } from '../api/types'
import { applyAlert, applyEvents, emptyFeed, type FeedState } from './feedStore'

export type Connection = 'connecting' | 'live' | 'reconnecting'

/** Subscribes to `GET /api/stream` for a scope and keeps the feed page.
 * Events are queued and applied once per animation frame (with a timer
 * fallback for background tabs), so a burst costs one React render. */
export function useFeedStream(team: string | null) {
  const [state, setState] = useState<FeedState>(emptyFeed)
  const [connection, setConnection] = useState<Connection>('connecting')
  const queue = useRef<StreamEvent[]>([])
  const scheduled = useRef<{ raf: number; timer: ReturnType<typeof setTimeout> } | null>(null)

  useEffect(() => {
    setState(emptyFeed)
    setConnection('connecting')
    queue.current = []

    const flush = () => {
      if (scheduled.current) {
        cancelAnimationFrame(scheduled.current.raf)
        clearTimeout(scheduled.current.timer)
        scheduled.current = null
      }
      const batch = queue.current
      if (batch.length === 0) return
      queue.current = []
      setState((s) => applyEvents(s, batch))
    }
    const schedule = () => {
      if (scheduled.current) return
      scheduled.current = { raf: requestAnimationFrame(flush), timer: setTimeout(flush, 250) }
    }

    let es: EventSource | null = null
    let retry: ReturnType<typeof setTimeout> | null = null
    let closed = false

    const connect = () => {
      es = new EventSource(streamUrl(team), { withCredentials: true })
      for (const type of STREAM_EVENT_TYPES) {
        es.addEventListener(type, (msg) => {
          let data: unknown
          try {
            data = JSON.parse((msg as MessageEvent<string>).data)
          } catch {
            console.warn('beacon: bad SSE payload', type)
            return
          }
          if (type === 'snapshot') {
            // Anything queued before the snapshot is superseded by it.
            queue.current = []
            setConnection('live')
          }
          queue.current.push({ type, data } as StreamEvent)
          schedule()
        })
      }
      es.onerror = () => {
        if (closed || !es) return
        setConnection('reconnecting')
        // CONNECTING: the browser retries by itself (server sent `retry:`).
        // CLOSED: e.g. an HTTP error status; retry ourselves.
        if (es.readyState === EventSource.CLOSED) {
          es = null
          retry = setTimeout(connect, 2000)
        }
      }
    }
    connect()

    return () => {
      closed = true
      es?.close()
      if (retry) clearTimeout(retry)
      if (scheduled.current) {
        cancelAnimationFrame(scheduled.current.raf)
        clearTimeout(scheduled.current.timer)
        scheduled.current = null
      }
    }
  }, [team])

  /** Feed a mutation result through the same version rule as the stream. */
  const applyLocal = useCallback((alert: FeedAlert) => setState((s) => applyAlert(s, alert)), [])

  return { state, connection, applyLocal }
}
