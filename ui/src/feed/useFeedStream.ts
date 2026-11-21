import { useCallback, useEffect, useRef, useState } from 'react'
import { errorText, loadAlertPage, streamUrl } from '../api/client'
import { STREAM_EVENT_TYPES, type FeedAlert, type StreamEvent } from '../api/types'
import { appendHistory, applyAlert, applyEvents, emptyFeed, type FeedState } from './feedStore'

export type Connection = 'connecting' | 'live' | 'reconnecting'

/** Subscribes to `GET /api/stream` for a scope and keeps the feed page.
 * Events are queued and applied once per animation frame (with a timer
 * fallback for background tabs), so a burst costs one React render. */
export function useFeedStream(team: string | null, recipient: string | null = null) {
  const [state, setState] = useState<FeedState>(emptyFeed)
  const [connection, setConnection] = useState<Connection>('connecting')
  const [historyError, setHistoryError] = useState<string | null>(null)
  const [historyLoading, setHistoryLoading] = useState(false)
  const historyRequest = useRef<AbortController | null>(null)
  const queue = useRef<StreamEvent[]>([])
  const scheduled = useRef<{ raf: number; timer: ReturnType<typeof setTimeout> } | null>(null)

  useEffect(() => {
    setState(emptyFeed)
    setConnection('connecting')
    setHistoryError(null)
    setHistoryLoading(false)
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
      es = new EventSource(streamUrl(team, recipient), { withCredentials: true })
      for (const type of STREAM_EVENT_TYPES) {
        es.addEventListener(type, (msg) => {
          if (closed) return
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
            historyRequest.current?.abort()
            historyRequest.current = null
            setHistoryLoading(false)
            setHistoryError(null)
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
      historyRequest.current?.abort()
      historyRequest.current = null
      if (retry) clearTimeout(retry)
      if (scheduled.current) {
        cancelAnimationFrame(scheduled.current.raf)
        clearTimeout(scheduled.current.timer)
        scheduled.current = null
      }
    }
  }, [team, recipient])

  const loadOlder = async () => {
    const cursor = state.nextCursor
    if (!cursor || historyRequest.current) return
    const controller = new AbortController()
    historyRequest.current = controller
    setHistoryLoading(true)
    setHistoryError(null)
    try {
      const page = await loadAlertPage(team, recipient, cursor, controller.signal)
      if (!controller.signal.aborted) setState((current) =>
        current.nextCursor === cursor && current.snapshotAt === state.snapshotAt ? appendHistory(current, page) : current)
    } catch (e) {
      if (!controller.signal.aborted) setHistoryError(errorText(e))
    } finally {
      if (historyRequest.current === controller) {
        historyRequest.current = null
        setHistoryLoading(false)
      }
    }
  }

  /** Feed a mutation result through the same version rule as the stream. */
  const applyLocal = useCallback((alert: FeedAlert) => setState((s) =>
    s.byId.has(alert.id) || s.history.has(alert.id) ? applyAlert(s, alert) : s), [])

  return { state, connection, applyLocal, loadOlder, historyError, historyLoading }
}
