import { useEffect, useRef, useState } from 'react'
import { getAlert } from '../api/client'
import type { AlertDetail as Detail, DeliveryStatus, FeedAlert } from '../api/types'
import type { TransitionOp } from '../api/client'
import { absTime, relTime } from '../lib/time'
import { colorToken, CountBadge, SeverityBadge, StatusPill } from './badges'

interface Props {
  /** Live projection from the feed store (count/status update in place). */
  alert: FeedAlert
  /** Changes on every (re)snapshot; triggers a refetch. */
  snapshotAt: string | null
  now: number
  pending: TransitionOp | null
  onAct: (op: TransitionOp) => void
  onClose: () => void
}

const REFETCH_MIN_MS = 1500

/** Detail data (samples, timeline) refetches when the alert's version changes
 * (throttled) and after every reconnect. */
function useDetail(id: string, version: number, snapshotAt: string | null) {
  const [detail, setDetail] = useState<Detail | null>(null)
  const [error, setError] = useState<string | null>(null)
  const last = useRef(0)

  useEffect(() => {
    setDetail(null)
    setError(null)
    last.current = 0
  }, [id])

  useEffect(() => {
    const ctl = new AbortController()
    const wait = Math.max(0, last.current + REFETCH_MIN_MS - Date.now())
    const t = setTimeout(() => {
      last.current = Date.now()
      getAlert(id, ctl.signal)
        .then((d) => {
          setDetail(d)
          setError(null)
        })
        .catch((e: unknown) => {
          if (!ctl.signal.aborted) setError(e instanceof Error ? e.message : String(e))
        })
    }, wait)
    return () => {
      clearTimeout(t)
      ctl.abort()
    }
  }, [id, version, snapshotAt])

  return { detail, error }
}

export function AlertDetail({ alert: a, snapshotAt, now, pending, onAct, onClose }: Props) {
  const { detail, error } = useDetail(a.id, a.version, snapshotAt)
  const headingRef = useRef<HTMLHeadingElement>(null)
  useEffect(() => headingRef.current?.focus({ preventScroll: true }), [a.id])

  return (
    <article className={`detail tok-${colorToken(a.color)}`} aria-labelledby="detail-title">
      <header className="card">
        <div className="card-stripe" aria-hidden="true" />
        <div className="card-main">
          <div className="card-top">
            <SeverityBadge severity={a.severity} />
            <StatusPill alert={a} />
            <CountBadge count={a.count} />
            <button className="icon-btn close" onClick={onClose} aria-label="Close detail (Esc)" title="Close (Esc)">
              ✕
            </button>
          </div>
          <h2 id="detail-title" ref={headingRef} tabIndex={-1}>
            {a.title}
          </h2>
          {a.body && <p className="card-body">{a.body}</p>}
          <div className="actions">
            <button
              className="btn btn-ack"
              disabled={a.status !== 'open' || pending !== null}
              onClick={() => onAct('ack')}
              aria-keyshortcuts="a"
            >
              {pending === 'ack' ? 'Acknowledging…' : 'Acknowledge'} <kbd>a</kbd>
            </button>
            <button
              className="btn btn-resolve"
              disabled={a.status === 'resolved' || pending !== null}
              onClick={() => onAct('resolve')}
              aria-keyshortcuts="r"
            >
              {pending === 'resolve' ? 'Resolving…' : 'Resolve'} <kbd>r</kbd>
            </button>
          </div>
        </div>
      </header>

      {a.fields.length > 0 && (
        <section className="section">
          <h3>Fields</h3>
          <dl className="fields">
            {a.fields.map((f) => (
              <div key={f.path} className="kv">
                <dt title={f.path}>{f.label}</dt>
                <dd>{f.value}</dd>
              </div>
            ))}
          </dl>
        </section>
      )}

      <section className="section">
        <h3>Runbook</h3>
        <div className="placeholder">
          {detail?.runbook ? (
            <>Runbook “{detail.runbook.title}” (rev {detail.runbook.revision}). Step rendering lands with runbooks (M5).</>
          ) : (
            <>No runbook yet. Matching runbooks, filled-in next steps and action buttons appear here once runbooks ship (M5).</>
          )}
        </div>
      </section>

      <section className="section">
        <h3>About</h3>
        <dl className="fields">
          <div className="kv"><dt>Rule</dt><dd>{a.rule.name} <span className="muted">({a.rule.id} · rev {a.rule.revision})</span></dd></div>
          <div className="kv"><dt>Team</dt><dd>{a.team}</dd></div>
          <div className="kv"><dt>Source</dt><dd>{a.source} <span className="muted">· {a.schema}</span></dd></div>
          <div className="kv"><dt>First seen</dt><dd>{absTime(a.first_seen)} <span className="muted">({relTime(a.first_seen, now)})</span></dd></div>
          <div className="kv"><dt>Last seen</dt><dd>{absTime(a.last_seen)} <span className="muted">({relTime(a.last_seen, now)})</span></dd></div>
          <div className="kv"><dt>Events</dt><dd>{a.count.toLocaleString()}</dd></div>
          {a.acked_at && <div className="kv"><dt>Acked</dt><dd>{a.acked_by} · {absTime(a.acked_at)}</dd></div>}
          {a.resolved_at && (
            <div className="kv"><dt>Resolved</dt><dd>{a.resolved_by} · {absTime(a.resolved_at)} {a.resolved_reason && <span className="muted">({a.resolved_reason})</span>}</dd></div>
          )}
        </dl>
      </section>

      <section className="section">
        <h3>Timeline</h3>
        {error && <div className="placeholder error">Could not load details: {error}</div>}
        {!detail && !error && <div className="placeholder">Loading…</div>}
        {detail && (
          <ol className="timeline">
            {[...detail.timeline].reverse().map((t, i) => (
              <li key={`${t.at}-${i}`} className={`tl tl-${t.kind}`}>
                <span className="tl-at" title={absTime(t.at)}>{relTime(t.at, now)}</span>
                <span className="tl-msg">{t.message}</span>
                <span className="tl-actor">{t.actor}</span>
              </li>
            ))}
          </ol>
        )}
      </section>

      <section className="section">
        <h3>Deliveries</h3>
        {detail && <Deliveries items={detail.deliveries} now={now} />}
      </section>

      {detail && detail.samples.length > 0 && (
        <section className="section">
          <h3>Event samples <span className="muted">(raw payloads: the first few and the latest)</span></h3>
          {detail.samples.map((s) => (
            <details key={s.event_id} className="sample">
              <summary>
                <code>{s.event_id}</code> <span className="muted">· {absTime(s.received_at)}</span>
              </summary>
              <pre>{JSON.stringify(s.raw, null, 2)}</pre>
            </details>
          ))}
        </section>
      )}
    </article>
  )
}

const STATE_LABEL: Record<string, string> = {
  pending: 'Pending',
  in_flight: 'Sending',
  sent: 'Sent',
  cancelled: 'Cancelled',
  dead: 'Failed',
}
const CHANNEL_LABEL: Record<string, string> = { email: 'Email', sms: 'SMS', phone: 'Phone call', ui: 'UI' }
const STATE_HINT: Record<string, string> = {
  cancelled: 'Not sent: the alert was acknowledged or resolved first',
  dead: 'Gave up after retries',
}

function Deliveries({ items, now }: { items: DeliveryStatus[]; now: number }) {
  if (items.length === 0) return <div className="placeholder">No external notifications for this alert: it is shown in the UI only.</div>
  const sorted = [...items].sort((a, b) => a.recipient.localeCompare(b.recipient) || a.channel.localeCompare(b.channel))
  return (
    <table className="deliveries">
      <thead>
        <tr><th>Recipient</th><th>Channel</th><th>Status</th><th>When</th></tr>
      </thead>
      <tbody>
        {sorted.map((d) => (
          <tr key={d.id}>
            <td>
              {d.recipient.replace(/^user:/, '')}
              <div className="muted small">{d.address}</div>
            </td>
            <td>{CHANNEL_LABEL[d.channel] ?? d.channel}</td>
            <td>
              <span className={`dstate dstate-${d.state}`} title={STATE_HINT[d.state]}>{STATE_LABEL[d.state] ?? d.state}</span>
              {d.attempts > 1 && <span className="muted small"> · {d.attempts} attempts</span>}
              {d.last_error && <div className="small err-text">{d.last_error}</div>}
            </td>
            <td className="muted small" title={absTime(d.sent_at ?? d.updated_at)}>{relTime(d.sent_at ?? d.updated_at, now)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
