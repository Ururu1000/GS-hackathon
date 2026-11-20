import { useEffect, useRef, useState } from 'react'
import { getAlert } from '../api/client'
import type { AlertDetail as Detail, DeliveryStatus, FeedAlert, TimelineEntry } from '../api/types'
import type { TransitionOp } from '../api/client'
import { absTime, relTime } from '../lib/time'
import { CheckIcon, CloseIcon, CopyIcon } from '../lib/icons'
import { actorName, colorToken, CountBadge, resolvedReasonText, resolvedText, SeverityBadge, STATUS_LABEL } from './badges'

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
      <header className="head">
        <div className="head-top">
          <SeverityBadge severity={a.severity} withLabel />
          <button className="icon-btn close" onClick={onClose} aria-label="Close (Esc)" title="Close (Esc)">
            <CloseIcon />
          </button>
        </div>
        <h2 id="detail-title" ref={headingRef} tabIndex={-1}>
          {a.title}
        </h2>
        {a.body && <p className="head-body">{a.body}</p>}

        <StatusLine alert={a} now={now} />

        {a.status !== 'resolved' && (
          <div className="actions">
            {a.status === 'open' && (
              <button className="btn btn-primary" disabled={pending !== null} onClick={() => onAct('ack')} aria-keyshortcuts="a" title="Shortcut: a">
                {pending === 'ack' ? 'Acknowledging…' : <span><b className="key">A</b>cknowledge</span>}
              </button>
            )}
            <button
              className={`btn ${a.status === 'acked' ? 'btn-primary' : ''}`}
              disabled={pending !== null}
              onClick={() => onAct('resolve')}
              aria-keyshortcuts="r"
              title="Shortcut: r"
            >
              {pending === 'resolve' ? 'Resolving…' : <span>Mark <b className="key">R</b>esolved</span>}
            </button>
          </div>
        )}
      </header>

      {a.fields.length > 0 && (
        <section className="section">
          <dl className="fields">
            {a.fields.map((f) => (
              <div key={f.path} className="kv">
                <dt>{f.label}</dt>
                <dd>
                  {f.value}
                  {isId(f.label) && <CopyButton value={f.value} label={f.label} />}
                </dd>
              </div>
            ))}
          </dl>
        </section>
      )}

      <section className="section">
        {error && <div className="placeholder error">Couldn’t load this part. It will retry automatically.</div>}
        {!detail && !error && <div className="placeholder">Loading…</div>}
        {detail && <Deliveries items={detail.deliveries} now={now} />}
      </section>

      <section className="section">
        <h3>History</h3>
        {detail && (
          <ol className="timeline">
            {[...detail.timeline].reverse().map((t, i) => (
              <li key={`${t.at}-${i}`} className={`tl tl-${t.kind}`}>
                <span className="tl-at" title={absTime(t.at)}>{relTime(t.at, now)}</span>
                <span className="tl-msg">{timelineText(t)}</span>
              </li>
            ))}
          </ol>
        )}
        {!detail && !error && <div className="placeholder">Loading…</div>}
      </section>

      <section className="section">
        <dl className="fields">
          <div className="kv"><dt>Team</dt><dd>{a.team}</dd></div>
          <div className="kv"><dt>Alert rule</dt><dd>{a.rule.name}</dd></div>
          {detail?.runbook && <div className="kv"><dt>Runbook</dt><dd>{detail.runbook.title}</dd></div>}
          <div className="kv"><dt>Started</dt><dd>{absTime(a.first_seen)}</dd></div>
          <div className="kv"><dt>Latest</dt><dd>{absTime(a.last_seen)}</dd></div>
        </dl>
      </section>

      <details className="section tech">
        <summary>Technical details</summary>
        <dl className="fields">
          <div className="kv"><dt>Alert ID</dt><dd><code>{a.id}</code><CopyButton value={a.id} label="alert ID" /></dd></div>
          <div className="kv"><dt>Rule</dt><dd><code>{a.rule.id}</code> <span className="muted">rev {a.rule.revision}</span></dd></div>
          <div className="kv"><dt>Source</dt><dd><code>{a.source}</code></dd></div>
          <div className="kv"><dt>Schema</dt><dd><code>{a.schema}</code></dd></div>
          {a.fields.map((f) => (
            <div key={f.path} className="kv"><dt>{f.label}</dt><dd><code>{f.path}</code></dd></div>
          ))}
        </dl>
        {detail && detail.samples.length > 0 && (
          <>
            <h4>Raw events</h4>
            {detail.samples.map((s) => (
              <details key={s.event_id} className="sample">
                <summary>
                  <code>{s.event_id}</code> <span className="muted">· {absTime(s.received_at)}</span>
                </summary>
                <pre>{JSON.stringify(s.raw, null, 2)}</pre>
              </details>
            ))}
          </>
        )}
      </details>
    </article>
  )
}

/** "Open 17s ago · 6 times" in one line. */
function StatusLine({ alert: a, now }: { alert: FeedAlert; now: number }) {
  const at = (iso: string | null) => <span title={absTime(iso)}>{relTime(iso, now)}</span>
  const why = a.status === 'resolved' ? resolvedReasonText(a.resolved_reason) : null
  return (
    <p className="state-line" role="status">
      <strong className={`status-${a.status}`}>{STATUS_LABEL[a.status]}</strong>{' '}
      {a.status === 'acked' && <>by {actorName(a.acked_by)} {at(a.acked_at)}</>}
      {a.status === 'resolved' && <>{a.resolved_by === 'system' ? 'automatically' : `by ${actorName(a.resolved_by)}`} {at(a.resolved_at)}</>}
      {a.status === 'open' && at(a.first_seen)}
      {why && <span className="muted"> ({why})</span>}
      {a.count > 1 && <span className="muted"> · <CountBadge count={a.count} /></span>}
    </p>
  )
}

/** Status changes are phrased from their kind; other entries use the server's message. */
function timelineText(t: TimelineEntry): string {
  if (t.kind === 'acked') return `Acknowledged by ${actorName(t.actor)}`
  if (t.kind === 'resolved') {
    const reason = t.message.match(/\((\w+)\)\s*$/)?.[1] ?? null
    return resolvedText(t.actor, reason)
  }
  if (t.kind === 'reopened') return 'Reopened: the problem came back'
  const person = t.actor !== 'system' && !t.actor.startsWith('rule:')
  return person ? `${t.message} · ${actorName(t.actor)}` : t.message
}

/** Identifier fields ("Workflow ID", "Trade ID") get a copy button. */
const isId = (label: string) => /\bid$/i.test(label.trim())

function CopyButton({ value, label }: { value: string; label: string }) {
  const [copied, setCopied] = useState(false)
  useEffect(() => {
    if (!copied) return
    const t = setTimeout(() => setCopied(false), 1500)
    return () => clearTimeout(t)
  }, [copied])
  // navigator.clipboard exists only on https or localhost.
  const copy = () => navigator.clipboard?.writeText(value).then(() => setCopied(true), () => {})
  return (
    <button className={`copy-btn ${copied ? 'is-copied' : ''}`} onClick={copy} aria-label={copied ? 'Copied' : `Copy ${label}`} title={copied ? 'Copied' : 'Copy'}>
      {copied ? <CheckIcon size={14} /> : <CopyIcon size={14} />}
    </button>
  )
}

const CHANNEL_LABEL: Record<string, string> = { email: 'Email', sms: 'SMS', phone: 'Call', ui: 'In-app' }
const STATE_LABEL: Record<string, string> = {
  pending: 'Queued',
  in_flight: 'Sending…',
  sent: 'Sent',
  cancelled: 'Skipped',
  dead: 'Failed',
}
const STATE_HINT: Record<string, string> = {
  cancelled: 'Alert was handled before this went out',
}

/** One line per notification: "Email to alexey  alexey@…   Sent 2h ago". */
function Deliveries({ items, now }: { items: DeliveryStatus[]; now: number }) {
  if (items.length === 0) return <div className="placeholder">No notifications sent</div>
  const sorted = [...items].sort((a, b) => a.recipient.localeCompare(b.recipient) || a.channel.localeCompare(b.channel))
  return (
    <ul className="deliveries">
      {sorted.map((d) => {
        const at = d.sent_at ?? d.updated_at
        return (
          <li key={d.id} className="dl">
            <div className="dl-main">
              {CHANNEL_LABEL[d.channel] ?? d.channel} to <strong>{actorName(d.recipient)}</strong>
              <span className="muted"> {d.address}</span>
              {d.last_error && <div className="small err-text">{d.last_error}</div>}
            </div>
            <div className="dl-side">
              <span className={`dstate dstate-${d.state}`} title={STATE_HINT[d.state]}>{STATE_LABEL[d.state] ?? d.state}</span>
              {d.attempts > 1 && <span className="muted"> after {d.attempts} tries</span>}
              <span className="muted" title={absTime(at)}> · {relTime(at, now)}</span>
            </div>
          </li>
        )
      })}
    </ul>
  )
}
