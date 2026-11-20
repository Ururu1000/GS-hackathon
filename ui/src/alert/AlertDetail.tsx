import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { getAlert } from '../api/client'
import type { AlertDetail as Detail, DeliveryStatus, FeedAlert, Occurrences, TimelineEntry } from '../api/types'
import type { TransitionOp } from '../api/client'
import { absTime, relTime } from '../lib/time'
import { CheckIcon, CloseIcon, CopyIcon } from '../lib/icons'
import { maskAddress } from '../lib/redact'
import { actorName, colorToken, CountBadge, resolvedReasonText, resolvedText, SeverityBadge, STATUS_LABEL } from './badges'
import { LIVE_STATES, Runbook } from './Runbook'

interface Props {
  /** Live projection from the feed store (count/status update in place). */
  alert: FeedAlert
  /** Changes on every (re)snapshot; triggers a refetch. */
  snapshotAt: string | null
  now: number
  pending: TransitionOp | null
  onAct: (op: TransitionOp) => void
  onClose: () => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
  onNavigate: (hash: string) => void
}

const SuggestChange = lazy(() => import('../proposals/SuggestChange').then((m) => ({ default: m.SuggestChange })))

const REFETCH_MIN_MS = 1500
const POLL_MS = 2000

/** Detail data (samples, runbook, timeline) refetches when the alert's version
 * changes (throttled), after every reconnect, right after the user acts, and
 * every 2 s while a runbook step is queued, running or awaiting approval (the
 * backend may not bump the alert version for those). */
function useDetail(id: string, version: number, snapshotAt: string | null) {
  const [detail, setDetail] = useState<Detail | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [tick, setTick] = useState(0)
  const last = useRef(0)

  useEffect(() => {
    setDetail(null)
    setError(null)
    last.current = 0
  }, [id])

  const live = !!detail?.runbook?.steps.some((s) => LIVE_STATES.includes(s.state))
  useEffect(() => {
    if (!live) return
    const t = setInterval(() => setTick((n) => n + 1), POLL_MS)
    return () => clearInterval(t)
  }, [live])

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
  }, [id, version, snapshotAt, tick])

  const reload = () => {
    last.current = 0
    setTick((n) => n + 1)
  }
  return { detail, error, reload }
}

export function AlertDetail({ alert: a, snapshotAt, now, pending, onAct, onClose, onToast, onNavigate }: Props) {
  const { detail, error, reload } = useDetail(a.id, a.version, snapshotAt)
  const [suggesting, setSuggesting] = useState(false)
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

      {detail?.occurrences && <OccurrenceChart series={detail.occurrences} />}

      {detail?.runbook && <Runbook runbook={detail.runbook} alert={a} now={now} onChanged={reload} onToast={onToast} />}

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
                <span className="tl-msg">{timelineText(t, a)}</span>
              </li>
            ))}
          </ol>
        )}
        {!detail && !error && <div className="placeholder">Loading…</div>}
      </section>

      <section className="section">
        <dl className="fields">
          <div className="kv"><dt>Team</dt><dd>{a.team}</dd></div>
          <div className="kv"><dt>Alert rule</dt><dd><a href={`#/rules/${encodeURIComponent(a.rule.id)}`}>{a.rule.name}</a></dd></div>
          <div className="kv"><dt>Started</dt><dd>{absTime(a.first_seen)}</dd></div>
          <div className="kv"><dt>Latest</dt><dd>{absTime(a.last_seen)}</dd></div>
        </dl>
      </section>

      <section className="section suggest-row">
        <p className="small muted">Too noisy, wrong people, or missing something?</p>
        <button className="btn btn-sm" onClick={() => setSuggesting(true)}>Suggest a change</button>
      </section>

      {suggesting && (
        <Suspense fallback={null}>
          <SuggestChange alert={a} samples={detail?.samples ?? []} onClose={() => setSuggesting(false)} onToast={onToast} onNavigate={onNavigate} />
        </Suspense>
      )}

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
            <p className="hint">Payloads are redacted server-side, but may still contain sensitive operational data.</p>
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
function OccurrenceChart({ series }: { series: Occurrences }) {
  const buckets = series.counts.slice(-24)
  if (!buckets.length) return null
  const offset = series.counts.length - buckets.length
  const from = Date.parse(series.from) + offset * series.bucket_seconds * 1000
  const step = series.bucket_seconds * 1000
  const high = Math.max(1, ...buckets)
  const total = buckets.reduce((sum, count) => sum + count, 0)
  return (
    <section className="section">
      <h3>Occurrences</h3>
      <figure className="occurrence-chart">
        <div className="occurrence-bars" aria-hidden="true">
          {buckets.map((count, index) => (
            <div key={index} title={`${absTime(new Date(from + step * index).toISOString())}: ${count}`}
              style={{ height: `${count ? Math.max(6, (count / high) * 100) : 2}%` }} />
          ))}
        </div>
        <figcaption className="muted small">{total.toLocaleString()} events in {buckets.length} bucket{buckets.length === 1 ? '' : 's'}, starting {absTime(new Date(from).toISOString())}</figcaption>
      </figure>
      <details className="small">
        <summary>Occurrence counts</summary>
        <dl className="fields">
          {buckets.map((count, index) => <div key={index} className="kv">
            <dt>{absTime(new Date(from + step * index).toISOString())}</dt><dd>{count.toLocaleString()}</dd>
          </div>)}
        </dl>
      </details>
    </section>
  )
}

function StatusLine({ alert: a, now }: { alert: FeedAlert; now: number }) {
  const at = (iso: string | null) => <span title={absTime(iso)}>{relTime(iso, now)}</span>
  const why = a.status === 'resolved' ? resolvedReasonText(a.resolved_reason) : null
  return (
    <p className="state-line">
      <strong className={`status-${a.status}`}>{STATUS_LABEL[a.status]}</strong>{' '}
      {a.status === 'acked' && <>by {actorName(a.acked_by)} {at(a.acked_at)}</>}
      {a.status === 'resolved' && <>{a.resolved_by === 'system' ? 'automatically' : `by ${actorName(a.resolved_by)}`} {at(a.resolved_at)}</>}
      {a.status === 'open' && at(a.first_seen)}
      {why && <span className="muted"> ({why})</span>}
      {a.count > 1 && <span className="muted"> · <CountBadge count={a.count} /></span>}
    </p>
  )
}

/** Status changes are phrased from their kind; other entries use the server's message.
 * Only the alert's current resolution has a structured reason, so older resolves omit it. */
export function timelineText(t: TimelineEntry, a: Pick<FeedAlert, 'resolved_at' | 'resolved_reason'>): string {
  if (t.kind === 'acked') return `Acknowledged by ${actorName(t.actor)}`
  if (t.kind === 'resolved') return resolvedText(t.actor, a.resolved_at === t.at ? a.resolved_reason : null)
  if (t.kind === 'reopened') return 'Reopened: the problem came back'
  const person = t.actor !== 'system' && !t.actor.startsWith('rule:')
  return person ? `${t.message} · ${actorName(t.actor)}` : t.message
}

/** Identifier fields ("Workflow ID", "Trade ID") get a copy button. */
const isId = (label: string) => /\bid$/i.test(label.trim())

function CopyButton({ value, label }: { value: string; label: string }) {
  const [state, setState] = useState<'idle' | 'copied' | 'failed'>('idle')
  useEffect(() => {
    if (state === 'idle') return
    const t = setTimeout(() => setState('idle'), 1500)
    return () => clearTimeout(t)
  }, [state])
  // navigator.clipboard exists only on https or localhost; say so instead of failing silently.
  const copy = () => {
    if (!navigator.clipboard) return setState('failed')
    navigator.clipboard.writeText(value).then(() => setState('copied'), () => setState('failed'))
  }
  const text = state === 'copied' ? 'Copied' : state === 'failed' ? 'Copy failed: clipboard needs https or localhost' : 'Copy'
  return (
    <button className={`copy-btn ${state === 'copied' ? 'is-copied' : state === 'failed' ? 'is-failed' : ''}`} onClick={copy} aria-label={state === 'idle' ? `Copy ${label}` : text} title={text}>
      {state === 'copied' ? <CheckIcon size={14} /> : state === 'failed' ? <CloseIcon size={14} /> : <CopyIcon size={14} />}
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
              <span className="muted"> {maskAddress(d.channel, d.address)}</span>
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
