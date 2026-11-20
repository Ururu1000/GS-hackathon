import { useId } from 'react'
import { COLOR_TOKENS, type AlertStatus, type ColorToken, type FeedAlert, type Severity } from '../api/types'
import { fmtCount } from '../lib/time'

export const SEVERITY_LABEL: Record<Severity, string> = {
  critical: 'Critical',
  error: 'Error',
  warning: 'Warning',
  info: 'Info',
}

export const STATUS_LABEL: Record<AlertStatus, string> = {
  open: 'Open',
  acked: 'Acknowledged',
  resolved: 'Resolved',
}

/** Actor ids as people read them: `user:sam` → "sam", `system` → "Beacon". */
export function actorName(actor: string | null): string {
  if (!actor) return 'someone'
  if (actor === 'system') return 'Beacon'
  if (actor.startsWith('rule:')) return 'an alert rule'
  return actor.replace(/^user:/, '')
}

/** `resolved_reason` codes in plain words; null when a person resolved it by hand. */
export function resolvedReasonText(reason: string | null): string | null {
  switch (reason) {
    case null:
    case 'user':
      return null
    case 'quiet_for':
      return 'no new events for a while'
    case 'recovered':
      return 'the source reported it recovered'
    default:
      return reason.replace(/_/g, ' ')
  }
}

/** "Resolved by sam", or "Resolved automatically: no new events for a while". */
export function resolvedText(by: string | null, reason: string | null): string {
  const why = resolvedReasonText(reason)
  if (by === 'system') return why ? `Resolved automatically: ${why}` : 'Resolved automatically'
  return `Resolved by ${actorName(by)}${why ? `: ${why}` : ''}`
}

/** Unknown colour tokens fall back to grey (contract: render, don't fail). */
export const colorToken = (c: string): ColorToken => ((COLOR_TOKENS as readonly string[]).includes(c) ? (c as ColorToken) : 'grey')

/** Severity reads from silhouette and weight, not colour alone: solid octagon,
 * solid circle, solid triangle, then an outlined circle for info. The glyph is
 * cut out with a mask so it shows the row background in every state. */
const SHAPE: Record<Exclude<Severity, 'info'>, React.ReactNode> = {
  critical: <path d="M8.3 2.75h7.4l5.55 5.55v7.4l-5.55 5.55H8.3l-5.55-5.55V8.3z" />,
  error: <circle cx="12" cy="12" r="9.75" />,
  warning: <path d="M12 3.25 21.75 20.25H2.25z" />,
}
const CUT: Record<Exclude<Severity, 'info'>, React.ReactNode> = {
  critical: (
    <>
      <path d="M12 7.25v5.5" />
      <circle cx="12" cy="16.6" r="1.4" fill="#000" stroke="none" />
    </>
  ),
  error: <path d="M8.75 8.75l6.5 6.5M15.25 8.75l-6.5 6.5" />,
  warning: (
    <>
      <path d="M12 9.5v4.25" />
      <circle cx="12" cy="17.05" r="1.3" fill="#000" stroke="none" />
    </>
  ),
}

export function SeverityIcon({ severity, size = 16 }: { severity: Severity; size?: number }) {
  const label = SEVERITY_LABEL[severity] ?? severity
  const mask = `sev-${useId().replace(/[^\w-]/g, '')}`
  const sev = severity in CUT ? (severity as keyof typeof CUT) : null
  return (
    <svg className={`sev-icon sev-${severity}`} width={size} height={size} viewBox="0 0 24 24" role="img" aria-label={label}>
      <title>{label}</title>
      {sev ? (
        <>
          <mask id={mask}>
            <rect width="24" height="24" fill="#fff" />
            <g fill="none" stroke="#000" strokeWidth="2.4" strokeLinecap="round">{CUT[sev]}</g>
          </mask>
          <g mask={`url(#${mask})`} fill="currentColor" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round">
            {SHAPE[sev]}
          </g>
        </>
      ) : (
        <g fill="none" stroke="currentColor" strokeLinecap="round">
          <circle cx="12" cy="12" r="9.25" strokeWidth="1.75" />
          <path d="M12 11v5.5" strokeWidth="2.2" />
          <circle cx="12" cy="7.6" r="1.3" fill="currentColor" stroke="none" />
        </g>
      )}
    </svg>
  )
}

/** Icon, plus the word when there's room for it (detail header). */
export function SeverityBadge({ severity, withLabel = false }: { severity: Severity; withLabel?: boolean }) {
  if (!withLabel) return <SeverityIcon severity={severity} />
  return (
    <span className={`sev-badge sev-${severity}`}>
      <SeverityIcon severity={severity} size={14} />
      {SEVERITY_LABEL[severity] ?? severity}
    </span>
  )
}

/** Plain-text state. `quiet` hides "open": it's the default, so a list full of it is noise. */
export function StatusPill({ alert, quiet = false }: { alert: FeedAlert; quiet?: boolean }) {
  if (quiet && alert.status === 'open') return <span className="sr-only">Open</span>
  const who = alert.status === 'acked' ? alert.acked_by : alert.status === 'resolved' ? alert.resolved_by : null
  const title = alert.status === 'resolved' ? resolvedText(alert.resolved_by, alert.resolved_reason) : who ? `${STATUS_LABEL[alert.status]} by ${actorName(who)}` : undefined
  return (
    <span className={`status status-${alert.status}`} title={title}>
      {STATUS_LABEL[alert.status] ?? alert.status}
      {who && who !== 'system' ? <span className="status-who"> · {actorName(who)}</span> : null}
    </span>
  )
}

/** Live dedup counter ("3 times"). Keyed by value so the CSS pulse replays on change. */
export function CountBadge({ count }: { count: number }) {
  if (count <= 1) return null
  return (
    <span key={count} className="count count-bump" title={`${count.toLocaleString()} matching events`}>
      {fmtCount(count)} times
    </span>
  )
}
