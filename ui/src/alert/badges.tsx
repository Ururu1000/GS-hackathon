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
  acked: 'Acked',
  resolved: 'Resolved',
}

/** Unknown colour tokens fall back to grey (contract: render, don't fail). */
export const colorToken = (c: string): ColorToken => ((COLOR_TOKENS as readonly string[]).includes(c) ? (c as ColorToken) : 'grey')

/** Severity is carried by silhouette (octagon / circle-x / triangle / circle-i), not by colour alone. */
const GLYPH: Record<Severity, React.ReactNode> = {
  critical: (
    // Solid, with the "!" cut out: the heaviest glyph for the heaviest severity.
    <path
      d="M8.6 2.5h6.8l6.1 6.1v6.8l-6.1 6.1H8.6l-6.1-6.1V8.6zM11 6.8h2v6.4h-2zM12 14.9a1.3 1.3 0 1 0 0 2.6 1.3 1.3 0 0 0 0-2.6z"
      fill="currentColor"
      fillRule="evenodd"
      strokeWidth="1.2"
    />
  ),
  error: (
    <>
      <circle cx="12" cy="12" r="9.5" fill="currentColor" fillOpacity=".16" />
      <path d="M8.8 8.8l6.4 6.4M15.2 8.8l-6.4 6.4" />
    </>
  ),
  warning: (
    <>
      <path d="M12 3 22 20.5H2z" fill="currentColor" fillOpacity=".16" />
      <path d="M12 9.5v4.5M12 17.2h.01" />
    </>
  ),
  info: (
    <>
      <circle cx="12" cy="12" r="9.5" fill="currentColor" fillOpacity=".16" />
      <path d="M12 11v5.5M12 7.6h.01" />
    </>
  ),
}

export function SeverityIcon({ severity, size = 16 }: { severity: Severity; size?: number }) {
  const label = SEVERITY_LABEL[severity] ?? severity
  return (
    <svg
      className={`sev-icon sev-${severity}`}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      role="img"
      aria-label={label}
    >
      <title>{label}</title>
      {GLYPH[severity] ?? GLYPH.info}
    </svg>
  )
}

/** Icon, plus the word when there's room for it (detail header). */
export function SeverityBadge({ severity, withLabel = false }: { severity: Severity; withLabel?: boolean }) {
  if (!withLabel) return <SeverityIcon severity={severity} />
  return (
    <span className="sev-badge">
      <SeverityIcon severity={severity} />
      {SEVERITY_LABEL[severity] ?? severity}
    </span>
  )
}

/** Plain-text state. `quiet` hides "open": it's the default, so a list full of it is noise. */
export function StatusPill({ alert, quiet = false }: { alert: FeedAlert; quiet?: boolean }) {
  if (quiet && alert.status === 'open') return <span className="sr-only">Open</span>
  const who = alert.status === 'acked' ? alert.acked_by : alert.status === 'resolved' ? alert.resolved_by : null
  return (
    <span className={`status status-${alert.status}`} title={who ? `${STATUS_LABEL[alert.status]} by ${who}` : undefined}>
      {STATUS_LABEL[alert.status] ?? alert.status}
      {who ? <span className="status-who"> · {who.replace(/^user:/, '')}</span> : null}
    </span>
  )
}

/** Live dedup counter. Keyed by value so the CSS pulse replays on change. */
export function CountBadge({ count }: { count: number }) {
  if (count <= 1) return null
  return (
    <span key={count} className="count count-bump" title={`${count.toLocaleString()} matching events folded into this alert`}>
      ×{fmtCount(count)}
    </span>
  )
}
