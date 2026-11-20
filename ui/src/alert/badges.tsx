import { COLOR_TOKENS, type AlertStatus, type ColorToken, type FeedAlert, type Severity } from '../api/types'
import { fmtCount } from '../lib/time'

// Shape + text, never colour alone (colour-blind safe).
const SEVERITY_ICON: Record<Severity, string> = {
  critical: '▲',
  error: '■',
  warning: '◆',
  info: '●',
}

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

export function SeverityBadge({ severity }: { severity: Severity }) {
  return (
    <span className={`sev sev-${severity}`}>
      <span aria-hidden="true">{SEVERITY_ICON[severity] ?? '●'}</span>
      {SEVERITY_LABEL[severity] ?? severity}
    </span>
  )
}

export function StatusPill({ alert }: { alert: FeedAlert }) {
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
  if (count <= 1) return <span className="count count-one" title="1 event">×1</span>
  return (
    <span key={count} className="count count-bump" title={`${count.toLocaleString()} matching events folded into this alert`}>
      ×{fmtCount(count)}
    </span>
  )
}
