import { memo } from 'react'
import type { FeedAlert } from '../api/types'
import { colorToken, CountBadge, SeverityBadge, StatusPill } from '../alert/badges'
import { relTime } from '../lib/time'

interface Props {
  alert: FeedAlert
  selected: boolean
  fresh: boolean
  /** Rounded to the minute by the caller, so rows don't re-render every tick. */
  now: number
  onSelect: (id: string) => void
}

export const AlertRow = memo(function AlertRow({ alert: a, selected, fresh, now, onSelect }: Props) {
  return (
    <div
      role="option"
      aria-selected={selected}
      id={`row-${a.id}`}
      className={`row tok-${colorToken(a.color)} ${a.status === 'resolved' ? 'is-resolved' : ''} ${selected ? 'is-selected' : ''} ${fresh ? 'is-fresh' : ''}`}
      onClick={() => onSelect(a.id)}
    >
      <SeverityBadge severity={a.severity} />
      <div className="row-main">
        <div className="row-title">
          <span className="row-title-text">{a.title}</span>
          <StatusPill alert={a} quiet />
        </div>
        <div className="row-sub">
          <span className="row-body">{a.body}</span>
          <span className="row-rule">{a.rule.name}</span>
        </div>
      </div>
      <div className="row-side">
        <CountBadge count={a.count} />
        <span className="row-last" title={`Last event ${a.last_seen}`}>
          {relTime(a.last_seen, now)}
        </span>
      </div>
    </div>
  )
})
