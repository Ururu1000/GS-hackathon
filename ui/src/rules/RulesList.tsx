// Rules list with fire counts (spec §7.3), plus the "new rule" templates.

import { useEffect, useMemo, useState } from 'react'
import { errorText, listRules } from '../api/client'
import type { RuleItem } from '../api/types'
import { colorToken, SeverityIcon } from '../alert/badges'
import { fmtCount, relTime } from '../lib/time'
import { TEMPLATES } from './draft'
import { CHANNEL_NAME } from './sections'

interface Props {
  team: string | null
  teamName: string
  onNavigate: (hash: string) => void
}

/** 24 hourly bars, scaled to the busiest hour. */
export function Sparkline({ values, label }: { values: number[]; label: string }) {
  const max = Math.max(1, ...values)
  const w = 72
  const h = 18
  const bw = w / values.length
  return (
    <svg className="spark" width={w} height={h} viewBox={`0 0 ${w} ${h}`} role="img" aria-label={label}>
      <title>{label}</title>
      {values.map((v, i) => {
        const bh = v === 0 ? 1 : Math.max(2, (v / max) * h)
        return <rect key={i} x={i * bw + 0.5} y={h - bh} width={Math.max(1, bw - 1)} height={bh} rx={0.5} className={v === 0 ? 'spark-zero' : ''} />
      })}
    </svg>
  )
}

function routeSummary(r: RuleItem['rule']): string {
  return r.route
    .map((x) => {
      const [kind, id] = x.to.split(':')
      const who = kind === 'oncall' ? `on-call ${id}` : kind === 'team' ? `team ${id}` : id
      const how = x.channels?.length ? x.channels.map((c) => CHANNEL_NAME[c] ?? c).join(' + ') : 'their preferences'
      return `${who} · ${how}`
    })
    .join('; ')
}

export function RulesList({ team, teamName, onNavigate }: Props) {
  const [items, setItems] = useState<RuleItem[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [picking, setPicking] = useState(false)

  useEffect(() => {
    setItems(null)
    listRules(team).then(setItems, (e: unknown) => setError(errorText(e)))
  }, [team])

  const shown = useMemo(() => {
    const q = query.trim().toLowerCase()
    return (items ?? []).filter((r) => !q || `${r.rule.name} ${r.rule.id} ${r.rule.team}`.toLowerCase().includes(q))
  }, [items, query])

  const now = Date.now()
  return (
    <div className="screen">
      <header className="screen-head">
        <div className="screen-title">
          <h1>Rules</h1>
          <span className="muted small">Which events become alerts for {team ? teamName : 'each team'}, who hears about them, and how they look.</span>
        </div>
        <span className="spacer" />
        <input className="input" type="search" placeholder="Find a rule" value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Find a rule" />
        <button className="btn btn-primary" onClick={() => setPicking(true)}>New rule</button>
      </header>

      {error && <div className="notice notice-error banner">Couldn’t load rules: {error}</div>}
      {!items && !error && <div className="empty"><p>Loading…</p></div>}
      {items && shown.length === 0 && (
        <div className="empty">
          <p>{items.length === 0 ? `No rules for ${teamName} yet.` : 'No rule matches that search.'}</p>
          {items.length === 0 && <button className="btn btn-primary" onClick={() => setPicking(true)}>Create the first one</button>}
        </div>
      )}
      {shown.length > 0 && (
        <div className="table-wrap">
          <table className="table rules-table">
            <thead>
              <tr>
                <th scope="col">Rule</th>
                <th scope="col">Team</th>
                <th scope="col">Notifies</th>
                <th scope="col" className="num">Last 24 h</th>
                <th scope="col" className="num">Open</th>
                <th scope="col">Changed</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((r) => {
                const href = `#/rules/${encodeURIComponent(r.rule.id)}`
                return (
                  <tr key={r.rule.id} className={`tok-${colorToken(r.rule.present.color ?? 'grey')} ${r.rule.enabled === false ? 'is-off' : ''}`} onClick={() => onNavigate(href)}>
                    <td>
                      <div className="rule-cell">
                        <span className="tok-bar" aria-hidden="true" />
                        {r.rule.severity ? <SeverityIcon severity={r.rule.severity} /> : <span className="sev-any" role="img" aria-label="Severity comes from the event" title="Severity comes from the event" />}
                        <div className="rule-cell-text">
                          <a href={href} onClick={(e) => e.stopPropagation()}>{r.rule.name || r.rule.id}</a>
                          <span className="muted small mono">{r.rule.id}{r.rule.enabled === false ? ' · disabled' : ''}{r.rule.runbook ? ' · runbook' : ''}</span>
                        </div>
                      </div>
                    </td>
                    <td>{r.rule.team}</td>
                    <td className="small">{routeSummary(r.rule)}</td>
                    <td className="num">
                      <span className="fires">
                        <Sparkline values={r.stats.hourly} label={`${r.stats.fires_24h} matching events in the last 24 hours`} />
                        {fmtCount(r.stats.fires_24h)}
                      </span>
                    </td>
                    <td className="num">{r.stats.open_alerts || <span className="muted">0</span>}</td>
                    <td className="small muted">{relTime(r.updated_at, now)} · {r.updated_by.replace(/^user:/, '')}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      {picking && (
        <div className="modal-backdrop" onClick={() => setPicking(false)}>
          <div className="modal modal-wide" role="dialog" aria-modal="true" aria-labelledby="tpl-title" onClick={(e) => e.stopPropagation()} onKeyDown={(e) => e.key === 'Escape' && setPicking(false)}>
            <h2 id="tpl-title">What should this rule catch?</h2>
            <div className="template-grid">
              {TEMPLATES.map((t, i) => (
                <button key={t.id} className="template-card" autoFocus={i === 0} onClick={() => onNavigate(`#/rules/new?from=${t.id}`)}>
                  <strong>{t.label}</strong>
                  <span className="muted small">{t.description}</span>
                </button>
              ))}
            </div>
            <p className="hint">Each starts with a sample event in the test panel, so you can see the alert before you save.</p>
            <div className="inline end">
              <button className="btn" onClick={() => setPicking(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
