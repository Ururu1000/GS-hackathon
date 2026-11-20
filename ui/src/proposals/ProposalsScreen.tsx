// Suggested rule changes (spec §6.5, §7.7): from people via "Suggest a change"
// on an alert, or from Beacon itself. Accepting one is an ordinary publish
// against the revision it was written for.

import { useEffect, useMemo, useState } from 'react'
import { decideProposal, errorText, isConflict, listProposals } from '../api/client'
import type { Proposal } from '../api/types'
import { actorName } from '../alert/badges'
import { diffLines, withContext } from '../lib/diff'
import { relTime } from '../lib/time'
import { toYaml } from '../rules/draft'

interface Props {
  id: string | null
  team: string | null
  onNavigate: (hash: string) => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
  onChanged: () => void
}

export const KIND_LABEL: Record<string, string> = {
  noisy: 'Too noisy',
  reroute: 'Wrong people',
  template: 'Missing or unclear info',
  other: 'Other',
}

const STATE_LABEL: Record<string, string> = { pending: 'Waiting for review', accepted: 'Accepted', rejected: 'Rejected', stale: 'Out of date' }

export function RuleDiff({ before, after }: { before: string; after: string }) {
  const lines = useMemo(() => withContext(diffLines(before.trimEnd(), after.trimEnd()), 2), [before, after])
  const changed = lines.some((l) => l.op === '+' || l.op === '-')
  if (!changed) return <p className="hint">No change to the rule itself; see the note.</p>
  return (
    <pre className="diff" aria-label="Changes to the rule">
      {lines.map((l, i) => (
        <div key={i} className={`diff-line diff-${l.op === '+' ? 'add' : l.op === '-' ? 'del' : l.op === '…' ? 'gap' : 'ctx'}`}>
          <span className="diff-op" aria-hidden="true">{l.op === '…' ? '' : l.op}</span>
          <span className="sr-only">{l.op === '+' ? 'added: ' : l.op === '-' ? 'removed: ' : ''}</span>
          {l.text}
        </div>
      ))}
    </pre>
  )
}

export function ProposalsScreen({ id, team, onNavigate, onToast, onChanged }: Props) {
  const [items, setItems] = useState<Proposal[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [show, setShow] = useState<'pending' | 'all'>('pending')
  const [busy, setBusy] = useState<string | null>(null)

  const load = () => listProposals().then(setItems, (e: unknown) => setError(errorText(e)))
  useEffect(() => {
    void load()
  }, [])

  useEffect(() => {
    if (!id || !items) return
    document.getElementById(`proposal-${id}`)?.scrollIntoView({ block: 'start' })
  }, [id, items])

  const scoped = (items ?? []).filter((p) => !team || p.base_rule.team === team)
  const shown = scoped.filter((p) => show === 'all' || p.state === 'pending' || p.id === id)
  const pending = scoped.filter((p) => p.state === 'pending').length

  const decide = async (p: Proposal, op: 'accept' | 'reject') => {
    setBusy(p.id)
    try {
      await decideProposal(p.id, op)
      onToast('ok', op === 'accept' ? `Accepted. “${p.rule_name}” now has the change.` : 'Rejected.')
    } catch (e) {
      onToast(isConflict(e) ? 'warn' : 'error', errorText(e))
    } finally {
      setBusy(null)
      await load()
      onChanged()
    }
  }

  return (
    <div className="screen">
      <header className="screen-head">
        <div className="screen-title">
          <h1>Proposals</h1>
          <span className="muted small">Changes people (and Beacon) suggested for alert rules. The rule’s team reviews them.</span>
        </div>
        <span className="spacer" />
        <div className="chips" role="group" aria-label="Show">
          <button className="chip" aria-pressed={show === 'pending'} onClick={() => setShow('pending')}>Waiting <span className="chip-n">{pending}</span></button>
          <button className="chip" aria-pressed={show === 'all'} onClick={() => setShow('all')}>All <span className="chip-n">{scoped.length}</span></button>
        </div>
      </header>
      {error && <div className="notice notice-error banner">Couldn’t load proposals: {error}</div>}
      {!items && !error && <div className="empty"><p>Loading…</p></div>}
      {items && shown.length === 0 && (
        <div className="empty">
          <p>{show === 'pending' ? 'Nothing waiting for review.' : 'No proposals yet.'}</p>
          <p className="small">Open an alert and choose “Suggest a change” when it’s too noisy, goes to the wrong people, or is missing information.</p>
        </div>
      )}
      <div className="proposal-list">
        {shown.map((p) => (
          <article key={p.id} id={`proposal-${p.id}`} className={`proposal state-${p.state} ${p.id === id ? 'is-target' : ''}`} aria-labelledby={`ph-${p.id}`}>
            <header className="proposal-head">
              <div>
                <h2 id={`ph-${p.id}`}>{p.summary}</h2>
                <div className="small muted">
                  <span className="pill">{KIND_LABEL[p.kind] ?? p.kind}</span>{' '}
                  for <a href={`#/rules/${encodeURIComponent(p.rule_id)}`}>{p.rule_name}</a> ·{' '}
                  {p.auto ? <span className="pill pill-auto" title="Beacon suggested this from how the rule’s alerts were handled">Suggested by Beacon</span> : <>by {actorName(p.author)}</>} ·{' '}
                  {relTime(p.created_at, Date.now())}
                  {p.alert_id && <> · from <a href={`#/alerts/${encodeURIComponent(p.alert_id)}`}>an alert</a></>}
                </div>
              </div>
              <span className={`pstate pstate-${p.state}`}>{STATE_LABEL[p.state] ?? p.state}{p.decided_by ? ` · ${actorName(p.decided_by)}` : ''}</span>
            </header>
            {p.note && <p className="proposal-note">{p.note}</p>}
            <RuleDiff before={toYaml(p.base_rule)} after={toYaml(p.proposed_rule)} />
            {p.state === 'stale' && <p className="small muted">The rule changed after this was written (it was against revision {p.base_revision}). Suggest it again on the current rule if it still applies.</p>}
            {p.state === 'pending' && (
              <div className="inline end">
                <button className="btn" disabled={busy === p.id} onClick={() => void decide(p, 'reject')}>Reject</button>
                <button className="btn" onClick={() => onNavigate(`#/rules/${encodeURIComponent(p.rule_id)}`)}>Open rule</button>
                <button className="btn btn-primary" disabled={busy === p.id} onClick={() => void decide(p, 'accept')}>{busy === p.id ? 'Publishing…' : 'Accept and publish'}</button>
              </div>
            )}
          </article>
        ))}
      </div>
    </div>
  )
}
