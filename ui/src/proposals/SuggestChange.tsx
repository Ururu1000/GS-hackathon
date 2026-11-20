// "Suggest a change" on an alert (spec §6.5): a typed proposal against the
// rule's current revision, for the rule's team to accept or reject.

import { useEffect, useMemo, useRef, useState } from 'react'
import { createProposal, errorText, getRule, HttpError, listDirectory, listTeams } from '../api/client'
import type { EventSample, FeedAlert, PersonDirectoryEntry, ProposalKind, RuleItem, RuleSpec, TeamView } from '../api/types'
import { flattenPaths, ruleEquals, toYaml } from '../rules/draft'
import { NoiseEditor, PresentEditor, RouteEditor, type EditorEnv } from '../rules/sections'
import { KIND_LABEL, RuleDiff } from './ProposalsScreen'

interface Props {
  alert: FeedAlert
  samples: EventSample[]
  onClose: () => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
  onNavigate: (hash: string) => void
}

const KINDS: { id: ProposalKind; hint: string }[] = [
  { id: 'noisy', hint: 'Fires too often, or repeats should be merged' },
  { id: 'reroute', hint: 'Goes to the wrong team, person or channel' },
  { id: 'template', hint: 'The title, text or fields don’t say enough' },
  { id: 'other', hint: 'Anything else: describe it' },
]

export function autoSummary(kind: ProposalKind, base: RuleSpec, next: RuleSpec): string {
  if (kind === 'noisy') {
    if (next.group && JSON.stringify(next.group) !== JSON.stringify(base.group))
      return `Send a digest${next.group.by.length ? ` per ${next.group.by.map((p) => p.split('.').pop()).join(', ')}` : ''} every ${next.group.window}`
    if (next.throttle && JSON.stringify(next.throttle) !== JSON.stringify(base.throttle)) return `At most ${next.throttle.max} notifications per ${next.throttle.per} per person`
    if (next.dedup?.window !== base.dedup?.window && next.dedup) return `Merge repeats within ${next.dedup.window}`
    if (JSON.stringify(next.dedup?.key) !== JSON.stringify(base.dedup?.key)) return 'Change what counts as the same alert'
    return 'Make this rule quieter'
  }
  if (kind === 'reroute') {
    const to = next.route.map((r) => r.to.replace(':', ' ')).join(', ')
    return next.team !== base.team ? `Move to team ${next.team}, notify ${to}` : `Notify ${to}`
  }
  if (kind === 'template') {
    const added = (next.present.fields ?? []).filter((f) => !JSON.stringify(base.present.fields ?? []).includes(JSON.stringify(f)))
    if (added.length) return `Show ${added.map((f) => (typeof f === 'string' ? f : f.label || f.path)).join(', ')} on the alert`
    if (next.present.title !== base.present.title) return 'Reword the alert title'
    return 'Improve how the alert reads'
  }
  return ''
}

export function SuggestChange({ alert, samples, onClose, onToast, onNavigate }: Props) {
  const [item, setItem] = useState<RuleItem | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [teams, setTeams] = useState<TeamView[]>([])
  const [people, setPeople] = useState<PersonDirectoryEntry[]>([])
  const [kind, setKind] = useState<ProposalKind>('noisy')
  const [draft, setDraft] = useState<RuleSpec | null>(null)
  const [summary, setSummary] = useState('')
  const summaryTouched = useRef(false)
  const [note, setNote] = useState('')
  const [sending, setSending] = useState(false)
  const [problems, setProblems] = useState<{ path: string; message: string }[]>([])

  useEffect(() => {
    getRule(alert.rule.id).then(
      (r) => {
        setItem(r)
        setDraft(r.rule)
      },
      (e: unknown) => setError(errorText(e)),
    )
    listTeams().then(setTeams, () => {})
    listDirectory().then(setPeople, () => {})
  }, [alert.rule.id])

  useEffect(() => {
    if (item && draft && !summaryTouched.current) setSummary(autoSummary(kind, item.rule, draft))
  }, [kind, draft, item])

  const env: EditorEnv = useMemo(() => {
    const raw = samples[samples.length - 1]?.raw
    return { teams, people, paths: raw === undefined ? [] : flattenPaths('raw', raw), problems }
  }, [samples, teams, people, problems])

  const pickKind = (k: ProposalKind) => {
    setKind(k)
    if (item) setDraft(item.rule)
  }

  const send = async () => {
    if (!item || !draft) return
    setSending(true)
    setProblems([])
    try {
      await createProposal({ rule_id: item.rule.id, base_revision: item.revision, kind, summary, note, proposed_rule: draft, alert_id: alert.id })
      onToast('ok', `Sent to ${item.rule.team} for review.`)
      onClose()
    } catch (e) {
      const d = e instanceof HttpError ? e.body?.error.details : undefined
      if (d) setProblems(d)
      onToast('error', errorText(e))
    } finally {
      setSending(false)
    }
  }

  const changed = !!item && !!draft && !ruleEquals(item.rule, draft)
  const canSend = !!draft && summary.trim() !== '' && (changed || (kind === 'other' && note.trim() !== ''))

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal modal-wide suggest" role="dialog" aria-modal="true" aria-labelledby="suggest-title" onClick={(e) => e.stopPropagation()} onKeyDown={(e) => e.key === 'Escape' && onClose()}>
        <h2 id="suggest-title">Suggest a change to “{alert.rule.name}”</h2>
        {error && <p className="problems-inline">Couldn’t load the rule: {error}</p>}
        {!item && !error && <p className="muted">Loading…</p>}
        {item && draft && (
          <>
            <fieldset className="kind-picker">
              <legend className="small">What’s wrong with this alert?</legend>
              {KINDS.map((k, i) => (
                <label key={k.id} className={`kind-card ${kind === k.id ? 'is-on' : ''}`}>
                  <input type="radio" name="kind" checked={kind === k.id} onChange={() => pickKind(k.id)} autoFocus={i === 0} />
                  <strong>{KIND_LABEL[k.id]}</strong>
                  <span className="muted small">{k.hint}</span>
                </label>
              ))}
            </fieldset>

            <div className="suggest-body">
              {kind === 'noisy' && <NoiseEditor rule={draft} onChange={setDraft} env={env} />}
              {kind === 'reroute' && (
                <div className="stack">
                  <div className="inline">
                    <span className="small">Owned by team</span>
                    <select className="select" value={draft.team} onChange={(e) => setDraft({ ...draft, team: e.target.value })} aria-label="Team">
                      {teams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
                    </select>
                  </div>
                  <RouteEditor rule={draft} onChange={setDraft} env={env} />
                </div>
              )}
              {kind === 'template' && <PresentEditor rule={draft} onChange={setDraft} env={env} />}
            </div>

            <div className="form-field">
              <label htmlFor="sg-summary">In one line</label>
              <input id="sg-summary" className="input" maxLength={200} value={summary} onChange={(e) => { summaryTouched.current = true; setSummary(e.target.value) }} placeholder="What should change" />
            </div>
            <div className="form-field">
              <label htmlFor="sg-note">Why <span className="muted">(optional{kind === 'other' ? ' if you changed the rule' : ''})</span></label>
              <textarea id="sg-note" className="input" rows={3} maxLength={4000} value={note} onChange={(e) => setNote(e.target.value)} placeholder="This fired 30 times during the deploy window and nobody needed to act." />
              <p className="hint">Visible to reviewers on the rule’s team. Do not paste personal data or secrets.</p>
            </div>

            {changed && (
              <details className="diff-preview" open>
                <summary className="small">Change to the rule</summary>
                <RuleDiff before={toYaml(item.rule)} after={toYaml(draft)} />
              </details>
            )}

            <div className="inline end">
              <button className="btn btn-ghost" onClick={() => { onClose(); onNavigate('#/proposals') }}>See all proposals</button>
              <span className="spacer" />
              <button className="btn" onClick={onClose}>Cancel</button>
              <button className="btn btn-primary" disabled={!canSend || sending} onClick={() => void send()}>{sending ? 'Sending…' : 'Send for review'}</button>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
