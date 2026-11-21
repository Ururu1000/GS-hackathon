// Channel preferences per person (first match wins, optional time window) and
// per team (members, lead, on-call rotation, default channels). Spec §4.4, §7.5.

import { useEffect, useMemo, useState } from 'react'
import { errorText, HttpError, isConflict, listPeople, listTeams, savePreferences, saveTeam } from '../api/client'
import { SEVERITIES, type PersonView, type Preference, type Severity, type TeamView } from '../api/types'
import { SEVERITY_LABEL } from '../alert/badges'
import { ChevronDownIcon, ChevronUpIcon, CloseIcon } from '../lib/icons'
import { maskAddress } from '../lib/redact'
import { ChannelPicker, CHANNEL_NAME } from '../rules/sections'
import { parseWindow, resolveChannels } from './resolve'

interface Props {
  kind: 'person' | 'team' | null
  id: string | null
  me: string | null
  onNavigate: (hash: string) => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
}

const ZONES = ['Europe/London', 'Europe/Paris', 'Europe/Kyiv', 'America/New_York', 'Asia/Singapore', 'UTC']
const channelsText = (c: string[]) => (c.length ? c.map((x) => CHANNEL_NAME[x] ?? x).join(' + ') : 'nothing')

export function PreferencesScreen({ kind, id, me, onNavigate, onToast }: Props) {
  const [people, setPeople] = useState<PersonView[] | null>(null)
  const [teams, setTeams] = useState<TeamView[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([listPeople(), listTeams()]).then(
      ([p, t]) => {
        setPeople(p)
        setTeams(t)
      },
      (e: unknown) => setError(errorText(e)),
    )
  }, [])

  const selKind = kind ?? 'person'
  const selId = id ?? me ?? people?.[0]?.id ?? null
  const person = selKind === 'person' ? people?.find((p) => p.id === selId) ?? null : null
  const team = selKind === 'team' ? teams?.find((t) => t.id === selId) ?? null : null

  return (
    <div className="screen">
      <h1 className="sr-only">Preferences</h1>
      {error && <div className="notice notice-error banner">Couldn’t load people and teams: {error}</div>}
      <div className="split">
        <nav className="side-list" aria-label="People and teams">
          <h2>People</h2>
          {people?.map((p) => (
            <a key={p.id} href={`#/preferences/person/${encodeURIComponent(p.id)}`} aria-current={selKind === 'person' && selId === p.id ? 'page' : undefined}>
              {p.name}
              {p.id === me && <span className="muted small"> · you</span>}
            </a>
          ))}
          <h2>Teams</h2>
          {teams?.map((t) => (
            <a key={t.id} href={`#/preferences/team/${encodeURIComponent(t.id)}`} aria-current={selKind === 'team' && selId === t.id ? 'page' : undefined}>
              {t.name}
            </a>
          ))}
        </nav>
        <div className="split-main">
          {!people && !error && <div className="empty"><p>Loading…</p></div>}
          {person && teams && (
            <PersonEditor
              key={`${person.id}:${person.revision}`}
              person={person}
              me={me}
              teams={teams}
              onSaved={(p) => setPeople((ps) => ps?.map((x) => (x.id === p.id ? p : x)) ?? null)}
              onToast={onToast}
            />
          )}
          {team && people && (
            <TeamEditor
              key={`${team.id}:${team.revision}`}
              team={team}
              me={me}
              people={people}
              onSaved={(t) => setTeams((ts) => ts?.map((x) => (x.id === t.id ? t : x)) ?? null)}
              onNavigate={onNavigate}
              onToast={onToast}
            />
          )}
          {people && !person && !team && <div className="empty"><p>Nobody by that name.</p></div>}
        </div>
      </div>
    </div>
  )
}

// ---- person ---------------------------------------------------------------------

type Line = Extract<Preference, { match: unknown }>

function SeverityPicker({ value, onChange }: { value: Severity[]; onChange: (v: Severity[]) => void }) {
  return (
    <div className="chips" role="group" aria-label="Severities">
      {SEVERITIES.map((s) => {
        const on = value.includes(s)
        return (
          <button key={s} type="button" className="chip" aria-pressed={on} onClick={() => onChange(on ? value.filter((x) => x !== s) : SEVERITIES.filter((x) => x === s || value.includes(x)))}>
            {SEVERITY_LABEL[s]}
          </button>
        )
      })}
    </div>
  )
}

function PrefLine({ line, index, count, hit, teams, onChange, onMove, onRemove }: {
  line: Line
  index: number
  count: number
  /** This line decides the "Try it" example. */
  hit: boolean
  teams: TeamView[]
  onChange: (l: Line) => void
  onMove: (d: -1 | 1) => void
  onRemove: () => void
}) {
  const sev = line.match.severity === undefined ? [] : Array.isArray(line.match.severity) ? line.match.severity : [line.match.severity]
  const setMatch = (m: Line['match']) => {
    const clean = Object.fromEntries(Object.entries(m).filter(([, v]) => v !== undefined && v !== '' && !(Array.isArray(v) && v.length === 0))) as Line['match']
    onChange({ ...line, match: clean })
  }
  const badWindow = line.when && !parseWindow(line.when.between)
  const [from = '08:00', to = '20:00'] = (line.when?.between ?? '08:00-20:00').split('-')
  return (
    <li className={`pref-line ${hit ? 'is-hit' : ''}`}>
      <div className="pref-body">
        <div className="pref-line-head">
          <span className="pref-num">Line {index + 1}</span>
          {hit && <span className="pref-hit">Matches example</span>}
        </div>
        <div className="inline wrap">
          <span className="small">When severity is</span>
          <SeverityPicker value={sev} onChange={(v) => setMatch({ ...line.match, severity: v.length === 1 ? v[0] : v })} />
          {sev.length === 0 && <span className="muted small">any</span>}
        </div>
        <div className="inline wrap">
          <span className="small">and service is</span>
          <input className="input w-md" value={line.match.service ?? ''} placeholder="any service" onChange={(e) => setMatch({ ...line.match, service: e.target.value || undefined })} aria-label="Service" />
          <span className="small">and team is</span>
          <select className="select" value={line.match.team ?? ''} onChange={(e) => setMatch({ ...line.match, team: e.target.value || undefined })} aria-label="Team">
            <option value="">any team</option>
            {teams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
          </select>
        </div>
        <div className="inline wrap">
          <span className="small">send by</span>
          <ChannelPicker label="Channels" value={line.channels} onChange={(channels) => onChange({ ...line, channels })} />
        </div>
        <label className="check small">
          <input type="checkbox" checked={!!line.when} onChange={(e) => {
            if (e.target.checked) onChange({ ...line, when: { between: '08:00-20:00', tz: 'Europe/London' }, otherwise: line.otherwise ?? ['email'] })
            else {
              const { when: _w, otherwise: _o, ...rest } = line
              onChange(rest)
            }
          }} />
          Only during certain hours
        </label>
        {line.when && (
          <div className="stack tight indent">
            <div className="inline wrap">
              <span className="small">between</span>
              <input className="input w-sm" type="time" value={from} onChange={(e) => onChange({ ...line, when: { ...line.when!, between: `${e.target.value}-${to}` } })} aria-label="From" />
              <span className="small">and</span>
              <input className="input w-sm" type="time" value={to} onChange={(e) => onChange({ ...line, when: { ...line.when!, between: `${from}-${e.target.value}` } })} aria-label="Until" />
              <select className="select" value={line.when.tz} onChange={(e) => onChange({ ...line, when: { ...line.when!, tz: e.target.value } })} aria-label="Time zone">
                {[...new Set([line.when.tz, ...ZONES])].map((z) => <option key={z} value={z}>{z.replace('_', ' ')}</option>)}
              </select>
            </div>
            {badWindow && <p className="problems-inline">Use times like 08:00 and 20:00.</p>}
            <div className="inline wrap">
              <span className="small">otherwise send by</span>
              <ChannelPicker label="Outside those hours" value={line.otherwise ?? []} onChange={(otherwise) => onChange({ ...line, otherwise })} />
              {!line.otherwise?.length && <span className="muted small">(skip this line outside those hours)</span>}
            </div>
          </div>
        )}
      </div>
      <div className="pref-tools">
        <button type="button" className="icon-btn sm move-btn" disabled={index === 0} onClick={() => onMove(-1)} aria-label="Move up" title="Move up"><ChevronUpIcon size={14} /></button>
        <button type="button" className="icon-btn sm move-btn" disabled={index === count - 1} onClick={() => onMove(1)} aria-label="Move down" title="Move down"><ChevronDownIcon size={14} /></button>
        <button type="button" className="icon-btn sm" onClick={onRemove} aria-label="Remove this line" title="Remove"><CloseIcon size={13} /></button>
      </div>
    </li>
  )
}

function localInput(d: Date): string {
  const p = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}T${p(d.getHours())}:${p(d.getMinutes())}`
}

function PersonEditor({ person, me, teams, onSaved, onToast }: { person: PersonView; me: string | null; teams: TeamView[]; onSaved: (p: PersonView) => void; onToast: Props['onToast'] }) {
  const split = (prefs: Preference[]) => ({
    lines: prefs.filter((p): p is Line => 'match' in p),
    fallback: (prefs.find((p) => 'default' in p) as { default: string[] } | undefined)?.default ?? null,
  })
  const readOnly = person.can_edit === false || (person.can_edit === undefined && (me === null || person.id !== me))
  const initial = split(person.preferences)
  const [lines, setLines] = useState<Line[]>(initial.lines)
  const [fallback, setFallback] = useState<string[] | null>(initial.fallback)
  const [saving, setSaving] = useState(false)
  const [problem, setProblem] = useState<string | null>(null)
  const prefs: Preference[] = useMemo(() => [...lines, ...(fallback ? [{ default: fallback }] : [])], [lines, fallback])
  const dirty = !readOnly && JSON.stringify(prefs) !== JSON.stringify(person.preferences)

  // "Try it"
  const memberOf = teams.filter((t) => t.members.includes(person.id))
  const [trySev, setTrySev] = useState<Severity>('critical')
  const [tryService, setTryService] = useState('payments-api')
  const [tryTeam, setTryTeam] = useState(memberOf[0]?.id ?? teams[0]?.id ?? '')
  const [tryAt, setTryAt] = useState(() => localInput(new Date()))
  const at = new Date(tryAt)
  const teamDefault = teams.find((t) => t.id === tryTeam)?.default_channels ?? []
  const res = Number.isNaN(at.getTime()) ? null : resolveChannels(prefs, { severity: trySev, service: tryService || undefined, team: tryTeam || undefined }, at, teamDefault)

  const save = async () => {
    if (readOnly) return
    setSaving(true)
    setProblem(null)
    try {
      const saved = await savePreferences(person.id, prefs, person.revision)
      onSaved(saved)
      onToast('ok', `Saved ${person.name}’s preferences. New alerts use them.`)
    } catch (e) {
      if (isConflict(e) && e.body?.error.current) {
        onToast('warn', 'Someone else changed these preferences. Showing the latest; make your change again.')
        onSaved(e.body.error.current as PersonView)
      } else setProblem(e instanceof HttpError ? errorText(e) : String(e))
    } finally {
      setSaving(false)
    }
  }

  const move = (i: number, d: -1 | 1) =>
    setLines((ls) => {
      const next = [...ls]
      ;[next[i], next[i + d]] = [next[i + d]!, next[i]!]
      return next
    })

  const contactBits = [
    person.contact.email ? (readOnly ? maskAddress('email', person.contact.email) : person.contact.email) : null,
    person.contact.phone ? (readOnly ? maskAddress('phone', person.contact.phone) : person.contact.phone) : null,
  ].filter(Boolean)

  return (
    <div className="pref-editor">
      <div className="pref-head">
        <h2>{person.name}</h2>
        <span className="muted small">
          {contactBits.join(' · ')}
          {memberOf.length > 0 && <>{contactBits.length > 0 ? ' · ' : ''}{memberOf.map((t) => t.name).join(', ')}</>}
        </span>
      </div>

      {readOnly ? (
        <div className="notice notice-warn banner" role="status">
          Private: only {person.name} or an admin can view or edit their personal contact details and notification schedule.
        </div>
      ) : (
        <>
          <div className="pref-grid">
          <div className="stack">
          <p className="muted">Checked from the top; the first line that fits the alert decides.</p>
          <ol className="pref-lines">
            {lines.map((l, i) => (
              <PrefLine
                key={i}
                line={l}
                index={i}
                count={lines.length}
                hit={res?.via === 'preference' && res.line === i}
                teams={teams}
                onChange={(next) => setLines((ls) => ls.map((x, j) => (j === i ? next : x)))}
                onMove={(d) => move(i, d)}
                onRemove={() => setLines((ls) => ls.filter((_, j) => j !== i))}
              />
            ))}
            <li className={`pref-line pref-fallback ${res?.via === 'default' ? 'is-hit' : ''}`}>
              <div className="pref-body">
                {res?.via === 'default' && <div className="pref-line-head"><span className="pref-hit">Matches example</span></div>}
                <label className="check small">
                  <input type="checkbox" checked={fallback !== null} onChange={(e) => setFallback(e.target.checked ? ['ui'] : null)} />
                  Anything else
                </label>
                {fallback !== null ? (
                  <div className="inline wrap">
                    <span className="small">send by</span>
                    <ChannelPicker label="Anything else" value={fallback} onChange={setFallback} />
                  </div>
                ) : (
                  <p className="hint">Not set: the team’s default channels apply, or in-app only.</p>
                )}
              </div>
            </li>
          </ol>
          <div className="inline">
            <button className="btn btn-sm" onClick={() => setLines((ls) => [...ls, { match: { severity: 'critical' }, channels: ['sms'] }])}>Add a line</button>
          </div>
          </div>

          <section className="try-it" aria-label="Try these preferences">
            <h3>Try it</h3>
            <p className="muted">Pick an example alert to see which line decides and where it goes.</p>
            <div className="try-form">
              <label htmlFor="try-sev">Severity</label>
              <select id="try-sev" className="select" value={trySev} onChange={(e) => setTrySev(e.target.value as Severity)}>
                {SEVERITIES.map((s) => <option key={s} value={s}>{SEVERITY_LABEL[s]}</option>)}
              </select>
              <label htmlFor="try-service">Service</label>
              <input id="try-service" className="input" value={tryService} onChange={(e) => setTryService(e.target.value)} placeholder="any service" />
              <label htmlFor="try-team">Team</label>
              <select id="try-team" className="select" value={tryTeam} onChange={(e) => setTryTeam(e.target.value)}>
                {teams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
              </select>
              <label htmlFor="try-at">At</label>
              <input id="try-at" className="input" type="datetime-local" value={tryAt} onChange={(e) => setTryAt(e.target.value)} />
            </div>
            {res && (
              <div className="try-result" aria-live="polite">
                <p>Sent by <strong>{channelsText(res.channels)}</strong></p>
                <p className="muted">
                  {res.via === 'preference'
                    ? `Line ${res.line! + 1} decides${res.in_window === true ? ', inside its hours' : res.in_window === false ? ', outside its hours' : ''}.`
                    : res.via === 'default'
                      ? 'No line fits, so “Anything else” decides.'
                      : res.via === 'team'
                        ? 'No line fits, so the team’s default channels apply.'
                        : 'No line fits and the team has no default, so in-app only.'}
                </p>
                {res.problem && <p className="err-text">{res.problem}</p>}
              </div>
            )}
            <p className="hint">Rules that name channels explicitly override this.</p>
          </section>
          </div>

          {problem && <p className="problems-inline" role="alert">{problem}</p>}
          <footer className={`save-bar ${dirty ? 'is-dirty' : ''}`}>
            <span className="small">{dirty ? 'Unsaved changes' : 'No unsaved changes'}</span>
            <span className="spacer" />
            {dirty && <button className="btn" onClick={() => { const s = split(person.preferences); setLines(s.lines); setFallback(s.fallback) }}>Discard</button>}
            <button className="btn btn-primary" disabled={!dirty || saving} onClick={() => void save()}>{saving ? 'Saving…' : 'Save'}</button>
          </footer>
        </>
      )}
    </div>
  )
}

// ---- team ------------------------------------------------------------------------------

function TeamEditor({ team, me, people, onSaved, onNavigate, onToast }: { team: TeamView; me: string | null; people: PersonView[]; onSaved: (t: TeamView) => void; onNavigate: Props['onNavigate']; onToast: Props['onToast'] }) {
  const [t, setT] = useState<TeamView>(team)
  const [saving, setSaving] = useState(false)
  const [problem, setProblem] = useState<string | null>(null)
  const [newSchedule, setNewSchedule] = useState('')
  const readOnly = team.can_edit === false || (team.can_edit === undefined && me !== null && team.lead !== me)
  const dirty = !readOnly && JSON.stringify(t) !== JSON.stringify(team)
  const name = (id: string) => people.find((p) => p.id === id)?.name ?? id
  const nonMembers = people.filter((p) => !t.members.includes(p.id))

  const setRotation = (s: string, list: string[]) => setT({ ...t, oncall: { ...t.oncall, [s]: list } })
  const removeMember = (id: string) => {
    const oncall = Object.fromEntries(Object.entries(t.oncall).map(([k, v]) => [k, v.filter((x) => x !== id)]))
    setT({ ...t, members: t.members.filter((x) => x !== id), lead: t.lead === id ? null : t.lead, oncall })
  }

  const save = async () => {
    if (readOnly) return
    setSaving(true)
    setProblem(null)
    try {
      const saved = await saveTeam(t)
      onSaved(saved)
      onToast('ok', `Saved ${t.name}.`)
    } catch (e) {
      if (isConflict(e) && e.body?.error.current) {
        onToast('warn', 'Someone else changed this team. Showing the latest; make your change again.')
        onSaved(e.body.error.current as TeamView)
      } else setProblem(errorText(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="pref-editor is-narrow">
      <div className="pref-head">
        <h2>{t.name}</h2>
        <span className="muted small">Team <code>{t.id}</code> · rules route here with <code>team:{t.id}</code></span>
      </div>

      {readOnly && (
        <div className="notice notice-warn banner" role="status">
          Read-only: only the team lead or an admin can change team membership, lead, or on-call rotations.
        </div>
      )}

      <section className="editor-section">
        <h3>Members</h3>
        <ul className="member-list">
          {t.members.map((m) => (
            <li key={m}>
              <a href={`#/preferences/person/${encodeURIComponent(m)}`} onClick={(e) => { e.preventDefault(); onNavigate(`#/preferences/person/${encodeURIComponent(m)}`) }}>{name(m)}</a>
              {t.lead === m && <span className="pill">Lead</span>}
              <span className="spacer" />
              {!readOnly && t.lead !== m && <button className="btn btn-xs" onClick={() => setT({ ...t, lead: m })}>Make lead</button>}
              {!readOnly && <button className="icon-btn sm" onClick={() => removeMember(m)} aria-label={`Remove ${name(m)}`} title="Remove from team"><CloseIcon size={13} /></button>}
            </li>
          ))}
        </ul>
        {!readOnly && nonMembers.length > 0 && (
          <select className="select" value="" onChange={(e) => e.target.value && setT({ ...t, members: [...t.members, e.target.value] })} aria-label="Add a member">
            <option value="">Add a member…</option>
            {nonMembers.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
        )}
        <p className="hint">The lead approves risky runbook steps and is the last escalation step.</p>
      </section>

      <section className="editor-section">
        <h3>On call</h3>
        {Object.keys(t.oncall).length === 0 && <p className="hint">No rotation yet. Rules can page <code>oncall:&lt;name&gt;</code> once there is one.</p>}
        {Object.entries(t.oncall).map(([s, list]) => (
          <div key={s} className="rotation">
            <div className="inline">
              <strong>oncall:{s}</strong>
              <span className="spacer" />
              {!readOnly && <button className="btn btn-xs" onClick={() => { const { [s]: _d, ...rest } = t.oncall; setT({ ...t, oncall: rest }) }}>Remove rotation</button>}
            </div>
            <ol className="rotation-list">
              {list.map((u, i) => (
                <li key={u}>
                  <span>{name(u)}</span>
                  {i === 0 && <span className="pill pill-live">On call now</span>}
                  {i === 1 && <span className="muted small">next, if nobody acknowledges</span>}
                  <span className="spacer" />
                  {!readOnly && (
                    <>
                      <button className="icon-btn sm move-btn" disabled={i === 0} onClick={() => { const n = [...list]; [n[i - 1], n[i]] = [n[i]!, n[i - 1]!]; setRotation(s, n) }} aria-label="Earlier" title="Earlier"><ChevronUpIcon size={14} /></button>
                      <button className="icon-btn sm move-btn" disabled={i === list.length - 1} onClick={() => { const n = [...list]; [n[i + 1], n[i]] = [n[i]!, n[i + 1]!]; setRotation(s, n) }} aria-label="Later" title="Later"><ChevronDownIcon size={14} /></button>
                      <button className="icon-btn sm" onClick={() => setRotation(s, list.filter((x) => x !== u))} aria-label={`Take ${name(u)} off the rotation`} title="Remove"><CloseIcon size={13} /></button>
                    </>
                  )}
                </li>
              ))}
            </ol>
            {!readOnly && t.members.some((m) => !list.includes(m)) && (
              <select className="select select-sm" value="" onChange={(e) => e.target.value && setRotation(s, [...list, e.target.value])} aria-label="Add to rotation">
                <option value="">Add to rotation…</option>
                {t.members.filter((m) => !list.includes(m)).map((m) => <option key={m} value={m}>{name(m)}</option>)}
              </select>
            )}
          </div>
        ))}
        {!readOnly && (
          <div className="inline">
            <input className="input w-md mono" value={newSchedule} onChange={(e) => setNewSchedule(e.target.value.replace(/[^\w-]/g, ''))} placeholder={t.id} aria-label="New rotation name" />
            <button className="btn btn-sm" disabled={!newSchedule || newSchedule in t.oncall} onClick={() => { setRotation(newSchedule, []); setNewSchedule('') }}>Add rotation</button>
          </div>
        )}
      </section>

      <section className="editor-section">
        <h3>Default channels</h3>
        {readOnly ? (
          <p className="small">{channelsText(t.default_channels)}</p>
        ) : (
          <ChannelPicker label="Default channels" value={t.default_channels} onChange={(default_channels) => setT({ ...t, default_channels })} />
        )}
        <p className="hint">Used for members with no matching preference of their own. Empty means in-app only.</p>
      </section>

      {problem && <p className="problems-inline" role="alert">{problem}</p>}
      {!readOnly && (
        <footer className={`save-bar ${dirty ? 'is-dirty' : ''}`}>
          <span className="small">{dirty ? 'Unsaved changes' : 'No unsaved changes'}</span>
          <span className="spacer" />
          {dirty && <button className="btn" onClick={() => setT(team)}>Discard</button>}
          <button className="btn btn-primary" disabled={!dirty || saving} onClick={() => void save()}>{saving ? 'Saving…' : 'Save'}</button>
        </footer>
      )}
    </div>
  )
}
