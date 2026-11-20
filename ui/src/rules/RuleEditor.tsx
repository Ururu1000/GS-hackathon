// One authoritative draft (spec §7.3), shown as a visual builder or as YAML,
// with the live test panel next to it. Saving is a compare-and-set on the
// rule's revision.

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createRule, deleteRule, errorText, getRule, HttpError, isConflict, listPeople, listRunbooks, listTeams, updateRule } from '../api/client'
import { SEVERITIES, type NodeDiagnostic, type PersonView, type RuleItem, type RuleSpec, type RuleTestResult, type RunbookSummary, type TeamView } from '../api/types'
import { SEVERITY_LABEL } from '../alert/badges'
import { relTime } from '../lib/time'
import { ConditionBuilder } from './ConditionBuilder'
import { flattenPaths, fromYaml, ruleEquals, slugify, TEMPLATES, toYaml } from './draft'
import { NoiseEditor, PresentEditor, Problems, problemsAt, RouteEditor, type EditorEnv } from './sections'
import { TestPanel } from './TestPanel'

interface Props {
  id: string
  from: string | null
  team: string | null
  onNavigate: (hash: string) => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
}

type Tab = 'builder' | 'yaml'

const FORMAT_LABEL: Record<string, string> = { yaml: 'steps', markdown: 'Markdown page', link: 'wiki link', text: 'notes' }

export function RuleEditor({ id, from, team, onNavigate, onToast }: Props) {
  const isNew = id === 'new'
  const [item, setItem] = useState<RuleItem | null>(null)
  const [draft, setDraft] = useState<RuleSpec | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [teams, setTeams] = useState<TeamView[]>([])
  const [people, setPeople] = useState<PersonView[]>([])
  const [runbooks, setRunbooks] = useState<RunbookSummary[]>([])
  const [tab, setTab] = useState<Tab>('builder')
  const [yamlText, setYamlText] = useState('')
  const [yamlError, setYamlError] = useState<{ message: string; line: number | null; col: number | null } | null>(null)
  const [test, setTest] = useState<RuleTestResult | null>(null)
  const [testEvent, setTestEvent] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  const [conflict, setConflict] = useState<RuleItem | null>(null)
  const [saveProblems, setSaveProblems] = useState<{ path: string; message: string }[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const idTouched = useRef(!isNew)
  // The team scope only seeds a new rule; changing it later doesn't reset the draft.
  const teamRef = useRef(team)
  const template = TEMPLATES.find((t) => t.id === from) ?? TEMPLATES[0]!

  useEffect(() => {
    listTeams().then(setTeams, () => {})
    listPeople().then(setPeople, () => {})
    listRunbooks().then(setRunbooks, () => {})
  }, [])

  useEffect(() => {
    setItem(null)
    setDraft(null)
    setLoadError(null)
    setConflict(null)
    if (isNew) {
      setDraft(template.make(teamRef.current ?? 'settlements'))
      idTouched.current = false
      return
    }
    getRule(id).then(
      (r) => {
        setItem(r)
        setDraft(r.rule)
      },
      (e: unknown) => setLoadError(e instanceof HttpError && e.status === 404 ? 'This rule doesn’t exist (any more).' : errorText(e)),
    )
  }, [id, isNew, template])

  const dirty = !!draft && (isNew || (item !== null && !ruleEquals(draft, item.rule)))
  useEffect(() => {
    if (!dirty) return
    const on = (e: BeforeUnloadEvent) => e.preventDefault()
    addEventListener('beforeunload', on)
    return () => removeEventListener('beforeunload', on)
  }, [dirty])

  const onResult = useCallback((r: RuleTestResult | null, ev: unknown) => {
    setTest(r)
    setTestEvent(ev)
  }, [])

  const diagnostics = useMemo(() => new Map<string, NodeDiagnostic>((test?.nodes ?? []).map((n) => [n.node, n])), [test])
  const problems = useMemo(() => test?.errors ?? [], [test])
  const env: EditorEnv = useMemo(() => {
    const paths = testEvent === null ? [] : flattenPaths('raw', testEvent)
    const fieldPaths = Object.entries(test?.fields ?? {}).map(([k, v]) => ({ path: `fields.${k}`, value: v }))
    return { teams, people, paths: [...fieldPaths, ...paths, { path: 'schema', value: test?.schema ?? '' }, { path: 'source', value: '' }], problems: [...problems, ...saveProblems] }
  }, [teams, people, testEvent, test, problems, saveProblems])

  if (loadError)
    return (
      <div className="screen">
        <div className="empty">
          <p>{loadError}</p>
          <button className="btn" onClick={() => onNavigate('#/rules')}>Back to rules</button>
        </div>
      </div>
    )
  if (!draft) return <div className="screen"><div className="empty"><p>Loading…</p></div></div>

  const change = (r: RuleSpec) => {
    setSaveProblems([])
    if (isNew && !idTouched.current && r.name !== draft.name) r = { ...r, id: slugify(r.name ?? '') || r.id }
    setDraft(r)
  }

  const openTab = (t: Tab) => {
    if (t === 'yaml') {
      setYamlText(toYaml(draft))
      setYamlError(null)
    }
    setTab(t)
  }

  const onYaml = (text: string) => {
    setYamlText(text)
    const r = fromYaml(text)
    if (r.ok) {
      setYamlError(null)
      setDraft(isNew ? r.rule : { ...r.rule, id: draft.id })
    } else setYamlError({ message: r.message, line: r.line, col: r.col })
  }

  const save = async (overwrite?: RuleItem) => {
    setSaving(true)
    setSaveProblems([])
    try {
      if (isNew) {
        const created = await createRule(draft)
        onToast('ok', `Created “${created.rule.name || created.rule.id}”. New events are matched right away.`)
        setItem(created)
        onNavigate(`#/rules/${encodeURIComponent(created.rule.id)}`)
      } else {
        const saved = await updateRule(draft, (overwrite ?? item!).revision)
        setItem(saved)
        setDraft(saved.rule)
        setConflict(null)
        onToast('ok', `Saved revision ${saved.revision}. It applies to new events now.`)
      }
    } catch (e) {
      if (isConflict(e) && e.body?.error.code === 'revision_conflict' && e.body.error.current) setConflict(e.body.error.current as RuleItem)
      else if (e instanceof HttpError && e.body?.error.details) {
        setSaveProblems(e.body.error.details)
        onToast('error', 'The rule has problems; they’re marked below.')
      } else onToast('error', errorText(e))
    } finally {
      setSaving(false)
    }
  }

  const remove = async () => {
    if (!item) return
    try {
      await deleteRule(item.rule.id, item.revision)
      onToast('ok', `Deleted “${item.rule.name || item.rule.id}”. Its open alerts stay in the feed.`)
      onNavigate('#/rules')
    } catch (e) {
      onToast('error', errorText(e))
      setConfirmDelete(false)
    }
  }

  const back = () => {
    if (dirty && !confirm('Leave without saving your changes?')) return
    onNavigate('#/rules')
  }

  const blocking = problems.length > 0 || yamlError !== null
  const ruleTeam = teams.find((t) => t.id === draft.team)

  return (
    <div className="screen rule-editor">
      <header className="screen-head">
        <button className="btn btn-ghost" onClick={back}>← Rules</button>
        <div className="screen-title">
          <h1>{draft.name || (isNew ? 'New rule' : draft.id)}</h1>
          <span className="muted small">
            {isNew ? `From “${template.label}”` : item ? `Revision ${item.revision} · changed ${relTime(item.updated_at, Date.now())} by ${item.updated_by.replace(/^user:/, '')}` : ''}
          </span>
        </div>
        <span className="spacer" />
        {!isNew && item && (
          <button className="btn btn-ghost danger" onClick={() => setConfirmDelete(true)}>Delete</button>
        )}
      </header>

      {conflict && (
        <div className="notice notice-warn banner" role="alert">
          <strong>{conflict.updated_by.replace(/^user:/, '')} saved this rule while you were editing (revision {conflict.revision}).</strong>
          <div className="inline">
            <button className="btn btn-sm" onClick={() => { setItem(conflict); setDraft(conflict.rule); setConflict(null) }}>Load their version</button>
            <button className="btn btn-sm" onClick={() => void save(conflict)}>Keep mine and overwrite</button>
          </div>
        </div>
      )}

      <div className="editor-grid">
        <div className="editor-main">
          <div className="tabs" role="tablist">
            <button role="tab" aria-selected={tab === 'builder'} className="tab" onClick={() => openTab('builder')}>Builder</button>
            <button role="tab" aria-selected={tab === 'yaml'} className="tab" onClick={() => openTab('yaml')}>YAML</button>
          </div>

          {tab === 'yaml' ? (
            <div className="yaml-pane">
              <textarea className="input mono yaml-input" value={yamlText} onChange={(e) => onYaml(e.target.value)} spellCheck={false} aria-label="Rule as YAML" aria-invalid={!!yamlError || undefined} />
              {yamlError ? (
                <p className="problems-inline" role="alert">
                  {yamlError.line !== null && <>Line {yamlError.line}{yamlError.col !== null ? `, column ${yamlError.col}` : ''}: </>}
                  {yamlError.message}. The builder keeps the last version that parsed.
                </p>
              ) : (
                <p className="hint">Same rule, same draft: edits here show up in the builder and the test panel. This is the format of <code>config/rules/*.yaml</code>.</p>
              )}
            </div>
          ) : (
            <div className="sections">
              <Section title="Basics">
                <div className="grid-2">
                  <div className="form-field">
                    <label htmlFor="rule-name">Name</label>
                    <input id="rule-name" className="input" value={draft.name ?? ''} onChange={(e) => change({ ...draft, name: e.target.value })} placeholder="Large rejected trades" />
                  </div>
                  <div className="form-field">
                    <label htmlFor="rule-id">ID</label>
                    <input id="rule-id" className="input mono" value={draft.id} disabled={!isNew} onChange={(e) => { idTouched.current = true; change({ ...draft, id: e.target.value }) }} spellCheck={false} />
                    <Problems list={problemsAt(env, 'id')} />
                  </div>
                  <div className="form-field">
                    <label htmlFor="rule-team">Team</label>
                    <select id="rule-team" className="select" value={draft.team} onChange={(e) => change({ ...draft, team: e.target.value })}>
                      {teams.map((t) => <option key={t.id} value={t.id}>{t.name}</option>)}
                      {!teams.some((t) => t.id === draft.team) && <option value={draft.team}>{draft.team}</option>}
                    </select>
                  </div>
                  <div className="form-field">
                    <label htmlFor="rule-sev">Severity</label>
                    <select id="rule-sev" className="select" value={draft.severity ?? ''} onChange={(e) => {
                      const { severity: _s, ...rest } = draft
                      change(e.target.value ? { ...rest, severity: e.target.value as RuleSpec['severity'] } : (rest as RuleSpec))
                    }}>
                      <option value="">From the event</option>
                      {SEVERITIES.map((s) => <option key={s} value={s}>{SEVERITY_LABEL[s]}</option>)}
                    </select>
                  </div>
                </div>
                <label className="check">
                  <input type="checkbox" checked={draft.enabled !== false} onChange={(e) => change({ ...draft, enabled: e.target.checked })} />
                  Enabled
                </label>
              </Section>

              <Section title="When an event…" problems={problemsAt(env, 'when')}>
                <ConditionBuilder value={draft.when} onChange={(when) => change({ ...draft, when })} env={env} diagnostics={diagnostics} />
                <p className="hint">
                  <code>raw.…</code> is the event as sent; <code>fields.…</code> are values Beacon recognised (type, severity, service…). A missing value never matches, not even “is not”.
                </p>
              </Section>

              <Section title="Notify">
                <RouteEditor rule={draft} onChange={change} env={env} />
                <div className="inline">
                  <span className="small muted">If nobody acknowledges within</span>
                  <input className="input w-sm" value={draft.escalate_after ?? ''} placeholder="never" aria-label="Escalate after" onChange={(e) => {
                    const { escalate_after: _e, ...rest } = draft
                    change(e.target.value ? { ...rest, escalate_after: e.target.value } : (rest as RuleSpec))
                  }} />
                  <span className="small muted">escalate to the next on-call person{ruleTeam?.lead ? `, then ${ruleTeam.lead}` : ''}</span>
                </div>
                <Problems list={problemsAt(env, 'escalate_after')} />
              </Section>

              <Section title="How the alert looks">
                <PresentEditor rule={draft} onChange={change} env={env} />
              </Section>

              <Section title="Avoid floods">
                <NoiseEditor rule={draft} onChange={change} env={env} />
              </Section>

              <Section title="Runbook">
                <select className="select" value={draft.runbook ?? ''} aria-label="Runbook" onChange={(e) => {
                  const { runbook: _r, ...rest } = draft
                  change(e.target.value ? { ...rest, runbook: e.target.value } : (rest as RuleSpec))
                }}>
                  <option value="">None</option>
                  {runbooks.map((r) => (
                    <option key={r.id} value={r.id}>{r.title} ({FORMAT_LABEL[r.format] ?? r.format}{r.steps ? `, ${r.steps} step${r.steps === 1 ? '' : 's'}` : ''})</option>
                  ))}
                  {draft.runbook && !runbooks.some((r) => r.id === draft.runbook) && <option value={draft.runbook}>{draft.runbook}</option>}
                </select>
                <p className="hint">Shown on every alert from this rule, with the event’s values filled in.</p>
              </Section>
            </div>
          )}
        </div>

        <aside className="editor-side">
          <TestPanel rule={draft} initialSample={isNew ? template.sample : guessSample(draft)} onResult={onResult} onToast={onToast} />
        </aside>
      </div>

      <footer className={`save-bar ${dirty ? 'is-dirty' : ''}`}>
        <span className="small">
          {!dirty ? 'No unsaved changes' : blocking ? `${problems.length + (yamlError ? 1 : 0)} problem${problems.length + (yamlError ? 1 : 0) === 1 ? '' : 's'} to fix before saving` : 'Unsaved changes'}
        </span>
        <span className="spacer" />
        {dirty && !isNew && <button className="btn" onClick={() => { setDraft(item!.rule); setYamlText(toYaml(item!.rule)); setYamlError(null) }}>Discard</button>}
        <button className="btn btn-primary" disabled={!dirty || saving || blocking} onClick={() => void save()}>
          {saving ? 'Saving…' : isNew ? 'Create rule' : 'Save'}
        </button>
      </footer>

      {confirmDelete && item && (
        <div className="modal-backdrop" onClick={() => setConfirmDelete(false)}>
          <div className="modal" role="dialog" aria-modal="true" aria-labelledby="del-title" onClick={(e) => e.stopPropagation()}>
            <h2 id="del-title">Delete “{item.rule.name || item.rule.id}”?</h2>
            <p className="muted">New events stop matching it. Alerts it already raised stay in the feed.</p>
            <div className="inline end">
              <button className="btn" onClick={() => setConfirmDelete(false)} autoFocus>Cancel</button>
              <button className="btn btn-danger" onClick={() => void remove()}>Delete rule</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function Section({ title, children, problems = [] }: { title: string; children: React.ReactNode; problems?: string[] }) {
  return (
    <section className="editor-section">
      <h2>{title}</h2>
      {children}
      <Problems list={problems} />
    </section>
  )
}

/** Open an existing rule with the sample that best fits its conditions. */
function guessSample(r: RuleSpec): string {
  const s = JSON.stringify(r.when)
  if (s.includes('trade') || s.includes('payload')) return 'trade-rejected'
  if (s.includes('payments-api') || s.includes('prometheus')) return 'prometheus'
  if (s.includes('pricing') || s.includes('severity_rank')) return 'pricing-log'
  return 'workflow-failed'
}
