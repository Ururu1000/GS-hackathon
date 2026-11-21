import { useEffect, useState } from 'react'
import { errorText, listRunbooks } from '../api/client'
import { previewRunbook, publishRunbook, type RunbookImport, type RunbookPreview } from '../api/screens'
import type { RunbookFormat, RunbookSummary, Session } from '../api/types'

const formats: { id: RunbookFormat; label: string }[] = [
  { id: 'yaml', label: 'Structured YAML' },
  { id: 'markdown', label: 'Markdown' },
  { id: 'link', label: 'Link' },
  { id: 'text', label: 'Plain text' },
]
const formatLabel = (format: string) => formats.find((entry) => entry.id === format)?.label ?? format

export function RunbooksScreen({ teams, team, canImport }: { teams: Session['teams']; team: string | null; canImport: boolean }) {
  const [items, setItems] = useState<RunbookSummary[] | null>(null)
  const [draft, setDraft] = useState<RunbookImport>({ title: '', content: '', format: 'markdown', team: '' })
  const [preview, setPreview] = useState<RunbookPreview | null>(null)
  const [busy, setBusy] = useState(false)
  const [problem, setProblem] = useState<string | null>(null)
  const [saved, setSaved] = useState<RunbookSummary | null>(null)
  const teamId = draft.team || team || teams[0]?.id || ''

  useEffect(() => {
    let active = true
    listRunbooks().then(
      (books) => { if (active) setItems(books) },
      (e: unknown) => { if (active) setProblem(errorText(e)) },
    )
    return () => { active = false }
  }, [])

  const change = (patch: Partial<RunbookImport>) => {
    setDraft((previous) => ({ ...previous, ...patch }))
    setPreview(null)
    setSaved(null)
    setProblem(null)
  }

  const importFile = async (file?: File) => {
    if (!file) return
    if (file.size > 64 * 1024) {
      setProblem('The file must be 64 KiB or smaller.')
      return
    }
    try {
      const content = await file.text()
      change({ content, title: draft.title || file.name.replace(/\.[^.]+$/, '') })
    } catch (e) {
      setProblem(errorText(e))
    }
  }

  const submit = async (publish: boolean) => {
    const input = { ...draft, team: teamId }
    setProblem(null)
    setSaved(null)
    setBusy(true)
    try {
      if (publish && preview) {
        const book = await publishRunbook(input, preview)
        setItems((previous) => [...(previous ?? []).filter((item) => item.id !== book.id), book])
        setSaved(book)
        setPreview(null)
        setDraft({ title: '', content: '', format: 'markdown', team: teamId })
      } else {
        setPreview(await previewRunbook(input))
      }
    } catch (e) {
      setProblem(errorText(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="screen feature-screen">
      <header className="screen-head">
        <div className="screen-title">
          <h1>Runbooks</h1>
          <span className="muted small">Step-by-step checklists that responders follow from an alert. Attach one to a rule in the rule editor.</span>
        </div>
      </header>
      <div className="feature-body">
      {!canImport && <div className="notice">The live service loads runbooks from its configuration.</div>}
      {!canImport && problem && <div className="notice notice-error" role="alert">{problem}</div>}
      {canImport && <div className="feature-columns">
        <section className="feature-card">
          <div className="card-head">
            <div>
              <h2>Import runbook</h2>
              <span className="hint">Paste or upload a document, or link to one. You can edit the steps before saving.</span>
            </div>
          </div>
          <label className="form-field">
            <span className="label">Title</span>
            <input className="input" value={draft.title} disabled={busy} maxLength={120} placeholder="e.g. Settlement timeout" onChange={(e) => change({ title: e.target.value })} />
          </label>
          <div className="grid-2">
            <label className="form-field">
              <span className="label">Format</span>
              <select className="select" value={draft.format} disabled={busy} onChange={(e) => {
                const format = formats.find((entry) => entry.id === e.target.value)
                if (format) change({ format: format.id })
              }}>
                {formats.map((entry) => <option key={entry.id} value={entry.id}>{entry.label}</option>)}
              </select>
            </label>
            <label className="form-field">
              <span className="label">Team</span>
              <select className="select" value={teamId} disabled={busy || !teams.length} onChange={(e) => change({ team: e.target.value })}>
                {!teams.length && <option value="">No available teams</option>}
                {teams.map((entry) => <option key={entry.id} value={entry.id}>{entry.name}</option>)}
              </select>
            </label>
          </div>
          <label className="form-field">
            <span className="label">{draft.format === 'link' ? 'Runbook URL' : 'Runbook content'}</span>
            {draft.format === 'link'
              ? <input className="input mono" type="url" value={draft.content} disabled={busy} onChange={(e) => change({ content: e.target.value })} placeholder="https://wiki.example/runbook" />
              : <textarea className="input mono feature-textarea" rows={10} value={draft.content} maxLength={64 * 1024} disabled={busy} onChange={(e) => change({ content: e.target.value })} />}
          </label>
          <div className="inline wrap">
            {draft.format !== 'link' && (
              <label className="inline">
                <span className="sr-only">Upload a file</span>
                <input className="file-input" type="file" accept=".md,.markdown,.yaml,.yml,.txt,text/plain" disabled={busy} onChange={(e) => void importFile(e.target.files?.[0])} />
              </label>
            )}
            <span className="spacer" />
            <button className="btn btn-primary" disabled={busy || !teamId || !draft.title.trim() || !draft.content.trim()} onClick={() => void submit(false)}>Preview runbook</button>
          </div>
        </section>
        <section className="feature-card">
          <div className="card-head">
            <div>
              <h2>Checklist preview</h2>
              <span className="hint">What responders will see. Edit or remove steps, then save.</span>
            </div>
          </div>
          {!preview && <p className="card-empty">Preview an import to edit its steps before saving.</p>}
          {preview && (
            <>
              {draft.format === 'link' && <p className="mono feature-wrap">{draft.content}</p>}
              {preview.steps.length > 0 && <ol className="feature-checklist">
                {preview.steps.map((step, index) => (
                  <li key={step.id}>
                    <span className="step-n" aria-hidden="true">{index + 1}</span>
                    <div className="stack tight">
                      {step.action && <span className="small muted">{step.action.label}</span>}
                      <textarea className="input feature-textarea" rows={2} maxLength={2_000} value={step.text} disabled={busy} aria-label={`Step ${index + 1}${step.action ? ` · ${step.action.label}` : ''}`} onChange={(e) => setPreview({
                        ...preview, steps: preview.steps.map((current) => current.id === step.id ? { ...current, text: e.target.value } : current),
                      })} />
                      {step.hint && <p className="hint">{step.hint}</p>}
                      {step.approval === 'required' && <span><span className="pill pill-approval">Approval required</span></span>}
                    </div>
                    <button className="icon-btn sm" disabled={busy} aria-label={`Remove step ${index + 1}`} title="Remove step" onClick={() => setPreview({ ...preview, steps: preview.steps.filter((current) => current.id !== step.id) })}>✕</button>
                  </li>
                ))}
              </ol>}
              <div className="inline">
                {draft.format !== 'link' && (
                  <button className="btn" disabled={busy || preview.steps.length >= 50} onClick={() => setPreview({
                    ...preview, steps: [...preview.steps, { id: `manual-${crypto.randomUUID()}`, kind: 'manual', text: '' }],
                  })}>Add step</button>
                )}
                <span className="spacer" />
                <button className="btn btn-primary" disabled={busy || (draft.format !== 'link' && !preview.steps.length) || preview.steps.some((step) => !step.text.trim())} onClick={() => void submit(true)}>Save runbook</button>
              </div>
            </>
          )}
          {problem && <div className="notice notice-error" role="alert">{problem}</div>}
          {saved && <div className="notice" role="status">Saved {saved.title}. Choose it in the <a href="#/rules">rule editor</a> to attach it.</div>}
        </section>
      </div>}
      <section className="feature-card">
        <div className="card-head">
          <h2>Saved runbooks {items && <span className="count">{items.length}</span>}</h2>
        </div>
        {items?.length ? <ul className="feature-items">{items.map((item) => (
          <li key={item.id}>
            <div className="grow">
              <strong>{item.title}</strong>
              <code>{item.id}</code>
            </div>
            <span className="pill">{formatLabel(item.format)}</span>
            <span className="muted small">{item.steps} {item.steps === 1 ? 'step' : 'steps'}</span>
          </li>
        ))}</ul> : <p className="card-empty">{items ? 'No saved runbooks.' : 'Runbook list unavailable while loading.'}</p>}
      </section>
      </div>
    </div>
  )
}
