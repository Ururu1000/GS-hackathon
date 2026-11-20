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
      <header className="screen-head"><div className="screen-title"><h1>Runbooks</h1></div></header>
      {canImport && <div className="feature-columns">
        <section className="editor-section stack">
          <h2>Import runbook</h2>
          <label className="form-field">
            <span className="label">Title</span>
            <input className="input" value={draft.title} disabled={busy} maxLength={120} onChange={(e) => change({ title: e.target.value })} />
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
          <div className="inline">
            {draft.format !== 'link' && (
              <label className="form-field">
                <span className="label">Upload a file</span>
                <input type="file" accept=".md,.markdown,.yaml,.yml,.txt,text/plain" disabled={busy} onChange={(e) => void importFile(e.target.files?.[0])} />
              </label>
            )}
            <span className="spacer" />
            <button className="btn" disabled={busy || !teamId || !draft.title.trim() || !draft.content.trim()} onClick={() => void submit(false)}>Preview runbook</button>
          </div>
        </section>
        <section className="editor-section stack">
          <h2>Checklist preview</h2>
          {!preview && <p className="muted">Preview an import to edit its steps before saving.</p>}
          {preview && (
            <>
              {draft.format === 'link' && <p className="mono feature-wrap">{draft.content}</p>}
              <ol className="feature-checklist">
                {preview.steps.map((step, index) => (
                  <li key={step.id}>
                    <label className="form-field">
                      <span className="label">Step {index + 1}{step.action ? ` · ${step.action.label}` : ''}</span>
                      <textarea className="input feature-textarea" rows={2} maxLength={2_000} value={step.text} disabled={busy} onChange={(e) => setPreview({
                        ...preview, steps: preview.steps.map((current) => current.id === step.id ? { ...current, text: e.target.value } : current),
                      })} />
                    </label>
                    {step.hint && <p className="hint">{step.hint}</p>}
                    {step.approval === 'required' && <p className="hint">Approval required</p>}
                    <button className="btn btn-sm btn-ghost" disabled={busy} onClick={() => setPreview({ ...preview, steps: preview.steps.filter((current) => current.id !== step.id) })}>Remove step {index + 1}</button>
                  </li>
                ))}
              </ol>
              <div className="inline">
                {draft.format !== 'link' && (
                  <button className="btn" disabled={busy || preview.steps.length >= 50} onClick={() => setPreview({
                    ...preview, steps: [...preview.steps, { id: `manual-${crypto.randomUUID()}`, kind: 'manual', text: '' }],
                  })}>Add step</button>
                )}
                <button className="btn btn-primary" disabled={busy || (draft.format !== 'link' && !preview.steps.length) || preview.steps.some((step) => !step.text.trim())} onClick={() => void submit(true)}>Save runbook</button>
              </div>
            </>
          )}
          {problem && <p className="notice notice-warn" role="alert">{problem}</p>}
          {saved && <p className="notice" role="status">Saved {saved.title}. Choose it in the <a href="#/rules">rule editor</a> to attach it.</p>}
        </section>
      </div>}
      {!canImport && <p className="muted">The live service loads runbooks from its configuration.</p>}
      {!canImport && problem && <p className="notice notice-warn" role="alert">{problem}</p>}
      <section className="editor-section">
        <h2>Saved runbooks</h2>
        {items?.length ? <ul className="feature-items">{items.map((item) => (
          <li key={item.id}><strong>{item.title}</strong><span className="muted">{formatLabel(item.format)} · {item.steps} steps</span><code>{item.id}</code></li>
        ))}</ul> : <p className="muted">{items ? 'No saved runbooks.' : 'Runbook list unavailable while loading.'}</p>}
      </section>
    </div>
  )
}
