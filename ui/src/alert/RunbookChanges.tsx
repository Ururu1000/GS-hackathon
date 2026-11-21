// "Runbook since this incident" on a resolved alert (spec §11 M7, post-incident
// diffs): what the runbook looked like when the alert notified, what the team
// changed since, and which steps were completed during the incident.

import { useEffect, useState } from 'react'
import { errorText, getRunbook, HttpError } from '../api/client'
import type { RunbookDetail, RunbookStep, RunbookView } from '../api/types'
import { redactText } from '../lib/redact'
import { absTime } from '../lib/time'
import { actorName } from './badges'
import { completedSteps, diffRunbook, type StepField } from './runbookDiff'

const FIELD_LABEL: Record<StepField, string> = { text: 'wording', kind: 'step type', action: 'action', approval: 'approval' }

type Current = { kind: 'loaded'; runbook: RunbookDetail } | { kind: 'gone' } | { kind: 'error'; message: string }

function stepLabel(s: { text: string; action: { label: string } | null }) {
  return redactText(s.action?.label ? `${s.action.label}: ${s.text}` : s.text, 200)
}

function doneNote(s: RunbookStep) {
  if (s.done_by) return `ticked by ${actorName(s.done_by)}${s.done_at ? ` · ${absTime(s.done_at)}` : ''}`
  return s.state === 'ok' ? 'ran successfully' : 'done'
}

export function RunbookChanges({ runbook }: { runbook: RunbookView }) {
  const [current, setCurrent] = useState<Current | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    getRunbook(runbook.id, controller.signal).then(
      (rb) => { if (!controller.signal.aborted) setCurrent({ kind: 'loaded', runbook: rb }) },
      (e: unknown) => {
        if (controller.signal.aborted) return
        setCurrent(e instanceof HttpError && e.status === 404 ? { kind: 'gone' } : { kind: 'error', message: errorText(e) })
      },
    )
    return () => controller.abort()
  }, [runbook.id, runbook.revision])

  const done = completedSteps(runbook)
  const diff = current?.kind === 'loaded' ? diffRunbook(runbook, current.runbook) : null
  const changeCount = diff ? diff.added.length + diff.removed.length + diff.changed.length : 0

  return (
    <section className="section rb-changes" aria-labelledby="rb-changes-title">
      <div className="rb-head">
        <h3 id="rb-changes-title">Runbook since this incident</h3>
        <span className="muted small">{runbook.title} · revision {runbook.revision} at the time</span>
      </div>

      {!current && <p className="small muted">Checking the current runbook…</p>}
      {current?.kind === 'error' && <p className="small muted">Couldn’t load the current runbook: {current.message}</p>}
      {current?.kind === 'gone' && <p className="small">This runbook has been removed since the incident.</p>}
      {current?.kind === 'loaded' && !diff && <p className="small">Unchanged since this incident (revision {runbook.revision}).</p>}

      {diff && (
        <>
          <p className="small">
            <strong>Changed since this incident: revision {diff.from} → {diff.to}</strong>
            {changeCount === 0 && ' (no step changes)'}
            {diff.reordered && ' · steps reordered'}
          </p>
          {changeCount > 0 && (
            <ul className="diff rb-diff" aria-label="Step changes">
              {diff.removed.map((s) => (
                <li key={`-${s.id}`} className="diff-line diff-del">
                  <span className="diff-op" aria-hidden="true">−</span>
                  <span className="sr-only">Removed: </span>{stepLabel(s)}
                </li>
              ))}
              {diff.changed.map((c) => (
                <li key={`~${c.id}`} className="rb-diff-changed">
                  <div className="diff-line diff-del"><span className="diff-op" aria-hidden="true">−</span><span className="sr-only">Before: </span>{stepLabel(c.before)}</div>
                  <div className="diff-line diff-add"><span className="diff-op" aria-hidden="true">+</span><span className="sr-only">Now: </span>{stepLabel(c.after)}</div>
                  <div className="diff-gap">changed {c.fields.map((f) => FIELD_LABEL[f]).join(', ')}</div>
                </li>
              ))}
              {diff.added.map((s) => (
                <li key={`+${s.id}`} className="diff-line diff-add">
                  <span className="diff-op" aria-hidden="true">+</span>
                  <span className="sr-only">Added: </span>{stepLabel(s)}
                </li>
              ))}
            </ul>
          )}
        </>
      )}

      <p className="small muted">
        {done.length
          ? `Completed during the incident: ${done.length} of ${runbook.steps.length} steps.`
          : `No steps were completed during the incident (${runbook.steps.length} in the runbook).`}
      </p>
      {done.length > 0 && (
        <ul className="rb-done">
          {done.map((s) => <li key={s.id} className="small"><span>{stepLabel(s)}</span> <span className="muted">· {doneNote(s)}</span></li>)}
        </ul>
      )}
    </section>
  )
}
