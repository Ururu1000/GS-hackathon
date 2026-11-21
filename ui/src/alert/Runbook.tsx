// The alert's runbook with filled-in steps (spec §6, §7.2): manual checklist
// items, action buttons, and approvals for risky steps. Every change goes
// through the API; the pane then refetches.

import { useState } from 'react'
import { checkStep, decideApproval, errorText, runStep } from '../api/client'
import type { ApprovalRequest, Execution, FeedAlert, RunbookStep, RunbookView } from '../api/types'
import { absTime, relTime } from '../lib/time'
import { actorName } from './badges'

interface Props {
  runbook: RunbookView
  alert: FeedAlert
  now: number
  onChanged: () => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
}

const FORMAT_LABEL: Record<string, string> = { yaml: 'Steps', markdown: 'Checklist', link: 'Wiki page', text: 'Notes' }

/** Only http(s) links without embedded credentials are followed; anything else is shown as text. */
export function safeUrl(u: string | null): string | null {
  if (!u || u.length > 2048 || /[\x00-\x1f\x7f]/.test(u)) return null
  try {
    const url = new URL(u)
    if ((url.protocol !== 'https:' && url.protocol !== 'http:') || url.username || url.password) return null
    return url.href
  } catch {
    return null
  }
}

/** States that change on their own; the pane polls while any step is in one. */
export const LIVE_STATES = ['queued', 'running', 'awaiting_approval']

export function Runbook({ runbook: rb, alert, now, onChanged, onToast }: Props) {
  const [busy, setBusy] = useState<string | null>(null)
  const url = safeUrl(rb.url)
  const closed = alert.status === 'resolved'

  const act = async (key: string, fn: () => Promise<unknown>) => {
    setBusy(key)
    try {
      await fn()
    } catch (e) {
      onToast('error', errorText(e))
    } finally {
      setBusy(null)
      onChanged()
    }
  }

  const run = (s: RunbookStep) =>
    act(s.id, async () => {
      const r = await runStep(alert.id, s.id, alert.version)
      if (r.kind === 'approval') onToast('ok', `Approval requested for “${s.action?.label ?? s.text}”.`)
    })

  return (
    <section className="section runbook" aria-labelledby="rb-title">
      <div className="rb-head">
        <h3 id="rb-title">What to do</h3>
        <span className="muted small">{rb.title} · {FORMAT_LABEL[rb.format] ?? rb.format}</span>
      </div>

      {rb.format === 'link' && (
        url ? (
          <a className="btn rb-link" href={url} target="_blank" rel="noopener noreferrer">
            Open “{rb.title}” ↗
          </a>
        ) : (
          <p className="small muted">Runbook link: <code>{rb.url}</code> (not an http(s) link, so it isn’t opened from here)</p>
        )
      )}

      {rb.format === 'text' && rb.text && <blockquote className="rb-text">{rb.text}</blockquote>}

      {rb.steps.length > 0 && (
        <ol className="steps">
          {rb.steps.map((s, i) => (
            <Step key={s.id} step={s} n={i + 1} now={now} closed={closed} busy={busy} onRun={() => void run(s)}
              onCheck={(done) => void act(s.id, () => checkStep(alert.id, s.id, done))}
              onDecide={(op, note) => void act(`${s.id}:${op}`, async () => {
                await decideApproval(s.approval_request!.id, op, note)
                onToast('ok', op === 'approve' ? `Approved. Running “${s.action?.label ?? s.text}”.` : 'Rejected.')
              })}
            />
          ))}
        </ol>
      )}

      {rb.format === 'markdown' && rb.text && (
        <details className="rb-original">
          <summary className="small">Original page</summary>
          <pre>{rb.text}</pre>
        </details>
      )}
    </section>
  )
}

const RUN_TEXT: Record<Execution['state'], string> = { queued: 'Queued', running: 'Running…', ok: 'Done', failed: 'Failed', uncertain: 'Outcome unknown', cancelled: 'Cancelled' }

function Step({ step: s, n, now, closed, busy, onRun, onCheck, onDecide }: {
  step: RunbookStep
  n: number
  now: number
  closed: boolean
  busy: string | null
  onRun: () => void
  onCheck: (done: boolean) => void
  onDecide: (op: 'approve' | 'reject', note?: string) => void
}) {
  const last = s.runs[s.runs.length - 1] ?? null
  const running = s.state === 'queued' || s.state === 'running'
  const failures = s.runs.filter((r) => r.state === 'failed').length
  const isBusy = busy === s.id

  if (s.kind === 'manual')
    return (
      <li className={`step step-manual ${s.state === 'done' ? 'is-done' : ''}`}>
        <label className="step-check">
          <input type="checkbox" checked={s.state === 'done'} disabled={isBusy} onChange={(e) => onCheck(e.target.checked)} />
          <span className="step-text">{s.text}</span>
        </label>
        {s.hint && <p className="step-hint">{s.hint}</p>}
        {s.done_by && <p className="step-meta">Done by {actorName(s.done_by)} <span title={absTime(s.done_at)}>{relTime(s.done_at, now)}</span></p>}
      </li>
    )

  if (s.kind === 'condition' || !s.action) return (
    <li className="step">
      <span className="step-text">{s.text}</span>
      {s.hint && <p className="step-hint">{s.hint}</p>}
    </li>
  )

  const needsApproval = s.approval === 'required'
  const req = s.approval_request
  const label = s.action?.label ?? 'Run'
  const disabled = closed || isBusy || running || s.state === 'awaiting_approval' || !!s.disabled_reason
  return (
    <li className={`step step-action state-${s.state} ${s.disabled_reason ? 'is-locked' : ''}`}>
      <div className="step-row">
        <span className="step-n" aria-hidden="true">{n}</span>
        <div className="step-main">
          <span className="step-text">{s.text}</span>
          {s.action && (
            <div className="step-params">
              <code>{s.action.name}</code>
              {Object.entries(s.action.params).map(([k, v]) => (
                <span key={k} className="param"><span className="muted">{k}</span> {v}</span>
              ))}
              {s.action.risk === 'high' && <span className="pill pill-risk">Risky</span>}
              {needsApproval && <span className="pill pill-approval">Needs approval</span>}
            </div>
          )}
          {s.hint && <p className="step-hint">{s.hint}</p>}
        </div>
        <button className={`btn btn-sm ${s.state === 'idle' && !s.disabled_reason ? 'btn-primary' : ''}`} disabled={disabled} onClick={onRun}
          title={closed ? 'The alert is resolved' : s.disabled_reason ?? undefined}>
          {isBusy ? 'Starting…' : running ? RUN_TEXT[last?.state ?? 'queued'] : needsApproval ? (s.state === 'rejected' ? 'Ask again' : s.state === 'awaiting_approval' ? 'Waiting…' : 'Ask for approval') : last ? 'Run again' : label}
        </button>
      </div>

      {s.disabled_reason && <p className="step-meta">{s.disabled_reason}</p>}

      {req && (req.state === 'pending' || s.state === 'rejected') && <Approval req={req} now={now} busy={busy} stepId={s.id} closed={closed} onDecide={onDecide} />}

      {last && (
        <p className={`step-meta run-${last.state}`} aria-live="polite">
          <strong>{RUN_TEXT[last.state]}</strong>
          {last.output && <> · {last.output}</>}
          <span className="muted"> · {actorName(last.requested_by)} {relTime(last.finished_at ?? last.started_at ?? null, now)}</span>
          {failures > 1 && <span className="muted"> · failed {failures} times</span>}
          {last.state === 'uncertain' && <span> · Check the target system before running it again.</span>}
        </p>
      )}
    </li>
  )
}

function Approval({ req, now, busy, stepId, closed, onDecide }: { req: ApprovalRequest; now: number; busy: string | null; stepId: string; closed: boolean; onDecide: (op: 'approve' | 'reject', note?: string) => void }) {
  const [note, setNote] = useState('')
  if (req.state === 'rejected')
    return (
      <p className="step-meta run-failed">
        Rejected by {actorName(req.decided_by)} {relTime(req.decided_at, now)}{req.note ? `: “${req.note}”` : ''}
      </p>
    )
  const expiresIn = Math.max(0, Math.round((Date.parse(req.expires_at) - now) / 60_000))
  return (
    <div className="approval" role="group" aria-label="Approval request">
      <p className="small">
        <strong>Waiting for approval.</strong> {actorName(req.requested_by)} asked <span title={absTime(req.requested_at)}>{relTime(req.requested_at, now)}</span>; expires in {expiresIn} min.
        <br />
        <span className="muted">
          Approvers: {req.eligible.length > 0 ? req.eligible.map(actorName).join(', ') : 'none configured'}
          {req.can_decide && req.sms_sent && <> · or reply to the SMS sent to your phone</>}
        </span>
      </p>
      {req.can_decide ? (
        <div className="inline wrap">
          <input
            className="input input-sm grow"
            value={note}
            maxLength={2000}
            onChange={(e) => setNote(e.target.value)}
            placeholder="Note (optional — logged in audit trail; avoid PII/secrets)"
            aria-label="Approval note"
          />
          <button className="btn btn-sm" disabled={busy !== null || closed} onClick={() => onDecide('reject', note)}>{busy === `${stepId}:reject` ? 'Rejecting…' : 'Reject'}</button>
          <button className="btn btn-sm btn-primary" disabled={busy !== null || closed} onClick={() => onDecide('approve', note)}>{busy === `${stepId}:approve` ? 'Approving…' : 'Approve and run'}</button>
        </div>
      ) : (
        <p className="small muted">You can’t approve this one.</p>
      )}
    </div>
  )
}
