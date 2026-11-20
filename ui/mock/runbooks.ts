// Runbook state for one alert instance in the dev mock: manual ticks,
// executions and approval requests per step, turned into the RunbookView the
// UI renders (spec §6). Execution and approval *transitions* live in mock.ts,
// next to the alert they bump.

import type { ApprovalRequest, Execution, RunbookStep, RunbookView, StepState } from '../src/api/types.ts'
import { render, type Ctx } from './engine.ts'
import type { RunbookDef, RunbookStepDef } from './scenario.ts'

export interface StepRuntime {
  done_by: string | null
  done_at: string | null
  runs: Execution[]
  approvals: ApprovalRequest[]
}

export type RunbookRuntime = Map<string, StepRuntime>

export function runtimeOf(rt: RunbookRuntime, step: string): StepRuntime {
  let s = rt.get(step)
  if (!s) rt.set(step, (s = { done_by: null, done_at: null, runs: [], approvals: [] }))
  return s
}

export const failedRuns = (rt: RunbookRuntime, step: string) =>
  (rt.get(step)?.runs ?? []).filter((r) => r.state === 'failed').length

/** Pending requests past their expiry read as expired. */
export function expire(a: ApprovalRequest, now: number): ApprovalRequest {
  return a.state === 'pending' && Date.parse(a.expires_at) <= now ? { ...a, state: 'expired' } : a
}

export function stepState(def: RunbookStepDef, rt: RunbookRuntime, now: number): { state: StepState; disabled_reason: string | null } {
  const s = rt.get(def.id)
  let disabled_reason: string | null = null
  if (def.after_failures) {
    const n = failedRuns(rt, def.after_failures.step)
    if (n < def.after_failures.count)
      disabled_reason = `Available after ${def.after_failures.count} failed retries (${n} so far)`
  }
  if (def.kind === 'manual') return { state: s?.done_by ? 'done' : 'idle', disabled_reason: null }
  const approval = s?.approvals.length ? expire(s.approvals[s.approvals.length - 1]!, now) : null
  const run = s?.runs[s.runs.length - 1] ?? null
  if (approval?.state === 'pending') return { state: 'awaiting_approval', disabled_reason }
  // A rejection newer than the latest run is what the step shows.
  const rejectedLast = approval?.state === 'rejected' && (!run || (run.started_at ?? '') < (approval.decided_at ?? ''))
  if (rejectedLast) return { state: 'rejected', disabled_reason }
  if (run) return { state: run.state, disabled_reason }
  return { state: 'idle', disabled_reason }
}

export function viewRunbook(def: RunbookDef, ctx: Ctx, rt: RunbookRuntime, viewer: string, now = Date.now()): RunbookView {
  const steps: RunbookStep[] = def.steps.map((d) => {
    const s = rt.get(d.id)
    const { state, disabled_reason } = stepState(d, rt, now)
    const last = s?.approvals.length ? expire(s.approvals[s.approvals.length - 1]!, now) : null
    return {
      id: d.id,
      kind: d.kind,
      text: render(d.text, ctx),
      hint: d.hint ?? null,
      action: d.action
        ? { ...d.action, params: Object.fromEntries(Object.entries(d.action.params).map(([k, v]) => [k, render(v, ctx)])) }
        : null,
      approval: d.approval ?? 'none',
      state,
      disabled_reason,
      done_by: s?.done_by ?? null,
      done_at: s?.done_at ?? null,
      runs: s?.runs ?? [],
      approval_request: last ? { ...last, can_decide: last.eligible.includes(viewer) } : null,
    }
  })
  return { id: def.id, revision: 1, title: def.title, format: def.format, url: def.url ?? null, text: def.text ?? null, steps }
}
