// Rule sections shared by the rule editor and "Suggest a change": recipients,
// presentation, and noise controls (dedup, grouping, throttle).

import { useId } from 'react'
import { CHANNELS, COLOR_TOKENS, type ColorToken, type FieldSpec, type PersonDirectoryEntry, type RouteSpec, type RuleSpec, type TeamView } from '../api/types'
import { CloseIcon } from '../lib/icons'

export interface EditorEnv {
  teams: TeamView[]
  people: PersonDirectoryEntry[]
  /** Paths from the current test event, for suggestions. */
  paths: { path: string; value: unknown }[]
  problems: { path: string; message: string }[]
}

export const CHANNEL_NAME: Record<string, string> = { phone: 'Call', sms: 'SMS', email: 'Email', ui: 'In-app' }

export function Problems({ list }: { list: string[] }) {
  if (!list.length) return null
  return (
    <ul className="problems" role="alert">
      {list.map((m, i) => (
        <li key={i}>{m}</li>
      ))}
    </ul>
  )
}

export function problemsAt(env: EditorEnv, prefix: string): string[] {
  return env.problems.filter((p) => p.path === prefix || p.path.startsWith(`${prefix}.`)).map((p) => p.message)
}

/** Toggle buttons for a channel set. */
export function ChannelPicker({ value, onChange, label }: { value: string[]; onChange: (v: string[]) => void; label: string }) {
  return (
    <div className="chips channel-picker" role="group" aria-label={label}>
      {CHANNELS.map((c) => {
        const on = value.includes(c)
        return (
          <button
            key={c}
            type="button"
            className="chip"
            aria-pressed={on}
            onClick={() => onChange(on ? value.filter((x) => x !== c) : CHANNELS.filter((x) => x === c || value.includes(x)))}
          >
            {CHANNEL_NAME[c]}
          </button>
        )
      })}
    </div>
  )
}

/** Text input with event-path suggestions. */
export function PathInput({ value, onChange, env, label, className = '' }: { value: string; onChange: (v: string) => void; env: EditorEnv; label: string; className?: string }) {
  const list = useId()
  return (
    <>
      <input className={`input mono ${className}`} value={value} onChange={(e) => onChange(e.target.value)} list={list} aria-label={label} spellCheck={false} placeholder="raw.…" />
      <datalist id={list}>
        {env.paths.map((p) => (
          <option key={p.path} value={p.path}>
            {String(p.value).slice(0, 60)}
          </option>
        ))}
      </datalist>
    </>
  )
}

// ---- recipients -------------------------------------------------------------

type TargetKind = 'team' | 'user' | 'oncall'
const KIND_LABEL: Record<TargetKind, string> = { team: 'Everyone in team', user: 'Person', oncall: 'Whoever is on call for' }

export function RouteEditor({ rule, onChange, env }: { rule: RuleSpec; onChange: (r: RuleSpec) => void; env: EditorEnv }) {
  const schedules = env.teams.flatMap((t) => Object.keys(t.oncall))
  const set = (i: number, r: RouteSpec | null) =>
    onChange({ ...rule, route: r === null ? rule.route.filter((_, j) => j !== i) : rule.route.map((x, j) => (j === i ? r : x)) })
  return (
    <div className="stack">
      {rule.route.map((r, i) => {
        const [kind = 'team', id = ''] = r.to.split(':') as [TargetKind, string]
        const options = kind === 'team' ? env.teams.map((t) => [t.id, t.name]) : kind === 'user' ? env.people.map((p) => [p.id, p.name]) : schedules.map((s) => [s, s])
        return (
          <div key={i} className="route-row">
            <div className="inline">
              <select className="select" value={kind} aria-label="Who" onChange={(e) => set(i, { ...r, to: `${e.target.value}:` })}>
                {(Object.keys(KIND_LABEL) as TargetKind[]).map((k) => (
                  <option key={k} value={k}>{KIND_LABEL[k]}</option>
                ))}
              </select>
              <select className="select" value={id} aria-label="Recipient" onChange={(e) => set(i, { ...r, to: `${kind}:${e.target.value}` })}>
                <option value="" disabled>Choose…</option>
                {options.map(([v, l]) => (
                  <option key={v} value={v}>{l}</option>
                ))}
                {id && !options.some(([v]) => v === id) && <option value={id}>{id} (unknown)</option>}
              </select>
              {rule.route.length > 1 && (
                <button type="button" className="icon-btn" onClick={() => set(i, null)} aria-label="Remove recipient" title="Remove recipient">
                  <CloseIcon size={14} />
                </button>
              )}
            </div>
            <div className="inline">
              <span className="muted small">by</span>
              <ChannelPicker
                label="Channels"
                value={r.channels ?? []}
                onChange={(c) => {
                  const { channels: _drop, ...rest } = r
                  set(i, c.length ? { ...rest, channels: c } : rest)
                }}
              />
              {!r.channels?.length && <span className="muted small">their own preferences</span>}
            </div>
            <Problems list={problemsAt(env, `route.${i}`)} />
          </div>
        )
      })}
      <Problems list={env.problems.filter((p) => p.path === 'route').map((p) => p.message)} />
      <div>
        <button type="button" className="btn btn-sm" onClick={() => onChange({ ...rule, route: [...rule.route, { to: `team:${rule.team}` }] })}>
          Add recipient
        </button>
      </div>
    </div>
  )
}

// ---- presentation ---------------------------------------------------------------

const asField = (f: FieldSpec | string): FieldSpec => (typeof f === 'string' ? { path: f, label: f.split('.').pop() ?? f } : f)

export function PresentEditor({ rule, onChange, env }: { rule: RuleSpec; onChange: (r: RuleSpec) => void; env: EditorEnv }) {
  const p = rule.present
  const fields = (p.fields ?? []).map(asField)
  const setP = (next: Partial<RuleSpec['present']>) => onChange({ ...rule, present: { ...p, ...next } })
  const setField = (i: number, f: FieldSpec | null) => setP({ fields: f === null ? fields.filter((_, j) => j !== i) : fields.map((x, j) => (j === i ? f : x)) })
  const titleId = useId()
  const bodyId = useId()
  return (
    <div className="stack">
      <div className="form-field">
        <label htmlFor={titleId}>Title</label>
        <input id={titleId} className="input mono" value={p.title} onChange={(e) => setP({ title: e.target.value })} spellCheck={false} placeholder="Rejected {{ raw.payload.currency }} trade {{ raw.payload.tradeId }}" />
        <Problems list={problemsAt(env, 'present.title')} />
      </div>
      <div className="form-field">
        <label htmlFor={bodyId}>Body <span className="muted">(optional)</span></label>
        <textarea id={bodyId} className="input mono" rows={2} value={p.body ?? ''} onChange={(e) => setP({ body: e.target.value || undefined })} spellCheck={false} />
        <Problems list={problemsAt(env, 'present.body')} />
        <p className="hint">
          Use <code>{'{{ raw.path }}'}</code> or <code>{'{{ fields.name }}'}</code> to fill in values. Filters: <code>| money</code>, <code>| upper</code>, <code>| lower</code>.
        </p>
      </div>
      <div className="form-field">
        <span className="label">Fields shown on the alert</span>
        {fields.map((f, i) => (
          <div key={i} className="inline field-row">
            <input className="input field-label-input" value={f.label} onChange={(e) => setField(i, { ...f, label: e.target.value })} aria-label="Label" placeholder="Label" />
            <PathInput value={f.path} onChange={(v) => setField(i, { ...f, path: v })} env={env} label="Path" className="grow" />
            <button type="button" className="icon-btn" onClick={() => setField(i, null)} aria-label={`Remove field ${f.label}`} title="Remove field">
              <CloseIcon size={14} />
            </button>
          </div>
        ))}
        <Problems list={problemsAt(env, 'present.fields')} />
        <div>
          <button type="button" className="btn btn-sm" onClick={() => setP({ fields: [...fields, { path: '', label: '' }] })}>
            Add field
          </button>
        </div>
      </div>
      <div className="form-field">
        <span className="label">Colour in the feed</span>
        <div className="swatches" role="radiogroup" aria-label="Colour">
          {COLOR_TOKENS.map((c: ColorToken) => (
            <button key={c} type="button" role="radio" aria-checked={(p.color ?? 'grey') === c} className={`swatch tok-${c}`} onClick={() => setP({ color: c })} title={c} aria-label={c} />
          ))}
        </div>
        <p className="hint">The severity label is always shown too, so colour is never the only signal.</p>
      </div>
    </div>
  )
}

// ---- noise ----------------------------------------------------------------------------

function PathList({ value, onChange, env, label }: { value: string[]; onChange: (v: string[]) => void; env: EditorEnv; label: string }) {
  return (
    <div className="stack tight">
      {value.map((k, i) => (
        <div key={i} className="inline">
          <PathInput value={k} onChange={(v) => onChange(value.map((x, j) => (j === i ? v : x)))} env={env} label={`${label} ${i + 1}`} className="grow" />
          <button type="button" className="icon-btn" onClick={() => onChange(value.filter((_, j) => j !== i))} aria-label={`Remove ${k}`} title="Remove">
            <CloseIcon size={14} />
          </button>
        </div>
      ))}
      <div>
        <button type="button" className="btn btn-sm" onClick={() => onChange([...value, ''])}>
          Add path
        </button>
      </div>
    </div>
  )
}

export function NoiseEditor({ rule, onChange, env }: { rule: RuleSpec; onChange: (r: RuleSpec) => void; env: EditorEnv }) {
  const dedup = rule.dedup ?? { key: [], window: '10m' }
  const without = <K extends keyof RuleSpec>(k: K): RuleSpec => {
    const { [k]: _drop, ...rest } = rule
    return rest as RuleSpec
  }
  return (
    <div className="stack">
      <div className="form-field">
        <span className="label">Same alert when these match</span>
        <PathList value={dedup.key} onChange={(key) => onChange({ ...rule, dedup: { ...dedup, key } })} env={env} label="Dedup key" />
        <div className="inline">
          <span className="small muted">and it happened again within</span>
          <input className="input w-sm" value={dedup.window} onChange={(e) => onChange({ ...rule, dedup: { ...dedup, window: e.target.value } })} aria-label="Dedup window" placeholder="10m" />
        </div>
        <Problems list={problemsAt(env, 'dedup')} />
        <p className="hint">Repeats bump the counter on one alert instead of raising new ones.</p>
      </div>
      <div className="form-field">
        <label className="check">
          <input type="checkbox" checked={!!rule.group} onChange={(e) => onChange(e.target.checked ? { ...rule, group: { by: [], window: '5m' } } : without('group'))} />
          Send a digest instead of one alert each
        </label>
        {rule.group && (
          <div className="stack tight indent">
            <PathList value={rule.group.by} onChange={(by) => onChange({ ...rule, group: { ...rule.group!, by } })} env={env} label="Group by" />
            <div className="inline">
              <span className="small muted">every</span>
              <input className="input w-sm" value={String(rule.group.window)} onChange={(e) => onChange({ ...rule, group: { ...rule.group!, window: e.target.value } })} aria-label="Digest window" />
            </div>
          </div>
        )}
      </div>
      <div className="form-field">
        <label className="check">
          <input type="checkbox" checked={!!rule.throttle} onChange={(e) => onChange(e.target.checked ? { ...rule, throttle: { max: 20, per: '1m' } } : without('throttle'))} />
          Limit notifications per person
        </label>
        {rule.throttle && (
          <div className="inline indent">
            <span className="small muted">at most</span>
            <input className="input w-xs" type="number" min={1} value={rule.throttle.max} onChange={(e) => onChange({ ...rule, throttle: { ...rule.throttle!, max: Number(e.target.value) } })} aria-label="Max notifications" />
            <span className="small muted">per</span>
            <input className="input w-sm" value={rule.throttle.per} onChange={(e) => onChange({ ...rule, throttle: { ...rule.throttle!, per: e.target.value } })} aria-label="Throttle period" />
          </div>
        )}
        <Problems list={problemsAt(env, 'throttle')} />
      </div>
    </div>
  )
}
