// Live test: runs the draft rule against a pasted or picked event on the
// server (`POST /api/rules/test`, zero writes, zero dispatches). Debounced;
// stale responses are dropped.

import { useEffect, useRef, useState } from 'react'
import { errorText, ingest, recentEvents, testRule } from '../api/client'
import type { DiscoveredSchema } from '../api/screens'
import type { RecentEvent, RuleSpec, RuleTestResult } from '../api/types'
import { colorToken, SeverityBadge } from '../alert/badges'
import { relTime } from '../lib/time'
import { SAMPLES } from './draft'

interface Props {
  rule: RuleSpec
  team?: string | null
  initialSample?: string
  initialEvent?: DiscoveredSchema | null
  mockTransports?: boolean
  onResult: (r: RuleTestResult | null, event: unknown) => void
  onToast: (kind: 'ok' | 'warn' | 'error', text: string) => void
}

const DEBOUNCE_MS = 300
const MAX_EVENT_CHARS = 64 * 1024
const ALLOWED_SOURCES = [...new Set([...SAMPLES.map((s) => s.source), 'http:playground'])]

export function TestPanel({ rule, team, initialSample, initialEvent, mockTransports = false, onResult, onToast }: Props) {
  const discovered = initialEvent ? { id: 'discovered', label: 'Discovered event', source: initialEvent.source, event: initialEvent.sample } : null
  const available = discovered ? [discovered, ...SAMPLES] : SAMPLES
  const first = discovered ?? SAMPLES.find((s) => s.id === initialSample) ?? SAMPLES[0]!
  const replaySources = mockTransports ? ALLOWED_SOURCES : ALLOWED_SOURCES.filter((source) => source.startsWith('http:'))
  const allowedSources = [...new Set([...replaySources, ...(initialEvent && (mockTransports || initialEvent.source.startsWith('http:')) ? [initialEvent.source] : [])])]
  const [text, setText] = useState(() => JSON.stringify(first.event, null, 2))
  const [source, setSource] = useState(first.source)
  const [picked, setPicked] = useState(`sample:${first.id}`)
  const [recent, setRecent] = useState<RecentEvent[]>([])
  const [result, setResult] = useState<RuleTestResult | null>(null)
  const [failure, setFailure] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [sending, setSending] = useState(false)
  const seq = useRef(0)

  const tooLarge = text.length > MAX_EVENT_CHARS
  let parsed: { ok: true; value: unknown } | { ok: false; message: string }
  if (tooLarge) {
    parsed = { ok: false, message: `Event JSON exceeds ${MAX_EVENT_CHARS} bytes` }
  } else {
    try {
      parsed = { ok: true, value: JSON.parse(text) }
    } catch (e) {
      parsed = { ok: false, message: (e as Error).message }
    }
  }
  const eventKey = parsed.ok ? JSON.stringify(parsed.value) : null
  const ruleKey = JSON.stringify(rule)
  const eventSource = source || 'http:playground'
  const wouldAlert = rule.enabled !== false && result?.matched === true

  useEffect(() => {
    recentEvents(20, team).then(setRecent, () => setRecent([]))
  }, [team])

  useEffect(() => {
    const my = ++seq.current
    if (eventKey === null) {
      setBusy(false)
      setResult(null)
      setFailure(null)
      onResult(null, null)
      return
    }
    const ctl = new AbortController()
    setBusy(true)
    const t = setTimeout(() => {
      const event: unknown = JSON.parse(eventKey)
      testRule({ rule, event, source: eventSource }, ctl.signal)
        .then((r) => {
          if (my !== seq.current) return
          setResult(r)
          setFailure(null)
          onResult(r, event)
        })
        .catch((e: unknown) => {
          if (ctl.signal.aborted || my !== seq.current) return
          setFailure(errorText(e))
        })
        .finally(() => my === seq.current && setBusy(false))
    }, DEBOUNCE_MS)
    return () => {
      clearTimeout(t)
      ctl.abort()
    }
    // `rule` is covered by ruleKey; onResult is stable in the editor.
  }, [ruleKey, eventKey, eventSource, onResult])

  const pick = (v: string) => {
    setPicked(v)
    if (v.startsWith('sample:')) {
      const s = available.find((x) => `sample:${x.id}` === v)
      if (s) {
        setText(JSON.stringify(s.event, null, 2))
        setSource(s.source)
      }
    } else if (v.startsWith('recent:')) {
      const e = recent.find((x) => `recent:${x.event_id}` === v)
      if (e) {
        setText(JSON.stringify(e.raw, null, 2))
        setSource(e.source)
      }
    }
  }

  const send = async () => {
    if (!parsed.ok || !allowedSources.includes(eventSource)) return
    setSending(true)
    try {
      const r = await ingest(eventSource, parsed.value)
      const count = r.matched_alerts?.length
      if (count === undefined) onToast('ok', 'The ingest service accepted the event.')
      else if (count === 0) onToast('warn', 'Sent. No published rule matched it.')
      else onToast('ok', `Sent. It reached ${count} alert${count === 1 ? '' : 's'} in the feed.`)
    } catch (e) {
      onToast('error', errorText(e))
    } finally {
      setSending(false)
    }
  }

  return (
    <section className="test-panel" aria-label="Test this rule">
      <div className="panel-head">
        <h3>Try it on an event</h3>
        {busy && <span className="muted small">Checking…</span>}
      </div>
      <div className="inline">
        <select className="select grow" value={picked} onChange={(e) => pick(e.target.value)} aria-label="Event to test with">
          <optgroup label="Sample events">
            {available.map((s) => (
              <option key={s.id} value={`sample:${s.id}`}>{s.label}</option>
            ))}
          </optgroup>
          {recent.length > 0 && (
            <optgroup label="Recently received">
              {recent.map((e) => (
                <option key={e.event_id} value={`recent:${e.event_id}`}>
                  {e.schema} · {e.source} · {relTime(e.received_at, Date.now())}
                </option>
              ))}
            </optgroup>
          )}
          {picked === 'custom' && <option value="custom">Edited event</option>}
        </select>
      </div>
      <textarea
        className="input mono event-input"
        value={text}
        onChange={(e) => {
          setText(e.target.value)
          setPicked('custom')
        }}
        spellCheck={false}
        aria-label="Event JSON"
        aria-invalid={!parsed.ok || undefined}
        rows={12}
      />
      {!parsed.ok && <p className="problems-inline" role="alert">Not valid JSON: {parsed.message}</p>}

      {failure && <p className="problems-inline" role="alert">Couldn’t test: {failure}</p>}

      {parsed.ok && result && (
        <div className="test-result" aria-live="polite">
          <div className={`verdict ${wouldAlert ? 'is-match' : 'is-miss'}`}>
            <strong>{rule.enabled === false
              ? 'No alert: this rule is disabled'
              : wouldAlert ? 'This rule would raise an alert' : 'No alert: the conditions don’t match'}</strong>
            <span className="muted small">
              Detected as <code>{result.schema}</code>
            </span>
          </div>

          {result.card && (
            <div className={`card-preview tok-${colorToken(result.card.color)} ${wouldAlert ? '' : 'is-dim'}`}>
              <div className="card-preview-top">
                <SeverityBadge severity={result.card.severity} withLabel />
              </div>
              <div className="card-preview-title">{result.card.title || <span className="muted">(empty title)</span>}</div>
              {result.card.body && <div className="card-preview-body">{result.card.body}</div>}
              {result.card.fields.length > 0 && (
                <dl className="card-preview-fields">
                  {result.card.fields.map((f) => (
                    <div key={f.path}>
                      <dt>{f.label}</dt>
                      <dd>{f.value}</dd>
                    </div>
                  ))}
                </dl>
              )}
              {result.card.dedup_key.length > 0 && (
                <div className="small muted">
                  Repeats with key <code>{result.card.dedup_key.join(' · ')}</code> fold into this alert
                </div>
              )}
            </div>
          )}

          {result.errors.length > 0 && (
            <div className="notice notice-error">
              <strong>Fix before saving</strong>
              <ul>
                {result.errors.map((e, i) => (
                  <li key={i}>
                    <code>{e.path || 'rule'}</code> {e.message}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {result.warnings.length > 0 && (
            <div className="notice notice-warn">
              <strong>Check the template</strong>
              <ul>
                {result.warnings.map((w, i) => (
                  <li key={i}>{w}</li>
                ))}
              </ul>
            </div>
          )}

          <details className="normalized">
            <summary>Fields Beacon found ({Object.keys(result.fields).length})</summary>
            <dl className="fields">
              {Object.entries(result.fields).map(([k, v]) => (
                <div className="kv" key={k}>
                  <dt><code>fields.{k}</code></dt>
                  <dd>{JSON.stringify(v)}</dd>
                </div>
              ))}
            </dl>
          </details>
        </div>
      )}

      <div className="inline send-row">
        <select className="select mono grow" value={eventSource} onChange={(e) => setSource(e.target.value)} aria-label="Source" title="Allowed playground source">
          {!allowedSources.includes(eventSource) && <option value={eventSource} disabled>{eventSource} (test only)</option>}
          {allowedSources.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <button type="button" className="btn" disabled={!parsed.ok || sending || !allowedSources.includes(eventSource)} onClick={() => void send()} title="Ingest this event for real (uses the published rules, not this draft)">
          {sending ? 'Sending…' : 'Send to the feed'}
        </button>
      </div>
      <p className="hint">Testing never creates alerts or sends anything. Avoid pasting real PII or secrets; “Send to the feed” dispatches real alerts under published rules.</p>
    </section>
  )
}
