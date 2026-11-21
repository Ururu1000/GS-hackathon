import { useCallback, useEffect, useState } from 'react'
import { parseDocument, stringify } from 'yaml'
import { errorText, ingest } from '../api/client'
import {
  listDiscoveredSchemas, listSchemaMappings, listSources, previewSchemaMapping, saveSchemaMapping,
  type DiscoveredSchema, type SchemaMapping, type SchemaPreview, type SourceView,
} from '../api/screens'
import { FORBIDDEN_KEYS, hasForbiddenKey, isSensitivePath, redactValue } from '../lib/redact'
import { absTime } from '../lib/time'
import { flattenPaths, slugify } from '../rules/draft'

const MAX_INPUT_CHARS = 64 * 1024
const keyFor = (item: DiscoveredSchema) => JSON.stringify([item.schema, item.source])

function mappingTemplate(sample: DiscoveredSchema | null) {
  const path = sample
    ? flattenPaths('', sample.sample).find(({ path: p }) =>
      !p.split('.').some((seg) => FORBIDDEN_KEYS.has(seg)) && !isSensitivePath(p))?.path
    : undefined
  return stringify({
    schema: path ? `custom.${slugify(path) || 'event'}` : 'custom.event',
    priority: 50,
    detect: { ...(sample ? { source: sample.source } : {}), all: [{ path: path ?? 'type', op: 'exists' }] },
    fields: { type: path ? { path } : { value: 'Custom event' } },
  })
}

export function SourcesScreen({ onCreateRule }: { onCreateRule: (sample: DiscoveredSchema) => void }) {
  const [data, setData] = useState<{ sources: SourceView[]; discovered: DiscoveredSchema[]; mappings: SchemaMapping[] } | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [payload, setPayload] = useState('')
  const [mapping, setMapping] = useState<{ text: string; saved?: SchemaMapping } | null>(null)
  const [previewInput, setPreviewInput] = useState<'discovered' | 'paste' | null>(null)
  const [preview, setPreview] = useState<SchemaPreview | null>(null)
  const [busy, setBusy] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [feedback, setFeedback] = useState<{ kind: 'error' | 'ok'; text: string } | null>(null)
  const sample = data?.discovered.find((entry) => keyFor(entry) === selected) ?? data?.discovered[0] ?? null
  const previewFrom = previewInput ?? (sample ? 'discovered' : 'paste')

  const load = useCallback(async (signal?: AbortSignal) => {
    try {
      const [sources, discovered, mappings] = await Promise.all([
        listSources(signal), listDiscoveredSchemas(signal), listSchemaMappings(signal),
      ])
      if (signal?.aborted) return
      setData({ sources, discovered, mappings })
      setLoadError(null)
    } catch (e) {
      if (!signal?.aborted) setLoadError(errorText(e))
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    void load(controller.signal)
    const timer = setInterval(() => void load(controller.signal), 5_000)
    return () => { clearInterval(timer); controller.abort() }
  }, [load])

  const submitEvent = async () => {
    setBusy(true)
    setFeedback(null)
    try {
      if (payload.length > MAX_INPUT_CHARS) throw new Error('Event JSON must be 64 KiB or smaller.')
      const raw: unknown = JSON.parse(payload)
      await ingest('http:playground', raw)
      await load()
      setSelected(null)
      setFeedback({ kind: 'ok', text: 'Sample ingested on http:playground.' })
    } catch (e) {
      setFeedback({ kind: 'error', text: errorText(e) })
    } finally {
      setBusy(false)
    }
  }

  const editMapping = (saved?: SchemaMapping) => {
    setMapping({ saved, text: saved ? stringify(saved.spec) : mappingTemplate(sample) })
    setPreview(null)
    setFeedback(null)
  }

  const applyMapping = async (save: boolean) => {
    if (!mapping) return
    setBusy(true)
    setFeedback(null)
    try {
      setPreview(null)
      if (mapping.text.length > MAX_INPUT_CHARS) throw new Error('Mapping YAML must be 64 KiB or smaller.')
      const doc = parseDocument(mapping.text, { uniqueKeys: true })
      if (doc.errors[0]) throw new Error(doc.errors[0].message)
      const spec: unknown = doc.toJS({ maxAliasCount: 20 })
      JSON.stringify(spec)
      const forbidden = hasForbiddenKey(spec)
      if (forbidden) throw new Error(`Forbidden key "${forbidden}" in mapping YAML.`)
      if (save) {
        const saved = await saveSchemaMapping(spec, mapping.saved && { schema: mapping.saved.spec.schema, revision: mapping.saved.revision })
        editMapping(saved)
        await load()
        setFeedback({ kind: 'ok', text: `Saved ${saved.spec.schema} revision ${saved.revision}.` })
      } else {
        const selectedSample = previewFrom === 'discovered' ? sample : null
        if (!selectedSample && payload.length > MAX_INPUT_CHARS) throw new Error('Event JSON must be 64 KiB or smaller.')
        const raw: unknown = selectedSample ? selectedSample.sample : JSON.parse(payload)
        setPreview(await previewSchemaMapping(spec, raw, selectedSample?.source ?? 'http:playground'))
      }
    } catch (e) {
      setFeedback({ kind: 'error', text: errorText(e) })
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="screen feature-screen">
      <h1 className="sr-only">Sources &amp; schemas</h1>
      <div className="feature-body">
        <section className="feature-card">
          <div className="card-head">
            <h2>Sources {data && <span className="count">{data.sources.length}</span>}</h2>
            <span className="spacer" />
            <button className="btn btn-sm" onClick={() => void load()} disabled={busy}>Refresh</button>
          </div>
          {loadError && <div className="notice notice-error" role="alert">Couldn’t load sources and schemas: {loadError}</div>}
          {!data && !loadError && <div className="empty" role="status"><p>Loading…</p></div>}
          {data && (data.sources.length ? (
            <div className="feature-table-wrap">
              <table className="table table-static">
                <thead><tr><th scope="col">Source</th><th scope="col">Transport</th><th scope="col">Last received</th><th scope="col" className="num">Events in 24 h</th></tr></thead>
                <tbody>{data.sources.map((source) => (
                  <tr key={source.id}>
                    <td><span className="mono">{source.id}</span>{source.topics?.length ? <div className="muted small">{source.topics.join(', ')}</div> : null}</td>
                    <td><span className={`source-kind ${source.last_seen ? 'is-live' : ''}`}>{source.kind === 'http' ? 'Webhook' : source.kind === 'kafka' ? 'Kafka' : 'Log file'}</span></td>
                    <td className={source.last_seen ? '' : 'muted'}>{source.last_seen ? absTime(source.last_seen) : 'No events yet'}</td>
                    <td className="num">{source.events_24h ? source.events_24h.toLocaleString() : <span className="muted">0</span>}</td>
                  </tr>
                ))}</tbody>
              </table>
            </div>
          ) : <p className="card-empty">No sources have sent events yet.</p>)}
        </section>
      {data && (
        <>
          <div className="feature-columns">
            <section className="feature-card">
              <div className="card-head">
                <div>
                  <h2>New schemas</h2>
                  <span className="hint">Event shapes no built-in or saved mapping recognises.</span>
                </div>
              </div>
              {!data.discovered.length && <p className="card-empty">No unknown event shapes yet. Send an unfamiliar payload to see it here.</p>}
              {data.discovered.length > 0 && (
                <div className="stack">
                  <label className="form-field">
                    <span className="label">Discovered sample</span>
                    <select className="select" value={sample ? keyFor(sample) : ''} disabled={busy} onChange={(e) => { setSelected(e.target.value); setPreview(null) }}>
                      {data.discovered.map((entry) => <option key={keyFor(entry)} value={keyFor(entry)}>{entry.source} · {entry.schema} · {entry.count.toLocaleString()} events</option>)}
                    </select>
                  </label>
                  {sample && (
                    <>
                      <pre className="feature-code">{JSON.stringify(redactValue(sample.sample), null, 2)}</pre>
                      <p className="hint">Last received {absTime(sample.last_seen)}</p>
                      <div className="inline wrap">
                        <button className="btn btn-primary" onClick={() => onCreateRule(sample)}>Create rule from this event</button>
                        <button className="btn" onClick={() => editMapping()} disabled={busy}>Map this schema</button>
                      </div>
                    </>
                  )}
                </div>
              )}
            </section>
            <section className="feature-card">
              <div className="card-head">
                <div>
                  <h2>Send a sample</h2>
                  <span className="hint">Ingests into the live feed on <code>http:playground</code>. Avoid pasting real secrets or personal data.</span>
                </div>
              </div>
              <label className="form-field">
                <span className="label">Event JSON</span>
                <textarea className="input mono feature-textarea" rows={7} maxLength={64 * 1024} spellCheck={false} value={payload} disabled={busy} onChange={(e) => { setPayload(e.target.value); if (previewFrom === 'paste') setPreview(null) }} placeholder={'{ "type": "example", "amount": 125 }'} />
              </label>
              <div className="inline">
                <span className="spacer" />
                <button className="btn" disabled={busy || !payload.trim()} onClick={() => void submitEvent()}>Ingest sample</button>
              </div>
            </section>
          </div>
          <section className="feature-card">
            <div className="card-head">
              <div>
                <h2>Schema mappings <span className="count">{data.mappings.length}</span></h2>
                <span className="hint">Map a payload’s fields onto alert fields so rules can use them.</span>
              </div>
              <span className="spacer" />
              <button className="btn btn-sm" onClick={() => editMapping()} disabled={busy}>New mapping</button>
            </div>
            {mapping && (
              <div className="stack">
                <label className="form-field">
                  <span className="label">Mapping YAML</span>
                  <textarea className="input mono feature-textarea" rows={11} maxLength={64 * 1024} value={mapping.text} onChange={(e) => { setMapping({ ...mapping, text: e.target.value }); setPreview(null) }} disabled={busy} spellCheck={false} />
                </label>
                <p className="hint">Use payload-relative paths such as <code>labels.service</code>.</p>
                <div className="inline wrap">
                  <label className="inline">
                    <span className="small muted">Preview event</span>
                    <select className="select" value={previewFrom} disabled={busy} onChange={(e) => {
                      setPreviewInput(e.target.value === 'discovered' ? 'discovered' : 'paste')
                      setPreview(null)
                    }}>
                      <option value="discovered" disabled={!sample}>Discovered sample</option>
                      <option value="paste">Pasted JSON on http:playground</option>
                    </select>
                  </label>
                  <span className="spacer" />
                  <button className="btn btn-ghost" disabled={busy} onClick={() => { setMapping(null); setPreview(null) }}>Close editor</button>
                  <button className="btn" disabled={busy || (previewFrom === 'discovered' ? !sample : !payload.trim())} onClick={() => void applyMapping(false)}>Preview mapping</button>
                  <button className="btn btn-primary" disabled={busy} onClick={() => void applyMapping(true)}>Save mapping</button>
                </div>
                {preview && (
                  <div className={preview.matched ? 'notice' : 'notice notice-warn'}>
                    <p role="status"><strong>{preview.matched ? 'The detector matches this sample.' : 'The detector does not match this sample.'}</strong></p>
                    <pre className="feature-code">{JSON.stringify(redactValue(preview.fields), null, 2)}</pre>
                    {preview.warnings.length > 0 && <ul>{preview.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>}
                  </div>
                )}
              </div>
            )}
            {data.mappings.length ? (
              <ul className="feature-items">
                {data.mappings.map((entry) => (
                  <li key={entry.spec.schema}>
                    <span className="grow">{entry.spec.schema}</span>
                    <span className="muted small">Revision {entry.revision}</span>
                    <button className="btn btn-sm" disabled={busy} onClick={() => editMapping(entry)}>Edit</button>
                  </li>
                ))}
              </ul>
            ) : !mapping && <p className="card-empty">No saved mappings.</p>}
          </section>
        </>
      )}
      {feedback && <div className={feedback.kind === 'error' ? 'notice notice-error' : 'notice'} role={feedback.kind === 'error' ? 'alert' : 'status'}>{feedback.text}</div>}
      </div>
    </div>
  )
}
