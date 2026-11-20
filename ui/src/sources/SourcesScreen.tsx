import { useCallback, useEffect, useState } from 'react'
import { parseDocument, stringify } from 'yaml'
import { errorText, ingest } from '../api/client'
import {
  listDiscoveredSchemas, listSchemaMappings, listSources, previewSchemaMapping, saveSchemaMapping,
  type DiscoveredSchema, type SchemaMapping, type SchemaPreview, type SourceView,
} from '../api/screens'
import { absTime } from '../lib/time'
import { flattenPaths, slugify } from '../rules/draft'

const keyFor = (item: DiscoveredSchema) => JSON.stringify([item.schema, item.source])

function mappingTemplate(sample: DiscoveredSchema | null) {
  const path = sample ? flattenPaths('', sample.sample)[0]?.path : undefined
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
      const doc = parseDocument(mapping.text, { uniqueKeys: true })
      if (doc.errors[0]) throw new Error(doc.errors[0].message)
      const spec: unknown = doc.toJS({ maxAliasCount: 20 })
      if (save) {
        const saved = await saveSchemaMapping(spec, mapping.saved?.revision)
        editMapping(saved)
        await load()
        setFeedback({ kind: 'ok', text: `Saved ${saved.spec.schema} revision ${saved.revision}.` })
      } else {
        const selectedSample = previewFrom === 'discovered' ? sample : null
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
      <header className="screen-head">
        <div className="screen-title"><h1>Sources &amp; schemas</h1></div>
        <span className="spacer" />
        <button className="btn" onClick={() => void load()} disabled={busy}>Refresh</button>
      </header>

      {loadError && <p className="notice notice-warn" role="alert">Couldn’t load sources and schemas. {loadError}</p>}
      {!data && !loadError && <p className="muted" role="status">Loading sources…</p>}
      {data && (
        <>
          <section className="editor-section">
            <h2>Sources</h2>
            <div className="feature-table-wrap">
              <table className="feature-table">
                <thead><tr><th>Source</th><th>Transport</th><th>Last received</th><th className="num">Events in 24h</th></tr></thead>
                <tbody>{data.sources.map((source) => (
                  <tr key={source.id}>
                    <td><code>{source.id}</code>{source.topics?.length ? <div className="muted small">{source.topics.join(', ')}</div> : null}</td>
                    <td>{source.kind === 'http' ? 'Webhook' : source.kind === 'kafka' ? 'Kafka' : 'Log file'}</td>
                    <td>{source.last_seen ? absTime(source.last_seen) : 'No events yet'}</td>
                    <td className="num">{source.events_24h.toLocaleString()}</td>
                  </tr>
                ))}</tbody>
              </table>
              {!data.sources.length && <p className="muted">No sources have sent events yet.</p>}
            </div>
          </section>
          <div className="feature-columns">
            <section className="editor-section">
              <h2>New schemas</h2>
              {!data.discovered.length && <p className="muted">No unknown event shapes yet. Send an unfamiliar payload to see it here.</p>}
              {data.discovered.length > 0 && (
                <div className="stack tight">
                  <label className="form-field">
                    <span className="label">Discovered sample</span>
                    <select className="select" value={sample ? keyFor(sample) : ''} disabled={busy} onChange={(e) => { setSelected(e.target.value); setPreview(null) }}>
                      {data.discovered.map((entry) => <option key={keyFor(entry)} value={keyFor(entry)}>{entry.source} · {entry.schema} · {entry.count.toLocaleString()} events</option>)}
                    </select>
                  </label>
                  {sample && (
                    <>
                      <pre className="feature-code">{JSON.stringify(sample.sample, null, 2)}</pre>
                      <p className="muted small">Last received {absTime(sample.last_seen)}</p>
                      <div className="inline">
                        <button className="btn btn-primary" onClick={() => onCreateRule(sample)}>Create rule from this event</button>
                        <button className="btn" onClick={() => editMapping()} disabled={busy}>Map this schema</button>
                      </div>
                    </>
                  )}
                </div>
              )}
            </section>
            <section className="editor-section">
              <h2>Send a sample</h2>
              <label className="form-field">
                <span className="label">Event JSON</span>
                <textarea className="input mono feature-textarea" rows={7} maxLength={64 * 1024} spellCheck={false} value={payload} disabled={busy} onChange={(e) => { setPayload(e.target.value); if (previewFrom === 'paste') setPreview(null) }} placeholder={'{ "type": "example", "amount": 125 }'} />
              </label>
              <p className="hint">Ingests into the live feed on <code>http:playground</code>.</p>
              <button className="btn" disabled={busy || !payload.trim()} onClick={() => void submitEvent()}>Ingest sample</button>
            </section>
          </div>
          <section className="editor-section">
            <div className="inline">
              <h2>Schema mappings</h2>
              <span className="spacer" />
              <button className="btn" onClick={() => editMapping()} disabled={busy}>New mapping</button>
            </div>
            {data.mappings.length ? (
              <ul className="feature-items">
                {data.mappings.map((entry) => <li key={entry.spec.schema}><code>{entry.spec.schema}</code><span className="muted">Revision {entry.revision}</span><button className="btn btn-sm" disabled={busy} onClick={() => editMapping(entry)}>Edit</button></li>)}
              </ul>
            ) : <p className="muted">No saved mappings.</p>}
            {mapping && (
              <div className="stack">
                <label className="form-field">
                  <span className="label">Mapping YAML</span>
                  <textarea className="input mono feature-textarea" rows={11} maxLength={64 * 1024} value={mapping.text} onChange={(e) => { setMapping({ ...mapping, text: e.target.value }); setPreview(null) }} disabled={busy} spellCheck={false} />
                </label>
                <p className="hint">Use payload-relative paths such as <code>labels.service</code>.</p>
                <label className="form-field">
                  <span className="label">Preview event</span>
                  <select className="select" value={previewFrom} disabled={busy} onChange={(e) => {
                    setPreviewInput(e.target.value === 'discovered' ? 'discovered' : 'paste')
                    setPreview(null)
                  }}>
                    <option value="discovered" disabled={!sample}>Discovered sample</option>
                    <option value="paste">Pasted JSON on http:playground</option>
                  </select>
                </label>
                <div className="inline">
                  <button className="btn" disabled={busy || (previewFrom === 'discovered' ? !sample : !payload.trim())} onClick={() => void applyMapping(false)}>Preview mapping</button>
                  <button className="btn btn-primary" disabled={busy} onClick={() => void applyMapping(true)}>Save mapping</button>
                  <button className="btn btn-ghost" disabled={busy} onClick={() => { setMapping(null); setPreview(null) }}>Close editor</button>
                </div>
                {preview && (
                  <div>
                    <p role="status">{preview.matched ? 'The detector matches this sample.' : 'The detector does not match this sample.'}</p>
                    <pre className="feature-code">{JSON.stringify(preview.fields, null, 2)}</pre>
                    {preview.warnings.length > 0 && <ul>{preview.warnings.map((warning) => <li key={warning}>{warning}</li>)}</ul>}
                  </div>
                )}
              </div>
            )}
          </section>
        </>
      )}
      {feedback && <p className={feedback.kind === 'error' ? 'notice notice-warn' : 'notice'} role={feedback.kind === 'error' ? 'alert' : 'status'}>{feedback.text}</p>}
    </div>
  )
}
