import { useEffect, useMemo, useState } from 'react'
import { API_BASE, errorText } from '../api/client'
import { listSimulatorMessages, replyToSimulatorMessage, type SimulatorMessage, type SimulatorPage, type SmsReplyOutcome } from '../api/screens'
import { internationalPhone } from '../lib/phone'
import { absTime } from '../lib/time'

const replyStatus: Record<SmsReplyOutcome, string> = {
  approved: 'Reply accepted. Request approved.',
  rejected: 'Reply accepted. Request rejected.',
  acked: 'Alert acknowledged.',
  already_decided: 'This approval request is already closed.',
  wrong_code: 'The gateway rejected the code. Check the SMS and try again.',
  too_many_attempts: 'The gateway blocked further attempts for this request.',
  unknown: 'The gateway found no matching request.',
}

const MAILPIT_URL = import.meta.env.VITE_MAILPIT_URL || 'http://localhost:8025'

function mergeMessages(first: SimulatorMessage[], second: SimulatorMessage[]) {
  const messages = new Map(first.map((item) => [item.receipt_id, item]))
  for (const item of second) {
    const existing = messages.get(item.receipt_id)
    if (!existing || existing.duplicates <= item.duplicates) messages.set(item.receipt_id, item)
  }
  return [...messages.values()].sort((a, b) => Date.parse(b.received_at) - Date.parse(a.received_at) || b.id - a.id)
}

export function SimulatorScreen() {
  const [page, setPage] = useState<SimulatorPage | null>(null)
  const [connected, setConnected] = useState(false)
  const [selected, setSelected] = useState<string | null>(null)
  const [recipient, setRecipient] = useState('')
  const [reply, setReply] = useState('')
  const [decided, setDecided] = useState<Record<string, SmsReplyOutcome>>({})
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [sending, setSending] = useState(false)
  const recipients = useMemo(() => [...new Set(page?.items.map((item) => item.recipient) ?? [])].sort(), [page])
  const items = recipient ? (page?.items ?? []).filter((item) => item.recipient === recipient) : page?.items ?? []
  const current = items.find((item) => item.receipt_id === selected) ?? items[0] ?? null
  const from = current?.channel === 'sms' ? internationalPhone(current.to) : null
  const command = current?.approval_challenge?.trim().match(/^(?:Y\s+)?(\d{4})$/i)
  const approvalReply = command?.[1] ? `Y ${command[1]}` : null

  useEffect(() => {
    const controller = new AbortController()
    let hasSnapshot = false
    const stream = new EventSource(`${API_BASE}/api/simulator/stream`)
    const badEvent = () => setError('The gateway returned an unreadable event.')
    stream.addEventListener('snapshot', (event: MessageEvent<string>) => {
      try {
        const snapshot: SimulatorPage = JSON.parse(event.data)
        if (!Array.isArray(snapshot.items)) throw new Error('Missing simulator messages')
        hasSnapshot = true
        setConnected(true)
        setError(null)
        setPage(snapshot)
      } catch { badEvent() }
    })
    stream.addEventListener('simulator.message', (event: MessageEvent<string>) => {
      try {
        const message: SimulatorMessage = JSON.parse(event.data)
        if (typeof message.receipt_id !== 'string') throw new Error('Missing simulator receipt')
        setPage((previous) => ({
          ...(previous ?? { next_cursor: null }), items: mergeMessages(previous?.items ?? [], [message]),
        }))
      } catch { badEvent() }
    })
    stream.onerror = () => setConnected(false)
    listSimulatorMessages(undefined, controller.signal).then(
      (snapshot) => { if (!hasSnapshot && !controller.signal.aborted) setPage(snapshot) },
      (e: unknown) => { if (!hasSnapshot && !controller.signal.aborted) setError(errorText(e)) },
    )
    return () => { controller.abort(); stream.close() }
  }, [])

  const older = async () => {
    if (!page?.next_cursor || loading) return
    setLoading(true)
    setError(null)
    try {
      const olderPage = await listSimulatorMessages(page.next_cursor)
      setPage((previous) => ({ ...olderPage, items: mergeMessages(previous?.items ?? [], olderPage.items) }))
    } catch (e) {
      setError(errorText(e))
    } finally {
      setLoading(false)
    }
  }

  const send = async (text: string) => {
    if (!current || !from || !text.trim() || sending) return
    const receipt = current.receipt_id
    setError(null)
    setSending(true)
    try {
      const result = await replyToSimulatorMessage(from, text.trim())
      if (result.outcome === 'wrong_code' || result.outcome === 'unknown') {
        setError(replyStatus[result.outcome])
      } else {
        setDecided((previous) => ({ ...previous, [receipt]: result.outcome }))
        setReply('')
      }
    } catch (e) {
      setError(errorText(e))
    } finally {
      setSending(false)
    }
  }

  return (
    <div className="screen feature-screen">
      <header className="screen-head">
        <div className="screen-title"><h1>Device simulator</h1><span className="muted small" role="status">{connected ? 'Live gateway feed' : 'Gateway disconnected'}</span></div>
        <span className="spacer" />
        <a className="btn" href={MAILPIT_URL} target="_blank" rel="noreferrer">Open Mailpit</a>
      </header>
      <div className="feature-columns simulator-layout">
        <section className="editor-section">
          <h2>Inbox</h2>
          <label className="form-field">
            <span className="label">Recipient</span>
            <select className="select" value={recipient} onChange={(e) => { setRecipient(e.target.value); setSelected(null); setReply('') }}>
              <option value="">All recipients</option>
              {recipients.map((name) => <option key={name} value={name}>{name.replace(/^user:/, '')}</option>)}
            </select>
          </label>
          {items.length ? <ul className="feature-items simulator-inbox">
            {items.map((item) => (
              <li key={item.receipt_id}>
                <button className="btn btn-ghost" aria-pressed={current?.receipt_id === item.receipt_id} onClick={() => { setSelected(item.receipt_id); setReply('') }}>
                  <span>{item.channel === 'sms' ? 'SMS' : 'Call'} · {item.recipient.replace(/^user:/, '')}</span>
                  <span className="muted small">{item.text}</span>
                </button>
              </li>
            ))}
          </ul> : <p className="muted">{page ? 'No messages for this recipient.' : 'Waiting for the gateway…'}</p>}
          {page?.next_cursor && <button className="btn" disabled={loading} onClick={() => void older()}>{loading ? 'Loading…' : 'Load older messages'}</button>}
        </section>
        <section className="simulator-phone stack" aria-label="Simulated phone">
          <div className="simulator-phone-header">
            <h2>{current?.channel === 'phone' ? 'Incoming call' : 'Messages'}</h2>
          </div>
          {current && (
            <>
              <p className="small muted">{current.recipient.replace(/^user:/, '')} · {absTime(current.received_at)}</p>
              <div className="simulator-message">{current.text}</div>
              {current.duplicates > 0 && <p className="hint">{current.duplicates} duplicate receipt{current.duplicates === 1 ? '' : 's'}</p>}
              <a href={`#/alerts/${encodeURIComponent(current.alert_id)}`}>Open alert</a>
              {current.channel === 'sms' && current.approval_challenge && (
                decided[current.receipt_id] ? <p className="notice" role="status">{replyStatus[decided[current.receipt_id]!]}</p>
                  : from ? (
                    <form className="stack tight" onSubmit={(e) => { e.preventDefault(); void send(reply) }}>
                      <label className="form-field">
                        <span className="label">SMS reply</span>
                        <input className="input mono" maxLength={160} value={reply} disabled={sending} onChange={(e) => setReply(e.target.value)} placeholder={approvalReply ?? undefined} />
                      </label>
                      <div className="inline">
                        {approvalReply && <button className="btn" type="button" disabled={sending} onClick={() => void send(approvalReply)}>Reply {approvalReply}</button>}
                        <button className="btn btn-primary" type="submit" disabled={sending || !reply.trim()}>Send reply</button>
                      </div>
                    </form>
                  ) : <p className="hint">This receipt has no reply-capable phone number.</p>
              )}
            </>
          )}
          {!current && <p className="muted">Select a receipt to open it here.</p>}
        </section>
      </div>
      {error && <p className="notice notice-warn" role="alert">{error}</p>}
    </div>
  )
}
