import { useEffect, useMemo, useState } from 'react'
import { safeUrl } from '../alert/Runbook'
import { API_BASE, errorText } from '../api/client'
import { listSimulatorMessages, replyToSimulatorMessage, type SimulatorMessage, type SimulatorPage, type SmsReplyOutcome, type SmsReplyResult } from '../api/screens'
import { internationalPhone } from '../lib/phone'
import { maskAddress, redactText } from '../lib/redact'
import { absTime } from '../lib/time'
import { OutageControl } from './OutageControl'

const replyStatus: Record<SmsReplyOutcome, string> = {
  approved: 'Reply accepted. Request approved.',
  rejected: 'Reply accepted. Request rejected.',
  acked: 'Alert acknowledged.',
  already_decided: 'This approval request is already closed.',
  wrong_code: 'The gateway rejected the code. Check the SMS and try again.',
  too_many_attempts: 'The gateway blocked further attempts for this request.',
  unknown: 'The gateway found no matching request.',
}

const MAILPIT_URL = safeUrl(import.meta.env.VITE_MAILPIT_URL || 'http://localhost:8025')
const MAX_SIMULATOR_MESSAGES = 500

function isSimulatorMessage(value: unknown): value is SimulatorMessage {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const m = value as Record<string, unknown>
  return (
    typeof m.id === 'number' &&
    typeof m.receipt_id === 'string' &&
    typeof m.alert_id === 'string' &&
    (m.channel === 'sms' || m.channel === 'phone') &&
    typeof m.recipient === 'string' &&
    typeof m.to === 'string' &&
    typeof m.text === 'string' &&
    typeof m.received_at === 'string'
  )
}

function mergeMessages(first: SimulatorMessage[], second: SimulatorMessage[]) {
  const messages = new Map(first.map((item) => [item.receipt_id, item]))
  for (const item of second) {
    const existing = messages.get(item.receipt_id)
    if (!existing || existing.duplicates <= item.duplicates) messages.set(item.receipt_id, item)
  }
  return [...messages.values()]
    .sort((a, b) => Date.parse(b.received_at) - Date.parse(a.received_at) || b.id - a.id)
    .slice(0, MAX_SIMULATOR_MESSAGES)
}

function approvalCode(message: SimulatorMessage) {
  return message.approval_challenge?.trim().match(/^(?:Y\s+)?(\d{4})$/i)?.[1]
}

export function SimulatorScreen({ mockReplies }: { mockReplies: boolean }) {
  const [page, setPage] = useState<SimulatorPage | null>(null)
  const [connected, setConnected] = useState(false)
  const [selected, setSelected] = useState<string | null>(null)
  const [recipient, setRecipient] = useState('')
  const [draft, setDraft] = useState<{ receipt: string; text: string } | null>(null)
  const [decided, setDecided] = useState<Record<string, SmsReplyResult>>({})
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [sending, setSending] = useState(false)
  const recipients = useMemo(() => [...new Set(page?.items.map((item) => item.recipient) ?? [])].sort(), [page])
  const items = recipient ? (page?.items ?? []).filter((item) => item.recipient === recipient) : page?.items ?? []
  const current = items.find((item) => item.receipt_id === selected) ?? items[0] ?? null
  const reply = draft && current?.receipt_id === draft.receipt ? draft.text : ''
  const from = current?.channel === 'sms' ? internationalPhone(current.to) : null
  const code = current ? approvalCode(current) : undefined
  const approvalReply = code ? `Y ${code}` : null

  useEffect(() => {
    const controller = new AbortController()
    let hasSnapshot = false
    const stream = new EventSource(`${API_BASE}/api/simulator/stream`, { withCredentials: true })
    const badEvent = () => setError('The gateway returned an unreadable event.')
    stream.addEventListener('snapshot', (event: MessageEvent<string>) => {
      try {
        const snapshot: SimulatorPage = JSON.parse(event.data)
        if (!Array.isArray(snapshot.items) || !snapshot.items.every(isSimulatorMessage)) throw new Error('Missing simulator messages')
        hasSnapshot = true
        setConnected(true)
        setError(null)
        setPage({ ...snapshot, items: snapshot.items.slice(0, MAX_SIMULATOR_MESSAGES) })
      } catch { badEvent() }
    })
    stream.addEventListener('simulator.message', (event: MessageEvent<string>) => {
      try {
        const message: unknown = JSON.parse(event.data)
        if (!isSimulatorMessage(message)) throw new Error('Missing simulator receipt')
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
    const submittedCode = text.trim().match(/^[YN]\s+(\d{4})$/i)?.[1]
    if (!submittedCode || submittedCode !== code) {
      setError('Reply Y or N with the code shown in this SMS.')
      return
    }
    if (!mockReplies && page?.items.some((item) => item.receipt_id !== receipt &&
      internationalPhone(item.to) === from && approvalCode(item) === submittedCode)) {
      setError('Multiple messages share this code. Open the alert to decide the intended request.')
      return
    }
    setSelected(receipt)
    setError(null)
    setSending(true)
    try {
      const result = await replyToSimulatorMessage(from, text.trim(), mockReplies ? receipt : undefined)
      if (result.outcome === 'wrong_code' || result.outcome === 'unknown') {
        setError(replyStatus[result.outcome])
      } else {
        setDecided((previous) => ({ ...previous, [receipt]: result }))
        setDraft((previous) => previous?.receipt === receipt ? null : previous)
      }
    } catch (e) {
      setError(errorText(e))
    } finally {
      setSending(false)
    }
  }

  return (
    <div className="screen feature-screen">
      <h1 className="sr-only">Device simulator</h1>
      <div className="feature-body">
      {error && <div className="notice notice-error" role="alert">{error}</div>}
      <OutageControl onError={setError} />
      <div className="feature-columns simulator-layout">
        <section className="feature-card">
          <div className="card-head simulator-inbox-head">
            <h2>Inbox {page && <span className="count">{items.length}</span>}</h2>
            <span className={connected ? 'sr-only' : 'conn conn-down'} role="status">{connected ? 'Gateway connected' : 'Gateway disconnected'}</span>
            <span className="spacer" />
            <label>
              <span className="sr-only">Recipient</span>
              <select className="select select-sm" value={recipient} onChange={(e) => { setRecipient(e.target.value); setSelected(null); setDraft(null) }}>
                <option value="">All recipients</option>
                {recipients.map((name) => <option key={name} value={name}>{name.replace(/^user:/, '')}</option>)}
              </select>
            </label>
            {MAILPIT_URL && <a className="btn btn-sm" href={MAILPIT_URL} target="_blank" rel="noopener noreferrer">Open Mailpit</a>}
          </div>
          {items.length ? <ul className="simulator-inbox">
            {items.map((item) => (
              <li key={item.receipt_id}>
                <button className="sim-item" aria-pressed={current?.receipt_id === item.receipt_id} onClick={() => { setSelected(item.receipt_id); setDraft(null) }}>
                  <span className={`pill ${item.channel === 'sms' ? 'pill-auto' : 'pill-risk'}`}>{item.channel === 'sms' ? 'SMS' : 'Call'}</span>
                  <span className="sim-who">{item.recipient.replace(/^user:/, '')}</span>
                  <span className="sim-time">{absTime(item.received_at)}</span>
                  <span className="sim-text">{redactText(item.text, 240)}</span>
                </button>
              </li>
            ))}
          </ul> : <p className="card-empty">{page ? 'No messages for this recipient.' : 'Waiting for the gateway…'}</p>}
          {page?.next_cursor && <div className="inline"><span className="spacer" /><button className="btn btn-sm" disabled={loading} onClick={() => void older()}>{loading ? 'Loading…' : 'Load older messages'}</button><span className="spacer" /></div>}
        </section>
        <section className="feature-card simulator-phone" aria-label="Simulated phone">
          <div className="card-head">
            <div>
              <h2>{current?.channel === 'phone' ? 'Incoming call' : 'Messages'}</h2>
              {current && <span className="hint">{current.recipient.replace(/^user:/, '')}{current.to ? ` · ${maskAddress(current.channel, current.to)}` : ''} · {absTime(current.received_at)}</span>}
            </div>
            <span className="spacer" />
            {current && <a className="btn btn-sm" href={`#/alerts/${encodeURIComponent(current.alert_id)}`}>Open alert</a>}
          </div>
          {current && (
            <>
              <div className={`simulator-message ${current.channel === 'phone' ? 'is-call' : ''}`}>{redactText(current.text, 400)}</div>
              {current.duplicates > 0 && <p className="hint">{current.duplicates} duplicate receipt{current.duplicates === 1 ? '' : 's'}</p>}
              {current.channel === 'sms' && current.approval_challenge && (
                decided[current.receipt_id] ? <div className="notice" role="status">{replyStatus[decided[current.receipt_id]!.outcome]}</div>
                  : from ? (
                    <form className="stack tight" onSubmit={(e) => { e.preventDefault(); void send(reply) }}>
                      <label className="form-field">
                        <span className="label">SMS reply</span>
                        <input className="input mono" maxLength={160} value={reply} disabled={sending} onChange={(e) => {
                          setSelected(current.receipt_id)
                          setDraft({ receipt: current.receipt_id, text: e.target.value })
                        }} placeholder={approvalReply ?? undefined} />
                      </label>
                      <div className="inline">
                        <span className="spacer" />
                        {approvalReply && <button className="btn" type="button" disabled={sending} onClick={() => void send(approvalReply)}>Reply {approvalReply}</button>}
                        <button className="btn btn-primary" type="submit" disabled={sending || !reply.trim()}>Send reply</button>
                      </div>
                    </form>
                  ) : <p className="hint">This receipt has no reply-capable phone number.</p>
              )}
            </>
          )}
          {!current && <p className="card-empty">Select a message to open it here.</p>}
        </section>
      </div>
      </div>
    </div>
  )
}
