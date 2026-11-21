import { useEffect, useState } from 'react'
import { errorText } from '../api/client'
import { endOutage, listOutages, startOutage, type GatewayOutage, type OutageChannel } from '../api/screens'

const CHANNEL_LABEL: Record<OutageChannel, string> = { sms: 'SMS', phone: 'Phone' }
const DURATIONS = [30, 60, 120, 300]
const REFRESH_MS = 10_000

/** Seconds left on an outage at `now`, never negative. */
export function secondsLeft(outage: GatewayOutage, now: number): number {
  return Math.max(0, Math.ceil((Date.parse(outage.until) - now) / 1000))
}

/**
 * Takes the demo gateway's SMS or phone channel down for a while, so delivery's
 * retries, breaker and fallback can be shown with a click instead of a curl.
 */
export function OutageControl({ onError }: { onError: (message: string | null) => void }) {
  const [outages, setOutages] = useState<GatewayOutage[] | null>(null)
  const [channel, setChannel] = useState<OutageChannel>('sms')
  const [seconds, setSeconds] = useState(60)
  const [busy, setBusy] = useState(false)
  const [now, setNow] = useState(() => Date.now())

  useEffect(() => {
    const controller = new AbortController()
    const load = () => listOutages(controller.signal).then(
      (list) => { if (!controller.signal.aborted) setOutages(list.outages) },
      (e: unknown) => { if (!controller.signal.aborted) onError(errorText(e)) },
    )
    void load()
    const timer = setInterval(() => void load(), REFRESH_MS)
    return () => { controller.abort(); clearInterval(timer) }
  }, [onError])

  const active = (outages ?? []).filter((o) => secondsLeft(o, now) > 0)
  useEffect(() => {
    if (!active.length) return
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [active.length])

  const run = async (call: () => Promise<{ outages: GatewayOutage[] }>) => {
    if (busy) return
    setBusy(true)
    onError(null)
    try {
      const list = await call()
      setNow(Date.now())
      setOutages(list.outages)
    } catch (e) {
      onError(errorText(e))
    } finally {
      setBusy(false)
    }
  }

  const current = active.find((o) => o.channel === channel)

  return (
    <section className="feature-card outage-card" aria-label="Gateway outage">
      <div className="card-head">
        <div>
          <h2>Gateway outage</h2>
          <span className="hint">Fails every send on a channel. Delivery retries, then falls back.</span>
        </div>
        <span className="spacer" />
        <ul className="outage-status" aria-live="polite">
          {active.length === 0
            ? <li className="hint">{outages ? 'All channels up' : 'Checking…'}</li>
            : active.map((o) => (
              <li key={o.channel} className="conn conn-down">
                {CHANNEL_LABEL[o.channel]} down · {secondsLeft(o, now)} s left
              </li>
            ))}
        </ul>
      </div>
      <div className="inline">
        <label>
          <span className="sr-only">Channel</span>
          <select className="select select-sm" value={channel} disabled={busy} onChange={(e) => setChannel(e.target.value as OutageChannel)}>
            <option value="sms">SMS</option>
            <option value="phone">Phone</option>
          </select>
        </label>
        <label>
          <span className="sr-only">Duration</span>
          <select className="select select-sm" value={seconds} disabled={busy || !!current} onChange={(e) => setSeconds(Number(e.target.value))}>
            {DURATIONS.map((s) => <option key={s} value={s}>{s} s</option>)}
          </select>
        </label>
        <span className="spacer" />
        {current
          ? <button className="btn btn-sm" disabled={busy} onClick={() => void run(() => endOutage(channel))}>End {CHANNEL_LABEL[channel]} outage now</button>
          : <button className="btn btn-sm btn-danger" disabled={busy} onClick={() => void run(() => startOutage(channel, seconds))}>Take {CHANNEL_LABEL[channel]} down for {seconds} s</button>}
      </div>
    </section>
  )
}
