import type { IncomingMessage, ServerResponse } from 'node:http'
import type { SimulatorMessage, SimulatorPage } from '../src/api/screens.ts'
import { maskAddress } from '../src/lib/redact.ts'

export interface DeviceRecord {
  message: SimulatorMessage
  team: string
  rawPhone?: string
  approval?: { id: string; step: string }
}

export type ApprovalLookup =
  | { kind: 'match'; record: DeviceRecord }
  | { kind: 'mismatch'; pending: DeviceRecord[] }
  | { kind: 'ambiguous' }
  | { kind: 'none' }

const MAX_RECEIPTS = 500
const PAGE_LIMIT = 50
const MAX_STREAMS = 100

export function createSimulator(
  canSeeTeam: (team: string) => boolean,
  canManageTeam: (team: string) => boolean = canSeeTeam,
) {
  const receipts = new Map<string, DeviceRecord>()
  const streams = new Set<ServerResponse>()
  let sequence = 0

  const write = (res: ServerResponse, event: string, payload: unknown) =>
    res.write(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`)

  const present = (entry: DeviceRecord): SimulatorMessage => {
    if (entry.approval && !canManageTeam(entry.team)) {
      return {
        ...entry.message,
        to: maskAddress(entry.message.channel, entry.message.to),
        approval_challenge: null,
        text: 'Approval request (restricted to team members)',
      }
    }
    return entry.message
  }

  const list = (cursor?: number): SimulatorPage => {
    const all = [...receipts.values()].reverse()
      .filter((entry) => canSeeTeam(entry.team) && (cursor === undefined || entry.message.id < cursor))
    const items = all.slice(0, PAGE_LIMIT).map(present)
    return { items, next_cursor: all.length > PAGE_LIMIT ? String(items.at(-1)?.id) : null }
  }

  return {
    list,
    send(team: string, message: Omit<SimulatorMessage, 'id'>, approval?: DeviceRecord['approval'], rawPhone?: string) {
      if (receipts.has(message.receipt_id)) return
      const item: SimulatorMessage = { ...message, id: ++sequence }
      const record: DeviceRecord = { message: item, team, approval, rawPhone }
      receipts.set(item.receipt_id, record)
      if (receipts.size > MAX_RECEIPTS) {
        const oldest = receipts.keys().next()
        if (!oldest.done) receipts.delete(oldest.value)
      }
      if (canSeeTeam(team)) {
        const view = present(record)
        for (const res of streams) write(res, 'simulator.message', view)
      }
    },
    approvalSms(
      from: string,
      isPending: (entry: DeviceRecord) => boolean,
      codeMatches: (entry: DeviceRecord) => boolean,
      receiptId?: string,
    ): ApprovalLookup {
      const eligible = (entry: DeviceRecord) =>
        Boolean(entry.approval) &&
        entry.message.channel === 'sms' &&
        (entry.rawPhone ?? entry.message.to) === from &&
        canSeeTeam(entry.team) &&
        canManageTeam(entry.team)
      if (receiptId !== undefined) {
        const entry = receipts.get(receiptId)
        return entry && eligible(entry) ? { kind: 'match', record: entry } : { kind: 'none' }
      }
      const candidates = [...receipts.values()].reverse().filter(eligible)
      const pending = candidates.filter(isPending)
      if (pending.length > 0) {
        const matches = pending.filter(codeMatches)
        if (matches.length > 1) return { kind: 'ambiguous' }
        if (matches.length === 1) return { kind: 'match', record: matches[0]! }
        return { kind: 'mismatch', pending }
      }
      return candidates[0] ? { kind: 'match', record: candidates[0] } : { kind: 'none' }
    },
    stream(req: IncomingMessage, res: ServerResponse) {
      if (streams.size >= MAX_STREAMS) {
        res.writeHead(503, {
          'Content-Type': 'application/json',
          'Cache-Control': 'no-store',
          'X-Content-Type-Options': 'nosniff',
        })
        res.end(JSON.stringify({ error: { code: 'service_unavailable', message: 'Too many open streams' } }))
        return
      }
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-store',
        'X-Content-Type-Options': 'nosniff',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      })
      streams.add(res)
      write(res, 'snapshot', list())
      req.on('close', () => streams.delete(res))
    },
    ping() {
      for (const res of streams) res.write(': ping\n\n')
    },
    close() {
      for (const res of streams) res.end()
      streams.clear()
    },
  }
}
