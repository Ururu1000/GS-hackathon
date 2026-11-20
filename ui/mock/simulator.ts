import type { IncomingMessage, ServerResponse } from 'node:http'
import type { SimulatorMessage, SimulatorPage } from '../src/api/screens.ts'

interface DeviceRecord {
  message: SimulatorMessage
  team: string
  approval?: { id: string; step: string; attempts: number }
}

const MAX_RECEIPTS = 500
const PAGE_LIMIT = 50

export function createSimulator(canSeeTeam: (team: string) => boolean) {
  const receipts = new Map<string, DeviceRecord>()
  const streams = new Set<ServerResponse>()
  let sequence = 0

  const write = (res: ServerResponse, event: string, payload: unknown) =>
    res.write(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`)

  const list = (cursor?: number): SimulatorPage => {
    const all = [...receipts.values()].reverse()
      .filter((entry) => canSeeTeam(entry.team) && (cursor === undefined || entry.message.id < cursor))
    const items = all.slice(0, PAGE_LIMIT).map(({ message }) => message)
    return { items, next_cursor: all.length > PAGE_LIMIT ? String(items.at(-1)?.id) : null }
  }

  return {
    list,
    send(team: string, message: Omit<SimulatorMessage, 'id'>, approval?: DeviceRecord['approval']) {
      if (receipts.has(message.receipt_id)) return
      const item: SimulatorMessage = { ...message, id: ++sequence }
      receipts.set(item.receipt_id, { message: item, team, approval })
      if (receipts.size > MAX_RECEIPTS) {
        const oldest = receipts.keys().next()
        if (!oldest.done) receipts.delete(oldest.value)
      }
      if (canSeeTeam(team)) for (const res of streams) write(res, 'simulator.message', item)
    },
    approvalSms(from: string, code?: string) {
      const candidates = [...receipts.values()].reverse().filter((entry) =>
        entry.approval && entry.message.channel === 'sms' && entry.message.to === from && canSeeTeam(entry.team))
      return candidates.find((entry) => entry.message.approval_challenge === `Y ${code}`) ?? candidates[0] ?? null
    },
    stream(req: IncomingMessage, res: ServerResponse) {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
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
