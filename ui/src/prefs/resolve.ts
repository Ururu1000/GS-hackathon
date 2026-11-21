// Channel choice for one person (spec §4.4, steps 2–3). Shared by the
// preferences "try it" panel and the dev mock's simulated deliveries, so both
// tell the same story. Delivery (Rust) is authoritative.

import type { Preference, PreferenceMatch, Severity, TimeWindow } from '../api/types.ts'

export interface AlertFacts {
  severity: Severity
  service?: string
  team?: string
}

export interface Resolution {
  channels: string[]
  /** Index of the preference line that matched, or null for a fallback. */
  line: number | null
  /** Which fallback applied when no line matched. */
  via: 'preference' | 'default' | 'team' | 'ui'
  /** For a line with a time window: whether `at` was inside it. */
  in_window: boolean | null
  /** Set when the window could not be evaluated (bad zone or format). */
  problem: string | null
}

export function matchesFacts(m: PreferenceMatch, a: AlertFacts): boolean {
  if (m.severity !== undefined) {
    const list = Array.isArray(m.severity) ? m.severity : [m.severity]
    if (!list.includes(a.severity)) return false
  }
  if (m.service !== undefined && m.service !== a.service) return false
  if (m.team !== undefined && m.team !== a.team) return false
  return true
}

const HHMM = /^([01]\d|2[0-3]):([0-5]\d)$/

export function parseWindow(between: string): [number, number] | null {
  const [a, b] = between.split('-').map((s) => s.trim())
  const ma = a?.match(HHMM)
  const mb = b?.match(HHMM)
  if (!ma || !mb) return null
  return [Number(ma[1]) * 60 + Number(ma[2]), Number(mb[1]) * 60 + Number(mb[2])]
}

/** Minutes after local midnight in `tz`, or null for an unknown zone. */
export function localMinutes(at: Date, tz: string): number | null {
  try {
    const parts = new Intl.DateTimeFormat('en-GB', { timeZone: tz, hour: '2-digit', minute: '2-digit', hourCycle: 'h23' }).formatToParts(at)
    const h = Number(parts.find((p) => p.type === 'hour')?.value)
    const m = Number(parts.find((p) => p.type === 'minute')?.value)
    return Number.isFinite(h) && Number.isFinite(m) ? h * 60 + m : null
  } catch {
    return null
  }
}

/** Half-open [start, end); a window like 22:00-06:00 wraps midnight. */
export function inWindow(w: TimeWindow, at: Date): boolean | null {
  const range = parseWindow(w.between)
  const t = localMinutes(at, w.tz)
  if (!range || t === null) return null
  const [s, e] = range
  return s <= e ? t >= s && t < e : t >= s || t < e
}

/** `digest` holds an alert for the morning email, but critical alerts are never held: the
 *  backend router sends them as email at once (crates/beacon-delivery/src/router.rs). */
function holdUnlessCritical(channels: string[], a: AlertFacts): string[] {
  if (a.severity !== 'critical' || !channels.includes('digest')) return channels
  return [...new Set(channels.map((c) => (c === 'digest' ? 'email' : c)))]
}

export function resolveChannels(prefs: Preference[], a: AlertFacts, at: Date, teamDefault: string[] = []): Resolution {
  for (const [i, p] of prefs.entries()) {
    if ('default' in p) return { channels: p.default, line: i, via: 'default', in_window: null, problem: null }
    if (!matchesFacts(p.match, a)) continue
    if (!p.when) return { channels: p.channels, line: i, via: 'preference', in_window: null, problem: null }
    const inside = inWindow(p.when, at)
    if (inside === null)
      return { channels: p.otherwise ?? p.channels, line: i, via: 'preference', in_window: null, problem: `Can't read the time window "${p.when.between}" in ${p.when.tz}` }
    // Outside the window with no `otherwise`: this line doesn't apply, keep looking.
    if (!inside && !p.otherwise?.length) continue
    return { channels: inside ? p.channels : holdUnlessCritical(p.otherwise!, a), line: i, via: 'preference', in_window: inside, problem: null }
  }
  if (teamDefault.length) return { channels: teamDefault, line: null, via: 'team', in_window: null, problem: null }
  return { channels: ['ui'], line: null, via: 'ui', in_window: null, problem: null }
}

/** The digest replaces paging: it combines only with In-app (the backend rejects the rest).
 *  Turning digest on keeps only `ui` beside it; turning a real channel on drops the digest. */
export function digestExclusive(prev: string[], next: string[]): string[] {
  const added = next.filter((c) => !prev.includes(c))
  if (added.includes('digest')) return next.filter((c) => c === 'digest' || c === 'ui')
  if (added.some((c) => c !== 'ui')) return next.filter((c) => c !== 'digest')
  return next
}
