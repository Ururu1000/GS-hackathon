import { useEffect, useState } from 'react'

/** Re-renders every `ms` so relative times stay fresh. */
export function useNow(ms = 15_000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), ms)
    return () => clearInterval(t)
  }, [ms])
  return now
}

export function relTime(iso: string | null, now: number): string {
  if (!iso) return '—'
  const s = Math.round((now - Date.parse(iso)) / 1000)
  if (s < 5) return 'just now'
  if (s < 60) return `${s}s ago`
  const m = Math.round(s / 60)
  if (m < 60) return `${m}m ago`
  const h = Math.round(m / 60)
  if (h < 48) return `${h}h ago`
  return `${Math.round(h / 24)}d ago`
}

const fmt = new Intl.DateTimeFormat(undefined, {
  year: 'numeric',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
})
export const absTime = (iso: string | null) => (iso ? fmt.format(new Date(iso)) : '—')

export const fmtCount = (n: number) =>
  n >= 10_000 ? `${(n / 1000).toFixed(n >= 100_000 ? 0 : 1)}k` : n.toLocaleString()
