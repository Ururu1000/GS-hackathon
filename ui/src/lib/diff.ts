// Line diff (LCS) for proposal previews. Rules are small, so O(n·m) is fine.

export type DiffLine = { op: ' ' | '+' | '-'; text: string }

export function diffLines(a: string, b: string): DiffLine[] {
  const x = a.split('\n')
  const y = b.split('\n')
  const n = x.length
  const m = y.length
  const lcs: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0))
  for (let i = n - 1; i >= 0; i--)
    for (let j = m - 1; j >= 0; j--) lcs[i]![j] = x[i] === y[j] ? lcs[i + 1]![j + 1]! + 1 : Math.max(lcs[i + 1]![j]!, lcs[i]![j + 1]!)
  const out: DiffLine[] = []
  let i = 0
  let j = 0
  while (i < n && j < m) {
    if (x[i] === y[j]) out.push({ op: ' ', text: x[i++]! }), j++
    else if (lcs[i + 1]![j]! >= lcs[i]![j + 1]!) out.push({ op: '-', text: x[i++]! })
    else out.push({ op: '+', text: y[j++]! })
  }
  while (i < n) out.push({ op: '-', text: x[i++]! })
  while (j < m) out.push({ op: '+', text: y[j++]! })
  return out
}

/** Keep `context` unchanged lines around changes; collapse the rest to a marker. */
export function withContext(lines: DiffLine[], context = 2): (DiffLine | { op: '…'; text: string })[] {
  const keep = lines.map(() => false)
  lines.forEach((l, i) => {
    if (l.op === ' ') return
    for (let k = Math.max(0, i - context); k <= Math.min(lines.length - 1, i + context); k++) keep[k] = true
  })
  const out: (DiffLine | { op: '…'; text: string })[] = []
  let skipped = 0
  lines.forEach((l, i) => {
    if (keep[i]) {
      if (skipped) out.push({ op: '…', text: `${skipped} unchanged line${skipped === 1 ? '' : 's'}` })
      skipped = 0
      out.push(l)
    } else skipped++
  })
  if (skipped) out.push({ op: '…', text: `${skipped} unchanged line${skipped === 1 ? '' : 's'}` })
  return out
}
