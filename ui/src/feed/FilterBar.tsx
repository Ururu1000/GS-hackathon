import { forwardRef } from 'react'
import { SEVERITIES, STATUSES, type AlertStatus, type Severity } from '../api/types'
import { SEVERITY_LABEL, STATUS_LABEL } from '../alert/badges'
import type { Filters } from './filters'

interface Props {
  filters: Filters
  counts: {
    status: Record<AlertStatus, number>
    severity: Record<Severity, number>
    rules: Map<string, { name: string; count: number }>
  }
  onChange: (f: Filters) => void
}

function toggle<T>(set: ReadonlySet<T>, v: T): Set<T> {
  const next = new Set(set)
  if (next.has(v)) next.delete(v)
  else next.add(v)
  return next
}

export const FilterBar = forwardRef<HTMLInputElement, Props>(function FilterBar({ filters, counts, onChange }, searchRef) {
  return (
    <div className="filters" role="toolbar" aria-label="Filters">
      <fieldset className="chips">
        <legend>Status</legend>
        {STATUSES.map((s) => (
          <button
            key={s}
            className={`chip chip-status-${s}`}
            aria-pressed={filters.status.has(s)}
            onClick={() => onChange({ ...filters, status: toggle(filters.status, s) })}
          >
            {STATUS_LABEL[s]} <span className="chip-n">{counts.status[s]}</span>
          </button>
        ))}
      </fieldset>
      <fieldset className="chips">
        <legend>Severity</legend>
        {SEVERITIES.map((s) => (
          <button
            key={s}
            className={`chip chip-sev-${s}`}
            aria-pressed={filters.severity.has(s)}
            onClick={() => onChange({ ...filters, severity: toggle(filters.severity, s) })}
          >
            {SEVERITY_LABEL[s]} <span className="chip-n">{counts.severity[s]}</span>
          </button>
        ))}
      </fieldset>
      <label className="field">
        <span className="field-label">Rule</span>
        <select value={filters.rule} onChange={(e) => onChange({ ...filters, rule: e.target.value })}>
          <option value="">All rules</option>
          {[...counts.rules.entries()]
            .sort((a, b) => a[1].name.localeCompare(b[1].name))
            .map(([id, r]) => (
              <option key={id} value={id}>
                {r.name} ({r.count})
              </option>
            ))}
          {filters.rule && !counts.rules.has(filters.rule) && <option value={filters.rule}>{filters.rule} (0)</option>}
        </select>
      </label>
      <label className="field field-grow">
        <span className="field-label">Search</span>
        <input
          ref={searchRef}
          type="search"
          placeholder="Title, body or field value  ( / )"
          value={filters.query}
          onChange={(e) => onChange({ ...filters, query: e.target.value })}
          onKeyDown={(e) => {
            if (e.key === 'Escape') (e.target as HTMLInputElement).blur()
          }}
        />
      </label>
    </div>
  )
})
