import { forwardRef } from 'react'
import { SEVERITIES, STATUSES, type AlertStatus, type Severity } from '../api/types'
import { SEVERITY_LABEL, SeverityIcon, STATUS_LABEL } from '../alert/badges'
import { isDefault, type Filters } from './filters'
import { SearchIcon, StatusIcon } from '../lib/icons'

interface Props {
  filters: Filters
  counts: {
    status: Record<AlertStatus, number>
    severity: Record<Severity, number>
    rules: Map<string, { name: string; count: number }>
  }
  onChange: (f: Filters) => void
  onReset: () => void
}

function toggle<T>(set: ReadonlySet<T>, v: T): Set<T> {
  const next = new Set(set)
  if (next.has(v)) next.delete(v)
  else next.add(v)
  return next
}

/** Status, severity and rule filters, shown in the top bar. */
export function FilterChips({ filters, counts, onChange }: Omit<Props, 'onReset'>) {
  return (
    <div className="filter-chips" role="toolbar" aria-label="Filters">
      <fieldset className="chip-group">
        <legend className="sr-only">Status</legend>
        <div className="chips">
          {STATUSES.map((s) => (
            <button
              key={s}
              className={`chip chip-status-${s}`}
              aria-pressed={filters.status.has(s)}
              title={STATUS_LABEL[s]}
              onClick={() => onChange({ ...filters, status: toggle(filters.status, s) })}
            >
              <StatusIcon status={s} size={13} className="chip-status-icon" />
              <span className="chip-label">{STATUS_LABEL[s]}</span> <span className="chip-n">{counts.status[s]}</span>
            </button>
          ))}
        </div>
      </fieldset>
      <fieldset className="chip-group">
        <legend className="sr-only">Severity</legend>
        <div className="chips">
          {SEVERITIES.map((s) => (
            <button
              key={s}
              className={`chip chip-sev-${s}`}
              aria-pressed={filters.severity.has(s)}
              title={SEVERITY_LABEL[s]}
              onClick={() => onChange({ ...filters, severity: toggle(filters.severity, s) })}
            >
              <SeverityIcon severity={s} size={13} />
              <span className="chip-label">{SEVERITY_LABEL[s]}</span> <span className="chip-n">{counts.severity[s]}</span>
            </button>
          ))}
        </div>
      </fieldset>
      <label className="field">
        <span className="field-label">Alert rule</span>
        <select className="select select-rule" value={filters.rule} onChange={(e) => onChange({ ...filters, rule: e.target.value })}>
          <option value="">All alert rules</option>
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
    </div>
  )
}

/** Search above the list, with "Reset filters" inside it on the right. */
export const SearchBox = forwardRef<HTMLInputElement, Pick<Props, 'filters' | 'onChange' | 'onReset'>>(function SearchBox({ filters, onChange, onReset }, searchRef) {
  return (
    <div className="filters">
      <label className={`field field-grow search ${isDefault(filters) ? '' : 'has-reset'}`}>
        <span className="field-label">Search</span>
        <SearchIcon size={15} className="search-icon" />
        <input
          className="input"
          ref={searchRef}
          type="search"
          placeholder="Search, e.g. a trade or workflow ID"
          value={filters.query}
          onChange={(e) => onChange({ ...filters, query: e.target.value })}
          onKeyDown={(e) => {
            if (e.key === 'Escape') (e.target as HTMLInputElement).blur()
          }}
        />
        {!isDefault(filters) ? (
          <button className="search-reset" onClick={(e) => { e.preventDefault(); onReset() }}>
            Reset filters
          </button>
        ) : (
          <kbd className="search-kbd" aria-hidden="true">/</kbd>
        )}
      </label>
    </div>
  )
})
