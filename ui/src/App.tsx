import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { getSession, transition, type TransitionOp } from './api/client'
import type { FeedAlert, Session } from './api/types'
import { AlertDetail } from './alert/AlertDetail'
import { FeedList } from './feed/FeedList'
import { FilterChips, SearchBox } from './feed/FilterBar'
import { defaultFilters, facetCounts, loadFilters, matches, saveFilters, type Filters } from './feed/filters'
import { useFeedStream, type Connection } from './feed/useFeedStream'
import { BeaconLogo, KeyboardIcon, MoonIcon, SunIcon } from './lib/icons'
import { useNow } from './lib/time'

// ---- tiny hash router: #/alerts/<id> selects an alert ----------------------

function readHash(): string | null {
  const m = location.hash.match(/^#\/alerts\/(.+)$/)
  return m ? decodeURIComponent(m[1]!) : null
}

function useSelection(): [string | null, (id: string | null) => void] {
  const [id, setId] = useState(readHash)
  useEffect(() => {
    const on = () => setId(readHash())
    addEventListener('hashchange', on)
    return () => removeEventListener('hashchange', on)
  }, [])
  const select = useCallback((next: string | null) => {
    const hash = next ? `#/alerts/${encodeURIComponent(next)}` : '#/'
    if (location.hash !== hash) history.replaceState(null, '', hash)
    setId(next)
  }, [])
  return [id, select]
}

// ---- toasts -----------------------------------------------------------------

interface Toast {
  id: number
  kind: 'ok' | 'warn' | 'error'
  text: string
}

function useToasts() {
  const [toasts, setToasts] = useState<Toast[]>([])
  const seq = useRef(0)
  const push = useCallback((kind: Toast['kind'], text: string) => {
    const id = ++seq.current
    setToasts((t) => [...t.slice(-3), { id, kind, text }])
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), kind === 'ok' ? 2500 : 5000)
  }, [])
  return { toasts, push }
}

// ---- persisted team scope & theme --------------------------------------------

const TEAM_KEY = 'beacon.team'
function loadTeam(): string | null {
  try {
    return localStorage.getItem(TEAM_KEY) || null
  } catch {
    return null
  }
}

type ThemePref = 'system' | 'light' | 'dark'
const THEME_KEY = 'beacon.theme'

function loadTheme(): ThemePref {
  try {
    const v = localStorage.getItem(THEME_KEY)
    return v === 'light' || v === 'dark' ? v : 'system'
  } catch {
    return 'system'
  }
}

function useTheme(): ['light' | 'dark', () => void] {
  const [pref, setPref] = useState<ThemePref>(loadTheme)
  const [systemDark, setSystemDark] = useState(() =>
    typeof matchMedia === 'function' ? matchMedia('(prefers-color-scheme: dark)').matches : true,
  )

  useEffect(() => {
    if (typeof matchMedia !== 'function') return
    const mq = matchMedia('(prefers-color-scheme: dark)')
    const on = () => setSystemDark(mq.matches)
    mq.addEventListener('change', on)
    return () => mq.removeEventListener('change', on)
  }, [])

  const resolved: 'light' | 'dark' = pref === 'system' ? (systemDark ? 'dark' : 'light') : pref

  useEffect(() => {
    const root = document.documentElement
    if (pref === 'system') delete root.dataset.theme
    else root.dataset.theme = pref
    try {
      if (pref === 'system') localStorage.removeItem(THEME_KEY)
      else localStorage.setItem(THEME_KEY, pref)
    } catch {
      /* ignore */
    }
  }, [pref])

  // Flipping to the theme the OS already uses drops the override, so "system" is reachable again.
  const toggle = useCallback(() => {
    const next = resolved === 'dark' ? 'light' : 'dark'
    setPref(next === (systemDark ? 'dark' : 'light') ? 'system' : next)
  }, [resolved, systemDark])

  return [resolved, toggle]
}

const CONNECTION_LABEL: Record<Connection, string> = {
  connecting: 'Connecting…',
  live: '',
  reconnecting: 'Reconnecting…',
}

export function App() {
  const [session, setSession] = useState<Session | null>(null)
  const [sessionError, setSessionError] = useState<string | null>(null)
  const [team, setTeam] = useState<string | null>(loadTeam)
  const [filters, setFilters] = useState<Filters>(loadFilters)
  const [selectedId, select] = useSelection()
  const [pending, setPending] = useState<{ id: string; op: TransitionOp } | null>(null)
  const [help, setHelp] = useState(false)
  const [theme, toggleTheme] = useTheme()
  const searchRef = useRef<HTMLInputElement>(null)
  const { toasts, push } = useToasts()
  const now = useNow(15_000)

  useEffect(() => {
    getSession().then(setSession, (e: unknown) => setSessionError(e instanceof Error ? e.message : String(e)))
  }, [])

  const { state, connection, applyLocal } = useFeedStream(team)

  const changeTeam = (t: string | null) => {
    setTeam(t)
    try {
      if (t) localStorage.setItem(TEAM_KEY, t)
      else localStorage.removeItem(TEAM_KEY)
    } catch {
      /* ignore */
    }
  }
  const changeFilters = (f: Filters) => {
    setFilters(f)
    saveFilters(f)
  }

  const all = useMemo(() => state.order.map((id) => state.byId.get(id)!).filter(Boolean), [state.order, state.byId])
  const visible = useMemo(() => all.filter((a) => matches(a, filters)), [all, filters])
  const counts = useMemo(() => facetCounts(all, filters), [all, filters])
  const selected = selectedId ? state.byId.get(selectedId) ?? null : null

  const act = useCallback(
    async (alert: FeedAlert, op: TransitionOp) => {
      if (pending) return
      if (op === 'ack' && alert.status !== 'open') {
        push('warn', alert.status === 'acked' ? 'Already acknowledged.' : 'This alert is already resolved.')
        return
      }
      if (op === 'resolve' && alert.status === 'resolved') {
        push('warn', 'Already resolved.')
        return
      }
      setPending({ id: alert.id, op })
      const res = await transition(alert.id, op, alert.version)
      setPending(null)
      if (res.ok) {
        applyLocal(res.alert)
        push('ok', `${op === 'ack' ? 'Acknowledged' : 'Resolved'}: ${alert.title}`)
      } else {
        if (res.current) applyLocal(res.current)
        push('error', res.message)
      }
    },
    [pending, applyLocal, push],
  )

  // ---- keyboard ---------------------------------------------------------------
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return
      const t = e.target as HTMLElement | null
      if (t && (t.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(t.tagName))) return
      const idx = selectedId ? visible.findIndex((a) => a.id === selectedId) : -1
      const move = (d: number) => {
        const next = visible[Math.min(visible.length - 1, Math.max(0, idx < 0 ? 0 : idx + d))]
        if (next) select(next.id)
      }
      switch (e.key) {
        case 'j':
        case 'ArrowDown':
          move(1)
          break
        case 'k':
        case 'ArrowUp':
          move(-1)
          break
        case 'a':
        case 'r':
          if (!selected) push('warn', 'Select an alert first.')
          else void act(selected, e.key === 'a' ? 'ack' : 'resolve')
          break
        case 'Escape':
          if (help) setHelp(false)
          else select(null)
          break
        case '/':
          searchRef.current?.focus()
          break
        case '?':
          setHelp((h) => !h)
          break
        default:
          return
      }
      e.preventDefault()
    }
    addEventListener('keydown', onKey)
    return () => removeEventListener('keydown', onKey)
  }, [visible, selectedId, selected, select, act, push, help])

  const teamName = team ? session?.teams.find((t) => t.id === team)?.name ?? team : 'All teams'

  return (
    <div className={`app ${selected ? 'has-detail' : ''}`}>
      <header className="topbar">
        <div className="brand">
          <BeaconLogo size={26} />
          <span>Beacon</span>
        </div>
        <label>
          <span className="sr-only">Team</span>
          <select className="select" value={team ?? ''} onChange={(e) => changeTeam(e.target.value || null)}>
            <option value="">All teams</option>
            {session?.teams.map((t) => (
              <option key={t.id} value={t.id}>{t.name}</option>
            ))}
          </select>
        </label>
        <FilterChips filters={filters} counts={counts} onChange={changeFilters} />
        <div className="spacer" />
        {/* Shown only when the stream is down: a live feed needs no badge. */}
        <div className={`conn conn-${connection}`} role="status">
          {connection !== 'live' && (
            <>
              <span className="conn-dot" aria-hidden="true" />
              {CONNECTION_LABEL[connection]}
            </>
          )}
        </div>
        <div className="user">{session?.user.name ?? (sessionError ? 'Can’t reach the server' : '…')}</div>
        <div className="topbar-actions">
          <button
            className="icon-btn"
            onClick={toggleTheme}
            aria-label={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
            title={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
          >
            {theme === 'dark' ? <SunIcon size={15} /> : <MoonIcon size={15} />}
          </button>
          <button
            className="icon-btn"
            onClick={() => setHelp((h) => !h)}
            aria-label="Keyboard shortcuts (?)"
            title="Keyboard shortcuts (?)"
          >
            <KeyboardIcon size={15} />
          </button>
        </div>
      </header>

      <main className="layout">
        <section className="feed" aria-label={`Live feed: ${teamName}`}>
          <SearchBox ref={searchRef} filters={filters} onChange={changeFilters} onReset={() => changeFilters(defaultFilters)} />
          <FeedList
            alerts={visible}
            inserted={state.inserted}
            selectedId={selectedId}
            now={now}
            onSelect={select}
            empty={
              <div className="empty">
                {connection !== 'live' && state.stats.snapshots === 0 ? (
                  <p>Connecting to the live feed…</p>
                ) : all.length === 0 ? (
                  <p>No alerts for {teamName} right now. New ones appear here automatically.</p>
                ) : (
                  <>
                    <p>No alerts match your filters. {all.length} alert{all.length === 1 ? ' is' : 's are'} hidden.</p>
                    <button className="btn" onClick={() => changeFilters({ ...defaultFilters, status: new Set(['open', 'acked', 'resolved']) })}>
                      Show everything
                    </button>
                  </>
                )}
              </div>
            }
          />
        </section>

        {selectedId && (
          <aside className="detail-pane" aria-label="Alert detail">
            {selected ? (
              <AlertDetail
                alert={selected}
                snapshotAt={state.snapshotAt}
                now={now}
                pending={pending?.id === selected.id ? pending.op : null}
                onAct={(op) => void act(selected, op)}
                onClose={() => select(null)}
              />
            ) : (
              <div className="empty">
                <p>{state.stats.snapshots === 0 ? 'Loading…' : 'This alert is not in the current feed. It may have expired or belong to another team.'}</p>
                <button className="btn" onClick={() => select(null)}>Close</button>
              </div>
            )}
          </aside>
        )}
      </main>

      <div className="toasts" aria-live="polite">
        {toasts.map((t) => (
          <div key={t.id} className={`toast toast-${t.kind}`}>{t.text}</div>
        ))}
      </div>

      {help && (
        <div className="modal-backdrop" onClick={() => setHelp(false)}>
          <div className="modal" role="dialog" aria-modal="true" aria-labelledby="help-title" onClick={(e) => e.stopPropagation()}>
            <h2 id="help-title">Keyboard shortcuts</h2>
            <dl className="shortcuts">
              <dt><kbd>j</kbd> <kbd>↓</kbd></dt><dd>Next alert</dd>
              <dt><kbd>k</kbd> <kbd>↑</kbd></dt><dd>Previous alert</dd>
              <dt><kbd>a</kbd></dt><dd>Acknowledge</dd>
              <dt><kbd>r</kbd></dt><dd>Resolve</dd>
              <dt><kbd>/</kbd></dt><dd>Search</dd>
              <dt><kbd>Esc</kbd></dt><dd>Close</dd>
            </dl>
            <button className="btn btn-primary" onClick={() => setHelp(false)} autoFocus>Close</button>
          </div>
        </div>
      )}
    </div>
  )
}
