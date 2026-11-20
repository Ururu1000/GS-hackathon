import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { countPendingProposals, errorText, getAlert, getSession, listDirectory, transition, type TransitionOp } from './api/client'
import type { DiscoveredSchema } from './api/screens'
import type { FeedAlert, PersonDirectoryEntry, Session } from './api/types'
import { AlertDetail } from './alert/AlertDetail'
import { FeedList } from './feed/FeedList'
import { FilterChips, SearchBox } from './feed/FilterBar'
import { defaultFilters, facetCounts, loadFilters, matches, saveFilters, type Filters } from './feed/filters'
import { compareFeed } from './feed/feedStore'
import { useFeedStream, type Connection } from './feed/useFeedStream'
import { BeaconLogo, KeyboardIcon, MoonIcon, SunIcon } from './lib/icons'
import { useRoute, type Route } from './lib/router'
import { useNow } from './lib/time'

// Management screens load on first visit, so the feed's first paint stays small.
const RulesList = lazy(() => import('./rules/RulesList').then((m) => ({ default: m.RulesList })))
const RuleEditor = lazy(() => import('./rules/RuleEditor').then((m) => ({ default: m.RuleEditor })))
const PreferencesScreen = lazy(() => import('./prefs/PreferencesScreen').then((m) => ({ default: m.PreferencesScreen })))
const ProposalsScreen = lazy(() => import('./proposals/ProposalsScreen').then((m) => ({ default: m.ProposalsScreen })))
const SourcesScreen = lazy(() => import('./sources/SourcesScreen').then((m) => ({ default: m.SourcesScreen })))
const RunbooksScreen = lazy(() => import('./runbooks/RunbooksScreen').then((m) => ({ default: m.RunbooksScreen })))
const SimulatorScreen = lazy(() => import('./simulator/SimulatorScreen').then((m) => ({ default: m.SimulatorScreen })))
const screenFallback = <div className="screen"><div className="empty"><p>Loading…</p></div></div>

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

// ---- persisted team scope (tab session) & theme ------------------------------

const TEAM_KEY = 'beacon.team'
function loadTeam(): string | null {
  try {
    return sessionStorage.getItem(TEAM_KEY) || null
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
  const [recipient, setRecipient] = useState<string | null>(null)
  const [directory, setDirectory] = useState<PersonDirectoryEntry[]>([])
  const [linkedAlert, setLinkedAlert] = useState<FeedAlert | null>(null)
  const [linkedAlertError, setLinkedAlertError] = useState<string | null>(null)
  const [filters, setFilters] = useState<Filters>(loadFilters)
  const [route, go, setRouteDirty] = useRoute()
  const [ruleSample, setRuleSample] = useState<{ sample: DiscoveredSchema; ruleId: string | null } | null>(null)
  const createRuleFromSample = (sample: DiscoveredSchema) => {
    setRuleSample({ sample, ruleId: null })
    go('#/rules/new?from=event')
  }
  const selectedId = route.view === 'alerts' ? route.id : null
  const select = useCallback((id: string | null) => go({ view: 'alerts', id }, { replace: true }), [go])
  const navigate = useCallback((hash: string) => go(hash), [go])
  const [pendingProposals, setPendingProposals] = useState(0)
  const refreshProposals = useCallback(() => {
    countPendingProposals(team).then(setPendingProposals, () => {})
  }, [team])
  useEffect(refreshProposals, [refreshProposals, route.view])
  const [pending, setPending] = useState<{ id: string; op: TransitionOp } | null>(null)
  const [help, setHelp] = useState(false)
  const [theme, toggleTheme] = useTheme()
  const searchRef = useRef<HTMLInputElement>(null)
  const { toasts, push } = useToasts()
  const now = useNow(15_000)

  useEffect(() => {
    getSession().then(setSession, (e: unknown) => setSessionError(e instanceof Error ? e.message : String(e)))
  }, [])

  const recipientFeed = session?.capabilities?.recipient_feed === true
  useEffect(() => {
    if (!recipientFeed) return
    let active = true
    listDirectory().then((items) => { if (active) setDirectory(items) }, () => {})
    return () => { active = false }
  }, [recipientFeed])

  const { state, connection, applyLocal, loadOlder, historyError, historyLoading } =
    useFeedStream(team, recipientFeed ? recipient : null)

  const changeTeam = (t: string | null) => {
    setTeam(t)
    setRecipient(null)
    if (route.view === 'alerts') select(null)
    try {
      if (t) sessionStorage.setItem(TEAM_KEY, t)
      else sessionStorage.removeItem(TEAM_KEY)
    } catch {
      /* ignore */
    }
  }
  const changeFilters = (f: Filters) => {
    setFilters(f)
    saveFilters(f)
  }

  const all = useMemo(() => {
    const live = state.order.flatMap((id) => {
      const alert = state.byId.get(id)
      return alert ? [alert] : []
    })
    return state.history.size ? [...live, ...state.history.values()].sort(compareFeed) : live
  }, [state.order, state.byId, state.history])
  const visible = useMemo(() => all.filter((a) => matches(a, filters)), [all, filters])
  const counts = useMemo(() => facetCounts(all, filters), [all, filters])
  const cachedAlert = selectedId ? state.byId.get(selectedId) ?? state.history.get(selectedId) : undefined
  const selected = cachedAlert ?? (linkedAlert?.id === selectedId ? linkedAlert : null)

  useEffect(() => {
    setLinkedAlertError(null)
    setLinkedAlert(null)
    if (!selectedId || cachedAlert) return
    const controller = new AbortController()
    getAlert(selectedId, controller.signal).then(
      (alert) => { if (!controller.signal.aborted) setLinkedAlert(alert) },
      (e: unknown) => { if (!controller.signal.aborted) setLinkedAlertError(errorText(e)) },
    )
    return () => controller.abort()
  }, [selectedId, Boolean(cachedAlert)])

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
        setLinkedAlert((current) => current?.id === res.alert.id ? res.alert : current)
        push('ok', `${op === 'ack' ? 'Acknowledged' : 'Resolved'}: ${alert.title}`)
      } else {
        if (res.current) {
          const next = res.current
          applyLocal(next)
          setLinkedAlert((current) => current?.id === next.id ? next : current)
        }
        push('error', res.message)
      }
    },
    [pending, applyLocal, push],
  )

  // ---- keyboard ---------------------------------------------------------------
  useEffect(() => {
    if (route.view !== 'alerts') return
    const onKey = (e: KeyboardEvent) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return
      // A dialog owns the keyboard while it's open.
      if (document.querySelector('.modal-backdrop') && !help) return
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
  }, [route.view, visible, selectedId, selected, select, act, push, help])

  const teamName = team ? session?.teams.find((t) => t.id === team)?.name ?? team : 'All teams'

  return (
    <div className={`app view-${route.view} ${selected ? 'has-detail' : ''}`}>
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
        <Nav route={route} pendingProposals={pendingProposals} onNavigate={navigate} />
        {route.view === 'alerts' && <FilterChips filters={filters} counts={counts} onChange={changeFilters} />}
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

      <Suspense key={route.view} fallback={screenFallback}>
      {route.view === 'rules' && (
        route.id ? (
          <RuleEditor key={JSON.stringify([route.id, route.from])} id={route.id} from={route.from} team={team} initialEvent={ruleSample && (route.from === 'event' || route.id === ruleSample.ruleId) ? ruleSample.sample : null} onEventRuleCreated={(ruleId) => setRuleSample((current) => current ? { ...current, ruleId } : null)} mockTransports={session?.capabilities?.mock_transports === true} onNavigate={navigate} onDirtyChange={setRouteDirty} onToast={push} />
        ) : (
          <RulesList team={team} teamName={teamName} onNavigate={navigate} />
        )
      )}
      {route.view === 'preferences' && <PreferencesScreen kind={route.kind} id={route.id} me={session?.user.id ?? null} onNavigate={navigate} onToast={push} />}
      {route.view === 'proposals' && <ProposalsScreen id={route.id} team={team} onNavigate={navigate} onToast={push} onChanged={refreshProposals} />}
      {route.view === 'sources' && <SourcesScreen onCreateRule={createRuleFromSample} />}
      {route.view === 'runbooks' && <RunbooksScreen team={team} teams={session?.teams ?? []} canImport={session?.capabilities?.runbook_import === true} />}
      {route.view === 'simulator' && <SimulatorScreen />}
      </Suspense>

      {route.view === 'alerts' && (
      <main className="layout">
        <section className="feed" aria-label={`Live feed: ${teamName}`}>
          <SearchBox ref={searchRef} filters={filters} onChange={changeFilters} onReset={() => changeFilters(defaultFilters)} />
          {recipientFeed && (
            <label className="inline feed-recipient">
              <span className="small muted">Recipient</span>
              <select className="select" value={recipient ?? ''} onChange={(e) => { setRecipient(e.target.value || null); select(null) }}>
                <option value="">All recipients</option>
                {directory.map((person) =>
                  <option key={person.id} value={person.id}>{person.name}</option>)}
              </select>
            </label>
          )}
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
          {(state.nextCursor || historyError) && (
            <div className="inline feed-history">
              {state.nextCursor && <button className="btn btn-sm" onClick={() => void loadOlder()} disabled={historyLoading}>
                {historyLoading ? 'Loading older alerts…' : 'Load older alerts'}
              </button>}
              {historyError && <span className="small problems-inline" role="alert">{historyError}</span>}
            </div>
          )}
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
                onToast={push}
                onNavigate={navigate}
              />
            ) : (
              <div className="empty">
                <p>{linkedAlertError ?? 'Loading alert…'}</p>
                <button className="btn" onClick={() => select(null)}>Close</button>
              </div>
            )}
          </aside>
        )}
      </main>
      )}

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

const NAV: { view: Route['view']; label: string; hash: string }[] = [
  { view: 'alerts', label: 'Alerts', hash: '#/' },
  { view: 'rules', label: 'Rules', hash: '#/rules' },
  { view: 'sources', label: 'Sources', hash: '#/sources' },
  { view: 'runbooks', label: 'Runbooks', hash: '#/runbooks' },
  { view: 'preferences', label: 'Preferences', hash: '#/preferences' },
  { view: 'proposals', label: 'Proposals', hash: '#/proposals' },
  { view: 'simulator', label: 'Devices', hash: '#/simulator' },
]

function Nav({ route, pendingProposals, onNavigate }: { route: Route; pendingProposals: number; onNavigate: (hash: string) => void }) {
  return (
    <nav className="nav" aria-label="Sections">
      {NAV.map((n) => (
        <a key={n.view} href={n.hash} className="nav-link" aria-current={route.view === n.view ? 'page' : undefined} onClick={(e) => {
          if (e.button !== 0 || e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return
          e.preventDefault()
          onNavigate(n.hash)
        }}>
          {n.label}
          {n.view === 'proposals' && pendingProposals > 0 && (
            <span className="nav-badge" aria-label={`${pendingProposals} waiting for review`}>{pendingProposals}</span>
          )}
        </a>
      ))}
    </nav>
  )
}
