import { useCallback, useEffect, useRef, useState } from 'react'

// Hash routes, so the static build works from any path with no SPA fallback:
//   #/                     live feed          #/alerts/<id>   feed + alert detail
//   #/rules                rules list         #/rules/<id>    rule editor (#/rules/new?from=<template>)
//   #/preferences          people and teams   #/preferences/person/<id>, #/preferences/team/<id>
//   #/proposals            suggested changes  #/proposals/<id>

export type Route =
  | { view: 'alerts'; id: string | null }
  | { view: 'rules'; id: string | null; from: string | null }
  | { view: 'preferences'; kind: 'person' | 'team' | null; id: string | null }
  | { view: 'proposals'; id: string | null }

const dec = (s: string | undefined): string | null => {
  if (s === undefined) return null
  try {
    return decodeURIComponent(s)
  } catch {
    return null
  }
}

export function parseHash(hash: string): Route {
  const [path = '', query = ''] = hash.replace(/^#/, '').split('?')
  const q = new URLSearchParams(query)
  const seg = path.split('/').filter(Boolean)
  switch (seg[0]) {
    case 'rules':
      return { view: 'rules', id: dec(seg[1]), from: q.get('from') }
    case 'preferences':
      return seg[1] === 'person' || seg[1] === 'team'
        ? { view: 'preferences', kind: seg[1], id: dec(seg[2]) }
        : { view: 'preferences', kind: null, id: null }
    case 'proposals':
      return { view: 'proposals', id: dec(seg[1]) }
    case 'alerts':
      return { view: 'alerts', id: dec(seg[1]) }
    default:
      return { view: 'alerts', id: null }
  }
}

const enc = encodeURIComponent

export function hashFor(r: Route): string {
  switch (r.view) {
    case 'alerts':
      return r.id ? `#/alerts/${enc(r.id)}` : '#/'
    case 'rules':
      return r.id ? `#/rules/${enc(r.id)}${r.from === null ? '' : `?from=${enc(r.from)}`}` : '#/rules'
    case 'preferences':
      return r.kind && r.id ? `#/preferences/${r.kind}/${enc(r.id)}` : '#/preferences'
    case 'proposals':
      return r.id ? `#/proposals/${enc(r.id)}` : '#/proposals'
  }
}

type Navigate = (to: Route | string, opts?: { replace?: boolean }) => void

function historyIndex(): number | null {
  const index: unknown = history.state?.beaconRouteIndex
  return typeof index === 'number' ? index : null
}

const historyState = (index: number) => ({ ...history.state, beaconRouteIndex: index })

export function useRoute(): [Route, Navigate, (dirty: boolean) => void] {
  const [route, setRoute] = useState(() => parseHash(location.hash))
  const current = useRef({ hash: location.hash, index: historyIndex() ?? 0 })
  const dirty = useRef(false)
  const setDirty = useCallback((value: boolean) => { dirty.current = value }, [])
  const canLeave = useCallback(() => !dirty.current || confirm('Leave without saving your changes?'), [])
  const show = useCallback((hash: string, index: number) => {
    current.current = { hash, index }
    setRoute(parseHash(hash))
  }, [])

  useEffect(() => {
    history.replaceState(historyState(current.current.index), '')
    const onHashChange = () => {
      const previous = current.current
      const hash = location.hash
      const stored = historyIndex()
      // A native anchor or assignment to location.hash creates an unmarked entry.
      const index = stored ?? previous.index + 1
      if (stored === null) history.replaceState(historyState(index), '')
      if (hash === previous.hash) {
        current.current = { hash, index }
      } else if (canLeave()) {
        show(hash, index)
      } else {
        history.go(previous.index - index)
      }
    }
    const onPopState = () => {
      // Traversing between entries with the same fragment emits no hashchange.
      const index = historyIndex()
      if (location.hash === current.current.hash && index !== null) current.current.index = index
    }
    const onUnload = (e: BeforeUnloadEvent) => {
      if (dirty.current) e.preventDefault()
    }
    addEventListener('hashchange', onHashChange)
    addEventListener('popstate', onPopState)
    addEventListener('beforeunload', onUnload)
    return () => {
      removeEventListener('hashchange', onHashChange)
      removeEventListener('popstate', onPopState)
      removeEventListener('beforeunload', onUnload)
    }
  }, [canLeave, show])

  const go = useCallback<Navigate>((to, opts) => {
    const hash = typeof to === 'string' ? to : hashFor(to)
    if (location.hash === hash || (hash === '#/' && location.hash === '')) return
    if (!canLeave()) return
    const index = current.current.index + (opts?.replace ? 0 : 1)
    if (opts?.replace) history.replaceState(historyState(index), '', hash)
    else history.pushState(historyState(index), '', hash)
    show(hash, index)
  }, [canLeave, show])
  return [route, go, setDirty]
}
