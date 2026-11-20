import { useCallback, useEffect, useState } from 'react'

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
      return r.id ? `#/rules/${enc(r.id)}${r.from ? `?from=${enc(r.from)}` : ''}` : '#/rules'
    case 'preferences':
      return r.kind && r.id ? `#/preferences/${r.kind}/${enc(r.id)}` : '#/preferences'
    case 'proposals':
      return r.id ? `#/proposals/${enc(r.id)}` : '#/proposals'
  }
}

/** Current route plus `go`. `replace` swaps the history entry (feed selection
 * uses it so j/k don't flood Back). */
export function useRoute(): [Route, (to: Route | string, opts?: { replace?: boolean }) => void] {
  const [route, setRoute] = useState(() => parseHash(location.hash))
  useEffect(() => {
    const on = () => setRoute(parseHash(location.hash))
    addEventListener('hashchange', on)
    return () => removeEventListener('hashchange', on)
  }, [])
  const go = useCallback((to: Route | string, opts?: { replace?: boolean }) => {
    const hash = typeof to === 'string' ? to : hashFor(to)
    if (location.hash === hash || (hash === '#/' && location.hash === '')) return setRoute(parseHash(hash))
    if (opts?.replace) {
      history.replaceState(null, '', hash)
      setRoute(parseHash(hash))
    } else location.hash = hash
  }, [])
  return [route, go]
}
