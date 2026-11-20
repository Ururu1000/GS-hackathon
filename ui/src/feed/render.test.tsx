import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { SeverityIcon } from '../alert/badges'
import { defaultFilters } from './filters'
import { SearchBox } from './FilterBar'

const noop = () => {}

describe('SearchBox', () => {
  it('shows the / hint and no reset button with default filters', () => {
    const html = renderToStaticMarkup(<SearchBox filters={defaultFilters} onChange={noop} onReset={noop} />)
    expect(html).toContain('search-kbd')
    expect(html).not.toContain('Reset filters')
  })

  it('shows Reset filters outside any label once filters change', () => {
    const html = renderToStaticMarkup(<SearchBox filters={{ ...defaultFilters, query: 'x' }} onChange={noop} onReset={noop} />)
    expect(html).toContain('Reset filters')
    expect(html).not.toContain('<label')
    expect(html).toContain('aria-label="Search"')
  })
})

describe('SeverityIcon', () => {
  it('labels each severity and ids masks uniquely', () => {
    const html = renderToStaticMarkup(
      <>
        <SeverityIcon severity="critical" />
        <SeverityIcon severity="critical" />
      </>,
    )
    const ids = [...html.matchAll(/<mask id="([^"]+)"/g)].map((m) => m[1])
    expect(ids).toHaveLength(2)
    expect(new Set(ids).size).toBe(2)
    expect(html).toContain('aria-label="Critical"')
  })

  it('falls back to the info glyph for unknown severities, including prototype names', () => {
    for (const sev of ['bogus', 'toString']) {
      const html = renderToStaticMarkup(<SeverityIcon severity={sev as never} />)
      expect(html).not.toContain('<mask')
      expect(html).toContain('<circle')
    }
  })
})
